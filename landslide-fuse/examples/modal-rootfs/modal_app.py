"""Modal demo: an untrusted agent on a synced, read-only rootfs.

Topology (see entrypoint.sh for the full security argument):
  * VM Sandbox (real kernel => FUSE is supported per Modal docs).
  * Root in the outer context runs `landslidefs follow` — the synced replica
    mount of the volume — plus an overlay for agent scratch.
  * Agents are only ever run via `/agent-run.sh`: chrooted into the synced
    rootfs, uid 65534, capability set emptied, no_new_privs, scrubbed env.
    They can neither kill the syncer (EPERM, uid mismatch) nor umount or
    escape (no CAP_SYS_ADMIN), nor see its pid (hidepid /proc), nor reach
    /dev/fuse. For an agent with FULL root-equivalent privilege, run the
    syncer in a Sandbox Sidecar (separate container; alpha, allowlisted)
    and serve content over the bridge network instead.

Prereqs:
  1. Build the CLI against this repo and drop it next to this file:
       docker run --rm -v "$PWD":/w -w /w rust:1-bookworm \
         cargo build --release -p landslide-fuse --features fuse --bin landslidefs
       cp target/release/landslidefs landslide-fuse/examples/modal-rootfs/bin-landslidefs
  2. Seed a volume (any writer with bucket creds — e.g. write files through
     `landslidefs mount` on a Linux box, or your build pipeline uses the Rust API,
     then `landslidefs checkpoint <vol>`).
  3. A Modal secret named "aws-creds" holding LANDSLIDE_BUCKET + AWS creds
     (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, and AWS_REGION
     or LANDSLIDE_REGION). The agent never sees these: entrypoint.sh scrubs env.

Run:  modal run landslide-fuse/examples/modal-rootfs/modal_app.py
"""

from pathlib import Path

import modal

HERE = Path(__file__).parent
BIN = HERE / "bin-landslidefs"
VOL = "agent-rootfs"

app = modal.App.lookup("landslide-rootfs-demo", create_if_missing=True)

assert BIN.exists(), f"build landslidefs first (see docstring); missing {BIN}"

image = (
    modal.Image.from_registry("debian:bookworm-slim", add_python="3.12")
    .apt_install("util-linux", "ca-certificates")  # setpriv, chroot, mount
    .add_local_file(BIN, "/usr/local/bin/landslidefs", copy=True)
    .add_local_file(HERE / "entrypoint.sh", "/entrypoint.sh", copy=True)
    .add_local_file(HERE / "agent-run.sh", "/agent-run.sh", copy=True)
)


def run(sb: modal.Sandbox, *args: str) -> tuple[str, str, int]:
    p = sb.exec(*args)
    out, err = p.stdout.read(), p.stderr.read()
    p.wait()
    return out, err, p.returncode


def sh(sb: modal.Sandbox, *args: str) -> str:
    out, err, rc = run(sb, *args)
    assert rc == 0, f"{args}: {err}"
    return out


def main():
    sb = modal.Sandbox.create(
        "sh", "/entrypoint.sh",
        app=app,
        image=image,
        secrets=[modal.Secret.from_name("aws-creds")],
        env={"LANDSLIDE_VOL": VOL, "LANDSLIDE_MODE": "fuse"},
        timeout=30 * 60,
        experimental_options={"vm_runtime": True},  # real kernel => FUSE
    )
    sb.set_tags({"demo": "landslide-rootfs"})
    try:
        out, err, rc = run(sb, "sh", "-c",
                           "for i in $(seq 1 120); do [ -f /tmp/rootfs-ready ] && exit 0; sleep 1; done; exit 1")
        assert rc == 0, f"rootfs never became ready: {err}"

        print("== agent identity (jailed, unprivileged):")
        print(sh(sb, "/agent-run.sh", "id"))

        print("== agent sees the synced rootfs:")
        print(sh(sb, "/agent-run.sh", "ls", "-la", "/"))
        print(sh(sb, "/agent-run.sh", "cat", "/etc/hostname"))

        syncer_pid = sh(sb, "cat", "/srv/syncer.pid").strip()
        print(f"== syncer pid {syncer_pid} (invisible to the agent); agent tries to kill it:")
        _, err, rc = run(sb, "/agent-run.sh", "kill", "-9", syncer_pid)
        print(f"  -> {err.strip()} (exit {rc})")

        print("== agent tries to umount the rootfs:")
        _, err, rc = run(sb, "/agent-run.sh", "umount", "/")
        print(f"  -> {err.strip()} (exit {rc})")

        print("== /proc inside the jail shows only the agent's own processes:")
        print(sh(sb, "/agent-run.sh", "sh", "-c",
                 "ls /proc | grep -E '^[0-9]+$' | wc -l").strip(), "pids visible")

        print("== agent write to /etc goes to the throwaway overlay, not the volume:")
        print(sh(sb, "/agent-run.sh", "sh", "-c",
                 "echo scratch > /etc/agent-note && cat /etc/agent-note"))

        print(f"Sandbox {sb.object_id} is live for 30 min. Edit/commit volume {VOL!r} "
              "from any writer and watch the agent's rootfs converge.")
    finally:
        sb.terminate()


if __name__ == "__main__":
    main()
