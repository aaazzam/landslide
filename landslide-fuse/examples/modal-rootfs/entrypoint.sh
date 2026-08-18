#!/bin/sh
# Boot sequence for a synced, sandboxed rootfs (Modal VM Sandbox, or any
# Linux host with the same privileges).
#
# Process model — this is the security boundary:
#   * THIS script runs as root in the sandbox's outer mount namespace. It
#     starts the sync process (FUSE replica mount or mirror daemon) HERE —
#     outside any jail the agent can reach.
#   * The untrusted agent runs via `agent-run.sh`: chrooted into the synced
#     rootfs, uid 65534, capability set emptied, no_new_privs, scrubbed env.
#     => kill(2) on the syncer fails EPERM (uid mismatch), umount/pivot_root
#     fail (no CAP_SYS_ADMIN), /proc inside the jail is hidepid=2 (the
#     syncer's pid is invisible), and the jail mounts no /dev/fuse.
#
# Env: LANDSLIDE_VOL (volume name, required), LANDSLIDE_MODE ("fuse" [default; needs
# kernel FUSE, e.g. Modal VM sandboxes] | "mirror" [no FUSE needed, works
# under gVisor]), plus LANDSLIDE_BUCKET/AWS_* creds for the object store.
#
# Choose MIRROR when in doubt: identical jail, and the agent's file traffic
# never crosses a fuse connection (some environments — gVisor, and
# vendor-hardened kernels like OrbStack's — restrict fuse connections to
# the mounting user, which breaks the chrooted-agent-over-fuse topology).
set -eu

VOL="${LANDSLIDE_VOL:?set LANDSLIDE_VOL to the volume name}"
MODE="${LANDSLIDE_MODE:-fuse}"
ROOT=/srv/rootfs   # synced read-only replica of the volume
MERGED=/srv/merged # fuse mode: writable overlay view the agent lives in

mount --make-rprivate /
mkdir -p "$ROOT"

case "$MODE" in
    fuse)
        [ -e /dev/fuse ] || mknod -m 666 /dev/fuse c 10 229
        landslidefs follow "$VOL" "$ROOT" >/srv/syncer.log 2>&1 &
        echo $! > /srv/syncer.pid
        for _ in $(seq 1 250); do mountpoint -q "$ROOT" && break; sleep 0.2; done
        mountpoint -q "$ROOT"
        ;;
    mirror)
        landslidefs mirror follow "$VOL" "$ROOT" >/srv/syncer.log 2>&1 &
        echo $! > /srv/syncer.pid
        for _ in $(seq 1 250); do [ -f "$ROOT/.landslide-mirror.json" ] && break; sleep 0.2; done
        [ -f "$ROOT/.landslide-mirror.json" ]
        mkdir -p "$ROOT/proc" "$ROOT/dev" "$ROOT/tmp"
        MERGED="$ROOT" # no overlay: agent uid owns nothing, modes protect files
        ;;
    *) echo "LANDSLIDE_MODE must be fuse|mirror" >&2; exit 1 ;;
esac

if [ "$MODE" = fuse ]; then
    # Writable view for the agent: tmpfs upper over the synced RO lower.
    # Rootfs contents are kernel-EROFS for the agent; its writes evaporate
    # with the sandbox. (upper/work live on a tmpfs: overlay-over-overlay,
    # which the container's own / would impose, is illegal.)
    mkdir -p /tmp/landslidefs-overlay "$MERGED"
    mount -t tmpfs tmpfs /tmp/landslidefs-overlay
    mkdir -p /tmp/landslidefs-overlay/upper /tmp/landslidefs-overlay/work
    mount -t overlay overlay \
        -o "lowerdir=$ROOT,upperdir=/tmp/landslidefs-overlay/upper,workdir=/tmp/landslidefs-overlay/work" \
        "$MERGED"
    mkdir -p "$MERGED/proc" "$MERGED/dev" "$MERGED/tmp"
fi

# Pseudo-filesystems INSIDE the jail. Fresh /proc with hidepid=2: the agent
# cannot enumerate processes outside its uid (i.e. the syncer). Bind a
# minimal device set; deliberately NOT /dev/fuse.
mount -t proc -o hidepid=2 proc "$MERGED/proc"
mount -t tmpfs -o mode=1777 tmpfs "$MERGED/tmp"
for d in null zero full random urandom tty; do
    : > "$MERGED/dev/$d" || true
    mount --bind "/dev/$d" "$MERGED/dev/$d" || true
done
# DNS inside the jail (volume images usually lack it).
[ -f "$MERGED/etc/resolv.conf" ] || mount --bind /etc/resolv.conf "$MERGED/etc/resolv.conf" 2>/dev/null || true

# Stable jail alias for agent-run.sh (fuse: /srv/merged, mirror: /srv/rootfs).
ln -sfn "$MERGED" /srv/jail

touch /tmp/rootfs-ready
# Stay alive for the sandbox's lifetime; the driver runs agents via agent-run.sh.
sleep infinity &
wait $!
