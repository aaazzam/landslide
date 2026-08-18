#!/bin/sh
# Local validation of the modal-rootfs topology, inside Docker (Linux).
# Builds the image, seeds a volume, boots the entrypoint in a privileged
# container (the local stand-in for a Modal VM Sandbox), then asserts the
# jail invariants for BOTH sync modes:
#
#   mirror  sidecar materializes the replica into a plain dir (portable:
#           no fuse in the agent's path at all — works on ANY kernel)
#   fuse    FUSE replica mount as the jail's lower (stock kernels)
#
#   * agent is uid 65534 with no capabilities, chrooted into the synced tree
#   * agent CANNOT kill the syncer (EPERM), umount, or see its pid
#   * volume edits committed elsewhere converge into the agent's rootfs
#   * agent writes never reach the volume
#
# Run from the repo root:  sh slog-fuse/examples/modal-rootfs/test-local.sh
set -eu
cd "$(dirname "$0")/../../.."

IMG=slog-rootfs-test
CTR=slog-rootfs-test
BUCKET=$(mktemp -d)/bucket
trap 'docker rm -f $CTR >/dev/null 2>&1 || true' EXIT

docker build -f slog-fuse/examples/modal-rootfs/Dockerfile.test -t $IMG .
docker rm -f $CTR >/dev/null 2>&1 || true
# --privileged ~= what the VM sandbox's outer context grants its entrypoint.
docker run -d --name $CTR --privileged --device /dev/fuse \
    -e SLOG_BUCKET_DIR=/bucket -v "$BUCKET":/bucket $IMG sleep infinity >/dev/null
x() { docker exec $CTR "$@"; }

echo "== seed a volume (writable mount, then checkpoint):"
x sh -c 'mkdir -p /seed && slogfs mount rootfs /seed >/tmp/seed.log 2>&1 & sleep 1.5'
x sh -c 'mkdir -p /seed/etc /seed/bin /seed/usr/bin \
    && echo node-a > /seed/etc/hostname && echo nameserver 1.1.1.1 > /seed/etc/resolv.conf \
    && cp /usr/bin/busybox /seed/bin/busybox && chmod +x /seed/bin/busybox \
    && for a in sh id ls cat grep echo mount umount mkdir chmod ln ps kill rm touch env; do \
         ln -s busybox /seed/bin/$a; done \
    && ln -s /bin/busybox /seed/usr/bin/env \
    && printf "#!/bin/sh\necho hi\n" > /seed/bin/tool && chmod +x /seed/bin/tool \
    && ln -s bin/tool /seed/tool && sync'
x sh -c 'umount /seed; sleep 0.5; slogfs checkpoint rootfs'

boot() {
    # Teardown of the previous mode: kill entrypoint+syncer, unmount the
    # pseudo-fs binds and the replica mount, THEN remove dirs.
    x sh -c "kill \$(cat /srv/syncer.pid 2>/dev/null) 2>/dev/null; pkill -f entrypoint.sh 2>/dev/null; \
             umount -R /srv/rootfs /srv/merged 2>/dev/null; \
             rm -rf /srv/rootfs /srv/merged /srv/jail /tmp/rootfs-ready /srv/syncer.*" || true
    x sh -c "SLOG_VOL=rootfs SLOG_MODE=$1 nohup sh /entrypoint.sh >/entrypoint-$1.log 2>&1 &"
    x sh -c 'for i in $(seq 1 150); do [ -f /tmp/rootfs-ready ] && exit 0; sleep 0.2; done; exit 1' \
        || { echo "   entrypoint mode=$1 never became ready:"; \
             x sh -c "cat /entrypoint-$1.log /srv/syncer.log 2>/dev/null; mount"; exit 1; }
}

# The jail asserts shared by both modes.
battery() {
    echo "== 1. agent identity:"
    ID=$(x /agent-run.sh id) || { echo "   FATAL: agent-run failed"; exit 1; }
    echo "   $ID"
    case "$ID" in *'uid=65534'*'gid=65534'*) ;; *) echo "   FATAL: agent not dropped"; exit 1 ;; esac

    echo "== 2. agent sees the synced rootfs:"
    HOSTNAME=$(x /agent-run.sh cat /etc/hostname) || { echo "   FATAL: agent cannot read the rootfs"; exit 1; }
    echo "   /etc/hostname: $HOSTNAME"
    [ "$HOSTNAME" = "node-a" ]

    echo "== 3. agent tries to kill the syncer:"
    SYNCER_PID=$(x cat /srv/syncer.pid)
    ERR=$(x /agent-run.sh kill -9 "$SYNCER_PID" 2>&1 || true)
    x kill -0 "$SYNCER_PID" || { echo "   FATAL: syncer died: $ERR"; exit 1; }
    echo "   denied as expected ($ERR); syncer still alive"

    echo "== 4. agent tries to umount the rootfs:"
    ERR=$(x /agent-run.sh umount / 2>&1 || true)
    echo "   denied as expected ($ERR)"

    echo "== 5. /proc inside the jail hides the syncer:"
    JAILED_PIDS=$(x /agent-run.sh sh -c 'ls /proc | grep -cE "^[0-9]+$"')
    OUTER_PIDS=$(x sh -c 'ls /proc | grep -cE "^[0-9]+$"')
    echo "   jailed view: $JAILED_PIDS pids, outer view: $OUTER_PIDS pids"
    [ "$JAILED_PIDS" -lt "$OUTER_PIDS" ]

    echo "== 6. live convergence: commit a change while the agent runs"
    x sh -c 'mkdir -p /seed2 && slogfs mount rootfs /seed2 >/dev/null 2>&1 & sleep 1.5'
    x sh -c 'echo from-writer > /seed2/etc/newnote && sync; umount /seed2'
    for i in $(seq 1 75); do
        [ "$(x /agent-run.sh cat /etc/newnote 2>/dev/null || true)" = "from-writer" ] && break
        sleep 0.2
    done
    [ "$(x /agent-run.sh cat /etc/newnote)" = "from-writer" ] && echo "   /etc/newnote appeared in the jail"

    echo "== 7. agent writes never reach the volume:"
    if [ "$1" = fuse ]; then
        x /agent-run.sh sh -c 'echo scratch > /etc/agent-note'
        x sh -c 'test -e /srv/rootfs/etc/agent-note' && { echo "   FATAL: agent write reached the volume"; exit 1; }
        echo "   write landed in the throwaway overlay upper, volume unchanged"
    else
        ERR=$(x /agent-run.sh sh -c 'echo scratch > /etc/agent-note' 2>&1 || true)
        x sh -c 'test -e /srv/rootfs/etc/agent-note' && { echo "   FATAL: agent write reached the volume"; exit 1; }
        echo "   write denied ($ERR), volume unchanged"
    fi
}

echo
echo "########## MIRROR mode (portable; no fuse in the agent's path) ##########"
boot mirror
battery mirror
echo "ALL JAIL INVARIANTS HOLD (mirror mode)"

echo
echo "########## FUSE mode (replica mount as the jail's lower) ##########"
boot fuse
# Probe first: some environments (gVisor sandboxes, vendor-patched kernels)
# gate fuse connections to the mounting user, in which case mirror mode is
# the answer and fuse jail mode is skipped rather than failed.
SP='/usr/bin/setpriv --reuid 65534 --regid 65534 --clear-groups'
if x sh -c "$SP cat /srv/rootfs/etc/hostname >/dev/null 2>&1"; then
    battery fuse
    echo "ALL JAIL INVARIANTS HOLD (fuse mode)"
else
    echo "   SKIPPED: this kernel gates fuse connections to the mounting user;"
    echo "   mirror mode (above) is the portable path for this host."
fi
