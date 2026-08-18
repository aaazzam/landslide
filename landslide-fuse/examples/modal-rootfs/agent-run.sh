#!/bin/sh
# agent-run.sh CMD... — run CMD as the untrusted agent.
#
# Ordering is load-bearing:
#   1. setpriv: restrict the capability BOUNDING set to exactly
#      CAP_SYS_CHROOT + no_new_privs. (On the next execve, uid-0 recomputes
#      its permitted set from the bounding set — so chroot(1) gets exactly
#      the one cap it needs and nothing else. Emptying the bounding set
#      entirely makes the chroot syscall itself fail EPERM.)
#   2. chroot --userspec: enter the jail, then setuid to 65534 — the kernel
#      now clears effective+permitted caps and no_new_privs bars ever
#      regaining them. Gone forever: CAP_SYS_ADMIN (umount/pivot_root/
#      mount), CAP_SYS_PTRACE, CAP_KILL-over-uid, ... (chroot is coreutils,
#      at /usr/sbin on Debian).
#   3. env -i: scrub LANDSLIDE_BUCKET/AWS_* creds out of the agent's environment.
#      (The ROOTFS ITSELF must carry /usr/bin/env + CMD's interpreter —
#      any normal container base image provides both.)
#
# After this: kill(2) on the syncer -> EPERM (uid 65534 vs root), umount ->
# EPERM (no CAP_SYS_ADMIN), /proc shows only the agent's own processes, and
# there is no /dev/fuse to message the FUSE daemon through.
exec /usr/bin/setpriv --bounding-set=+sys_chroot --no-new-privs \
    /usr/sbin/chroot --userspec=65534:65534 --groups='' /srv/jail \
    /usr/bin/env -i PATH=/usr/local/bin:/usr/bin:/bin HOME=/tmp TERM=xterm "$@"
