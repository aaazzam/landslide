use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use slog::deps::slatedb;
use slog::{Config, EventStore, EventStoreReader, ReaderConfig};
use slog_fuse::mirror::Mirror;
use slog_fuse::{Delta, Volume};

struct Harness {
    store: Arc<EventStore>,
    reader: Arc<EventStoreReader>,
    bucket: Arc<InMemory>,
    dir: PathBuf,
}

async fn harness(name: &str) -> Harness {
    let bucket = Arc::new(InMemory::new());
    let store = Arc::new(
        EventStore::open(Config { path: "fs".into(), object_store: bucket.clone(), settings: None })
            .await
            .unwrap(),
    );
    let options = slatedb::config::DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(50),
        ..Default::default()
    };
    let reader = Arc::new(
        EventStoreReader::open(ReaderConfig {
            path: "fs".into(),
            object_store: bucket.clone(),
            options: Some(options),
        })
        .await
        .unwrap(),
    );
    let dir = std::env::temp_dir().join(format!("slog-mirror-{name}-{}", ulid::Ulid::new()));
    Harness { store, reader, bucket, dir }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Mirrors converge like replicas (the reader polls the db manifest), so
/// wait for the marker path rather than assuming one pass suffices.
async fn open_converged(h: &Harness, vol: &str, marker: &str) -> Mirror {
    let mut mirror = Mirror::open(h.reader.clone(), h.bucket.clone(), vol, &h.dir).await.unwrap();
    sync_until(h, &mut mirror, |h| h.dir.join(marker.trim_start_matches('/')).symlink_metadata().is_ok())
        .await;
    mirror
}

async fn sync_until(h: &Harness, mirror: &mut Mirror, pred: impl Fn(&Harness) -> bool) {
    for _ in 0..100 {
        mirror.sync_once().await.unwrap();
        if pred(h) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("mirror did not converge");
}

#[tokio::test]
async fn mirror_materializes_files_dirs_symlinks_modes() {
    let h = harness("basic").await;
    let mut vol = Volume::mount(h.store.clone(), h.bucket.clone(), "rootfs").await.unwrap();
    vol.mutate(Delta::Mkdir { path: "/etc".into() });
    vol.write_file("/etc/hostname", 0, "node-a".into());
    vol.write_file("/bin/tool", 0, "#!/bin/sh".into());
    vol.mutate(Delta::SetAttr {
        path: "/bin/tool".into(),
        mode: Some(0o755),
        uid: None,
        gid: None,
        mtime_ms: None,
    });
    vol.mutate(Delta::Symlink { path: "/tool".into(), target: "bin/tool".into() });
    vol.commit().await.unwrap();

    let _mirror = open_converged(&h, "rootfs", "/etc/hostname").await;
    assert_eq!(std::fs::read(h.dir.join("etc/hostname")).unwrap(), b"node-a");
    assert_eq!(std::fs::read(h.dir.join("bin/tool")).unwrap(), b"#!/bin/sh");
    assert_eq!(
        std::fs::read_link(h.dir.join("tool")).unwrap().to_str().unwrap(),
        "bin/tool"
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(std::fs::metadata(h.dir.join("bin/tool")).unwrap().permissions().mode() & 0o777, 0o755);
}

#[tokio::test]
async fn mirror_tracks_edits_renames_and_deletes() {
    let h = harness("churn").await;
    let mut vol = Volume::mount(h.store.clone(), h.bucket.clone(), "rootfs").await.unwrap();
    vol.write_file("/a", 0, "old".into());
    vol.write_file("/keep", 0, "same".into());
    vol.write_file("/bye", 0, "x".into());
    vol.commit().await.unwrap();

    let mut mirror = open_converged(&h, "rootfs", "/a").await;
    use std::os::unix::fs::MetadataExt;
    let keep_ino = std::fs::metadata(h.dir.join("keep")).unwrap().ino();

    vol.write_file("/a", 0, "new-and-longer".into());
    vol.mutate(Delta::Rename { from: "/bye".into(), to: "/moved".into() });
    vol.commit().await.unwrap();
    sync_until(&h, &mut mirror, |h| {
        std::fs::read(h.dir.join("a")).map(|b| b == b"new-and-longer").unwrap_or(false)
    })
    .await;

    assert_eq!(std::fs::read(h.dir.join("a")).unwrap(), b"new-and-longer");
    assert_eq!(std::fs::read(h.dir.join("moved")).unwrap(), b"x");
    assert!(h.dir.join("bye").symlink_metadata().is_err());
    // Untouched nodes were not rewritten (same inode).
    assert_eq!(std::fs::metadata(h.dir.join("keep")).unwrap().ino(), keep_ino);
}

#[tokio::test]
async fn mirror_reopen_repairs_and_stays_incremental() {
    let h = harness("restart").await;
    let mut vol = Volume::mount(h.store.clone(), h.bucket.clone(), "rootfs").await.unwrap();
    vol.write_file("/config", 0, "v1".into());
    vol.commit().await.unwrap();

    let _mirror = open_converged(&h, "rootfs", "/config").await;
    drop(_mirror);

    // While "down": the file is deleted locally AND unchanged remotely —
    // reopen must notice the missing entry even though its fingerprint is
    // still current.
    std::fs::remove_file(h.dir.join("config")).unwrap();

    let mut mirror = Mirror::open(h.reader.clone(), h.bucket.clone(), "rootfs", &h.dir).await.unwrap();
    assert_eq!(std::fs::read(h.dir.join("config")).unwrap(), b"v1");

    // And new remote commits flow on the next pass.
    vol.write_file("/config", 2, "-v2".into());
    vol.commit().await.unwrap();
    sync_until(&h, &mut mirror, |h| {
        std::fs::read(h.dir.join("config")).map(|b| b == b"v1-v2").unwrap_or(false)
    })
    .await;
    assert_eq!(std::fs::read(h.dir.join("config")).unwrap(), b"v1-v2");
}
