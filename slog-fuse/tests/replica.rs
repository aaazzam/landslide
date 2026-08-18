use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use slog::deps::slatedb;
use slog::{Config, EventStore, EventStoreReader, ReaderConfig};
use slog_fuse::replica::Replica;
use slog_fuse::Volume;

async fn harness() -> (Arc<EventStore>, Arc<EventStoreReader>, Arc<InMemory>) {
    let bucket = Arc::new(InMemory::new());
    let store = Arc::new(
        EventStore::open(Config { path: "fs".into(), object_store: bucket.clone(), settings: None })
            .await
            .unwrap(),
    );
    // Poll the db manifest eagerly so convergence in tests doesn't lag.
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
    (store, reader, bucket)
}

/// Retries sync until `pred` holds: the reader only sees new commits after
/// its manifest poll, so convergence is eventual even against an in-memory
/// backend.
async fn converge_until(replica: &mut Replica, mut pred: impl FnMut(&Replica) -> bool) {
    for _ in 0..100 {
        replica.sync().await.unwrap();
        if pred(replica) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("replica did not converge");
}

#[tokio::test]
async fn replica_follows_commits_and_never_fences() {
    let (store, reader, bucket) = harness().await;
    let mut vol = Volume::mount(store, bucket.clone(), "rootfs").await.unwrap();
    vol.write_file("/etc/hostname", 0, "node-a".into());
    vol.commit().await.unwrap();

    // The replica opens alongside the live writer — no fence taken.
    let mut replica = Replica::open(reader, bucket.clone(), "rootfs").await.unwrap();
    converge_until(&mut replica, |r| r.image.nodes.contains_key("/etc/hostname")).await;
    assert_eq!(replica.read_file("/etc/hostname").await.unwrap().unwrap().as_ref(), b"node-a");

    // The writer keeps committing (its fence was never stolen)...
    vol.write_file("/main.rs", 0, "fn main() {}".into());
    vol.commit().await.unwrap();
    // ...and the replica converges on the new deltas.
    converge_until(&mut replica, |r| r.image.nodes.contains_key("/main.rs")).await;
    assert_eq!(replica.read_file("/main.rs").await.unwrap().unwrap().as_ref(), b"fn main() {}");
}

#[tokio::test]
async fn replica_rebuilds_from_checkpoint_after_purge() {
    let (store, reader, bucket) = harness().await;
    let mut vol = Volume::mount(store.clone(), bucket.clone(), "rootfs").await.unwrap();
    vol.write_file("/boot/vmlinuz", 0, "kernel".into());
    vol.commit().await.unwrap();
    let record = vol.checkpoint().await.unwrap();
    // Retention: the deltas the segment now covers are gone for good.
    store.purge_below("rootfs", record.through_version + 1).await.unwrap();

    // A replica opening now has no readable backlog: it rebuilds from the
    // checkpoint's manifest + segments.
    let mut replica = Replica::open(reader, bucket.clone(), "rootfs").await.unwrap();
    converge_until(&mut replica, |r| r.image.nodes.contains_key("/boot/vmlinuz")).await;
    assert_eq!(replica.read_file("/boot/vmlinuz").await.unwrap().unwrap().as_ref(), b"kernel");

    // Post-checkpoint deltas flow through the same cursor afterwards.
    vol.write_file("/etc/os-release", 0, "slog".into());
    vol.commit().await.unwrap();
    converge_until(&mut replica, |r| r.image.nodes.contains_key("/etc/os-release")).await;
    assert_eq!(replica.read_file("/etc/os-release").await.unwrap().unwrap().as_ref(), b"slog");
}

#[tokio::test]
async fn unknown_volume_opens_empty() {
    let (_store, reader, bucket) = harness().await;
    let replica = Replica::open(reader, bucket, "does-not-exist").await.unwrap();
    assert!(replica.image.nodes.is_empty());
    assert_eq!(replica.cursor(), None);
}
