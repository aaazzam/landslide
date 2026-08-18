//! The scale story: hydration cost must be O(db pages + backlog), never
//! O(total commits) — bounded manifests via coalescing, bounded history via
//! retention purge, end-to-end over many checkpoints.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use landslide::{Config, EventStore};
use landslide_sqlite::{restore, restore_at, CheckpointOpts, Db, Manifest};

async fn harness() -> (EventStore, Arc<InMemory>) {
    let bucket = Arc::new(InMemory::new());
    let store = EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None })
        .await
        .unwrap();
    (store, bucket)
}

async fn fresh_store(bucket: &Arc<InMemory>) -> EventStore {
    EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None }).await.unwrap()
}

async fn latest_manifest(store: &EventStore, name: &str) -> Manifest {
    let snap = store.latest_snapshot(name).await.unwrap().unwrap();
    serde_json::from_slice(&snap.state).unwrap()
}

/// LTX segment objects currently in the bucket.
async fn segment_count(bucket: &InMemory, name: &str) -> usize {
    let prefix = object_store::path::Path::from(format!("ltx/{name}"));
    bucket.list_with_delimiter(Some(&prefix)).await.unwrap().objects.len()
}

/// A burst far beyond the store's 1000-event batch cap syncs as packed
/// chunks and restores exactly; a coalesced sync of hot-page churn folds to
/// a single event and still round-trips.
#[tokio::test]
async fn sync_bursts_chunk_and_coalesce() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("burst.db");

    let mut db = Db::open(store, bucket.clone(), "burst", &src).await.unwrap();
    db.connection().execute("CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)", []).unwrap();
    db.sync().await.unwrap();
    for i in 0..2500u32 {
        db.connection().execute("INSERT INTO kv VALUES (?1, 'payload')", [i]).unwrap();
    }
    assert_eq!(db.sync().await.unwrap(), 2500);
    assert_eq!(db.image.txid, 2501);

    let before = db.store().read_history("burst", ..).await.unwrap().len();
    for i in 0..100u32 {
        db.connection().execute("INSERT OR REPLACE INTO kv VALUES (?1, 'hot')", [i % 10]).unwrap();
    }
    assert_eq!(db.sync_coalesced().await.unwrap(), 100);
    let after = db.store().read_history("burst", ..).await.unwrap().len();
    assert_eq!(after - before, 1, "100 hot-page txns folded to one event");

    db.checkpoint().await.unwrap();
    let expected = std::mem::take(&mut db.image);
    drop(db);

    let store2 = fresh_store(&bucket).await;
    let (image, tail) = restore(&store2, &*bucket, "burst").await.unwrap();
    assert!(tail.is_some());
    assert_eq!(image.txid, 2601);
    let restored = dir.path().join("restored.db");
    image.write_to(&restored).unwrap();
    let ok: String = rusqlite::Connection::open(&restored)
        .unwrap()
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ok, "ok");
    assert_eq!(image, expected, "restored page map equals live image");
}

/// Coalescing keeps the manifest flat no matter how many checkpoints run,
/// and generational GC reclaims the replaced segment objects.
#[tokio::test]
async fn manifest_coalesces_and_segments_are_reclaimed() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let opts = CheckpointOpts { coalesce_at: 3, purge: false };

    let mut db = Db::open(store, bucket.clone(), "coal", &src).await.unwrap();
    db.connection().execute("CREATE TABLE t(n INTEGER PRIMARY KEY)", []).unwrap();
    db.sync().await.unwrap();
    // 12 checkpoint cycles; coalescing at >3 segments.
    for i in 0..12 {
        db.connection().execute("INSERT INTO t VALUES (?1)", [i]).unwrap();
        db.sync().await.unwrap();
        db.checkpoint_with(&opts).await.unwrap();
    }

    let manifest = latest_manifest(db.store(), "coal").await;
    assert!(manifest.segments.len() <= 4, "manifest stayed bounded: {} segments", manifest.segments.len());

    // One more checkpoint: the generation it retires is then collected...
    db.connection().execute("INSERT INTO t VALUES (1000)", []).unwrap();
    db.sync().await.unwrap();
    db.checkpoint_with(&opts).await.unwrap();
    // ...and the bucket holds no more objects than manifest refs + one
    // at-checkpoint generation of retirees.
    let manifest = latest_manifest(db.store(), "coal").await;
    let objects = segment_count(&bucket, "coal").await;
    assert!(
        objects <= manifest.segments.len() + manifest.retire.len(),
        "no orphans beyond one GC generation: {objects} objects vs {manifest:?}"
    );

    // Restore from the coalesced replica still matches the source exactly.
    // (A fresh store fences db's SlateDB client — only open it once db is done.)
    drop(db);
    let store2 = fresh_store(&bucket).await;
    let (image, _) = restore(&store2, &*bucket, "coal").await.unwrap();
    assert_eq!(image.txid, 14);
    let restored = dir.path().join("restored.db");
    image.write_to(&restored).unwrap();
    let conn = rusqlite::Connection::open(&restored).unwrap();
    let max: i64 = conn.query_row("SELECT max(n) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(max, 1000);
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
}

/// Retention: with `purge`, sealed history is physically gone — the manifest
/// carries the whole past, the backlog covers the present, and PITR into
/// deleted history fails loudly.
#[tokio::test]
async fn checkpoint_with_purge_bounds_history() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let opts = CheckpointOpts { coalesce_at: 10, purge: true };

    let mut db = Db::open(store, bucket.clone(), "ret", &src).await.unwrap();
    db.connection()
        .execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);")
        .unwrap();
    db.sync().await.unwrap();
    let txid_at_checkpoint = db.image.txid;
    db.checkpoint_with(&opts).await.unwrap();

    // Sealed events are physically gone; the manifest still restores them.
    assert_eq!(db.store().read_history("ret", ..).await.unwrap().len(), 0);
    let (image, _) = restore(db.store(), &*bucket, "ret").await.unwrap();
    assert_eq!(image.txid, txid_at_checkpoint);
    let revived = dir.path().join("revived.db");
    image.write_to(&revived).unwrap();
    let count: i64 = rusqlite::Connection::open(&revived)
        .unwrap()
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);

    // PITR at the checkpoint boundary = the segment: fine. PITR at a txid
    // *within* the purged backlog: fails loudly, no silent shortcut.
    restore_at(db.store(), &*bucket, "ret", txid_at_checkpoint).await.unwrap();
    let err = restore_at(db.store(), &*bucket, "ret", txid_at_checkpoint - 1).await.unwrap_err();
    assert!(err.to_string().contains("not reconstructable"), "got: {err}");

    // Keep working: new deltas accumulate, and a second purging checkpoint
    // still restores the full logical state.
    db.connection().execute_batch("INSERT INTO t VALUES (3)").unwrap();
    db.sync().await.unwrap();
    db.checkpoint_with(&opts).await.unwrap();
    let (image, _) = restore(db.store(), &*bucket, "ret").await.unwrap();
    image.write_to(&revived).unwrap();
    let count: i64 = rusqlite::Connection::open(&revived)
        .unwrap()
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

/// The "1M commits" scenario at test scale: 2000 transactions across 25
/// checkpoints (with coalescing), then a fresh VM hydrate is a flat read —
/// watch the manifest + backlog and assert they stay tiny.
#[tokio::test]
async fn hydrate_after_many_commits_is_flat() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("big.db");
    let opts = CheckpointOpts { coalesce_at: 8, purge: true };

    let mut db = Db::open(store, bucket.clone(), "big", &src).await.unwrap();
    db.connection()
        .execute("CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)", [])
        .unwrap();
    db.sync().await.unwrap();
    for i in 0..2000u32 {
        db.connection()
            .execute("INSERT OR REPLACE INTO kv VALUES (?1, 'payload')", [i % 500])
            .unwrap();
        if i % 80 == 79 {
            db.sync().await.unwrap();
            db.checkpoint_with(&opts).await.unwrap();
        }
    }
    db.sync().await.unwrap();
    db.checkpoint_with(&opts).await.unwrap();
    assert_eq!(db.image.txid, 2001);
    drop(db);

    // Fresh-VM hydrate: one manifest (≤ one coalesced segment), no backlog,
    // no retained history.
    let store2 = fresh_store(&bucket).await;
    let manifest = latest_manifest(&store2, "big").await;
    assert_eq!(manifest.segments.len(), 1, "coalesced down to one segment");
    assert_eq!(store2.read_history("big", ..).await.unwrap().len(), 0, "history purged");

    let (image, tail) = restore(&store2, &*bucket, "big").await.unwrap();
    assert!(tail.is_some() && image.txid == 2001);
    let restored = dir.path().join("restored.db");
    image.write_to(&restored).unwrap();
    let conn = rusqlite::Connection::open(&restored).unwrap();
    let count: i64 = conn.query_row("SELECT count(*) FROM kv", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 500);
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");

    // And the db keeps growing normally on the fresh hydrate.
    let mut db2 = Db::open(store2, bucket.clone(), "big", dir.path().join("live.db")).await.unwrap();
    db2.connection().execute("INSERT INTO kv VALUES (999, 'post-hydrate')", []).unwrap();
    assert_eq!(db2.sync().await.unwrap(), 1);
    assert_eq!(db2.image.txid, 2002);
}
