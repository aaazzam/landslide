use std::sync::Arc;

use object_store::memory::InMemory;
use slog::{Config, Error, EventStore};
use slog_sqlite::{restore, Db};

async fn harness(bucket: Arc<InMemory>) -> (EventStore, Arc<InMemory>) {
    (
        EventStore::open(Config {
            path: "db".into(),
            object_store: bucket.clone(),
            settings: None,
        })
        .await
        .unwrap(),
        bucket,
    )
}

#[tokio::test]
async fn create_sync_checkpoint_reopen() {
    let (store, bucket) = harness(Arc::new(InMemory::new())).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.db");

    let mut db = Db::open(store, bucket.clone(), "app-1", &path).await.unwrap();
    let conn = db.connection();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)", []).unwrap();
    conn.execute_batch("INSERT INTO t(v) VALUES ('a'); INSERT INTO t(v) VALUES ('b');").unwrap();
    let n = db.sync().await.unwrap();
    assert!(n >= 2); // create + two inserts, each its own tx

    // Checkpoint seals an LTX segment + manifest and truncates the WAL.
    db.checkpoint().await.unwrap();
    assert_eq!(
        std::fs::metadata(format!("{}-wal", path.display())).map(|m| m.len()).unwrap_or(0),
        0
    );
    let before = db.image.to_bytes();
    drop(db);

    // Forget everything: fresh image must come back from manifest+LTX alone.
    let store2 = EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None })
        .await
        .unwrap();
    let db2 = Db::open(store2, bucket, "app-1", dir.path().join("copy.db")).await.unwrap();
    assert_eq!(db2.image.to_bytes(), before);
    let count: i64 = db2.connection().query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 2);
    let ok: String =
        db2.connection().query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
}

/// Re-syncs with no new txns must cost O(1), not O(WAL size).
#[tokio::test]
async fn resyncing_a_large_wal_only_reads_the_new_tail() {
    let (store, bucket) = harness(Arc::new(InMemory::new())).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.db");

    let mut db = Db::open(store, bucket, "walreads", &path).await.unwrap();
    db.connection().execute("CREATE TABLE t(v BLOB)", []).unwrap();
    let blob = vec![7u8; 3000];
    for _ in 0..12_000 {
        db.connection().execute("INSERT INTO t(v) VALUES (?1)", [&blob[..]]).unwrap();
    }
    db.sync().await.unwrap();
    let wal = std::fs::metadata(format!("{}-wal", path.display())).unwrap().len();

    let t = std::time::Instant::now();
    for _ in 0..20 {
        assert_eq!(db.sync().await.unwrap(), 0);
    }
    let elapsed = t.elapsed();
    println!("SCALING wal_bytes={wal} empty_resyncs=20 elapsed_ms={:.1}", elapsed.as_secs_f64() * 1e3);
    assert!(elapsed < std::time::Duration::from_millis(250), "resyncs reread the WAL: {elapsed:?}");
}

#[tokio::test]
async fn restore_from_backlog_only() {
    let (store, bucket) = harness(Arc::new(InMemory::new())).await;
    let dir = tempfile::tempdir().unwrap();

    let mut db = Db::open(store, bucket.clone(), "v2", dir.path().join("a.db")).await.unwrap();
    db.connection()
        .execute_batch(
            "CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER);
             INSERT INTO kv VALUES ('x', 1);
             INSERT INTO kv VALUES ('y', 2);
             UPDATE kv SET v = 3 WHERE k = 'x';
             DELETE FROM kv WHERE k = 'y';",
        )
        .unwrap();
    db.sync().await.unwrap();
    drop(db); // no checkpoint: the delta backlog must suffice

    let store2 = EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None })
        .await
        .unwrap();
    let db2 = Db::open(store2, bucket, "v2", dir.path().join("b.db")).await.unwrap();
    let rows: Vec<(String, i64)> = db2
        .connection()
        .prepare("SELECT k, v FROM kv ORDER BY k")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, [("x".to_string(), 3)]);
}

#[tokio::test]
async fn reopen_fences_the_previous_writer() {
    let (store, bucket) = harness(Arc::new(InMemory::new())).await;
    let dir = tempfile::tempdir().unwrap();

    let mut db = Db::open(store, bucket.clone(), "v3", dir.path().join("a.db")).await.unwrap();
    db.connection().execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1);").unwrap();
    db.sync().await.unwrap();

    // A second open of the same database (failover) takes the fence...
    let store2 = EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None })
        .await
        .unwrap();
    let mut db2 = Db::open(store2, bucket.clone(), "v3", dir.path().join("b.db")).await.unwrap();
    // ...and sees what db wrote: b.db was materialized from the stream.
    let count: i64 = db2.connection().query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);

    // ...and the old writer's syncs are now rejected. Rejection surfaces at
    // one of two fencing layers: slog's cooperative stream fence
    // (`FenceMismatch`), or — when both stores share one SlateDB path, as
    // here — SlateDB's client fencing (the newer DB client hard-closes the
    // old one).
    db.connection().execute("INSERT INTO t VALUES (2)", []).unwrap();
    let err = db.sync().await.unwrap_err();
    let stream_fenced = matches!(&err, Error::FenceMismatch { .. });
    let client_fenced = matches!(
        &err,
        Error::Backend(e)
            if e.kind() == slog::deps::slatedb::ErrorKind::Closed(slog::deps::slatedb::CloseReason::Fenced)
    );
    assert!(stream_fenced || client_fenced, "expected fencing, got: {err}");

    db2.connection().execute("INSERT INTO t VALUES (3)", []).unwrap();
    db2.sync().await.unwrap();
    assert_eq!(db2.image.txid, 3); // create + insert(1) + insert(3)

    // restore() without fencing also sees the current state.
    let store3 = EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None })
        .await
        .unwrap();
    let (image, _) = restore(&store3, &*bucket, "v3").await.unwrap();
    assert_eq!(image.txid, 3);
}

#[tokio::test]
async fn many_transactions_across_checkpoints() {
    let (store, bucket) = harness(Arc::new(InMemory::new())).await;
    let dir = tempfile::tempdir().unwrap();

    let mut db = Db::open(store, bucket.clone(), "v4", dir.path().join("a.db")).await.unwrap();
    db.connection().execute("CREATE TABLE seq(n INTEGER PRIMARY KEY)", []).unwrap();
    for i in 0..50 {
        db.connection().execute("INSERT INTO seq VALUES (?1)", [i]).unwrap();
    }
    assert_eq!(db.sync().await.unwrap(), 51);
    db.checkpoint().await.unwrap();

    // More transactions after the first segment: reopen must merge
    // manifest+LTX with the backlog after it.
    for i in 50..75 {
        db.connection().execute("INSERT INTO seq VALUES (?1)", [i]).unwrap();
    }
    db.sync().await.unwrap();
    db.checkpoint().await.unwrap();
    drop(db);

    let store2 = EventStore::open(Config { path: "db".into(), object_store: bucket.clone(), settings: None })
        .await
        .unwrap();
    let db2 = Db::open(store2, bucket, "v4", dir.path().join("b.db")).await.unwrap();
    let max: i64 = db2.connection().query_row("SELECT max(n) FROM seq", [], |r| r.get(0)).unwrap();
    assert_eq!(max, 74);
    let ok: String =
        db2.connection().query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
}
