//! Behavioral coverage for slog-sqlite: happy-path round-trip, point-in-time
//! restore, crash recovery, restore-and-continue-after-loss,
//! checkpoint-truncate continuity, and a property test over random
//! transaction mixes. Binary-format conformance and parser fuzzing are
//! outside this suite; slog-sqlite uses its own packed binary format.
//!
//! The equality oracle is implemented natively in [`oracle`].

mod oracle;

use std::sync::Arc;

use object_store::memory::InMemory;
use slog::{Config, EventStore};
use slog_sqlite::{delta_events, restore, restore_at, Db, Manifest};

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

/// Captures several transactions, including a multi-page transaction,
/// restores them into a new path, and compares the restored database with the
/// source.
#[tokio::test]
async fn round_trip_reproduces_source() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let restored_path = dir.path().join("restored.db");

    let mut db = Db::open(store, bucket.clone(), "cell-1", &src).await.unwrap();
    for sql in [
        "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)",
        "INSERT INTO kv (k, v) VALUES ('a','1'),('b','2'),('c','3')",
        "UPDATE kv SET v='updated' WHERE k='a'",
        "INSERT INTO kv (k, v) VALUES ('d','4'),('e','5')",
        "DELETE FROM kv WHERE k='b'",
        // A larger transaction to exercise multi-page WAL frames.
        "CREATE TABLE big (id INTEGER PRIMARY KEY, blob TEXT);\
         INSERT INTO big (id, blob) SELECT value, hex(randomblob(200)) \
           FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value<500) SELECT value FROM c);",
    ] {
        db.connection().execute_batch(sql).unwrap();
        db.sync().await.unwrap();
    }
    assert!(db.image.txid >= 6, "at least 6 transactions captured");

    // Once the database is caught up, the stream contains every captured
    // transaction id in order.
    assert_eq!(db.sync().await.unwrap(), 0, "stream caught up to the db");
    let store2 = fresh_store(&bucket).await;
    let events = store2.read_history("cell-1", 0..).await.unwrap();
    let txids: Vec<u64> =
        events.iter().flat_map(|e| delta_events(e).unwrap()).map(|d| d.txid).collect();
    assert_eq!(txids, (1..=db.image.txid).collect::<Vec<_>>());

    drop(db);

    let (image, _) = restore(&store2, &*bucket, "cell-1").await.unwrap();
    image.write_to(&restored_path).unwrap();
    oracle::assert_equal(&src, &restored_path);
}

/// Restores an intermediate transaction id and compares the result with a
/// `VACUUM INTO` snapshot taken at that point, after a checkpoint has sealed
/// later transactions.
#[tokio::test]
async fn restore_to_target_txid_reproduces_point_in_time() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let snapshot = dir.path().join("at_txid.db");
    let pitr = dir.path().join("restored.db");

    let mut db = Db::open(store, bucket.clone(), "pit-1", &src).await.unwrap();
    db.connection()
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
        .unwrap();
    db.sync().await.unwrap();
    db.connection().execute_batch("INSERT INTO t (v) VALUES ('one'),('two')").unwrap();
    db.sync().await.unwrap();

    // Capture the database state at the target transaction id.
    let target = db.image.txid;
    db.connection()
        .execute("VACUUM INTO ?1", [snapshot.to_string_lossy().to_string()])
        .unwrap();

    // Continue mutating after the snapshot point; a checkpoint sealing them
    // must not leak post-target state into the PITR image.
    db.connection().execute_batch("UPDATE t SET v='TWO' WHERE id=2").unwrap();
    db.sync().await.unwrap();
    db.connection().execute_batch("INSERT INTO t (v) VALUES ('three')").unwrap();
    db.sync().await.unwrap();
    db.checkpoint().await.unwrap();
    drop(db);

    let store2 = fresh_store(&bucket).await;
    let image = restore_at(&store2, &*bucket, "pit-1", target).await.unwrap();
    assert_eq!(image.txid, target);
    image.write_to(&pitr).unwrap();
    oracle::assert_equal(&snapshot, &pitr);
}

/// Drops a `Db` without a clean shutdown, reopens it on the same path, and
/// verifies that synced transactions survive and later writes are captured.
#[tokio::test]
async fn crash_in_the_middle_then_reopen_and_restore() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let restored_path = dir.path().join("restored.db");

    {
        let mut db = Db::open(store, bucket.clone(), "cr-1", &src).await.unwrap();
        db.connection()
            .execute_batch(
                "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);\
                 INSERT INTO kv (k,v) VALUES ('a','1'),('b','2'),('c','3')",
            )
            .unwrap();
        db.sync().await.unwrap();

        // Committed locally without a sync.
        db.connection().execute_batch("INSERT INTO kv (k,v) VALUES ('x','9')").unwrap();
        // Simulate a crash by dropping the handle without close or sync.
    }

    // Reopening restores the local file from the stream; the unsynced commit
    // is absent.
    let store2 = fresh_store(&bucket).await;
    let mut db = Db::open(store2, bucket.clone(), "cr-1", &src).await.unwrap();
    let lost: i64 = db
        .connection()
        .query_row("SELECT count(*) FROM kv WHERE k = 'x'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lost, 0, "un-synced commit does not survive a crash");

    db.connection()
        .execute_batch(
            "UPDATE kv SET v='updated' WHERE k='a';\
             INSERT INTO kv (k,v) VALUES ('d','4'),('e','5');\
             DELETE FROM kv WHERE k='b';",
        )
        .unwrap();
    db.sync().await.unwrap();
    drop(db);

    let store3 = fresh_store(&bucket).await;
    let (image, _) = restore(&store3, &*bucket, "cr-1").await.unwrap();
    image.write_to(&restored_path).unwrap();
    oracle::assert_equal(&src, &restored_path);
}

/// Recovers to an earlier state, writes new data, syncs again, and restores
/// again. The new data must survive the recovery cycle.
#[tokio::test]
async fn restore_and_replicate_after_data_loss() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db.sqlite");

    // Create initial data and replicate it.
    {
        let mut db = Db::open(store, bucket.clone(), "loss-1", &db_path).await.unwrap();
        db.connection()
            .execute_batch("CREATE TABLE test(col1 INTEGER); INSERT INTO test VALUES (1);")
            .unwrap();
        db.sync().await.unwrap();
        db.checkpoint().await.unwrap();
    }

    // Remove the local database and its WAL files.
    for ext in ["", "-wal", "-shm"] {
        drop(std::fs::remove_file(format!("{}{ext}", db_path.display())));
    }

    // Reopen from the stream, insert new data, and sync it.
    {
        let store2 = fresh_store(&bucket).await;
        let mut db = Db::open(store2, bucket.clone(), "loss-1", &db_path).await.unwrap();
        let count: i64 =
            db.connection().query_row("SELECT count(*) FROM test", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1, "local state came back from the stream");
        db.connection().execute_batch("INSERT INTO test VALUES (2);").unwrap();
        db.sync().await.unwrap();
    }

    // Restore again to a path whose parent directory does not exist.
    let store3 = fresh_store(&bucket).await;
    let (image, _) = restore(&store3, &*bucket, "loss-1").await.unwrap();
    let restored_path = dir.path().join("restored").join("db.sqlite");
    image.write_to(&restored_path).unwrap();

    // The restored database includes the new row.
    let conn = rusqlite::Connection::open(&restored_path).unwrap();
    let count: i64 = conn.query_row("SELECT count(*) FROM test", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 2, "expected 2 rows (1 and 2) in restored database");
    let exists: bool = conn
        .query_row("SELECT EXISTS(SELECT 1 FROM test WHERE col1 = 2)", [], |r| r.get(0))
        .unwrap();
    assert!(exists, "new data (value=2) was not replicated");
}

/// Verifies that SQLite `TRUNCATE` checkpoints between writes preserve the
/// captured chain across WAL salt resets.
#[tokio::test]
async fn checkpoint_truncate_continuity_break_still_restores() {
    let (store, bucket) = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let restored_path = dir.path().join("restored.db");

    let mut db = Db::open(store, bucket.clone(), "ct-1", &src).await.unwrap();
    db.connection().execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    db.connection().execute_batch("INSERT INTO t (v) VALUES ('one'),('two')").unwrap();
    db.sync().await.unwrap();

    // Force a TRUNCATE checkpoint: restarts the WAL, rotates the salts.
    db.checkpoint().await.unwrap();
    assert_eq!(
        std::fs::metadata(format!("{}-wal", src.display())).map(|m| m.len()).unwrap_or(0),
        0,
        "checkpoint truncated the WAL"
    );

    db.connection().execute_batch("INSERT INTO t (v) VALUES ('three')").unwrap();
    db.sync().await.unwrap();
    db.connection().execute_batch("UPDATE t SET v='TWO' WHERE id=2").unwrap();
    db.sync().await.unwrap();
    // A second checkpoint mid-stream for good measure.
    db.checkpoint().await.unwrap();
    db.connection().execute_batch("INSERT INTO t (v) VALUES ('four'),('five')").unwrap();
    db.sync().await.unwrap();

    assert!(db.image.txid >= 5, "at least 5 transactions captured");
    let last_txid = db.image.txid;
    drop(db);

    let store2 = fresh_store(&bucket).await;
    // Both sealed segments are in the manifest.
    let snap = store2.latest_snapshot("ct-1").await.unwrap().unwrap();
    let manifest: Manifest = serde_json::from_slice(&snap.state).unwrap();
    assert_eq!(manifest.segments.len(), 2);
    let (image, _) = restore(&store2, &*bucket, "ct-1").await.unwrap();
    assert_eq!(image.txid, last_txid);
    image.write_to(&restored_path).unwrap();
    oracle::assert_equal(&src, &restored_path);
}

// ── property-based round-trip ────────────────────────────────────────────────

use proptest::prelude::*;

/// Number of random sequences to try.
const PROPTEST_CASES: u32 = 24;
/// Maximum number of transactions per generated sequence.
const MAX_OPS: usize = 14;

/// One generated transaction against a fixed `kv(k INTEGER PRIMARY KEY, v TEXT)`
/// schema plus occasional structural events.
#[derive(Debug, Clone)]
enum Op {
    /// `INSERT OR REPLACE` a key with a text value.
    Upsert { key: i64, val: String },
    /// `DELETE` a key (may match nothing — still a valid, empty-ish txn).
    Delete { key: i64 },
    /// `UPDATE` all rows whose key is below a bound (range write).
    UpdateBelow { bound: i64, val: String },
    /// Insert `n` rows in a single transaction (multi-page growth).
    BulkInsert { start: i64, n: i64, val: String },
    /// Create an extra table and seed one row (DDL + schema change).
    CreateAux { idx: u8 },
    /// Seal an LTX segment + TRUNCATE checkpoint (continuity break / salt reset).
    CheckpointTruncate,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let val = "[a-zA-Z0-9]{0,24}";
    prop_oneof![
        6 => (0i64..40, val).prop_map(|(key, v)| Op::Upsert { key, val: v }),
        3 => (0i64..40).prop_map(|key| Op::Delete { key }),
        3 => (0i64..40, val).prop_map(|(bound, v)| Op::UpdateBelow { bound, val: v }),
        3 => (0i64..30, 1i64..8, val).prop_map(|(start, n, v)| Op::BulkInsert { start, n, val: v }),
        1 => (0u8..3).prop_map(|idx| Op::CreateAux { idx }),
        2 => Just(Op::CheckpointTruncate),
    ]
}

/// Applies one op's SQL (checkpoint ops are handled by the caller, which owns
/// the `Db`).
fn apply_op(op: &Op, db: &Db) {
    let writer = db.connection();
    match op {
        Op::Upsert { key, val } => {
            writer.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)", (key, val)).unwrap();
        }
        Op::Delete { key } => {
            writer.execute("DELETE FROM kv WHERE k = ?1", [key]).unwrap();
        }
        Op::UpdateBelow { bound, val } => {
            writer.execute("UPDATE kv SET v = ?2 WHERE k < ?1", (bound, val)).unwrap();
        }
        Op::BulkInsert { start, n, val } => {
            let tx_sql: String = (0..*n)
                .map(|i| format!("INSERT OR REPLACE INTO kv (k, v) VALUES ({}, '{}');", start + i, val))
                .collect();
            // Wrap as a single transaction so it is one captured TXID.
            writer.execute_batch(&format!("BEGIN; {tx_sql} COMMIT;")).unwrap();
        }
        Op::CreateAux { idx } => {
            let i = (*idx % 3) as usize;
            writer
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS aux{i} (id INTEGER PRIMARY KEY, label TEXT);\
                     INSERT INTO aux{i} (id, label) VALUES (1, 'seed-{i}') \
                       ON CONFLICT(id) DO UPDATE SET label='seed-{i}';"
                ))
                .unwrap();
        }
        Op::CheckpointTruncate => unreachable!(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: PROPTEST_CASES,
        // Shrinking re-runs the (heavy) pipeline; cap it so a failure still
        // reports quickly. The seed alone reproduces any case.
        max_shrink_iters: 24,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// For a random sequence of transactions, sync, checkpoint, and restore
    /// produce a database logically identical to the source.
    #[test]
    fn random_txns_roundtrip_restores_source(ops in prop::collection::vec(op_strategy(), 1..=MAX_OPS)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (store, bucket) = harness().await;
            let dir = tempfile::tempdir().unwrap();
            let src = dir.path().join("source.db");
            let restored_path = dir.path().join("restored.db");

            let mut db = Db::open(store, bucket.clone(), "prop", &src).await.unwrap();
            // Base schema (one captured TXID).
            db.connection()
                .execute_batch("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            db.sync().await.unwrap();

            for op in &ops {
                match op {
                    Op::CheckpointTruncate => {
                        // Idle seals are a valid no-op (Ok(None)).
                        db.checkpoint().await.unwrap();
                    }
                    _ => {
                        apply_op(op, &db);
                        db.sync().await.unwrap();
                    }
                }
            }

            // Final seal so both the LTX path and the backlog are covered.
            db.checkpoint().await.unwrap();
            drop(db);

            let store2 = fresh_store(&bucket).await;
            let (image, _) = restore(&store2, &*bucket, "prop").await.unwrap();
            image.write_to(&restored_path).unwrap();
            oracle::assert_equal(&src, &restored_path);
        });
    }
}
