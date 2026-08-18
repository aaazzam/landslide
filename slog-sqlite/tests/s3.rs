//! End-to-end integration test against real Amazon S3: several hundred
//! transactions replicated to a bucket, checkpointed with coalescing and
//! retention purge, then hydrated on a "fresh VM" (new local path + fresh
//! store handle) and verified.
//!
//! Self-skips (passes vacuously) unless `SLOG_TEST_BUCKET` is set. Uses the
//! standard AWS credential chain via `AmazonS3Builder::from_env`; set
//! `AWS_REGION` as usual.
//!
//! ```sh
//! SLOG_TEST_BUCKET=my-bucket AWS_REGION=us-east-1 \
//!   cargo test -p slog-sqlite --test s3 -- --nocapture
//! ```
//!
//! Prefixes are unique per run (`db/{run}` for the SlateDB namespace,
//! `ltx/{name}` for segments); after a run:
//!
//! ```sh
//! aws s3 rm --recursive s3://$SLOG_TEST_BUCKET/db/<run> &
//! aws s3 rm --recursive s3://$SLOG_TEST_BUCKET/ltx/<name>
//! ```

use std::sync::Arc;
use std::time::Instant;

use object_store::aws::AmazonS3Builder;
use slog::{Config, EventStore};
use slog_sqlite::{restore, restore_at, CheckpointOpts, Db, Manifest};

// ── Metered object store for boot-cost probes ───────────────────────────────

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

/// An [`object_store::ObjectStore`] wrapper that records op counts, total
/// time, and (where cheap) bytes per phase label.
#[derive(Debug)]
struct Metered {
    inner: Arc<dyn object_store::ObjectStore>,
    phase: Mutex<String>,
    stats: Mutex<BTreeMap<(String, String), (u64, Duration, u64)>>,
}

impl Metered {
    fn from_arn(inner: Arc<dyn object_store::ObjectStore>) -> Arc<Self> {
        Arc::new(Self { inner, phase: Mutex::new("?".into()), stats: Mutex::new(BTreeMap::new()) })
    }

    fn mark(&self, label: &str) {
        *self.phase.lock().unwrap() = label.into();
    }

    async fn time<T>(&self, op: &str, bytes: u64, f: impl std::future::Future<Output = T>) -> T {
        let t = Instant::now();
        let out = f.await;
        let phase = self.phase.lock().unwrap().clone();
        let mut s = self.stats.lock().unwrap();
        let e = s.entry((phase, op.into())).or_default();
        e.0 += 1;
        e.1 += t.elapsed();
        e.2 += bytes;
        out
    }

    /// Like [`time`], but logs the object path.
    async fn time_path<T>(
        &self,
        op: &str,
        path: &object_store::path::Path,
        f: impl std::future::Future<Output = T>,
    ) -> T {
        let t = Instant::now();
        let out = f.await;
        println!("    [{}] {op} {path} {:?}", self.phase.lock().unwrap(), t.elapsed());
        out
    }

    fn print(&self) {
        let s = self.stats.lock().unwrap();
        let mut by_phase: BTreeMap<String, (u64, Duration, u64)> = BTreeMap::new();
        for ((phase, op), (n, dur, bytes)) in s.iter() {
            println!("  {phase:<14} {op:<22} x{n:<4} {dur:>10.0?}  {bytes} B");
            let e = by_phase.entry(phase.clone()).or_default();
            e.0 += n;
            e.1 += *dur;
            e.2 += bytes;
        }
        for (phase, (n, dur, bytes)) in by_phase {
            // Note: durations overlap between concurrent calls; sums are
            // per-op totals, not wall time.
            println!("  {phase:<14} TOTAL x{n:<4} {dur:>10.0?}  {bytes} B");
        }
    }
}

impl std::fmt::Display for Metered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Metered({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Metered {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let bytes = payload.content_length() as u64;
        self.time("put_opts", bytes, self.inner.put_opts(location, payload, opts)).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.time("put_multipart", 0, self.inner.put_multipart_opts(location, opts)).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if std::env::var("PROBE_PATHS").is_ok() {
            return self.time_path("get", location, self.inner.get_opts(location, options)).await;
        }
        self.time("get_opts", 0, self.inner.get_opts(location, options)).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.time("list", 0, self.inner.list_with_delimiter(prefix)).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.time("copy", 0, self.inner.copy_opts(from, to, options)).await
    }
}

struct Skip;

/// Builds the S3-backed store config, or Skip if the test isn't configured.
fn s3_config(run: &str) -> Result<(Config, Arc<dyn object_store::ObjectStore>), Skip> {
    let Ok(bucket) = std::env::var("SLOG_TEST_BUCKET") else {
        eprintln!("skipping s3 test: SLOG_TEST_BUCKET is not set");
        return Err(Skip);
    };
    let s3 = AmazonS3Builder::from_env().with_bucket_name(bucket).build().expect("s3 client");
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(s3);
    Ok((Config { path: format!("db/{run}"), object_store: s3.clone(), settings: None }, s3))
}

#[tokio::test]
async fn replicate_checkpoint_coalesce_rehydrate_on_fresh_vm() {
    let run = ulid::Ulid::new().to_string();
    let name = |s: &str| format!("s3test-{s}");
    let (config, s3) = match s3_config(&run) {
        Ok(v) => v,
        Err(Skip) => return,
    };
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.db");
    let opts = CheckpointOpts { coalesce_at: 3, purge: true };
    let db_name = name(&run);

    // ── Node A: ~450 transactions, sealed to S3 in three checkpoints. ──────
    let t0 = Instant::now();
    let store = EventStore::open(config.clone()).await.unwrap();
    println!("store open (incl. SlateDB init on S3): {:?}", t0.elapsed());

    let mut db = Db::open(store, s3.clone(), &db_name, &src).await.unwrap();
    db.connection()
        .execute("CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)", [])
        .unwrap();
    for i in 0..150 {
        db.connection()
            .execute("INSERT OR REPLACE INTO kv VALUES (?1, printf('v-%d', ?1))", [i])
            .unwrap();
    }
    let t = Instant::now();
    let n = db.sync().await.unwrap();
    println!("sync #1: {n} txns captured in {:?}", t.elapsed());

    let t = Instant::now();
    db.checkpoint_with(&opts).await.unwrap();
    println!("checkpoint #1: {:?}", t.elapsed());

    for round in 0..3u32 {
        for i in 150 * (round + 1)..150 * (round + 2) {
            db.connection()
                .execute("INSERT OR REPLACE INTO kv VALUES (?1, printf('v-%d', ?1))", [i])
                .unwrap();
        }
        let t = Instant::now();
        db.sync().await.unwrap();
        db.checkpoint_with(&opts).await.unwrap();
        println!("sync+checkpoint #{}: {:?}", round + 2, t.elapsed());
    }
    assert_eq!(db.image.txid, 601);

    // Manifest must have coalesced by now (>3 segments trigger) — one object
    // carries the whole database image — and purged history must be gone.
    let snap = db.store().latest_snapshot(&db_name).await.unwrap().unwrap();
    let manifest: Manifest = serde_json::from_slice(&snap.state).unwrap();
    println!("manifest after 4 checkpoints: {} segments", manifest.segments.len());
    assert_eq!(manifest.segments.len(), 1, "coalesced to a single segment");
    let backlog = db.store().read_history(&db_name, ..).await.unwrap().len();
    println!("retained events after purge: {backlog}");
    assert_eq!(backlog, 0);

    let source_copy = db.image.to_bytes();
    let txid = db.image.txid;
    drop(db);

    // ── Node B ("fresh VM"): hydrate purely from S3. ──────────────────────
    let t = Instant::now();
    let store2 = EventStore::open(config).await.unwrap();
    let (image, tail) = restore(&store2, &*s3, &db_name).await.unwrap();
    println!("hydrate on fresh store: {:?} (txid {})", t.elapsed(), image.txid);
    assert_eq!(image.txid, txid);
    assert!(tail.is_some());
    assert_eq!(image.to_bytes(), source_copy, "byte-exact page map");

    let restored_path = dir.path().join("restored.db");
    image.write_to(&restored_path).unwrap();
    let conn = rusqlite::Connection::open(&restored_path).unwrap();
    let count: i64 = conn.query_row("SELECT count(*) FROM kv", [], |r| r.get(0)).unwrap();
    let max: i64 = conn.query_row("SELECT max(k) FROM kv", [], |r| r.get(0)).unwrap();
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    println!("restored: {count} rows, max key {max}, integrity_check = {ok}");
    assert_eq!((count, max, ok.as_str()), (600, 599, "ok"));

    // PITR into purged history must fail loudly.
    let err = restore_at(&store2, &*s3, &db_name, 1).await.unwrap_err();
    println!("PITR into purged history: {err}");
    assert!(err.to_string().contains("not reconstructable"));

    // Node B takes over: new writes continue the txid chain.
    let mut db2 =
        Db::open(store2, s3.clone(), &db_name, dir.path().join("node-b.db")).await.unwrap();
    db2.connection().execute("INSERT INTO kv VALUES (999, 'from-node-b')", []).unwrap();
    let t = Instant::now();
    assert_eq!(db2.sync().await.unwrap(), 1);
    println!("node B sync after takeover: {:?} (txid {})", t.elapsed(), db2.image.txid);
    assert_eq!(db2.image.txid, txid + 1);

    println!("run prefix: db/{run} and ltx/{db_name} in $SLOG_TEST_BUCKET");
}

/// Boot-cost breakdown: what does opening against S3 actually spend on?
/// Prints object-store op counts/timings per phase (cold open, warm open,
/// Db::open = fence + restore) via the Metered wrapper.
#[tokio::test]
async fn boot_cost_breakdown() {
    let run = ulid::Ulid::new().to_string();
    let (bucket, _) = match std::env::var("SLOG_TEST_BUCKET") {
        Ok(b) => (b, ()),
        Err(_) => {
            eprintln!("skipping s3 test: SLOG_TEST_BUCKET is not set");
            return;
        }
    };
    let inner: Arc<dyn object_store::ObjectStore> =
        Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).build().unwrap());
    let m = Metered::from_arn(inner);
    let path = format!("probe/{run}");

    m.mark("open-cold");
    let t = Instant::now();
    let store = EventStore::open(Config { path: path.clone(), object_store: m.clone(), settings: None })
        .await
        .unwrap();
    println!("open-cold wall: {:?}", t.elapsed());

    // Populate a bit so warm opens replay real WAL state + a manifest exists.
    m.mark("populate");
    let mut db = Db::open(store, m.clone(), &format!("s3boot-{run}"), "boot.db").await.unwrap();
    db.connection().execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1)").unwrap();
    db.sync().await.unwrap();
    db.checkpoint().await.unwrap();
    drop(db);

    m.mark("open-warm");
    let t = Instant::now();
    let store2 = EventStore::open(Config {
        path: path.clone(),
        object_store: m.clone(),
        settings: None,
    })
    .await
    .unwrap();
    println!("open-warm wall: {:?}", t.elapsed());

    m.mark("db-open");
    let t = Instant::now();
    let db2 = Db::open(store2, m.clone(), &format!("s3boot-{run}"), "boot2.db").await.unwrap();
    println!("db-open wall: {:?}", t.elapsed());
    drop(db2);

    // Last: a second writer on the same path fences earlier clients, so the
    // no-GC probe must come after everything else that uses `store2`.
    m.mark("open-nogc");
    let t = Instant::now();
    let store_nogc = EventStore::open(Config {
        path: path.clone(),
        object_store: m.clone(),
        settings: Some(slog::deps::slatedb::config::Settings {
            garbage_collector_options: None,
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    println!("open-nogc wall: {:?}", t.elapsed());
    drop(store_nogc);

    m.print();
}
