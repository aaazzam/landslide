//! Write-churn latency probe: bursts of tiny autocommit transactions, then a
//! durability barrier (`sync`), measuring how much latency a commit pays to
//! become durable in the backend under churn. Prints machine-readable
//! `SAMPLE` CSV lines (parsed by scripts/churn_report.py):
//!
//!   SAMPLE,<backend>,<phase>,<round>,<txns>,<bytes>,<ms>
//!
//! Phases: write (SQLite exec wall time per round), sync (durability
//! barrier), checkpoint (seal + manifest), open/open_fresh (EventStore
//! init), hydrate (restore from the backend on a fresh store).
//!
//!   cargo run -p landslide-sqlite --release --example churn
//!
//! Backend: real S3 when LANDSLIDE_TEST_BUCKET is set (AWS_REGION plus env creds),
//! else a local dir at LANDSLIDE_BUCKET_DIR (default /tmp/landslide-churn-bucket).
//! Knobs: LANDSLIDE_CHURN_ROUNDS (30), LANDSLIDE_CHURN_TXNS (150), LANDSLIDE_CHURN_KEYS
//! (500), LANDSLIDE_CHURN_CKPT_EVERY (2), LANDSLIDE_CHURN_PROFILE (default |
//! fastflush | compact) — SlateDB flushing strategy for both store handles.

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use landslide::deps::slatedb;
use landslide::{Config, EventStore};
use landslide_sqlite::{restore, CheckpointOpts, Db};

/// SlateDB profiles under test:
///
/// - `default`: 100ms WAL flush tick and 64MB L0 threshold.
/// - `fastflush`: 10ms WAL flush tick.
/// - `compact`: 10ms WAL flush tick and 256KB L0 threshold.
///
/// WAL remains enabled for every profile. Without it, `await_durable` waits
/// for an L0 flush, which may require a 64MB freeze or `Db::close` in
/// SlateDB 0.15.
fn settings_for_profile(profile: &str) -> Option<slatedb::config::Settings> {
    match profile {
        "default" => None,
        "fastflush" => Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        }),
        "compact" => Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(10)),
            l0_sst_size_bytes: 256 * 1024,
            ..Default::default()
        }),
        "compact4mb" => Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(10)),
            l0_sst_size_bytes: 4 * 1024 * 1024,
            ..Default::default()
        }),
        other => panic!("unknown LANDSLIDE_CHURN_PROFILE '{other}' (default|fastflush|compact)"),
    }
}

fn backend() -> (&'static str, Arc<dyn ObjectStore>) {
    if let Ok(bucket) = std::env::var("LANDSLIDE_TEST_BUCKET") {
        let s3 = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("s3 client");
        ("s3", Arc::new(s3))
    } else {
        let dir = std::env::var("LANDSLIDE_BUCKET_DIR").unwrap_or_else(|_| "/tmp/landslide-churn-bucket".into());
        std::fs::create_dir_all(&dir).unwrap();
        ("local", Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap()))
    }
}

fn open_store(backend: &str, bucket: Arc<dyn ObjectStore>, profile: &str) -> Config {
    Config {
        path: format!("churn/{backend}-{profile}"),
        object_store: bucket,
        settings: settings_for_profile(profile),
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// xorshift64*: deterministic churn over the key space, no rand dep.
fn next_key(rng: &mut u64, keys: u64) -> u64 {
    *rng ^= *rng >> 12;
    *rng ^= *rng << 25;
    *rng ^= *rng >> 27;
    rng.wrapping_mul(0x2545F4914F6CDD1D) % keys
}

fn sample(backend: &str, phase: &str, round: usize, txns: usize, bytes: usize, t: Instant) {
    println!("SAMPLE,{backend},{phase},{round},{txns},{bytes},{:.3}", t.elapsed().as_secs_f64() * 1e3);
}

#[tokio::main]
async fn main() -> landslide::Result<()> {
    let rounds = env_usize("LANDSLIDE_CHURN_ROUNDS", 30);
    let txns = env_usize("LANDSLIDE_CHURN_TXNS", 150);
    let keys = env_usize("LANDSLIDE_CHURN_KEYS", 500) as u64;
    let ckpt_every = env_usize("LANDSLIDE_CHURN_CKPT_EVERY", 2);
    let (backend, bucket) = backend();
    let profile = std::env::var("LANDSLIDE_CHURN_PROFILE").unwrap_or_else(|_| "default".into());
    let label = format!("{backend}-{profile}");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("churn.db");
    let name = format!("churn-{}", ulid::Ulid::new());

    let t = Instant::now();
    let store = EventStore::open(open_store(backend, bucket.clone(), &profile)).await?;
    sample(&label, "open", 0, 0, 0, t);

    let mut db = Db::open(store, bucket.clone(), &name, &path).await?;
    db.connection().execute_batch("CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)").unwrap();

    let opts = CheckpointOpts { coalesce_at: 3, purge: true };
    let mut rng = 0x9E3779B97F4A7C15u64;
    let value = |k: u64| format!("v-{k:08x}-{}", "x".repeat(200));
    for round in 0..rounds {
        let t = Instant::now();
        for _ in 0..txns {
            let k = next_key(&mut rng, keys);
            db.connection()
                .execute("INSERT OR REPLACE INTO kv VALUES (?1, ?2)", (k as i64, value(k)))
                .unwrap();
        }
        sample(&label, "write", round, txns, txns * 215, t);

        let t = Instant::now();
        db.sync().await?;
        sample(&label, "sync", round, txns, txns * 215, t);

        if round % ckpt_every == ckpt_every - 1 {
            let t = Instant::now();
            db.checkpoint_with(&opts).await?;
            sample(&label, "checkpoint", round, 0, 0, t);
        }
    }
    let txid = db.image.txid;
    let image_bytes = db.image.to_bytes();
    drop(db);

    // Cold hydrate: fresh EventStore, reconstruct purely from the backend.
    let t = Instant::now();
    let store2 = EventStore::open(open_store(backend, bucket.clone(), &profile)).await?;
    sample(&label, "open_fresh", 0, 0, 0, t);
    let t = Instant::now();
    let (image, _) = restore(&store2, &*bucket, &name).await?;
    let n_pages = image.pages.len();
    sample(&label, "hydrate", 0, 0, n_pages * 4096, t);

    println!("VERIFY,txid,{txid},restored_txid,{},byte_equal,{}", image.txid, image.to_bytes() == image_bytes);
    eprintln!("[{label}] name={name} done: {rounds} rounds x {txns} txns");
    drop(store2);
    Ok(())
}
