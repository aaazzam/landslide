//! High-churn demo against a real object store.
//!
//!   cargo run -p slog-fuse --features fuse --example churn -- <cmd> [vol]
//!
//! Commands:
//!   mount <vol> <mountpoint>   FUSE-mount the volume read-write (blocking)
//!   follow <vol> <mountpoint>  FUSE-mount a read-only replica, kept synced
//!   verify <vol>               remount without FUSE and print volumes stats
//!   checkpoint <vol>           force a checkpoint (segment + manifest)
//!
//! Object store: real S3 when SLOG_BUCKET is set (creds via AWS_PROFILE /
//! AWS_* env / ~/.aws), else a local dir at SLOG_BUCKET_DIR (default
//! /tmp/slog-bucket). SLOG_REGION/AWS_REGION picks the region (default
//! us-east-1), SLOG_PATH the db prefix (default "slogfs/churn").

use std::sync::Arc;

use object_store::ObjectStore;
use slog::{Config, EventStore};
use slog_fuse::Volume;

fn bucket() -> Arc<dyn ObjectStore> {
    if let Ok(bucket_name) = std::env::var("SLOG_BUCKET") {
        let builder = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket_name)
            .with_region(
                std::env::var("SLOG_REGION")
                    .or_else(|_| std::env::var("AWS_REGION"))
                    .unwrap_or_else(|_| "us-east-1".into()),
            );
        Arc::new(builder.build().expect("s3 build"))
    } else {
        let dir = std::env::var("SLOG_BUCKET_DIR").unwrap_or_else(|_| "/tmp/slog-bucket".into());
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).expect("local fs"))
    }
}

#[tokio::main]
async fn main() -> slog::Result<()> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    let vol = args.next().unwrap_or_else(|| "churn-demo".into());
    let path = std::env::var("SLOG_PATH").unwrap_or_else(|_| "slogfs/churn".into());

    let (store, bucket) = (
        Arc::new(
            EventStore::open(Config { path: path.clone(), object_store: bucket(), settings: None })
                .await?,
        ),
        bucket(),
    );

    match cmd.as_str() {
        "mount" => {
            let mp = args.next().expect("usage: mount <vol> <mountpoint>");
            println!("mounting {vol} at {mp} (fence taken; ctrl-c to stop)");
            slog_fuse::fs::mount(store, bucket, &vol, &mp).await
        }
        "follow" => {
            let mp = args.next().expect("usage: follow <vol> <mountpoint>");
            let reader = Arc::new(
                slog::EventStoreReader::open(slog::ReaderConfig {
                    path,
                    object_store: bucket.clone(),
                    options: None,
                })
                .await?,
            );
            println!("read-only replica of {vol} at {mp} (syncs every 2s; ctrl-c to stop)");
            slog_fuse::fs::mount_replica(reader, bucket, &vol, &mp, std::time::Duration::from_secs(2))
                .await
        }
        "verify" => {
            let v = Volume::mount(store, bucket, &vol).await?;
            let (files, bytes) = v
                .image
                .nodes
                .iter()
                .map(|(p, n)| match n {
                    slog_fuse::Node::File { content, .. } => (p, content.len()),
                    _ => (p, 0),
                })
                .fold((0usize, 0u64), |(f, b), (_, s)| (f + (s > 0) as usize, b + s));
            println!("volume {vol}: {} nodes, {files} files, {bytes} content bytes", v.image.nodes.len());
            println!("tail version: {:?}, block cache after mount: {}", v.tail().await?, v.block_cache_len());
            for (path, node) in v.image.nodes.iter().take(5) {
                println!("  {path}: {:?}", std::mem::discriminant(node));
            }
            Ok(())
        }
        "checkpoint" => {
            let mut v = Volume::mount(store, bucket, &vol).await?;
            let record = v.checkpoint().await?;
            println!("checkpointed {vol} through v{}", record.through_version);
            Ok(())
        }
        _ => {
            eprintln!("usage: churn <mount|follow|verify|checkpoint> <vol> [mountpoint]");
            std::process::exit(1);
        }
    }
}
