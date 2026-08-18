//! landslidefs — the landslide-fuse volume CLI.
//!
//!   landslidefs follow <vol> <mountpoint> [interval-ms]   read-only replica mount, kept synced  [fuse]
//!   landslidefs mount <vol> <mountpoint>                  writable mount (takes the fence)      [fuse]
//!   landslidefs mirror once <vol> <dir>                   materialize the volume into dir, exit
//!   landslidefs mirror follow <vol> <dir> [interval-ms]   keep dir synced (no FUSE needed)
//!   landslidefs checkpoint <vol>                          force a checkpoint (segment + manifest)
//!   landslidefs verify <vol>                              print volume stats
//!
//! Object store: real S3 when LANDSLIDE_BUCKET is set (creds via AWS_PROFILE /
//! AWS_* env / ~/.aws), else a local dir at LANDSLIDE_BUCKET_DIR (default
//! /tmp/landslide-bucket). LANDSLIDE_REGION/AWS_REGION picks the region (default
//! us-east-1), LANDSLIDE_PATH the db prefix (default "landslidefs/churn").
//!
//! `follow` polls the db manifest every 500ms by default (LANDSLIDE_POLL_MS) —
//! that's what bounds sync lag together with `interval-ms`.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use landslide_fuse::Volume;

fn bucket() -> Arc<dyn ObjectStore> {
    if let Ok(bucket_name) = std::env::var("LANDSLIDE_BUCKET") {
        let builder = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket_name)
            .with_region(
                std::env::var("LANDSLIDE_REGION")
                    .or_else(|_| std::env::var("AWS_REGION"))
                    .unwrap_or_else(|_| "us-east-1".into()),
            );
        Arc::new(builder.build().expect("s3 build"))
    } else {
        let dir = std::env::var("LANDSLIDE_BUCKET_DIR").unwrap_or_else(|_| "/tmp/landslide-bucket".into());
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).expect("local fs"))
    }
}

async fn reader(bucket: Arc<dyn ObjectStore>) -> landslide::Result<Arc<landslide::EventStoreReader>> {
    let options = landslide::deps::slatedb::config::DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(
            std::env::var("LANDSLIDE_POLL_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(500),
        ),
        ..Default::default()
    };
    Ok(Arc::new(
        landslide::EventStoreReader::open(landslide::ReaderConfig {
            path: std::env::var("LANDSLIDE_PATH").unwrap_or_else(|_| "landslidefs/churn".into()),
            object_store: bucket,
            options: Some(options),
        })
        .await?,
    ))
}

async fn store(bucket: Arc<dyn ObjectStore>) -> landslide::Result<Arc<landslide::EventStore>> {
    Ok(Arc::new(
        landslide::EventStore::open(landslide::Config {
            path: std::env::var("LANDSLIDE_PATH").unwrap_or_else(|_| "landslidefs/churn".into()),
            object_store: bucket,
            settings: None,
        })
        .await?,
    ))
}

#[tokio::main]
async fn main() -> landslide::Result<()> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    let bucket = bucket();

    match cmd.as_str() {
        #[cfg(feature = "fuse")]
        "mount" => {
            let vol = args.next().expect("usage: landslidefs mount <vol> <mountpoint>");
            let mp = args.next().expect("usage: landslidefs mount <vol> <mountpoint>");
            eprintln!("mounting {vol} at {mp} (fence taken; ctrl-c to stop)");
            landslide_fuse::fs::mount(store(bucket.clone()).await?, bucket, &vol, &mp).await
        }
        #[cfg(feature = "fuse")]
        "follow" => {
            let vol = args.next().expect("usage: landslidefs follow <vol> <mountpoint> [interval-ms]");
            let mp = args.next().expect("usage: landslidefs follow <vol> <mountpoint> [interval-ms]");
            let interval = args.next().and_then(|s| s.parse().ok()).unwrap_or(2000);
            eprintln!("read-only replica of {vol} at {mp} (syncs every {interval}ms; ctrl-c to stop)");
            landslide_fuse::fs::mount_replica(
                reader(bucket.clone()).await?,
                bucket,
                &vol,
                &mp,
                Duration::from_millis(interval),
            )
            .await
        }
        "mirror" => {
            let sub = args.next().unwrap_or_else(|| "follow".into());
            let vol = args.next().expect("usage: landslidefs mirror <once|follow> <vol> <dir> [interval-ms]");
            let dir = args.next().expect("usage: landslidefs mirror <once|follow> <vol> <dir> [interval-ms]");
            match sub.as_str() {
                "once" => {
                    let v = landslide_fuse::mirror::materialize_once(reader(bucket.clone()).await?, bucket, &vol, &dir).await?;
                    println!("materialized {vol} into {dir} @ v{v}");
                    Ok(())
                }
                "follow" => {
                    let interval = args.next().and_then(|s| s.parse().ok()).unwrap_or(2000);
                    let mut mirror =
                        landslide_fuse::mirror::Mirror::open(reader(bucket.clone()).await?, bucket, &vol, &dir).await?;
                    eprintln!("mirroring {vol} into {dir} every {interval}ms (ctrl-c to stop)");
                    mirror.run(Duration::from_millis(interval)).await
                }
                _ => {
                    eprintln!("usage: landslidefs mirror <once|follow> <vol> <dir> [interval-ms]");
                    std::process::exit(1);
                }
            }
        }
        "checkpoint" => {
            let vol = args.next().expect("usage: landslidefs checkpoint <vol>");
            let mut v = Volume::mount(store(bucket.clone()).await?, bucket, &vol).await?;
            let record = v.checkpoint().await?;
            println!("checkpointed {vol} through v{}", record.through_version);
            Ok(())
        }
        "verify" => {
            let vol = args.next().expect("usage: landslidefs verify <vol>");
            let v = Volume::mount(store(bucket.clone()).await?, bucket, &vol).await?;
            let (files, bytes) = v
                .image
                .nodes
                .iter()
                .map(|(p, n)| match n {
                    landslide_fuse::Node::File { content, .. } => (p, content.len()),
                    _ => (p, 0),
                })
                .fold((0usize, 0u64), |(f, b), (_, s)| (f + (s > 0) as usize, b + s));
            println!("volume {vol}: {} nodes, {files} files, {bytes} content bytes", v.image.nodes.len());
            println!("tail version: {:?}, block cache after mount: {}", v.tail().await?, v.block_cache_len());
            Ok(())
        }
        _ => {
            eprintln!("usage: landslidefs <mount|follow|mirror|checkpoint|verify> <vol> [mountpoint|dir] [interval-ms]");
            std::process::exit(1);
        }
    }
}
