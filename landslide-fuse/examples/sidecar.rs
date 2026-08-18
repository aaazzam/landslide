//! Read-only replica sidecar WITHOUT FUSE: mirrors a volume into a real
//! directory and keeps it synced. Meant to run next to your app in any
//! container (gVisor sandboxes included): fill `/rootfs`, then run the app.
//!
//!   cargo run -p landslide-fuse --example sidecar -- <cmd> <vol> <dir> [interval-ms]
//!
//! Commands:
//!   once <vol> <dir>                 materialize current state and exit
//!   follow <vol> <dir> [interval]    keep dir synced (default 2000 ms)
//!
//! Object store: real S3 when LANDSLIDE_BUCKET is set (creds via AWS_PROFILE /
//! AWS_* env / ~/.aws), else a local dir at LANDSLIDE_BUCKET_DIR (default
//! /tmp/landslide-bucket). LANDSLIDE_REGION/AWS_REGION picks the region (default
//! us-east-1), LANDSLIDE_PATH the db prefix (default "landslidefs/churn").

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use landslide_fuse::mirror::Mirror;

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

#[tokio::main]
async fn main() -> landslide::Result<()> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    let vol = args.next().unwrap_or_else(|| "churn-demo".into());
    let dir = args.next().unwrap_or_else(|| "/tmp/landslide-rootfs".into());
    let interval = args.next().and_then(|s| s.parse().ok()).unwrap_or(2000);

    let bucket = bucket();
    // A follower polls the db manifest eagerly (the stock reader default is
    // 10s); this is what bounds sync lag together with `interval`.
    let options = landslide::deps::slatedb::config::DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(500),
        ..Default::default()
    };
    let reader = Arc::new(
        landslide::EventStoreReader::open(landslide::ReaderConfig {
            path: std::env::var("LANDSLIDE_PATH").unwrap_or_else(|_| "landslidefs/churn".into()),
            object_store: bucket.clone(),
            options: Some(options),
        })
        .await?,
    );

    match cmd.as_str() {
        "once" => {
            let v = landslide_fuse::mirror::materialize_once(reader, bucket, &vol, &dir).await?;
            println!("materialized {vol} into {dir} @ v{v}");
            Ok(())
        }
        "follow" => {
            let mut mirror = Mirror::open(reader, bucket, &vol, &dir).await?;
            println!("mirroring {vol} into {dir} every {interval}ms (ctrl-c to stop)");
            mirror.run(Duration::from_millis(interval)).await
        }
        _ => {
            eprintln!("usage: sidecar <once|follow> <vol> <dir> [interval-ms]");
            std::process::exit(1);
        }
    }
}
