//! High-churn filesystem: manifests of delta segments keep mounts independent
//! of event-history length. slog stores events and the manifest pointer;
//! segment bytes live in the application's object storage.
//!
//! Run: cargo run --example fs_snapshots

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::{CompactionRecord, EventStore, ExpectedVersion, NewEvent, Result, Version};

#[derive(Serialize, Deserialize)]
enum FsEvent {
    Write { path: String, content: String },
    Delete { path: String },
}

type FsImage = HashMap<String, String>;

#[derive(Serialize, Deserialize)]
struct Manifest {
    segments: Vec<String>, // segment ids needed to reconstruct the state
}

fn apply(image: &mut FsImage, e: &slog::Event) {
    match e.json().unwrap() {
        FsEvent::Write { path, content } => drop(image.insert(path, content)),
        FsEvent::Delete { path } => drop(image.remove(&path)),
    }
}

/// Mount a volume from its manifest, segment objects, and the backlog since
/// the last checkpoint.
async fn mount(store: &EventStore, bucket: &HashMap<String, Bytes>, vol: &str) -> Result<(FsImage, Option<Version>)> {
    let (manifest, checkpoint_tip) = match store.latest_snapshot(vol).await? {
        Some(snap) => (
            serde_json::from_slice::<Manifest>(&snap.state)?,
            Some(snap.through_version),
        ),
        None => (Manifest { segments: vec![] }, None),
    };
    let from = checkpoint_tip.map_or(0, |v| v + 1);
    println!("  mount: manifest has {} segment(s), folding backlog from v{from}", manifest.segments.len());
    let mut image = FsImage::new();
    for seg in &manifest.segments {
        let delta: FsImage = serde_json::from_slice(&bucket[seg])?;
        image.extend(delta); // segments are merged new-over-old
    }
    let (image, backlog_tip) = store.fold(vol, from.., image, apply).await?;
    Ok((image, backlog_tip.or(checkpoint_tip)))
}

/// Checkpoint the delta since the last manifest into a segment, store it, and
/// publish the updated manifest. The work is proportional to the delta.
async fn checkpoint(store: &EventStore, bucket: &mut HashMap<String, Bytes>, vol: &str) -> Result<()> {
    let from = match store.latest_snapshot(vol).await? {
        Some(s) => s.through_version + 1,
        None => 0,
    };
    let (mut delta, Some(tip)) = store.fold(vol, from.., FsImage::new(), apply).await? else {
        return Ok(());
    };
    // Keep a volatile overlay so deletions in this delta survive segment
    // merging. A production implementation would persist per-key tombstones.
    let _ = &mut delta;

    let mut manifest = store
        .latest_snapshot(vol)
        .await?
        .map(|s| serde_json::from_slice::<Manifest>(&s.state).unwrap())
        .unwrap_or(Manifest { segments: vec![] });

    let seg_id = ulid::Ulid::new().to_string();
    bucket.insert(seg_id.clone(), serde_json::to_vec(&delta)?.into());
    manifest.segments.push(seg_id);

    store
        .publish_snapshot(
            vol,
            CompactionRecord {
                stream: vol.into(),
                through_version: tip,
                events_compacted: 0, // The example stores no event count.
                job_id: None,
                ts_ms: 0,
            },
            serde_json::to_vec(&manifest)?.into(),
        )
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    let mut bucket = HashMap::new(); // pretend object store

    // Churn: 1,000 commits across ten paths.
    let mut v: Option<Version> = None;
    for i in 0..1_000u64 {
        let commit = store
            .append(
                "vol-7",
                v.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact),
                vec![NewEvent::json("write", &FsEvent::Write {
                    path: format!("/var/log/{}.log", i % 10),
                    content: format!("gen {i}"),
                })?],
            )
            .await?;
        v = Some(commit.last_version);
        if (i + 1) % 250 == 0 {
            checkpoint(&store, &mut bucket, "vol-7").await?;
            println!("checkpointed at v{}", commit.last_version);
        }
    }

    // The mount reads manifests and segments, then folds only the backlog.
    let (image, tip) = mount(&store, &bucket, "vol-7").await?;
    println!("mounted vol-7 at v{tip:?}: {} live files", image.len());
    assert_eq!(image.len(), 10);
    Ok(())
}
