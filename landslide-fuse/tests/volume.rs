use std::sync::Arc;

use object_store::memory::InMemory;
use landslide::{Config, Error, EventStore};
use landslide_fuse::{Content, Data, Delta, Node, Volume};

async fn harness() -> (Arc<EventStore>, Arc<InMemory>) {
    let bucket = Arc::new(InMemory::new());
    let store = Arc::new(
        EventStore::open(Config { path: "fs".into(), object_store: bucket.clone(), settings: None })
            .await
            .unwrap(),
    );
    (store, bucket)
}

async fn mount(store: &Arc<EventStore>, bucket: &Arc<InMemory>, vol: &str) -> Volume {
    Volume::mount(store.clone(), bucket.clone(), vol).await.unwrap()
}

#[tokio::test]
async fn mount_write_checkpoint_remount() {
    let (store, bucket) = harness().await;
    let mut vol = mount(&store, &bucket, "sandbox-1").await;
    vol.mutate(Delta::Mkdir { path: "/etc".into() });
    vol.mutate(Delta::Write { path: "/etc/config".into(), offset: 0, data: Data::Inline("key=val".into()) });
    vol.mutate(Delta::Write { path: "/main.rs".into(), offset: 0, data: Data::Inline("fn main() {}".into()) });
    vol.commit().await.unwrap();

    let record = vol.checkpoint().await.unwrap();
    assert_eq!(record.through_version, 2); // 1 commit of 3 deltas -> v0..v2
    drop(vol);

    let mut vol2 = mount(&store, &bucket, "sandbox-1").await;
    assert_eq!(vol2.read_file("/etc/config").await.unwrap().unwrap().as_ref(), b"key=val");
    assert_eq!(vol2.read_file("/main.rs").await.unwrap().unwrap().as_ref(), b"fn main() {}");
    assert!(matches!(vol2.image.nodes["/etc"], Node::Dir { .. }));
}

#[tokio::test]
async fn extent_writes_gaps_and_truncation() {
    let (store, bucket) = harness().await;
    let mut vol = mount(&store, &bucket, "v-ext").await;
    vol.write_file("/log", 0, "abc".into());
    vol.write_file("/log", 6, "xyz".into()); // gap
    vol.truncate("/log", 4).await.unwrap(); // shrink
    vol.truncate("/log", 10).await.unwrap(); // grow
    vol.commit().await.unwrap();
    vol.checkpoint().await.unwrap();
    drop(vol);

    let mut vol2 = mount(&store, &bucket, "v-ext").await;
    assert_eq!(vol2.read_file("/log").await.unwrap().unwrap().as_ref(), b"abc\0\0\0\0\0\0\0");
}

#[tokio::test]
async fn remove_and_rename_reconstruct() {
    let (store, bucket) = harness().await;
    let mut vol = mount(&store, &bucket, "v2").await;
    vol.mutate(Delta::Write { path: "/a/x".into(), offset: 0, data: Data::Inline("1".into()) });
    vol.mutate(Delta::Write { path: "/a/y".into(), offset: 0, data: Data::Inline("2".into()) });
    vol.mutate(Delta::Rename { from: "/a".into(), to: "/b".into() });
    vol.mutate(Delta::Remove { path: "/b/y".into() });
    vol.commit().await.unwrap();
    vol.checkpoint().await.unwrap();
    drop(vol);

    let mut vol2 = mount(&store, &bucket, "v2").await;
    assert_eq!(vol2.image.nodes.keys().collect::<Vec<_>>(), ["/b/x"]);
    assert_eq!(vol2.read_file("/b/x").await.unwrap().unwrap().as_ref(), b"1");
}

#[tokio::test]
async fn attrs_symlinks_xattrs_and_empty_dirs_survive() {
    let (store, bucket) = harness().await;
    let mut vol = mount(&store, &bucket, "v4").await;
    vol.mutate(Delta::Write { path: "/bin/tool".into(), offset: 0, data: Data::Inline("#!".into()) });
    vol.mutate(Delta::SetAttr { path: "/bin/tool".into(), mode: Some(0o755), uid: Some(1000), gid: None, mtime_ms: None });
    vol.mutate(Delta::SetXattr { path: "/bin/tool".into(), name: "user.tag".into(), value: Some("deploy".into()) });
    vol.mutate(Delta::Symlink { path: "/tool".into(), target: "/bin/tool".into() });
    vol.mutate(Delta::Mkdir { path: "/empty".into() });
    vol.commit().await.unwrap();
    vol.checkpoint().await.unwrap();
    drop(vol);

    let vol2 = mount(&store, &bucket, "v4").await;
    let Node::File { meta, .. } = &vol2.image.nodes["/bin/tool"] else { panic!() };
    assert_eq!((meta.mode, meta.uid), (0o755, 1000));
    assert_eq!(meta.xattrs["user.tag"].as_ref(), b"deploy");
    assert!(matches!(vol2.image.nodes["/tool"], Node::Symlink { .. }));
    assert!(matches!(vol2.image.nodes["/empty"], Node::Dir { .. }));
}

#[tokio::test]
async fn large_files_chunk_and_reconstruct_lazily() {
    let (store, bucket) = harness().await;
    let data = vec![7u8; 2_500_000];
    let mut vol = mount(&store, &bucket, "vbig").await;
    vol.write_file("/big.bin", 0, data.clone().into());
    vol.commit().await.unwrap();

    // 2.5 MiB > CHUNK: three content-addressed blocks, not inline bytes.
    let Node::File { content: Content::Blocks { blocks, size }, .. } = &vol.image.nodes["/big.bin"] else {
        panic!();
    };
    assert_eq!((blocks.len(), *size), (3, 2_500_000));
    vol.checkpoint().await.unwrap();
    drop(vol);

    // Remount: no content fetched yet; the image is block references only.
    let mut vol2 = mount(&store, &bucket, "vbig").await;
    assert_eq!(vol2.block_cache_len(), 0);
    let Node::File { content: Content::Blocks { .. }, .. } = &vol2.image.nodes["/big.bin"] else {
        panic!();
    };

    // Content arrives on demand, then is cached. Two identical chunks share
    // one content-addressed block (dedup), so 3 refs → 2 unique blocks.
    assert_eq!(vol2.read_file("/big.bin").await.unwrap().unwrap().as_ref(), &data[..]);
    assert_eq!(vol2.block_cache_len(), 2);
}

#[tokio::test]
async fn range_reads_fetch_only_overlapping_blocks() {
    let (store, bucket) = harness().await;
    // 3 distinct 1 MiB chunks (no dedup): [1]*MiB, [2]*MiB, [3]*(rest)
    let mut data = vec![0u8; 2_500_000];
    data[..1_048_576].fill(1);
    data[1_048_576..2_097_152].fill(2);
    data[2_097_152..].fill(3);
    let mut vol = mount(&store, &bucket, "vrange").await;
    vol.write_file("/big", 0, data.clone().into());
    vol.commit().await.unwrap();
    drop(vol);

    let mut vol2 = mount(&store, &bucket, "vrange").await;
    // A read entirely inside the last block fetches exactly one block.
    let r = vol2.read_file_range("/big", 2_200_000, 100).await.unwrap().unwrap();
    assert_eq!(r.as_ref(), &vec![3u8; 100][..]);
    assert_eq!(vol2.block_cache_len(), 1);
    // A range spanning blocks 1 and 2 fetches the other two — never more.
    let r = vol2.read_file_range("/big", 1_000_000, 200_000).await.unwrap().unwrap();
    assert_eq!(r.as_ref(), &data[1_000_000..1_200_000]);
    assert_eq!(vol2.block_cache_len(), 3);
    assert_eq!(r.len(), 200_000);
}

#[tokio::test]
async fn remount_fences_the_previous_writer() {
    let (store, bucket) = harness().await;
    let mut vol = mount(&store, &bucket, "v3").await;
    vol.mutate(Delta::Write { path: "/f".into(), offset: 0, data: Data::Inline("a".into()) });
    vol.commit().await.unwrap();

    // A second mount of the same volume (failover) takes the fence...
    let mut vol2 = mount(&store, &bucket, "v3").await;

    // ...and the old writer's commits are now rejected.
    vol.mutate(Delta::Write { path: "/f".into(), offset: 0, data: Data::Inline("stale".into()) });
    assert!(matches!(
        vol.commit().await.unwrap_err(),
        Error::FenceMismatch { .. }
    ));

    vol2.mutate(Delta::Write { path: "/f".into(), offset: 0, data: Data::Inline("b".into()) });
    vol2.commit().await.unwrap();
    assert_eq!(vol2.read_file("/f").await.unwrap().unwrap().as_ref(), b"b");
}
