//! Volume lifecycle: mount (reconstruct), mutate, commit, checkpoint.

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStoreExt;
use slog::{CompactionRecord, Error, EventStore, ExpectedVersion, NewEvent, Result};

use crate::{record, BlockRef, Content, Data, Delta, FsImage, Manifest, Node, Segment, Volume, CHUNK};
use sha2::Digest;

impl Volume {
    /// Mounts `vol`: takes the fencing token, then reconstructs state from the
    /// latest manifest + its segment objects + the delta backlog. Any prior
    /// mounter is fenced out of future commits.
    pub async fn mount(
        store: Arc<EventStore>,
        bucket: Arc<dyn object_store::ObjectStore>,
        vol: &str,
    ) -> Result<Self> {
        let token = ulid::Ulid::new().to_string();
        store.fence(vol, Some(&token)).await?;
        let (image, from) = match store.latest_snapshot(vol).await? {
            Some(snap) => (load_segments(&bucket, &snap.state).await?, snap.through_version + 1),
            None => (FsImage::default(), 0),
        };
        let (image, tail) = store
            .fold(vol, from.., image, |image, event| {
                if let Ok(delta) = event.json::<Delta>() {
                    image.apply(&delta);
                }
            })
            .await?;
        Ok(Self {
            store,
            bucket,
            vol: vol.into(),
            token,
            image,
            tail,
            pending: Vec::new(),
            blocks: Default::default(),
        })
    }

    /// Buffers a mutation; nothing is durable until [`commit`](Self::commit).
    pub fn mutate(&mut self, delta: Delta) {
        self.image.apply(&delta);
        self.pending.push(delta);
    }

    /// Convenience: extent write. Content backs up to blocks at commit if
    /// the resulting file is large.
    pub fn write_file(&mut self, path: &str, offset: u64, data: Bytes) {
        self.mutate(Delta::Write { path: path.into(), offset, data: Data::Inline(data) });
    }

    /// Materializes a size change as Remove+Write so a shrink drops the old
    /// tail instead of splicing the new content into the old file.
    pub async fn truncate(&mut self, path: &str, size: u64) -> Result<()> {
        let content = self.read_file(path).await?.unwrap_or_default();
        let mut c = content.to_vec();
        c.resize(size as usize, 0);
        self.mutate(Delta::Remove { path: path.into() });
        self.mutate(Delta::Write { path: path.into(), offset: 0, data: Data::Inline(c.into()) });
        Ok(())
    }

    /// Materializes a file from inline bytes or lazily fetched blocks, cached
    /// by block hash. Mounts fetch file content through this method.
    pub async fn read_file(&mut self, path: &str) -> Result<Option<Bytes>> {
        read_file(&self.image, &self.bucket, &mut self.blocks, path).await
    }

    /// Streaming read: fetches only the blocks overlapping
    /// `[offset, offset+len)`. The kernel-page-cache unit is one block.
    pub async fn read_file_range(
        &mut self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Bytes>> {
        read_file_range(&self.image, &self.bucket, &mut self.blocks, path, offset, len).await
    }

    /// Commits buffered mutations as one atomic batch. Large resulting files
    /// have their pending extents collapsed into a full-content block commit
    /// (content-addressed chunks uploaded to the bucket first — data before
    /// pointer).
    pub async fn commit(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Chunk any file whose new content exceeds CHUNK.
        let mut uploads = Vec::new();
        let mut upgraded = Vec::new();
        let mut chunked_paths = std::collections::HashSet::new();
        for delta in &self.pending {
            let Delta::Write { path, .. } = delta else {
                upgraded.push(delta.clone());
                continue;
            };
            if chunked_paths.contains(path) {
                continue; // the full replacement covers this extent too
            }
            let Some(Node::File { content: Content::Inline(content), .. }) =
                self.image.nodes.get(path)
            else {
                upgraded.push(delta.clone());
                continue;
            };
            if content.len() <= CHUNK {
                upgraded.push(delta.clone());
                continue;
            }
            chunked_paths.insert(path.clone());
            let mut blocks = Vec::new();
            for chunk in content.chunks(CHUNK) {
                let hash = format!("{:x}", sha2::Sha256::digest(chunk));
                let block = BlockRef { hash: hash.clone(), len: chunk.len() as u64 };
                if self.bucket.get(&format!("blocks/{hash}").into()).await.is_err() {
                    uploads.push((hash, Bytes::copy_from_slice(chunk)));
                }
                blocks.push(block);
            }
            upgraded.push(Delta::Write { path: path.clone(), offset: 0, data: Data::Blocks(blocks) });
        }
        for (hash, bytes) in uploads {
            self.bucket.put(&format!("blocks/{hash}").into(), bytes.into()).await.map_err(io)?;
        }
        // Keep the in-memory image consistent with the block-backed event.
        for delta in &upgraded {
            if matches!(delta, Delta::Write { data: Data::Blocks(_), .. }) {
                self.image.apply(delta);
            }
        }

        let events = upgraded
            .into_iter()
            .map(|d| NewEvent::json("delta", &d))
            .collect::<Result<Vec<_>>>()?;
        let info = self
            .store
            .append_with_token(&self.vol, &self.token, expected(self.tail), events)
            .await?;
        self.tail = Some(info.last_version);
        self.pending.clear();
        Ok(())
    }

    /// Folds changes since the last manifest into a new segment object and
    /// publishes the new manifest atomically. The fold costs O(changes).
    pub async fn checkpoint(&mut self) -> Result<CompactionRecord> {
        self.commit().await?;
        let through = self.tail.ok_or_else(|| Error::InvalidInput("nothing to checkpoint".into()))?;
        let (base, from) = match self.store.latest_snapshot(&self.vol).await? {
            Some(snap) => (
                load_segments(&self.bucket, &snap.state).await?,
                snap.through_version + 1,
            ),
            None => (FsImage::default(), 0),
        };

        // Fold the delta range into tombstone-aware entries, maintaining a
        // running image so renames/removals see state as-of each delta.
        let (delta, _) = self
            .store
            .fold(&self.vol, from.., DeltaImage { entries: Default::default(), live: base }, |delta, e| {
                if let Ok(d) = e.json::<Delta>() {
                    delta.apply(&d);
                }
            })
            .await?;

        let seg = Segment { entries: delta.entries };
        let seg_path =
            object_store::path::Path::from(format!("segments/{}/{}", self.vol, ulid::Ulid::new()));
        self.bucket
            .put(&seg_path, Bytes::from(serde_json::to_vec(&seg)?).into())
            .await
            .map_err(io)?;

        let mut manifest = self
            .store
            .latest_snapshot(&self.vol)
            .await?
            .map(|s| serde_json::from_slice::<Manifest>(&s.state))
            .transpose()?
            .unwrap_or_default();
        manifest.segments.push(seg_path.to_string());
        let record = record(&self.vol, through);
        self.store
            .publish_snapshot(&self.vol, record.clone(), Bytes::from(serde_json::to_vec(&manifest)?))
            .await?;
        Ok(record)
    }

    /// Latest committed version (the stream tail).
    pub async fn tail(&self) -> Result<Option<slog::Version>> {
        self.store.stream_version(&self.vol).await
    }

    /// Blocks fetched into the cache so far (diagnostics: proves lazy reads).
    pub fn block_cache_len(&self) -> usize {
        self.blocks.len()
    }
}

/// A checkpoint fold in progress: newest-wins whole-node entries with
/// tombstones, plus `live` = running image (segment base + deltas so far) so
/// renames and removals see each path's state as of their delta, not as of
/// the end of the window.
struct DeltaImage {
    entries: std::collections::BTreeMap<String, Option<Node>>,
    live: FsImage,
}

impl DeltaImage {
    fn descendants(&self, prefix: &str) -> Vec<String> {
        self.live
            .nodes
            .keys()
            .filter(|p| p.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn apply(&mut self, d: &Delta) {
        match d {
            Delta::Remove { path } => {
                self.entries.insert(path.clone(), None);
                for p in self.descendants(&format!("{path}/")) {
                    self.entries.insert(p, None);
                }
                self.live.apply(d);
            }
            Delta::Rename { from, to } => {
                self.entries.insert(from.clone(), None);
                if let Some(node) = self.live.nodes.get(from) {
                    self.entries.insert(to.clone(), Some(node.clone()));
                }
                for p in self.descendants(&format!("{from}/")) {
                    let node = self.live.nodes[&p].clone();
                    let newp = format!("{to}/{}", &p[from.len() + 1..]);
                    self.entries.insert(p, None);
                    self.entries.insert(newp, Some(node));
                }
                self.live.apply(d);
            }
            _ => {
                self.live.apply(d);
                let path = match d {
                    Delta::Write { path, .. }
                    | Delta::Mkdir { path }
                    | Delta::SetAttr { path, .. }
                    | Delta::SetXattr { path, .. }
                    | Delta::Symlink { path, .. } => path.clone(),
                    _ => unreachable!(),
                };
                if let Some(node) = self.live.nodes.get(&path) {
                    self.entries.insert(path, Some(node.clone()));
                }
            }
        }
    }
}

/// Shared lazy content read path (behind both [`Volume`] and
/// [`crate::replica::Replica`]): inline bytes direct, or only the blocks
/// overlapping the read, cached per content-addressed hash.
pub(crate) async fn read_file(
    image: &FsImage,
    bucket: &Arc<dyn object_store::ObjectStore>,
    blocks: &mut std::collections::HashMap<String, Bytes>,
    path: &str,
) -> Result<Option<Bytes>> {
    let Some(Node::File { content, .. }) = image.nodes.get(path) else {
        return Ok(None);
    };
    let size = content.len();
    read_content(bucket, blocks, content.clone(), 0, size).await.map(Some)
}

pub(crate) async fn read_file_range(
    image: &FsImage,
    bucket: &Arc<dyn object_store::ObjectStore>,
    blocks: &mut std::collections::HashMap<String, Bytes>,
    path: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Bytes>> {
    let Some(Node::File { content, .. }) = image.nodes.get(path) else {
        return Ok(None);
    };
    read_content(bucket, blocks, content.clone(), offset, len).await.map(Some)
}

async fn read_content(
    bucket: &Arc<dyn object_store::ObjectStore>,
    blocks: &mut std::collections::HashMap<String, Bytes>,
    content: Content,
    offset: u64,
    len: u64,
) -> Result<Bytes> {
    match content {
        Content::Inline(b) => {
            let s = (offset as usize).min(b.len());
            let e = (s + len as usize).min(b.len());
            Ok(b.slice(s..e))
        }
        Content::Blocks { blocks: refs, .. } => {
            let end = offset + len;
            let mut out = Vec::new();
            let mut pos = 0u64;
            for block in refs {
                let (bstart, bend) = (pos, pos + block.len);
                pos = bend;
                if bend <= offset {
                    continue;
                }
                if bstart >= end {
                    break;
                }
                let b = fetch_block(bucket, blocks, &block.hash).await?;
                let (lo, hi) = (
                    offset.saturating_sub(bstart) as usize,
                    (end.min(bend) - bstart) as usize,
                );
                out.extend_from_slice(&b[lo.min(b.len())..hi.min(b.len())]);
            }
            Ok(out.into())
        }
    }
}

async fn fetch_block(
    bucket: &Arc<dyn object_store::ObjectStore>,
    blocks: &mut std::collections::HashMap<String, Bytes>,
    hash: &str,
) -> Result<Bytes> {
    if let Some(b) = blocks.get(hash) {
        return Ok(b.clone());
    }
    let b = bucket
        .get(&format!("blocks/{hash}").into())
        .await
        .map_err(io)?
        .bytes()
        .await
        .map_err(io)?;
    blocks.insert(hash.to_string(), b.clone());
    Ok(b)
}

/// Loads and merges all segments of a manifest (JSON bytes) from the bucket.
pub(crate) async fn load_segments(
    bucket: &Arc<dyn object_store::ObjectStore>,
    manifest_bytes: &[u8],
) -> Result<FsImage> {
    let manifest: Manifest = serde_json::from_slice(manifest_bytes)?;
    let mut image = FsImage::default();
    for seg_path in &manifest.segments {
        let seg: Segment = serde_json::from_slice(
            &bucket
                .get(&seg_path.as_str().into())
                .await
                .map_err(io)?
                .bytes()
                .await
                .map_err(io)?,
        )?;
        image.merge_segment(seg);
    }
    Ok(image)
}

fn expected(tail: Option<slog::Version>) -> ExpectedVersion {
    tail.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact)
}

fn io(e: impl std::fmt::Display) -> Error {
    Error::InvalidInput(format!("bucket io: {e}"))
}
