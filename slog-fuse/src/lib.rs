//! slog-fuse: a filesystem whose state lives in a slog event stream.
//!
//! Model: one volume = one slog stream (`{vol}`). Every mutation (`commit`)
//! appends a [`Delta`] with a mount-unique fencing token, so a re-mount
//! anywhere fences the old writer instantly. Snapshots are manifests of
//! content segments uploaded to the bucket (see [`Volume::checkpoint`]);
//! mounting reads the latest manifest, its segments, and the delta backlog
//! since the snapshot.
//!
//! A mounted [`Volume`] is the volume's single writer. A
//! [`replica::Replica`] is a streamed read-only view that follows the
//! writer's stream. Replicas can be FUSE-mounted with
//! [`fs::mount_replica`] or mirrored into a real directory with
//! [`mirror::Mirror`] for use in sandboxes and containers.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::{CompactionRecord, EventStore, Version};

#[cfg(unix)]
pub mod mirror;
pub mod replica;
pub mod store;
#[cfg(feature = "fuse")]
pub mod fs;

/// File metadata persisted with every node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMeta {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_ms: i64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub xattrs: BTreeMap<String, Bytes>,
}

impl NodeMeta {
    pub fn file() -> Self {
        Self { mode: 0o644, uid: 0, gid: 0, mtime_ms: now_ms(), xattrs: Default::default() }
    }
    pub fn dir() -> Self {
        Self { mode: 0o755, uid: 0, gid: 0, mtime_ms: now_ms(), xattrs: Default::default() }
    }
    pub fn symlink() -> Self {
        Self { mode: 0o777, uid: 0, gid: 0, mtime_ms: now_ms(), xattrs: Default::default() }
    }
}

/// A filesystem node: file contents, directory marker, or symlink target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Node {
    File { content: Content, meta: NodeMeta },
    Dir { meta: NodeMeta },
    Symlink { target: String, meta: NodeMeta },
}

/// File bytes: inline (base64 string on the wire) for small writes, or a
/// list of content-addressed blocks in the bucket for anything past a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Content {
    Inline(#[serde(with = "b64")] Bytes),
    Blocks { blocks: Vec<BlockRef>, size: u64 },
}

/// Location of one content-addressed block object (`blocks/{hash}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub hash: String,
    pub len: u64,
}

impl Content {
    /// Total file size in bytes.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        match self {
            Content::Inline(b) => b.len() as u64,
            Content::Blocks { size, .. } => *size,
        }
    }
}

/// File data carried by a write delta: bytes inline (base64 string on the
/// wire), or block references after the commit path chunked them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Data {
    Inline(#[serde(with = "b64")] Bytes),
    Blocks(Vec<BlockRef>),
}

/// Chunk size for block storage.
pub const CHUNK: usize = 1024 * 1024;

/// One filesystem mutation, committed as an event on the volume stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Delta {
    /// Extent write: `Inline` splices into the file (zero-fill gap, creates
    /// if missing); `Blocks` replaces the file's entire content (the commit
    /// path upgrades large files to these).
    Write {
        path: String,
        offset: u64,
        data: Data,
    },
    Mkdir { path: String },
    /// Removes the file/symlink or (empty) dir at path.
    Remove { path: String },
    Rename { from: String, to: String },
    Symlink { path: String, target: String },
    /// Sets the given metadata fields; unset fields keep their values.
    SetAttr {
        path: String,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        mtime_ms: Option<i64>,
    },
    /// Sets (`Some`) or removes (`None`) an xattr.
    SetXattr {
        path: String,
        name: String,
        #[serde(with = "b64_opt")]
        value: Option<Bytes>,
    },
}

/// A sealed set of changes, uploaded as one object: path → node or
/// tombstone. Segments merge newest-wins per path.
#[derive(Debug, Serialize, Deserialize)]
pub struct Segment {
    pub entries: BTreeMap<String, Option<Node>>,
}

/// Filesystem image manifest: the ordered set of segments that compose the
/// current state. Stored as the state of a slog snapshot record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub segments: Vec<String>,
}

/// In-memory image of the volume.
#[derive(Debug, Default, Clone)]
pub struct FsImage {
    /// path → node. Directories are explicit nodes (empty dirs survive
    /// remounts).
    pub nodes: BTreeMap<String, Node>,
}

impl FsImage {
    pub fn apply(&mut self, d: &Delta) {
        match d {
            Delta::Write { path, offset, data } => {
                let node = match data {
                    // Full replacement with block-stored content.
                    Data::Blocks(blocks) => {
                        let meta = self
                            .meta_of(path)
                            .cloned()
                            .unwrap_or_else(NodeMeta::file);
                        let content = Content::Blocks {
                            blocks: blocks.clone(),
                            size: blocks.iter().map(|b| b.len).sum(),
                        };
                        Node::File { content, meta }
                    }
                    // Extent splice (inline content only; writers never emit
                    // inline splices into block-backed files).
                    Data::Inline(data) => {
                        let mut node = match self.nodes.remove(path) {
                            Some(Node::File { content: Content::Inline(b), meta }) => {
                                Node::File { content: Content::Inline(b), meta }
                            }
                            other => {
                                if other.is_some() {
                                    self.nodes.remove(path);
                                }
                                Node::File {
                                    content: Content::Inline(Bytes::new()),
                                    meta: NodeMeta::file(),
                                }
                            }
                        };
                        if let Node::File { content: Content::Inline(content), .. } = &mut node {
                            let mut c = content.to_vec();
                            let offset = *offset as usize;
                            if c.len() < offset + data.len() {
                                c.resize(offset + data.len(), 0);
                            }
                            c[offset..offset + data.len()].copy_from_slice(data);
                            *content = c.into();
                        }
                        node
                    }
                };
                self.nodes.insert(path.clone(), node);
            }
            Delta::Mkdir { path } => {
                self.nodes
                    .entry(path.clone())
                    .or_insert_with(|| Node::Dir { meta: NodeMeta::dir() });
            }
            Delta::Remove { path } => {
                self.nodes.remove(path);
                let prefix = format!("{path}/");
                self.nodes.retain(|p, _| !p.starts_with(&prefix));
            }
            Delta::Rename { from, to } => {
                if let Some(node) = self.nodes.remove(from) {
                    self.nodes.insert(to.clone(), node);
                }
                let prefix = format!("{from}/");
                let moved: Vec<String> = self
                    .nodes
                    .range(prefix.clone()..)
                    .take_while(|(p, _)| p.starts_with(&prefix))
                    .map(|(p, _)| p.clone())
                    .collect();
                for p in moved {
                    let node = self.nodes.remove(&p).unwrap();
                    self.nodes.insert(format!("{to}/{}", &p[prefix.len()..]), node);
                }
            }
            Delta::Symlink { path, target } => {
                self.nodes.insert(
                    path.clone(),
                    Node::Symlink { target: target.clone(), meta: NodeMeta::symlink() },
                );
            }
            Delta::SetAttr { path, mode, uid, gid, mtime_ms } => {
                if let Some(meta) = self.meta_mut(path) {
                    if let Some(v) = mode {
                        meta.mode = *v;
                    }
                    if let Some(v) = uid {
                        meta.uid = *v;
                    }
                    if let Some(v) = gid {
                        meta.gid = *v;
                    }
                    if let Some(v) = mtime_ms {
                        meta.mtime_ms = *v;
                    }
                }
            }
            Delta::SetXattr { path, name, value } => {
                if let Some(meta) = self.meta_mut(path) {
                    match value {
                        Some(v) => {
                            meta.xattrs.insert(name.clone(), v.clone());
                        }
                        None => {
                            meta.xattrs.remove(name);
                        }
                    }
                }
            }
        }
    }

    fn meta_of(&self, path: &str) -> Option<&NodeMeta> {
        Some(match self.nodes.get(path)? {
            Node::File { meta, .. } | Node::Dir { meta } | Node::Symlink { meta, .. } => meta,
        })
    }

    fn meta_mut(&mut self, path: &str) -> Option<&mut NodeMeta> {
        Some(match self.nodes.get_mut(path)? {
            Node::File { meta, .. } | Node::Dir { meta } | Node::Symlink { meta, .. } => meta,
        })
    }

    pub fn merge_segment(&mut self, seg: Segment) {
        for (path, node) in seg.entries {
            match node {
                Some(node) => {
                    self.nodes.insert(path, node);
                }
                None => {
                    self.nodes.remove(&path);
                    let prefix = format!("{path}/");
                    self.nodes.retain(|p, _| !p.starts_with(&prefix));
                }
            }
        }
    }
}

/// A mounted volume: the write handle.
pub struct Volume {
    pub(crate) store: Arc<EventStore>,
    pub(crate) bucket: Arc<dyn object_store::ObjectStore>,
    pub(crate) vol: String,
    pub(crate) token: String,
    /// The current in-memory view; always post-`commit` consistent with the
    /// volume stream.
    pub image: FsImage,
    pub(crate) tail: Option<Version>,
    pub(crate) pending: Vec<Delta>,
    /// Content block cache (hash → bytes), filled lazily by `read_file`.
    pub(crate) blocks: std::collections::HashMap<String, Bytes>,
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

mod b64 {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(b: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(b))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        STANDARD
            .decode(String::deserialize(d)?)
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    }
}

mod b64_opt {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(b: &Option<Bytes>, s: S) -> Result<S::Ok, S::Error> {
        b.as_ref().map(|b| STANDARD.encode(b)).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Bytes>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|s| STANDARD.decode(s).map(Bytes::from))
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

pub(crate) fn record(vol: &str, through_version: Version) -> CompactionRecord {
    CompactionRecord {
        stream: vol.into(),
        through_version,
        events_compacted: 0,
        job_id: None,
        ts_ms: now_ms(),
    }
}
