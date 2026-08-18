//! Directory mirroring materializes a [`Replica`] into a local directory and
//! keeps it synced. It works in container runtimes without FUSE or root
//! privileges and is suitable for booting a synchronized root filesystem.
//!
//! Each [`Mirror::sync_once`] fetches new deltas, compares the image with a
//! state file in the target directory, and updates changed paths. Files are
//! installed by rename, so concurrent readers see a complete old or new
//! file. uid/gid and xattrs are not applied; symlink targets are written
//! verbatim. Unix-only.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use landslide::{Error, EventStoreReader, Result, Version};

use crate::replica::Replica;
use crate::Node;

/// Bookkeeping file for mirror state, kept inside the target dir.
const STATE_FILE: &str = ".landslide-mirror.json";

/// Fingerprint of one materialized path: what was last written to disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StateEntry {
    size: u64,
    mtime_ms: i64,
    mode: u32,
    /// Symlink target, when the node is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target: Option<String>,
}

impl StateEntry {
    fn of(node: &Node) -> Self {
        match node {
            Node::File { content, meta } => Self {
                size: content.len(),
                mtime_ms: meta.mtime_ms,
                mode: meta.mode,
                target: None,
            },
            Node::Symlink { target, meta } => Self {
                size: target.len() as u64,
                mtime_ms: meta.mtime_ms,
                mode: meta.mode,
                target: Some(target.clone()),
            },
            Node::Dir { meta } => {
                Self { size: 0, mtime_ms: meta.mtime_ms, mode: meta.mode, target: None }
            }
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    /// volume-abs path ("/etc/hostname") → what was last written.
    entries: HashMap<String, StateEntry>,
}

/// A synced local copy of a volume: the mirror dir looks exactly like the
/// replica's image, apart from the bookkeeping file [`STATE_FILE`]. Passes
/// are restart-safe: the state file is the only memory between runs, and an
/// interrupted run converges on the next one.
pub struct Mirror {
    replica: Replica,
    dir: PathBuf,
    state: State,
}

impl Mirror {
    /// Opens a replica of `vol` and materializes it into `dir`, creating the
    /// directory when needed. An existing state file lets a restart update
    /// only changed files.
    pub async fn open(
        reader: Arc<EventStoreReader>,
        bucket: Arc<dyn ObjectStore>,
        vol: &str,
        dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(io)?;
        let state = match std::fs::read(dir.join(STATE_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(io(e)),
        };
        let mut mirror = Self { replica: Replica::open(reader, bucket, vol).await?, dir, state };
        mirror.materialize().await?;
        Ok(mirror)
    }

    /// Fetches new deltas and applies the changes to disk. Returns the stream
    /// version now reflected on disk.
    pub async fn sync_once(&mut self) -> Result<Version> {
        self.replica.sync().await?;
        self.materialize().await?;
        Ok(self.replica.cursor().unwrap_or(0))
    }

    /// Syncs every `interval`, forever. Transient failures are logged and
    /// retried on the next pass from the same state.
    pub async fn run(&mut self, interval: Duration) -> Result<()> {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            if let Err(e) = self.sync_once().await {
                eprintln!("landslide-mirror: sync of {:?} failed: {e}; retrying next pass", self.dir);
            }
        }
    }

    /// The replica behind the mirror (image reflects the last pass).
    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    /// Maps a volume path ("/etc/hostname") into the mirror dir.
    fn local(&self, vol_path: &str) -> PathBuf {
        let mut p = self.dir.clone();
        for c in Path::new(vol_path).components() {
            if let Component::Normal(c) = c {
                p.push(c);
            }
        }
        p
    }

    async fn materialize(&mut self) -> Result<()> {
        // Copy the image out: file reads re-borrow the replica (block cache).
        let image = self.replica.image.clone();
        let mut dir_modes: Vec<(PathBuf, u32)> = Vec::new();

        // Add/update: rewrite only nodes whose fingerprint changed or whose
        // on-disk path vanished.
        for (path, node) in &image.nodes {
            let entry = StateEntry::of(node);
            let local = self.local(path);
            if self.state.entries.get(path) == Some(&entry) && local.symlink_metadata().is_ok() {
                if matches!(node, Node::Dir { .. }) {
                    dir_modes.push((local, entry.mode));
                }
                continue;
            }
            match self.write_node(path, node).await {
                Ok(()) => {
                    if matches!(node, Node::Dir { .. }) {
                        dir_modes.push((local, entry.mode));
                    }
                    self.state.entries.insert(path.clone(), entry);
                }
                Err(e) => eprintln!("landslide-mirror: skipping {path}: {e}"),
            }
        }

        // Deletions: previously-materialized paths that left the image,
        // deepest first so each dir empties before it is removed.
        let mut gone: Vec<String> =
            self.state.entries.keys().filter(|p| !image.nodes.contains_key(*p)).cloned().collect();
        gone.sort_by_key(|p| std::cmp::Reverse(p.len()));
        for path in gone {
            remove_any(&self.local(&path));
            self.state.entries.remove(&path);
        }

        // Dir modes last: an early-applied read-only mode must never block
        // a file write into that dir above.
        for (local, mode) in dir_modes {
            apply_mode(&local, mode);
        }

        let tmp = self.dir.join(format!("{STATE_FILE}.tmp"));
        std::fs::write(&tmp, serde_json::to_vec(&self.state)?).map_err(io)?;
        std::fs::rename(&tmp, self.dir.join(STATE_FILE)).map_err(io)?;
        Ok(())
    }

    /// Writes one node, replacing whatever unrelated entry was on disk
    /// (file ↔ dir ↔ symlink flips included).
    async fn write_node(&mut self, path: &str, node: &Node) -> Result<()> {
        let local = self.local(path);
        if let Some(parent) = local.parent() {
            std::fs::create_dir_all(parent).map_err(io)?;
        }
        match node {
            Node::Dir { .. } => {
                if !local.is_dir() {
                    remove_any(&local);
                    std::fs::create_dir_all(&local).map_err(io)?;
                }
            }
            Node::Symlink { target, .. } => {
                remove_any(&local);
                std::os::unix::fs::symlink(target, &local).map_err(io)?;
            }
            Node::File { meta, .. } => {
                if local.is_dir() {
                    remove_any(&local);
                }
                let bytes = self
                    .replica
                    .read_file(path)
                    .await?
                    .ok_or_else(|| Error::InvalidInput(format!("{path}: vanished mid-sync")))?;
                write_file_atomic(&local, &bytes).map_err(io)?;
                apply_mode(&local, meta.mode);
                let mtime =
                    SystemTime::UNIX_EPOCH + Duration::from_millis(meta.mtime_ms.max(0) as u64);
                if let Ok(f) = std::fs::File::options().write(true).open(&local) {
                    let _ = f.set_times(std::fs::FileTimes::new().set_modified(mtime));
                }
            }
        }
        Ok(())
    }
}

/// rm `-f`: unlink files and symlinks, remove dirs with their contents.
fn remove_any(local: &Path) {
    match local.symlink_metadata() {
        Ok(md) if md.is_dir() && !md.file_type().is_symlink() => {
            let _ = std::fs::remove_dir_all(local);
        }
        Ok(_) => {
            let _ = std::fs::remove_file(local);
        }
        Err(_) => {}
    }
}

/// Writes `bytes` to `path` via a sibling temp file + rename, so a reader
/// racing the mirror sees either the old file or the new one, never half.
fn write_file_atomic(path: &Path, bytes: &Bytes) -> std::io::Result<()> {
    let tmp = path.with_extension("landslide-mirror.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

fn apply_mode(local: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let Ok(md) = local.symlink_metadata() else { return };
    if md.file_type().is_symlink() {
        return; // chmod on symlink targets is surprising; skip
    }
    let _ = std::fs::set_permissions(local, std::fs::Permissions::from_mode(mode & 0o7777));
}

fn io(e: impl std::fmt::Display) -> Error {
    Error::InvalidInput(format!("mirror io: {e}"))
}

/// Convenience: materialize the current volume state once, no follow.
/// Useful in entrypoints: fill `/rootfs`, then `exec` the app.
pub async fn materialize_once(
    reader: Arc<EventStoreReader>,
    bucket: Arc<dyn ObjectStore>,
    vol: &str,
    dir: impl Into<PathBuf>,
) -> Result<Version> {
    let mut mirror = Mirror::open(reader, bucket, vol, dir).await?;
    mirror.sync_once().await
}
