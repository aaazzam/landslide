//! FUSE adapter: exposes a [`Volume`] (read-write) or a [`Replica`]
//! (read-only, live-synced) as a mountable POSIX filesystem.
//!
//! Writes are staged per open handle as an extent list and committed as one
//! atomic batch of [`Delta::Write`]s (plus an mtime bump) on flush/fsync/
//! release. Handles are keyed by inode, so a rename re-points a dirty handle
//! onto its current path (POSIX inode semantics). Metadata (mode/uid/gid/
//! mtime, xattrs) round-trips through [`NodeMeta`] deltas. Hardlinks are
//! `ENOSYS`: in this model a path *is* the identity of a node.
//!
//! Replica mounts ([`mount_replica`]) are kernel-marked `ro`: every mutation
//! fails `EROFS`, and the view converges as a background task syncs the
//! replica with the volume stream.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use fuser::{
    BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, LockOwner, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use landslide::{EventStore, EventStoreReader};

use crate::replica::Replica;
use crate::{Data, now_ms, Delta, FsImage, Node, NodeMeta, Volume};

/// Attrs are never cached by the kernel: every request re-reads the image.
const TTL: Duration = Duration::ZERO;

/// Inode tables. Inodes are assigned incrementally on first sight and never
/// recycled; renames re-key paths so the ino stays attached to the node.
struct State {
    path_of: HashMap<u64, String>,
    ino_of: HashMap<String, u64>,
    next_ino: u64,
}

impl State {
    fn new() -> Self {
        Self {
            path_of: HashMap::from([(1, "/".into())]),
            ino_of: HashMap::from([("/".into(), 1)]),
            next_ino: 2,
        }
    }

    fn ino(&mut self, path: &str) -> INodeNo {
        match self.ino_of.get(path) {
            Some(&i) => INodeNo(i),
            None => {
                let i = self.next_ino;
                self.next_ino += 1;
                self.ino_of.insert(path.into(), i);
                self.path_of.insert(i, path.into());
                INodeNo(i)
            }
        }
    }

    /// Re-keys the renamed subtree, keeping inodes stable.
    fn rename_subtree(&mut self, from: &str, to: &str) {
        let prefix = format!("{from}/");
        let remap = |p: &str| -> Option<String> {
            if p == from {
                Some(to.to_string())
            } else {
                p.strip_prefix(&prefix).map(|rest| format!("{to}/{rest}"))
            }
        };
        let moved: Vec<(String, u64)> = self
            .ino_of
            .iter()
            .filter(|(p, _)| remap(p).is_some())
            .map(|(p, &i)| (p.clone(), i))
            .collect();
        for (p, i) in moved {
            self.ino_of.remove(&p);
            let np = remap(&p).unwrap();
            self.ino_of.insert(np.clone(), i);
            self.path_of.insert(i, np);
        }
    }
}

/// One open file: inode (rename-stable), staged content snapshot for
/// read-your-writes, and the pending extent list to commit.
struct Handle {
    ino: u64,
    extents: Vec<(u64, Bytes)>,
}

/// What the mount serves: the fenced writable [`Volume`], or a streamed
/// read-only [`Replica`] (mutations fail `EROFS`).
enum Backend {
    Volume(Arc<tokio::sync::Mutex<Volume>>),
    Replica(Arc<tokio::sync::Mutex<Replica>>),
}

/// A landslide filesystem behind a FUSE mount. All mutations go through
/// `Volume::mutate` + `Volume::commit` on a dedicated single-thread runtime
/// driven via `block_on` from fuser's session thread (the volume's own mutex
/// serializes commits, so one worker is sufficient).
pub struct LandslideFs {
    backend: Backend,
    rt: tokio::runtime::Runtime,
    state: Mutex<State>,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
}

fn image_of<T>(backend: &Backend, rt: &tokio::runtime::Runtime, f: impl FnOnce(&FsImage) -> T) -> T {
    match backend {
        Backend::Volume(vol) => f(&rt.block_on(vol.lock()).image),
        Backend::Replica(replica) => f(&rt.block_on(replica.lock()).image),
    }
}

impl LandslideFs {
    /// Wraps an already-mounted volume; seeds inode tables from the image.
    pub fn new(vol: Arc<tokio::sync::Mutex<Volume>>) -> Self {
        Self::with_backend(Backend::Volume(vol))
    }

    /// Wraps an open replica (mounted `ro` — see [`mount_replica`]).
    fn replica(replica: Arc<tokio::sync::Mutex<Replica>>) -> Self {
        Self::with_backend(Backend::Replica(replica))
    }

    fn with_backend(backend: Backend) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fuse io runtime");
        // Inode tables start empty: every protocol path assigns numbers
        // lazily on first sight (lookup, readdir, ...).
        Self {
            backend,
            rt,
            state: Mutex::new(State::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        }
    }

    fn image<T>(&self, f: impl FnOnce(&FsImage) -> T) -> T {
        image_of(&self.backend, &self.rt, f)
    }

    /// Buffers `deltas` and commits them durably; `EIO` on commit failure,
    /// `EROFS` on a replica mount.
    fn commit(&self, deltas: Vec<Delta>) -> Result<(), Errno> {
        let Backend::Volume(vol) = &self.backend else { return Err(Errno::EROFS) };
        self.rt
            .block_on(async {
                let mut vol = vol.lock().await;
                for d in deltas {
                    vol.mutate(d);
                }
                vol.commit().await
            })
            .map_err(|_| Errno::EIO)
    }

    /// Materializes a whole file through the backend's lazy read path.
    fn read_file(&self, path: &str) -> Result<Option<Bytes>, Errno> {
        match &self.backend {
            Backend::Volume(vol) => self.rt.block_on(async { vol.lock().await.read_file(path).await }),
            Backend::Replica(r) => self.rt.block_on(async { r.lock().await.read_file(path).await }),
        }
        .map_err(|_| Errno::EIO)
    }

    /// Streaming read through the backend's lazy read path.
    fn read_range(&self, path: &str, offset: u64, len: u64) -> Result<Option<Bytes>, Errno> {
        match &self.backend {
            Backend::Volume(vol) => {
                self.rt.block_on(async { vol.lock().await.read_file_range(path, offset, len).await })
            }
            Backend::Replica(r) => {
                self.rt.block_on(async { r.lock().await.read_file_range(path, offset, len).await })
            }
        }
        .map_err(|_| Errno::EIO)
    }

    /// Size change; `EIO` on failure, `EROFS` on a replica mount.
    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        match &self.backend {
            Backend::Volume(vol) => self
                .rt
                .block_on(async { vol.lock().await.truncate(path, size).await })
                .map_err(|_| Errno::EIO),
            Backend::Replica(_) => Err(Errno::EROFS),
        }
    }

    fn new_handle(&self, ino: u64) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().unwrap().insert(fh, Handle { ino, extents: Vec::new() });
        fh
    }

    /// Commits a handle's staged extents to the inode's current path.
    fn flush_fh(&self, fh: u64) -> Result<(), Errno> {
        let (ino, extents) = {
            let mut hs = self.handles.lock().unwrap();
            match hs.get_mut(&fh) {
                Some(h) if !h.extents.is_empty() => (h.ino, std::mem::take(&mut h.extents)),
                _ => return Ok(()),
            }
        };
        // Resolved at flush time: a rename since open() re-points the write.
        let Some(path) = self.path_of(INodeNo(ino)) else { return Ok(()) };
        let mut deltas: Vec<Delta> = extents
            .into_iter()
            .map(|(offset, data)| Delta::Write { path: path.clone(), offset, data: Data::Inline(data) })
            .collect();
        deltas.push(Delta::SetAttr {
            path,
            mode: None,
            uid: None,
            gid: None,
            mtime_ms: Some(now_ms()),
        });
        self.commit(deltas)
    }

    /// Resolves an ino to its current path.
    fn path_of(&self, ino: INodeNo) -> Option<String> {
        self.state.lock().unwrap().path_of.get(&ino.0).cloned()
    }

    fn attr_of(&self, ino: INodeNo, path: &str) -> Option<FileAttr> {
        self.image(|img| attr_of(img, ino, path))
    }
}

fn join(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn parent_path(path: &str) -> String {
    match path.rfind('/') {
        Some(0) | None => "/".into(),
        Some(i) => path[..i].into(),
    }
}

fn meta_of(node: &Node) -> &NodeMeta {
    match node {
        Node::File { meta, .. } | Node::Dir { meta } | Node::Symlink { meta, .. } => meta,
    }
}

/// Immediate children of `dir` as (name, kind), from the image's nodes.
fn children(img: &FsImage, dir: &str) -> BTreeMap<String, FileType> {
    let prefix = if dir == "/" { "/".into() } else { format!("{dir}/") };
    let mut out = BTreeMap::new();
    for (p, node) in &img.nodes {
        let Some(rest) = p.strip_prefix(&prefix) else { continue };
        if rest.is_empty() || rest.contains('/') {
            continue;
        }
        let kind = match node {
            Node::File { .. } => FileType::RegularFile,
            Node::Dir { .. } => FileType::Directory,
            Node::Symlink { .. } => FileType::Symlink,
        };
        out.insert(rest.to_string(), kind);
    }
    out
}

/// 2 + immediate subdirs for directories; 1 otherwise.
fn nlink_of(img: &FsImage, path: &str, kind: FileType) -> u32 {
    if kind != FileType::Directory {
        return 1;
    }
    let subdirs = children(img, path).values().filter(|&&k| k == FileType::Directory).count();
    2 + subdirs as u32
}

/// Builds kernel attrs from a node (or synthesized root) plus its meta.
fn attr_of(img: &FsImage, ino: INodeNo, path: &str) -> Option<FileAttr> {
    let (kind, size, meta) = if path == "/" {
        (FileType::Directory, 0, &ROOT_META)
    } else {
        match img.nodes.get(path)? {
            Node::File { content, meta } => (FileType::RegularFile, content.len(), meta),
            Node::Dir { meta } => (FileType::Directory, 0, meta),
            Node::Symlink { target, meta } => (FileType::Symlink, target.len() as u64, meta),
        }
    };
    let mtime = ts(meta.mtime_ms);
    Some(FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind,
        perm: (meta.mode & 0o7777) as u16,
        nlink: nlink_of(img, path, kind),
        uid: meta.uid,
        gid: meta.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    })
}

/// Synthesized meta for `/` (not a real node).
static ROOT_META: NodeMeta = NodeMeta {
    mode: 0o755,
    uid: 0,
    gid: 0,
    mtime_ms: 0,
    xattrs: BTreeMap::new(),
};

fn ts(ms: i64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

fn ts_ms(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl Filesystem for LandslideFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(parent_path) = self.path_of(parent) else { return reply.error(Errno::ENOENT) };
        let is_dir = self.image(|img| parent_path == "/" || matches!(img.nodes.get(&parent_path), Some(Node::Dir { .. })));
        if !is_dir {
            return reply.error(Errno::ENOTDIR);
        }
        let path = join(&parent_path, name);
        if !self.image(|img| img.nodes.contains_key(&path)) {
            return reply.error(Errno::ENOENT);
        }
        let ino = self.state.lock().unwrap().ino(&path);
        reply.entry(&TTL, &self.attr_of(ino, &path).unwrap(), Generation(0));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        match self.attr_of(ino, &path) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        let is_file = self.image(|img| matches!(img.nodes.get(&path), Some(Node::File { .. })));
        if path != "/" && !is_file && self.image(|img| !img.nodes.contains_key(&path)) {
            return reply.error(Errno::ENOENT);
        }
        let mut deltas = Vec::new();
        if let Some(size) = size {
            if !is_file {
                // Truncating anything but a regular file.
                return reply.error(Errno::EINVAL);
            }
            if let Err(e) = self.truncate(&path, size) {
                return reply.error(e);
            }
            // Keep staged handles on this inode coherent: resize the buffer
            // and drop/clip extents at or beyond the new size.
            let mut hs = self.handles.lock().unwrap();
            for h in hs.values_mut().filter(|h| h.ino == ino.0) {
                                h.extents.retain_mut(|(off, d)| {
                    if *off >= size {
                        false
                    } else {
                        d.truncate((size - *off) as usize);
                        true
                    }
                });
            }
        }
        let mut mtime_ms = mtime.map(|m| match m {
            TimeOrNow::SpecificTime(t) => ts_ms(t),
            TimeOrNow::Now => now_ms(),
        });
        if size.is_some() && mtime_ms.is_none() {
            mtime_ms = Some(now_ms());
        }
        if mode.is_some() || uid.is_some() || gid.is_some() || mtime_ms.is_some() {
            deltas.push(Delta::SetAttr {
                path: path.clone(),
                mode: mode.map(|m| m & 0o7777),
                uid,
                gid,
                mtime_ms,
            });
        }
        if !deltas.is_empty() {
            if let Err(e) = self.commit(deltas) {
                return reply.error(e);
            }
        }
        match self.attr_of(ino, &path) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        match self.image(|img| img.nodes.get(&path).cloned()) {
            Some(Node::Symlink { target, .. }) => reply.data(target.as_bytes()),
            Some(_) => reply.error(Errno::EINVAL),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let Some(name) = link_name.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(target) = target.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(parent_path) = self.path_of(parent) else { return reply.error(Errno::ENOENT) };
        let is_dir = self.image(|img| parent_path == "/" || matches!(img.nodes.get(&parent_path), Some(Node::Dir { .. })));
        if !is_dir {
            return reply.error(Errno::ENOTDIR);
        }
        let path = join(&parent_path, name);
        if self.image(|img| img.nodes.contains_key(&path)) {
            return reply.error(Errno::EEXIST);
        }
        let err = self.commit(vec![
            Delta::Symlink { path: path.clone(), target: target.to_string() },
            Delta::SetAttr {
                path: path.clone(),
                mode: None,
                uid: Some(req.uid()),
                gid: Some(req.gid()),
                mtime_ms: None,
            },
        ]);
        if let Err(e) = err {
            return reply.error(e);
        }
        let ino = self.state.lock().unwrap().ino(&path);
        reply.entry(&TTL, &self.attr_of(ino, &path).unwrap(), Generation(0));
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        // Paths are the identity of a node; no shared-node aliasing.
        reply.error(Errno::ENOSYS)
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let Some(dir) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        let is_dir =
            self.image(|img| dir == "/" || matches!(img.nodes.get(&dir), Some(Node::Dir { .. })));
        if !is_dir {
            return reply.error(Errno::ENOTDIR);
        }
        let kids = self.image(|img| children(img, &dir));
        let mut st = self.state.lock().unwrap();
        let mut entries: Vec<(INodeNo, FileType, String)> = vec![
            (ino, FileType::Directory, ".".into()),
            (st.ino(&parent_path(&dir)), FileType::Directory, "..".into()),
        ];
        for (name, kind) in kids {
            let cino = st.ino(&join(&dir, &name));
            entries.push((cino, kind, name));
        }
        drop(st);
        for (i, (cino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if !reply.add(cino, (i + 1) as u64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(parent_path) = self.path_of(parent) else { return reply.error(Errno::ENOENT) };
        let is_dir = self.image(|img| parent_path == "/" || matches!(img.nodes.get(&parent_path), Some(Node::Dir { .. })));
        if !is_dir {
            return reply.error(Errno::ENOTDIR);
        }
        let path = join(&parent_path, name);
        if self.image(|img| img.nodes.contains_key(&path)) {
            return reply.error(Errno::EEXIST);
        }
        let err = self.commit(vec![
            Delta::Mkdir { path: path.clone() },
            Delta::SetAttr {
                path: path.clone(),
                mode: Some(mode & !umask & 0o7777),
                uid: Some(req.uid()),
                gid: Some(req.gid()),
                mtime_ms: None,
            },
        ]);
        if let Err(e) = err {
            return reply.error(e);
        }
        let ino = self.state.lock().unwrap().ino(&path);
        reply.entry(&TTL, &self.attr_of(ino, &path).unwrap(), Generation(0));
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(parent_path) = self.path_of(parent) else { return reply.error(Errno::ENOENT) };
        let is_dir = self.image(|img| parent_path == "/" || matches!(img.nodes.get(&parent_path), Some(Node::Dir { .. })));
        if !is_dir {
            return reply.error(Errno::ENOTDIR);
        }
        let path = join(&parent_path, name);
        // Existing file: open in place (truncation arrives as a setattr; the
        // O_EXCL bit of `flags` is not readable without libc and is not
        // honored). New file: commit the empty file + real attrs from birth.
        let content = match self.image(|img| img.nodes.get(&path).cloned()) {
            Some(Node::File { .. }) => self
                .read_file(&path)
                .ok()
                .flatten()
                .map(|b| b.to_vec())
                .unwrap_or_default(),
            Some(Node::Dir { .. }) => return reply.error(Errno::EISDIR),
            Some(Node::Symlink { .. }) => return reply.error(Errno::EEXIST),
            None => {
                let err = self.commit(vec![
                    Delta::Write { path: path.clone(), offset: 0, data: Data::Inline(Bytes::new()) },
                    Delta::SetAttr {
                        path: path.clone(),
                        mode: Some(mode & !umask & 0o7777),
                        uid: Some(req.uid()),
                        gid: Some(req.gid()),
                        mtime_ms: None,
                    },
                ]);
                if let Err(e) = err {
                    return reply.error(e);
                }
                Vec::new()
            }
        };
        let size = content.len() as u64;
        let ino = self.state.lock().unwrap().ino(&path);
        let fh = self.new_handle(ino.0);
        // Re-read attrs so the kernel sees the committed meta.
        let attr = self.attr_of(ino, &path).unwrap_or_else(|| FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::RegularFile,
            perm: (mode & !umask & 0o7777) as u16,
            nlink: 1,
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0,
            blksize: 4096,
            flags: 0,
        });
        reply.created(&TTL, &attr, Generation(0), FileHandle(fh), FopenFlags::empty());
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        match self.image(|img| img.nodes.get(&path).cloned()) {
            Some(Node::File { .. }) => {
                let fh = self.new_handle(ino.0);
                reply.opened(FileHandle(fh), FopenFlags::empty());
            }
            Some(Node::Dir { .. }) => reply.error(Errno::EISDIR),
            Some(Node::Symlink { .. }) => reply.error(Errno::ELOOP),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        let base = self.read_range(&path, offset, size as u64);
        let mut buf = match base {
            Ok(Some(b)) => b.to_vec(),
            Ok(None) => return reply.error(Errno::ENOENT),
            Err(_) => return reply.error(Errno::EIO),
        };
        // Read-your-own-writes: uncommitted extents overlay the fetched range.
        for (off, d) in self.handles.lock().unwrap().get(&fh.0).map(|h| &h.extents[..]).unwrap_or(&[]) {
            let start = (*off).saturating_sub(offset) as usize;
            if start > buf.len() {
                continue;
            }
            let end = (start + d.len()).min(buf.len());
            let n = end - start;
            buf[start..end].copy_from_slice(&d[..n]);
        }
        reply.data(&buf);
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let mut hs = self.handles.lock().unwrap();
        let Some(h) = hs.get_mut(&fh.0) else { return reply.error(Errno::EBADF) };
        h.extents.push((offset, Bytes::copy_from_slice(data)));
        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.flush_fh(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.flush_fh(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let r = self.flush_fh(fh.0);
        self.handles.lock().unwrap().remove(&fh.0);
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(parent_path) = self.path_of(parent) else { return reply.error(Errno::ENOENT) };
        let path = join(&parent_path, name);
        match self.image(|img| img.nodes.get(&path).cloned()) {
            Some(Node::Dir { .. }) => return reply.error(Errno::EISDIR),
            Some(_) => {}
            None => return reply.error(Errno::ENOENT),
        }
        match self.commit(vec![Delta::Remove { path }]) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        let Some(parent_path) = self.path_of(parent) else { return reply.error(Errno::ENOENT) };
        let path = join(&parent_path, name);
        if !self.image(|img| matches!(img.nodes.get(&path), Some(Node::Dir { .. }))) {
            return reply.error(Errno::ENOTDIR);
        }
        if !self.image(|img| children(img, &path)).is_empty() {
            return reply.error(Errno::ENOTEMPTY);
        }
        match self.commit(vec![Delta::Remove { path }]) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        if !flags.is_empty() {
            // renameat2-only semantics (NOREPLACE/EXCHANGE)
            return reply.error(Errno::EINVAL);
        }
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            return reply.error(Errno::EINVAL);
        };
        let (Some(from_dir), Some(to_dir)) = (self.path_of(parent), self.path_of(newparent))
        else {
            return reply.error(Errno::ENOENT);
        };
        let (from, to) = (join(&from_dir, name), join(&to_dir, newname));
        let (from_kind, to_kind) = self.image(|img| {
            let kind = |n: Option<&Node>| match n {
                Some(Node::File { .. }) | Some(Node::Symlink { .. }) => Some(FileType::RegularFile),
                Some(Node::Dir { .. }) => Some(FileType::Directory),
                None => None,
            };
            (kind(img.nodes.get(&from)), kind(img.nodes.get(&to)))
        });
        let Some(fk) = from_kind else { return reply.error(Errno::ENOENT) };
        if let Some(tk) = to_kind {
            match (fk, tk) {
                (FileType::RegularFile, FileType::Directory) => {
                    return reply.error(Errno::EISDIR)
                }
                (FileType::Directory, FileType::RegularFile) => {
                    return reply.error(Errno::ENOTDIR)
                }
                (FileType::Directory, FileType::Directory)
                    if !self.image(|img| children(img, &to)).is_empty() =>
                {
                    return reply.error(Errno::ENOTEMPTY)
                }
                _ => {}
            }
        }
        match self.commit(vec![Delta::Rename { from: from.clone(), to: to.clone() }]) {
            Ok(()) => {
                // Re-points open handles on this subtree (they flush by ino).
                self.state.lock().unwrap().rename_subtree(&from, &to);
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        if path != "/" && !self.image(|img| img.nodes.contains_key(&path)) {
            return reply.error(Errno::ENOENT);
        }
        match self.commit(vec![Delta::SetXattr {
            path,
            name: match name.to_str() {
                Some(n) => n.to_string(),
                None => return reply.error(Errno::EINVAL),
            },
            value: Some(Bytes::copy_from_slice(value)),
        }]) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        let has = self
            .image(|img| img.nodes.get(&path).map(|n| meta_of(n).xattrs.contains_key(name)));
        match has {
            None => reply.error(Errno::ENOENT),
            Some(false) => reply.error(Errno::NO_XATTR),
            Some(true) => match self.commit(vec![Delta::SetXattr {
                path,
                name: name.to_string(),
                value: None,
            }]) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            },
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        let Some(name) = name.to_str() else { return reply.error(Errno::EINVAL) };
        if path != "/" && !self.image(|img| img.nodes.contains_key(&path)) {
            return reply.error(Errno::ENOENT);
        }
        let value =
            self.image(|img| img.nodes.get(&path).and_then(|n| meta_of(n).xattrs.get(name).cloned()));
        match value {
            None => reply.error(Errno::NO_XATTR),
            Some(v) if size == 0 => reply.size(v.len() as u32),
            Some(v) if (size as usize) < v.len() => reply.error(Errno::ERANGE),
            Some(v) => reply.data(&v),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let Some(path) = self.path_of(ino) else { return reply.error(Errno::ENOENT) };
        let names = self.image(|img| {
            img.nodes.get(&path).map(|n| {
                meta_of(n).xattrs.keys().flat_map(|k| k.bytes().chain([0])).collect::<Vec<u8>>()
            })
        });
        match names {
            None if path != "/" => reply.error(Errno::ENOENT),
            None | Some(_) => {
                let data = names.unwrap_or_default();
                if size == 0 {
                    reply.size(data.len() as u32);
                } else if (size as usize) < data.len() {
                    reply.error(Errno::ERANGE);
                } else {
                    reply.data(&data);
                }
            }
        }
    }
}

/// Mounts volume `vol` at `mountpoint` and serves FUSE requests until
/// unmounted (blocking). The mount uses fuser's default [`Config`]: a single
/// session thread, no extra mount options.
pub async fn mount(
    store: Arc<EventStore>,
    bucket: Arc<dyn object_store::ObjectStore>,
    vol: &str,
    mountpoint: &str,
) -> landslide::Result<()> {
    let volume = Volume::mount(store, bucket, vol).await?;
    let fs = LandslideFs::new(Arc::new(tokio::sync::Mutex::new(volume)));
    fuser::mount(fs, mountpoint, &Config::default())
        .map_err(|e| landslide::Error::InvalidInput(format!("fuse mount: {e}")))
}

/// Mounts a read-only [`Replica`] of `vol` at `mountpoint`, kept synced by a
/// background task polling [`Replica::sync`] every `sync_interval`, and
/// serves FUSE requests until unmounted (blocking).
///
/// The replica never fences: the writer and any number of replica mounts
/// coexist, converging within the reader's manifest poll interval (see
/// [`landslide::ReaderConfig::options`]) plus `sync_interval`. The kernel is told
/// `ro`, and mutations fail `EROFS`.
///
/// The sync loop runs on the caller's tokio runtime, which must keep driving
/// tasks while this blocks (e.g. a multi-threaded `#[tokio::main]`).
///
/// The mount allows access by ALL local users (`allow_other`): a replica is
/// a read-only shared view, and consumers like a chrooted/privilege-dropped
/// agent run under a different uid than the mounting process — without it
/// the kernel answers foreign-uid requests with `EACCES` (surfaced as
/// `ESTALE` through an overlayfs upper) itself. `default_permissions` makes
/// the kernel enforce the posix modes we serve. (For non-root mounters
/// `allow_other` requires `user_allow_other` in fuse.conf, as usual.)
pub async fn mount_replica(
    reader: Arc<EventStoreReader>,
    bucket: Arc<dyn object_store::ObjectStore>,
    vol: &str,
    mountpoint: &str,
    sync_interval: Duration,
) -> landslide::Result<()> {
    let replica = Arc::new(tokio::sync::Mutex::new(Replica::open(reader, bucket, vol).await?));
    // Sync loop: transient errors (io, a retention/checkpoint race) are
    // dropped — the next tick retries from the same cursor, unchanged.
    let syncer = replica.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(sync_interval);
        loop {
            tick.tick().await;
            let _ = syncer.lock().await.sync().await;
        }
    });
    let mut config = Config::default();
    config.mount_options.push(fuser::MountOption::CUSTOM("allow_other".into()));
    config.mount_options.push(fuser::MountOption::DefaultPermissions);
    config.mount_options.push(fuser::MountOption::RO);
    fuser::mount(LandslideFs::replica(replica), mountpoint, &config)
        .map_err(|e| landslide::Error::InvalidInput(format!("fuse mount: {e}")))
}
