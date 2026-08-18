//! landslide-sqlite: a SQLite database whose state lives in, and is reconstructed
//! from, a landslide event stream. Committed transactions are captured from the
//! WAL as page post-images, sealed into [`Ltx`] objects in the bucket, and any
//! point in time is reconstructed from the latest manifest, its LTX segments,
//! and the delta backlog since.
//!
//! Model: one database = one landslide stream (`{db}`). The db runs locally in
//! WAL mode through a [`Db`]; every [`sync`](Db::sync) parses newly
//! committed WAL frames — one [`Delta`] per committed transaction, packed
//! into compressed batch events — and appends them durably
//! with a mount-unique fencing token, so a re-[`open`](Db::open) anywhere
//! fences the old writer instantly. Snapshots are manifests of [`Ltx`]
//! segments uploaded to the bucket (see [`checkpoint`](Db::checkpoint));
//! Opening reads the latest manifest, its segments, and the delta backlog
//! since the snapshot, with the backlog scan range-bounded at the snapshot
//! version. Manifests self-maintain: beyond
//! [`CheckpointOpts::coalesce_at`] segments they collapse into one, keeping
//! hydrate cost O(db pages) regardless of total commits, and
//! [`CheckpointOpts::purge`] physically prunes sealed history.
//!
//! v1 scope: single-writer through the [`Db`] handle (auto-checkpoint off —
//! the handle owns WAL checkpoints); post-image page deltas are parsed by
//! salt, without frame-checksum verification; events and segment objects use
//! zstd-compressed bincode.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use landslide::{CompactionRecord, Error, EventStore, NewEvent, Result, Version};

pub mod store;
pub mod wal;

pub use store::{restore, restore_at};

/// One committed SQLite transaction: the post-image of every page it wrote,
/// plus the db size in pages at commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta {
    /// 1-based transaction sequence (the SQLite transaction id).
    pub txid: u64,
    pub page_size: u32,
    /// Db file size in pages after this transaction.
    pub db_size: u32,
    /// Pages written, in WAL frame order (a page may repeat within a tx;
    /// apply in order, newest wins).
    pub pages: Vec<Frame>,
}

/// One page post-image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// 1-based page number.
    pub pgno: u32,
    #[serde(with = "b64")]
    pub data: Bytes,
}

/// A sealed set of [`Delta`]s, uploaded as one object: pgno → post-image,
/// newest-wins. The LTX analog: replaying a manifest's segments in order
/// reproduces the image without replaying individual transactions.
#[derive(Debug, Serialize, Deserialize)]
pub struct Ltx {
    pub txid_start: u64,
    pub txid_end: u64,
    pub page_size: u32,
    pub db_size: u32,
    pub pages: Vec<Frame>,
}

/// Wire format for delta batches and LTX objects: zstd(bincode(T)).
pub(crate) fn pack<T: Serialize + ?Sized>(v: &T) -> Result<Bytes> {
    let raw = bincode::serialize(v).map_err(codec)?;
    Ok(zstd::stream::encode_all(&raw[..], 3).map_err(codec)?.into())
}

pub(crate) fn unpack<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
    let raw = zstd::stream::decode_all(data).map_err(codec)?;
    bincode::deserialize(&raw).map_err(codec)
}

fn codec(e: impl std::fmt::Display) -> Error {
    Error::InvalidInput(format!("codec: {e}"))
}

/// Delta batches stream as `"txb"` events, one per ~512KiB packed chunk, so
/// a sync of any size fits the store's per-record bytes cap.
pub(crate) const TXB: &str = "txb";
const TXB_CHUNK: u64 = 512 * 1024;

pub(crate) fn encode_deltas(deltas: &[Delta]) -> Result<Vec<NewEvent>> {
    let mut events = Vec::new();
    let mut start = 0;
    let mut size = 0;
    for (i, d) in deltas.iter().enumerate() {
        let n = bincode::serialized_size(d).map_err(codec)?;
        if size + n > TXB_CHUNK && start < i {
            events.push(NewEvent::new(TXB, pack(&deltas[start..i])?));
            start = i;
            size = 0;
        }
        size += n;
    }
    if start < deltas.len() {
        events.push(NewEvent::new(TXB, pack(&deltas[start..])?));
    }
    Ok(events)
}

/// Decodes the [`Delta`]s carried by one stream event: every delta of a
/// packed `"txb"` batch, one for a legacy `"tx"` JSON event, none for
/// unknown types.
pub fn delta_events(e: &landslide::Event) -> Result<Vec<Delta>> {
    match e.event_type.as_str() {
        TXB => unpack(&e.data),
        "tx" => Ok(e.json::<Delta>().ok().into_iter().collect()),
        _ => Ok(Vec::new()),
    }
}

/// A manifest pointer to one [`Ltx`] object, carrying its transaction range
/// so readers can select (or skip) segments without fetching them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRef {
    pub path: String,
    pub txid_start: u64,
    pub txid_end: u64,
    pub page_size: u32,
    pub db_size: u32,
}

impl SegmentRef {
    pub(crate) fn of(l: &Ltx, path: &str) -> Self {
        Self {
            path: path.into(),
            txid_start: l.txid_start,
            txid_end: l.txid_end,
            page_size: l.page_size,
            db_size: l.db_size,
        }
    }
}

/// Database image manifest: the ordered set of LTX objects that compose the
/// current state, plus a one-generation-retired GC list (deleted once a
/// newer manifest is durable, so in-flight restores never lose a segment
/// they're still fetching). Stored as the state of a landslide snapshot record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    /// Ordered by txid range: non-overlapping, contiguous from txid 1.
    pub segments: Vec<SegmentRef>,
    /// Segment paths replaced by a coalescing checkpoint; safe to delete
    /// once this manifest has itself been superseded by a durable successor.
    #[serde(default)]
    pub retire: Vec<String>,
}

/// Knobs for [`checkpoint_with`](Db::checkpoint_with).
#[derive(Debug, Clone, Copy)]
pub struct CheckpointOpts {
    /// Coalesce the manifest into a single segment when it holds more than
    /// this many. Keeps restore cost O(db pages), independent of how many
    /// checkpoints ever ran. Default: 8.
    pub coalesce_at: usize,
    /// Physically delete stream events covered by the published manifest
    /// (`EventStore::purge_below`; live fork pins are honored). Bounds
    /// history growth; [`restore_at`] into purged history fails. Default:
    /// false — retention must be opted into.
    pub purge: bool,
}

impl Default for CheckpointOpts {
    fn default() -> Self {
        Self { coalesce_at: 8, purge: false }
    }
}

/// In-memory image of the database: the page map (newest-wins post-images).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DbImage {
    pub page_size: u32,
    /// Db file size in pages.
    pub db_size: u32,
    /// Last applied transaction id.
    pub txid: u64,
    pub pages: BTreeMap<u32, Bytes>,
}

impl DbImage {
    pub fn apply(&mut self, d: &Delta) {
        self.page_size = d.page_size;
        self.db_size = d.db_size;
        self.txid = d.txid;
        for f in &d.pages {
            self.pages.insert(f.pgno, f.data.clone());
        }
    }

    pub fn merge_ltx(&mut self, l: &Ltx) {
        self.page_size = l.page_size;
        self.db_size = l.db_size;
        self.txid = l.txid_end;
        for f in &l.pages {
            self.pages.insert(f.pgno, f.data.clone());
        }
    }

    /// Writes [`to_bytes`](Self::to_bytes) to `path`, creating parent dirs
    /// during restore-like flows and removing stale `-wal`/`-shm` files (the
    /// image is authoritative).
    pub fn write_to(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        for ext in ["-wal", "-shm"] {
            drop(std::fs::remove_file(format!("{}{ext}", path.display())));
        }
        std::fs::write(path, self.to_bytes())
    }

    /// Serializes the image to a SQLite database file: page `i` at offset
    /// `(i-1) * page_size`, truncated to `db_size` pages. Pages beyond
    /// `db_size` (`VACUUM`/freelist leftovers) are dropped.
    pub fn to_bytes(&self) -> Vec<u8> {
        if self.page_size == 0 {
            return Vec::new();
        }
        let mut out = vec![0u8; self.db_size as usize * self.page_size as usize];
        for (&pgno, data) in &self.pages {
            if pgno == 0 || pgno > self.db_size {
                continue;
            }
            let start = (pgno - 1) as usize * self.page_size as usize;
            out[start..start + data.len()].copy_from_slice(data);
        }
        out
    }
}

/// A checkpoint fold in progress: newest-wins page post-images over the
/// deltas since the last checkpoint. Fed by every durable
/// [`sync`](Db::sync) and by backlog recovery at [`open`](Db::open), so
/// sealing never reads the stream back.
#[derive(Default)]
pub(crate) struct LtxBuilder {
    txid_start: Option<u64>,
    txid_end: u64,
    page_size: u32,
    db_size: u32,
    pages: BTreeMap<u32, Bytes>,
}

impl LtxBuilder {
    pub(crate) fn apply(&mut self, d: &Delta) {
        self.txid_start.get_or_insert(d.txid);
        self.txid_end = d.txid;
        self.page_size = d.page_size;
        self.db_size = d.db_size;
        for f in &d.pages {
            self.pages.insert(f.pgno, f.data.clone());
        }
    }

    /// Seals the builder into an [`Ltx`]; callers checked non-emptiness.
    pub(crate) fn into_ltx(self) -> Ltx {
        Ltx {
            txid_start: self.txid_start.expect("nonempty builder"),
            txid_end: self.txid_end,
            page_size: self.page_size,
            db_size: self.db_size,
            pages: self.pages.into_iter().map(|(pgno, data)| Frame { pgno, data }).collect(),
        }
    }
}

/// An open database: the write handle. The local file is a disposable
/// cache — the stream is the source of truth, and [`open`](Db::open) always
/// restores the file from it first.
pub struct Db {
    pub(crate) store: EventStore,
    pub(crate) bucket: Arc<dyn object_store::ObjectStore>,
    pub(crate) name: String,
    pub(crate) token: String,
    pub(crate) conn: rusqlite::Connection,
    pub(crate) path: std::path::PathBuf,
    /// Live database image (read-only observation; advance via [`sync`](Db::sync)).
    pub image: DbImage,
    pub(crate) tail: Option<Version>,
    pub(crate) cursor: wal::WalCursor,
    pub(crate) pending: LtxBuilder,
}

impl Db {
    /// The local WAL-mode connection, for all SQL. Committed transactions
    /// become durable in the stream on the next [`sync`](Db::sync).
    pub fn connection(&self) -> &rusqlite::Connection {
        &self.conn
    }

    /// The underlying event store (the stream's home).
    pub fn store(&self) -> &EventStore {
        &self.store
    }
}

/// Pages serialize base64 in human-readable formats (JSON), raw in binary
/// ones (bincode).
mod b64 {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(b: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&STANDARD.encode(b))
        } else {
            s.serialize_bytes(b)
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        if d.is_human_readable() {
            STANDARD
                .decode(String::deserialize(d)?)
                .map(Bytes::from)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Bytes::from(Vec::<u8>::deserialize(d)?))
        }
    }
}

pub(crate) fn record(name: &str, through_version: Version) -> CompactionRecord {
    CompactionRecord {
        stream: name.into(),
        through_version,
        events_compacted: 0,
        job_id: None,
        ts_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
    }
}
