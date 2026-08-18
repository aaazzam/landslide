//! Db lifecycle: open (reconstruct), sync, checkpoint, restore.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt};
use landslide::{CompactionRecord, Error, EventStore, ExpectedVersion, Result};

use crate::wal::{self, WalCursor};
use crate::{
    delta_events, encode_deltas, pack, record, unpack, CheckpointOpts, Db, DbImage, Delta, Frame,
    Ltx, LtxBuilder, Manifest, SegmentRef,
};

impl Db {
    /// Opens `name` at `path`: takes the fencing token, reconstructs state
    /// from the latest manifest + its LTX segments + the delta backlog, and
    /// materializes it as a fresh local db in WAL mode (stale `-wal`/`-shm`
    /// files removed). Any prior opener is fenced out of future syncs.
    pub async fn open(
        store: EventStore,
        bucket: Arc<dyn ObjectStore>,
        name: &str,
        path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let token = ulid::Ulid::new().to_string();
        // Fencing and reconstruction are independent: run them concurrently.
        let (_, (image, pending, tail)) =
            tokio::try_join!(store.fence(name, Some(&token)), restore_impl(&store, &*bucket, name))?;

        // The local file is a cache: replace it with the reconstructed image.
        let path = path.into();
        for ext in ["", "-wal", "-shm"] {
            drop(std::fs::remove_file(format!("{}{ext}", path.display())));
        }
        std::fs::write(&path, image.to_bytes()).map_err(io)?;
        let conn = rusqlite::Connection::open(&path).map_err(sql)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL;",
        )
        .map_err(sql)?;

        Ok(Self {
            store,
            bucket,
            name: name.into(),
            token,
            conn,
            path,
            image,
            tail,
            cursor: WalCursor::default(),
            pending,
        })
    }

    /// Captures newly committed transactions from the WAL and appends them
    /// — one [`Delta`] each, packed into compressed batch events, as a
    /// single atomic batch. Returns the number of transactions replicated.
    ///
    /// `sync` is a durability barrier: on return the batch is flushed to
    /// object storage and visible to any other opener of the stream.
    pub async fn sync(&mut self) -> Result<usize> {
        self.sync_inner(false).await
    }

    /// [`sync`](Self::sync) with the whole batch folded newest-wins into one
    /// delta: same durability, far fewer bytes under hot-page churn, at the
    /// cost of intra-batch [`restore_at`] granularity.
    pub async fn sync_coalesced(&mut self) -> Result<usize> {
        self.sync_inner(true).await
    }

    async fn sync_inner(&mut self, coalesce: bool) -> Result<usize> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = match std::fs::File::open(format!("{}-wal", self.path.display())) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(io(e)),
        };
        let mut wal = vec![0u8; wal::HEADER_LEN];
        if file.read_exact(&mut wal).is_err() {
            return Ok(0);
        }
        let Some(h) = wal::header(&wal) else { return Ok(0) };
        if (h.salt1, h.salt2) != (self.cursor.salt1, self.cursor.salt2) {
            // New WAL generation: checkpoint-truncated since the last sync, so
            // the prior generation was fully captured already. Start over.
            self.cursor = WalCursor { salt1: h.salt1, salt2: h.salt2, frame: 0 };
        }
        // Read only the tail past the cursor; `base` shifts frame indices
        // into file coordinates for `wal::frame`'s buffer-relative math.
        let base = self.cursor.frame;
        let off = wal.len() as u64 + base * (wal::FRAME_HEADER_LEN + h.page_size as usize) as u64;
        file.seek(SeekFrom::Start(off)).map_err(io)?;
        file.read_to_end(&mut wal).map_err(io)?;

        let mut deltas: Vec<Delta> = Vec::new();
        let mut tx_frames: Vec<Frame> = Vec::new();
        let mut idx = base;
        let mut txid = self.image.txid;
        loop {
            let Some((pgno, db_size, data)) = wal::frame(&wal, &h, idx - base) else { break };
            tx_frames.push(Frame { pgno, data: Bytes::copy_from_slice(data) });
            idx += 1;
            if db_size != 0 {
                txid += 1;
                deltas.push(Delta {
                    txid,
                    page_size: h.page_size,
                    db_size,
                    pages: std::mem::take(&mut tx_frames),
                });
            }
        }
        // Leave the cursor before any uncommitted tail: SQLite may still
        // overwrite those frames until their commit lands. Cursor, image and
        // pending only advance once the batch is durable, so a failed append
        // can simply be retried.
        let next_frame = idx - tx_frames.len() as u64;
        if deltas.is_empty() {
            self.cursor.frame = next_frame;
            return Ok(0);
        }

        let n = deltas.len();
        if coalesce && n > 1 {
            deltas = vec![fold(deltas)];
        }
        let events = encode_deltas(&deltas)?;
        let ticket = self
            .store
            .append_with_token_lazy(&self.name, &self.token, expected(self.tail), events, false)
            .await?;
        self.store.await_durable(&ticket).await?;
        self.tail = Some(ticket.info.last_version);
        for d in &deltas {
            self.image.apply(d);
            self.pending.apply(d);
        }
        self.cursor.frame = next_frame;
        Ok(n)
    }

    /// Seals the deltas since the last manifest into an [`Ltx`] object
    /// (newest-wins per page, accumulated in memory at every durable sync),
    /// publishes the new manifest atomically, then truncates the WAL —
    /// everything through the stream tail is now durable in the bucket.
    /// Cost is O(new pages) ordinarily, O(db pages) on the occasional
    /// coalescing run — never O(history). A no-op (`Ok(None)`) when there is
    /// nothing new to seal. Runs with [`CheckpointOpts::default`]; see
    /// [`checkpoint_with`](Self::checkpoint_with).
    pub async fn checkpoint(&mut self) -> Result<Option<CompactionRecord>> {
        self.checkpoint_with(&CheckpointOpts::default()).await
    }

    /// [`checkpoint`](Self::checkpoint) with explicit knobs: manifest
    /// coalescing and sealed-history purging.
    pub async fn checkpoint_with(
        &mut self,
        opts: &CheckpointOpts,
    ) -> Result<Option<CompactionRecord>> {
        self.sync().await?;
        let Some(through) = self.tail else { return Ok(None) };
        let mut manifest = match self.store.latest_snapshot(&self.name).await? {
            Some(s) => serde_json::from_slice::<Manifest>(&s.state)?,
            None => Manifest::default(),
        };
        if self.pending.txid_start.is_none() {
            return Ok(None);
        }

        let ltx = std::mem::take(&mut self.pending).into_ltx();
        let seg_path = put_ltx(&self.bucket, &self.name, &ltx).await?;
        let new_ref = SegmentRef::of(&ltx, &seg_path);
        manifest.segments.push(new_ref.clone());

        // Coalesce segments so the manifest and restore cost stay bounded as
        // checkpoints accumulate.
        let mut retire = Vec::new();
        if manifest.segments.len() > opts.coalesce_at {
            let mut pages = std::collections::BTreeMap::new();
            for segref in &manifest.segments {
                // The just-written segment's pages are still in memory.
                if segref.path == new_ref.path {
                    continue;
                }
                for f in fetch_ltx(&*self.bucket, &segref.path).await?.pages {
                    pages.insert(f.pgno, f.data);
                }
            }
            for f in ltx.pages {
                pages.insert(f.pgno, f.data);
            }
            // Drop pages truncated out of the db by the final tx.
            pages.retain(|&pgno, _| pgno <= new_ref.db_size);
            let merged = Ltx {
                txid_start: manifest.segments[0].txid_start,
                txid_end: new_ref.txid_end,
                page_size: new_ref.page_size,
                db_size: new_ref.db_size,
                pages: pages.into_iter().map(|(pgno, data)| Frame { pgno, data }).collect(),
            };
            let merged_path = put_ltx(&self.bucket, &self.name, &merged).await?;
            retire = manifest.segments.iter().map(|s| s.path.clone()).collect();
            manifest.segments = vec![SegmentRef::of(&merged, &merged_path)];
        }

        // Generational GC: delete the *previous* manifest's retire list — it
        // was superseded then, and is now another manifest older.
        for path in std::mem::take(&mut manifest.retire) {
            drop(self.bucket.delete(&path.as_str().into()).await);
        }

        let record = record(&self.name, through);
        manifest.retire = retire;
        self.store
            .publish_snapshot(&self.name, record.clone(), Bytes::from(serde_json::to_vec(&manifest)?))
            .await?;

        // Retention: the manifest covers everything ≤ through_version.
        // (After publish+flush — never delete data a reader can still reach.)
        if opts.purge {
            self.store.purge_below(&self.name, through + 1).await?;
        }

        // Everything through the tail is durable in the bucket: the WAL may
        // restart (the next sync sees the new salts and resumes at frame 0).
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").map_err(sql)?;
        Ok(Some(record))
    }

    /// Latest committed version (the stream tail).
    pub async fn tail(&self) -> Result<Option<landslide::Version>> {
        self.store.stream_version(&self.name).await
    }
}

/// Reconstructs the database image without fencing: latest manifest + its
    /// LTX segments plus the delta backlog since the latest snapshot. Returns
    /// the image and the stream version it is current through.
pub async fn restore(
    store: &EventStore,
    bucket: &dyn ObjectStore,
    name: &str,
) -> Result<(DbImage, Option<landslide::Version>)> {
    let (image, _, tail) = restore_impl(store, bucket, name).await?;
    Ok((image, tail))
}

/// [`restore`] plus the in-progress checkpoint fold for the backlog, so an
/// opener seals new checkpoints without a stream read-back.
pub(crate) async fn restore_impl(
    store: &EventStore,
    bucket: &dyn ObjectStore,
    name: &str,
) -> Result<(DbImage, LtxBuilder, Option<landslide::Version>)> {
    let (image, from, through) = match store.latest_snapshot(name).await? {
        Some(snap) => {
            let manifest: Manifest = serde_json::from_slice(&snap.state)?;
            let mut image = DbImage::default();
            for segref in &manifest.segments {
                image.merge_ltx(&fetch_ltx(bucket, &segref.path).await?);
            }
            (image, snap.through_version + 1, Some(snap.through_version))
        }
        None => (DbImage::default(), 0, None),
    };
    let ((image, pending, bad), last) = store
        .fold(name, from.., (image, LtxBuilder::default(), None), |(i, p, bad), e| {
            match delta_events(e) {
                Ok(ds) => ds.iter().for_each(|d| {
                    i.apply(d);
                    p.apply(d);
                }),
                Err(err) => *bad = Some(err),
            }
        })
        .await?;
    if let Some(err) = bad {
        return Err(err);
    }
    // An empty backlog leaves the tail at the snapshot's version, not None.
    Ok((image, pending, last.or(through)))
}

/// Point-in-time restore: reconstructs the database image as of transaction
/// `target_txid` (inclusive). Segments past the target are skipped from
/// their [`SegmentRef`] metadata; the exact tail comes from the retained
/// [`Delta`] backlog. Errors if the target is beyond the current tail or has
/// been purged by retention ([`CheckpointOpts::purge`]). The result is
/// read-only; use [`Db::open`] to open a writable database.
pub async fn restore_at(
    store: &EventStore,
    bucket: &dyn ObjectStore,
    name: &str,
    target_txid: u64,
) -> Result<DbImage> {
    let mut image = DbImage::default();
    if let Some(snap) = store.latest_snapshot(name).await? {
        let manifest: Manifest = serde_json::from_slice(&snap.state)?;
        for segref in &manifest.segments {
            // Segments have ordered, contiguous txid ranges: one past the
            // target ends the usable prefix without any fetching; one
            // straddling it can't be applied partially (the backlog covers
            // per-tx granularity there, if retained).
            if segref.txid_start > target_txid || segref.txid_end > target_txid {
                break;
            }
            image.merge_ltx(&fetch_ltx(bucket, &segref.path).await?);
        }
    }
    let base = image.txid;
    let ((image, bad), _) = store
        .fold(name, 0.., (image, None), |(image, bad), e| {
            match delta_events(e) {
                Ok(ds) => ds.iter().for_each(|d| {
                    if d.txid > base && d.txid <= target_txid {
                        image.apply(d);
                    }
                }),
                Err(err) => *bad = Some(err),
            }
        })
        .await?;
    if let Some(err) = bad {
        return Err(err);
    }
    if image.txid < target_txid {
        return Err(Error::InvalidInput(format!(
            "txid {target_txid} is not reconstructable (beyond tail or purged by retention); reached txid {}",
            image.txid
        )));
    }
    Ok(image)
}

/// Uploads `ltx` as a segment object; returns its bucket path.
async fn put_ltx(bucket: &Arc<dyn ObjectStore>, name: &str, ltx: &Ltx) -> Result<String> {
    let path = object_store::path::Path::from(format!("ltx/{}/{}", name, ulid::Ulid::new()));
    bucket.put(&path, pack(ltx)?.into()).await.map_err(io)?;
    Ok(path.to_string())
}

async fn fetch_ltx(bucket: &dyn ObjectStore, path: &str) -> Result<Ltx> {
    let bytes = bucket.get(&path.into()).await.map_err(io)?.bytes().await.map_err(io)?;
    unpack(&bytes)
}

/// Newest-wins fold of a batch into a single delta: the batch's end state.
fn fold(deltas: Vec<Delta>) -> Delta {
    let mut pages = BTreeMap::new();
    for d in &deltas {
        for f in &d.pages {
            pages.insert(f.pgno, f.data.clone());
        }
    }
    let last = deltas.last().expect("nonempty batch");
    Delta {
        txid: last.txid,
        page_size: last.page_size,
        db_size: last.db_size,
        pages: pages.into_iter().map(|(pgno, data)| Frame { pgno, data }).collect(),
    }
}

fn expected(tail: Option<landslide::Version>) -> ExpectedVersion {
    tail.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact)
}

fn io(e: impl std::fmt::Display) -> Error {
    Error::InvalidInput(format!("bucket io: {e}"))
}

fn sql(e: rusqlite::Error) -> Error {
    Error::InvalidInput(format!("sqlite: {e}"))
}
