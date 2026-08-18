//! Read-only replicas are streamed, live-updating views of a volume. They do
//! not acquire the fencing token, so the writer and any number of replicas
//! can coexist. Replication uses [`landslide::EventStoreReader`].
//!
//! Metadata converges on each [`Replica::sync`]. A
//! [`crate::fs::mount_replica`] mount syncs on an interval, and file content
//! is fetched lazily on read through the same block cache as the writer.
//! Following a volume costs O(new deltas) per sync; bootstrapping costs
//! O(segments).
//!
//! Convergence is bounded by the reader's manifest poll interval (see
//! [`landslide::ReaderConfig::options`]; SlateDB's default is 10s) plus the sync
//! interval, whichever is hit first.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use landslide::{Error, Event, EventStoreReader, Result, Version};

use crate::{store, Delta, FsImage};

/// A replicated, read-only view of a volume: catch up with [`sync`](Self::sync),
/// read through [`read_file`](Self::read_file) / [`read_file_range`](Self::read_file_range).
pub struct Replica {
    pub(crate) reader: Arc<EventStoreReader>,
    pub(crate) bucket: Arc<dyn ObjectStore>,
    pub(crate) vol: String,
    /// The current in-memory view; convergent with the volume stream as of
    /// the last [`sync`](Self::sync).
    pub image: FsImage,
    /// Last applied stream version.
    pub(crate) cursor: Option<Version>,
    /// Content block cache (hash → bytes), filled lazily by reads.
    pub(crate) blocks: HashMap<String, Bytes>,
}

impl Replica {
    /// Opens a replica of `vol`: latest manifest + its segment objects + the
    /// delta backlog since, exactly like [`Volume::mount`](crate::Volume::mount),
    /// but no fence — any number of replicas may coexist with the writer.
    pub async fn open(
        reader: Arc<EventStoreReader>,
        bucket: Arc<dyn ObjectStore>,
        vol: &str,
    ) -> Result<Self> {
        let mut replica = Self {
            reader,
            bucket,
            vol: vol.into(),
            image: FsImage::default(),
            cursor: None,
            blocks: HashMap::new(),
        };
        replica.sync().await?;
        Ok(replica)
    }

    /// Catches the image up with the volume stream. Cheap on the happy path
    /// (one range read of the deltas past the cursor). If retention
    /// ([`EventStore::purge_below`](landslide::EventStore::purge_below) /
    /// `trim_below`) cut the backlog out from under the cursor, rebuilds
    /// from the latest checkpoint's manifest instead.
    pub async fn sync(&mut self) -> Result<()> {
        let from = self.cursor.map_or(0, |c| c + 1);
        let events = self.reader.read_stream(&self.vol, from..).await?;
        let gap = match events.first() {
            // Versions are contiguous and append-only: a first version past
            // `from` means the window between was purged.
            Some(first) => first.version > from,
            // Nothing readable at or past the cursor: caught up — unless the
            // window was purged entirely, which a newer checkpoint reveals.
            None => match self.reader.latest_snapshot(&self.vol).await? {
                Some(snap) => snap.through_version >= from,
                None => false,
            },
        };
        let events = if gap { self.reload().await? } else { events };
        for event in events {
            if let Ok(delta) = event.json::<Delta>() {
                self.image.apply(&delta);
            }
            self.cursor = Some(event.version);
        }
        Ok(())
    }

    /// Rebuilds the image from the latest checkpoint, then reads the deltas
    /// committed since. The slow path of [`sync`](Self::sync).
    async fn reload(&mut self) -> Result<Vec<Event>> {
        let Some(snap) = self.reader.latest_snapshot(&self.vol).await? else {
            return Err(Error::InvalidInput(format!(
                "volume {:?}: backlog pruned but no checkpoint to rebuild from",
                self.vol
            )));
        };
        let image = store::load_segments(&self.bucket, &snap.state).await?;
        let from = snap.through_version + 1;
        let events = self.reader.read_stream(&self.vol, from..).await?;
        if let Some(first) = events.first() {
            if first.version > from {
                // Retention cut post-checkpoint deltas too: unrecoverable
                // until the writer publishes a newer checkpoint. The cursor
                // is left untouched, so the next sync retries from scratch.
                return Err(Error::InvalidInput(format!(
                    "volume {:?}: deltas v{from}..v{} pruned past the latest checkpoint (v{}); \
                     retry after the writer checkpoints",
                    self.vol,
                    first.version,
                    snap.through_version,
                )));
            }
        }
        self.image = image;
        self.cursor = Some(snap.through_version);
        Ok(events)
    }

    /// Materializes a file: inline bytes directly, or its blocks (fetched
    /// lazily, cached per block hash).
    pub async fn read_file(&mut self, path: &str) -> Result<Option<Bytes>> {
        store::read_file(&self.image, &self.bucket, &mut self.blocks, path).await
    }

    /// Streaming read: fetches only the blocks overlapping
    /// `[offset, offset+len)`.
    pub async fn read_file_range(
        &mut self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Bytes>> {
        store::read_file_range(&self.image, &self.bucket, &mut self.blocks, path, offset, len).await
    }

    /// The applied stream position (the replica's tail).
    pub fn cursor(&self) -> Option<Version> {
        self.cursor
    }

    /// Blocks fetched into the cache so far (diagnostics: proves lazy reads).
    pub fn block_cache_len(&self) -> usize {
        self.blocks.len()
    }
}
