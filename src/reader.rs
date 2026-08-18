//! Read-only access and live tailing — the scale-out half of the design.
//! Readers pull straight from object storage and can be replicated freely.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::envelope::{Envelope, Event, Version};
use crate::kv::{self, KvRead};
use crate::snapshot::CompactionRecord;
use crate::{store, Result, SnapshotRecord};

/// How to reach the database: path plus object store.
#[derive(Clone)]
pub struct ReaderConfig {
    pub path: String,
    pub object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    /// SlateDB reader tuning passthrough (manifest/WAL poll interval, cache).
    /// `None` = stock SlateDB reader defaults. Followers that tail actively
    /// should lower `manifest_poll_interval` (the default is 10s), since it
    /// bounds how quickly new commits become visible.
    pub options: Option<slatedb::config::DbReaderOptions>,
}

/// Read-only event store access.
pub struct EventStoreReader {
    db: Arc<slatedb::DbReader>,
}

impl EventStoreReader {
    pub async fn open(config: ReaderConfig) -> Result<Self> {
        let db = slatedb::DbReader::open(
            config.path,
            config.object_store,
            slatedb::DbReaderMode::default(),
            config.options.unwrap_or_default(),
        )
        .await?;
        Ok(Self { db: Arc::new(db) })
    }

    /// All events of `stream` with version in `range`, in order.
    pub async fn read_stream(
        &self,
        stream: &str,
        range: impl std::ops::RangeBounds<Version> + Send,
    ) -> Result<Vec<Event>> {
        kv::read_events(&*self.db, stream, range).await
    }

    /// Fork-resolved history: pinned parent prefixes plus local events,
    /// filtered by version range.
    pub async fn read_history(
        &self,
        stream: &str,
        range: impl std::ops::RangeBounds<Version> + Send,
    ) -> Result<Vec<Event>> {
        let (lo, hi) = store::bounds(range);
        let mut events =
            crate::fork::resolve_history(&*self.db, stream, &mut Default::default(), lo, hi).await?;
        events.retain(|e| !e.event_type.starts_with('$'));
        Ok(events)
    }

    pub async fn stream_version(&self, stream: &str) -> Result<Option<Version>> {
        Ok(self
            .read_stream(stream, ..)
            .await?
            .last()
            .map(|e| e.version))
    }

    pub async fn latest_snapshot(&self, stream: &str) -> Result<Option<SnapshotRecord>> {
        kv::latest_snapshot(&*self.db, stream).await
    }

    pub async fn compaction_records(&self) -> Result<Vec<CompactionRecord>> {
        KvRead::scan_prefix(&*self.db, kv::COMPACTIONS_PREFIX.as_bytes().to_vec())
            .await?
            .into_iter()
            .map(|(_, value)| Ok(serde_json::from_slice(&value)?))
            .collect()
    }

    /// Tails `stream`, yielding each new event with `version >= from_version`
    /// once, until the receiver is dropped. Polls every `poll_interval`;
    /// persist your own cursor if you need resume across processes.
    pub fn follow_stream(
        &self,
        stream: &str,
        from_version: Version,
        poll_interval: Duration,
    ) -> mpsc::Receiver<Result<Event>> {
        follow(
            self.db.clone(),
            kv::r_prefix(stream),
            poll_interval,
            move |_, value| {
                let env = Envelope::decode(&value)?;
                Ok((env.version >= from_version).then(|| (env.version, Event::from_parts(env))))
            },
        )
    }

    /// Tails `j/compactions/`, yielding a record for every compaction — the
    /// listening hook for rebuilding projections, invalidating caches, etc.
    pub fn follow_compactions(
        &self,
        poll_interval: Duration,
    ) -> mpsc::Receiver<Result<CompactionRecord>> {
        // Keys are ULID-suffixed, so lexicographic order is commit order.
        follow(
            self.db.clone(),
            kv::COMPACTIONS_PREFIX.as_bytes().to_vec(),
            poll_interval,
            |key, value| {
                Ok(Some((
                    key,
                    serde_json::from_slice::<CompactionRecord>(&value)?,
                )))
            },
        )
    }
}

/// Shared poll loop behind the `follow_*` methods: scans `prefix` every
/// `poll_interval`; `decode` turns each kv into `Some((cursor_key, item))`
/// to yield or `None` to skip, and only items past the cursor are sent —
/// the loop runs until the receiver is dropped.
fn follow<K, T, F>(
    db: Arc<slatedb::DbReader>,
    prefix: Vec<u8>,
    poll_interval: Duration,
    mut decode: F,
) -> mpsc::Receiver<Result<T>>
where
    K: Ord + Send + 'static,
    T: Send + 'static,
    F: FnMut(Bytes, Bytes) -> Result<Option<(K, T)>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let mut cursor: Option<K> = None;
        let mut interval = tokio::time::interval(poll_interval);
        loop {
            interval.tick().await;
            let poll = async {
                let mut out = Vec::new();
                for (key, value) in KvRead::scan_prefix(&*db, prefix.clone()).await? {
                    if let Some((k, item)) = decode(key, value)? {
                        if cursor.as_ref().is_none_or(|c| k.gt(c)) {
                            cursor = Some(k);
                            out.push(item);
                        }
                    }
                }
                Ok::<Vec<T>, crate::Error>(out)
            };
            match poll.await {
                Ok(items) => {
                    for item in items {
                        if tx.send(Ok(item)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
    });
    rx
}
