//! The read/write event store, on raw SlateDB transactions.
//!
//! State lives in the KV layout documented in [`crate::kv`]: one record key
//! `r/{stream}/{version}` per event, with tail/fence/trim/tombstone metadata
//! under `m/{stream}/…`. Every mutation is one serializable SlateDB
//! transaction — the tail key, fence key, and global sequence counter
//! (`g/seq`) replace all in-process caches and locks.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use slatedb::{config::WriteOptions, Db, DbTransaction, ErrorKind, IsolationLevel};

use crate::envelope::{Envelope, Event, ExpectedVersion, NewEvent, Version};
use crate::fork::resolve_history;
use crate::jobs::{JobEvent, JobStatus};
use crate::kv::{self, COMPACTIONS_PREFIX, TailRecord};
use crate::{Aggregate, CompactionRecord, Error, Result, SnapshotRecord};

/// Max records per append batch.
const MAX_BATCH_RECORDS: usize = 1000;
/// Commit-conflict retries per append, for same-stream tail-key races (the
/// one genuinely shared key; g/seq is unmarked). A loser replays against the
/// current tail, so the expected-version check re-runs before every attempt.
const COMMIT_RETRIES: usize = 8;
/// Max size of a single record's payload.
const MAX_RECORD_BYTES: usize = 1 << 20;
/// Prefix for compaction-job journal records: `j/jobs/{ulid}`.
const JOBS_PREFIX: &str = "j/jobs/";

/// Store configuration: SlateDB path + object storage.
#[derive(Clone)]
pub struct Config {
    /// Namespace of the SlateDB database within the object store.
    pub path: String,
    pub object_store: Arc<dyn object_store::ObjectStore>,
    /// SlateDB tuning passthrough (compactor/GC schedules, caches, flush
    /// intervals). `None` = slog's tuned profile (see
    /// [`default_settings`]); `Some(Settings::default())` = stock SlateDB.
    pub settings: Option<slatedb::config::Settings>,
}

impl Config {
    /// Volatile in-memory backend; for tests and development.
    pub fn in_memory() -> Self {
        Self {
            path: "slog".into(),
            object_store: Arc::new(object_store::memory::InMemory::new()),
            settings: None,
        }
    }
}

/// Outcome of a successful [`EventStore::append`].
#[derive(Debug, Clone, Copy)]
pub struct CommitInfo {
    pub first_version: Version,
    pub last_version: Version,
    /// Global sequence of the batch's first record; members are consecutive.
    pub start_sequence: u64,
}

/// A committed-but-not-yet-durable append, from [`EventStore::append_lazy`]
/// or [`EventStore::append_with_token_lazy`]; the barrier is
/// [`EventStore::await_durable`]. Splitting the two is the group-commit
/// primitive: all tickets on this store share one object-storage watermark,
/// so awaiting the highest ticket awaits them all.
#[derive(Debug, Clone, Copy)]
pub struct CommitTicket {
    pub info: CommitInfo,
    /// This store's durability watermark target (backend WAL sequence).
    seq: u64,
}

/// Bounds for [`EventStore::read_page`]: page size cap and payload byte cap.
#[derive(Debug, Clone, Copy)]
pub struct PageLimit {
    pub max_count: usize,
    pub max_bytes: usize,
}

/// Durable, atomic, ordered event streams on object storage.
///
/// Writes go through serializable SlateDB transactions; optimistic
/// concurrency comes from the per-stream tail key, not from in-process
/// locking. Readers ([`EventStoreReader`](crate::EventStoreReader)) use
/// `DbReader` over the same object store.
pub struct EventStore {
    db: Arc<Db>,
}

/// A cross-stream atomic transaction: appends and fence ops replayed into
/// one SlateDB transaction by [`EventStore::commit`].
pub struct Transaction {
    ops: Vec<Op>,
}

enum Op {
    Append {
        stream: String,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    },
    Fence {
        stream: String,
        token: Option<String>,
    },
}

impl Transaction {
    /// Queue a conditional append (same semantics as [`EventStore::append`]).
    pub fn append(
        &mut self,
        stream: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> &mut Self {
        self.ops.push(Op::Append {
            stream: stream.into(),
            expected,
            events,
        });
        self
    }

    /// Queue a fence set/clear (same semantics as [`EventStore::fence`]).
    pub fn fence(&mut self, stream: &str, token: Option<String>) -> &mut Self {
        self.ops.push(Op::Fence {
            stream: stream.into(),
            token,
        });
        self
    }
}

impl EventStore {
    pub async fn open(config: Config) -> Result<Self> {
        let builder = Db::builder(config.path.as_str(), config.object_store)
            .with_settings(config.settings.unwrap_or_else(default_settings));
        let db = builder.build().await?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Volatile in-memory backend; for tests and development.
    pub async fn open_in_memory() -> Result<Self> {
        Self::open(Config::in_memory()).await
    }

    /// The underlying SlateDB handle.
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Atomically commits the batch to `stream` iff its version matches
    /// `expected`; the whole batch lands or none does. Retrying an already
    /// committed batch returns [`Error::VersionConflict`], never a duplicate.
    pub async fn append(
        &self,
        stream: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<CommitInfo> {
        Ok(self.append_lazy(stream, expected, events).await?.info)
    }

    /// Appends `events` starting at `from_version`, overwriting any existing
    /// events at those versions. This lets a fenced writer resolve a conflict
    /// by replacing its tail in place.
    pub async fn append_at(
        &self,
        stream: &str,
        from_version: Version,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<CommitInfo> {
        self.commit_appends(stream, expected, Some(from_version), None, events, true)
            .await
            .map(|t| t.info)
    }

    /// Like [`append`](Self::append), but fails with [`Error::FenceMismatch`]
    /// if the stream is fenced and `token` isn't the current fence token.
    pub async fn append_with_token(
        &self,
        stream: &str,
        token: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<CommitInfo> {
        self.commit_appends(stream, expected, None, Some(token), events, true)
            .await
            .map(|t| t.info)
    }

    /// [`append`](Self::append) without the durability wait: the batch is
    /// committed (visible to readers) on return, and
    /// [`await_durable`](Self::await_durable) is the barrier for when to
    /// claim it in object storage.
    pub async fn append_lazy(
        &self,
        stream: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<CommitTicket> {
        self.commit_appends(stream, expected, None, None, events, true).await
    }

    /// [`append_with_token`](Self::append_with_token) without the durability
    /// wait. Set `index_ts` to `false` when the stream does not use
    /// [`seek_timestamp`](Self::seek_timestamp); this omits one key per event.
    pub async fn append_with_token_lazy(
        &self,
        stream: &str,
        token: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
        index_ts: bool,
    ) -> Result<CommitTicket> {
        self.commit_appends(stream, expected, None, Some(token), events, index_ts)
            .await
    }

    /// The durability barrier for lazy appends. Resolves once `ticket`'s
    /// batch is durable in object storage and visible to any other opener.
    /// The wait covers the WAL write without freezing the memtable or rotating
    /// the WAL.
    pub async fn await_durable(&self, ticket: &CommitTicket) -> Result<()> {
        self.await_seq(ticket.seq).await
    }

    /// [`append_with_token`](Self::append_with_token), durable on return. Use
    /// this when a replication acknowledgement must mean that the commit is
    /// visible to another client.
    pub async fn append_with_token_durable(
        &self,
        stream: &str,
        token: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<CommitInfo> {
        let t = self
            .append_with_token_lazy(stream, token, expected, events, true)
            .await?;
        self.await_durable(&t).await?;
        Ok(t.info)
    }

    /// Like [`append`](Self::append), but returns only once the batch is
    /// durable in object storage.
    pub async fn append_durable(
        &self,
        stream: &str,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<CommitInfo> {
        let t = self.append_lazy(stream, expected, events).await?;
        self.await_durable(&t).await?;
        Ok(t.info)
    }

    /// Sets (`Some`) or clears (`None`/empty) the stream's fence token.
    /// [`append_with_token`](Self::append_with_token) validates the token;
    /// [`append`](Self::append) does not require one.
    ///
    /// Durable on return (`await_durable`), so a re-open fences the old
    /// writer for other clients immediately.
    ///
    /// The returned `CommitInfo` is bookkeeping only — fence is pure state,
    /// no events are written: `first_version`/`last_version` carry the
    /// stream's current tail (or 0) and `start_sequence` is 0.
    pub async fn fence(&self, stream: &str, token: Option<&str>) -> Result<CommitInfo> {
        let txn = self.begin().await?;
        let tail = kv::tail(&txn, stream).await?;
        replay_fence(&txn, stream, token)?;
        let handle = txn
            .commit_with_options(&slatedb::config::WriteOptions {
                await_durable: false,
                ..Default::default()
            })
            .await?;
        if let Some(h) = handle {
            self.await_seq(h.seqnum()).await?;
        }
        Ok(tail_commit_info(tail))
    }

    /// Forks `stream`: creates `child` sharing `stream`'s history through
    /// `at_version` without copying any events. `child` must not exist and
    /// `parent` must already have `at_version`. Child appends continue at
    /// `at_version + 1`. One transaction: fork ref, child tail, parent pin.
    pub async fn fork(&self, parent: &str, at_version: Version, child: &str) -> Result<()> {
        let txn = self.begin().await?;
        let Some(parent_tail) = kv::tail(&txn, parent).await? else {
            return Err(Error::InvalidInput(format!("parent '{parent}' does not exist")));
        };
        if parent_tail.version < at_version {
            return Err(Error::InvalidInput(format!(
                "parent '{parent}' is at v{}, cannot fork at v{at_version}",
                parent_tail.version
            )));
        }
        let child_tail = kv::tail(&txn, child).await?;
        if child_tail.is_some() {
            return Err(Error::VersionConflict {
                stream: child.into(),
                expected: ExpectedVersion::NoStream,
                actual: child_tail.map(|t| t.version),
            });
        }
        txn.put(
            kv::m_key(child, "fork"),
            serde_json::to_vec(&crate::fork::ForkRef {
                stream: parent.into(),
                at_version,
            })?,
        )?;
        txn.put(
            kv::m_key(child, "tail"),
            serde_json::to_vec(&TailRecord {
                version: at_version,
                ts_ms: parent_tail.ts_ms,
            })?,
        )?;
        txn.put(
            kv::m_key(parent, &format!("forks/{child}")),
            at_version.to_be_bytes(),
        )?;
        txn.commit().await?;
        Ok(())
    }

    /// Hides shadow appends with `version >= from_version` and global sequence
    /// greater than `txn`, allowing the live writer to reclaim the branch.
    /// Logical only. See [`fence`](Self::fence) for the `CommitInfo` shape.
    pub async fn trim(&self, stream: &str, from_version: Version, txn: u64) -> Result<CommitInfo> {
        let t = self.begin().await?;
        let tail = kv::tail(&t, stream).await?;
        t.put(
            kv::m_key(stream, &format!("rollback/{from_version:016}")),
            txn.to_be_bytes(),
        )?;
        t.commit().await?;
        Ok(tail_commit_info(tail))
    }

    /// Trim point: hides all events with `version < floor`. Logical only —
    /// the stream and its future appends survive and continue numbering.
    /// See [`fence`](Self::fence) for the `CommitInfo` shape.
    pub async fn trim_below(&self, stream: &str, floor: Version) -> Result<CommitInfo> {
        let txn = self.begin().await?;
        let tail = kv::tail(&txn, stream).await?;
        // Floors only move forward and can only cover the existing history:
        // clamp past-the-tail floors so future appends stay visible.
        let floor = floor
            .min(tail.as_ref().map_or(u64::MAX, |t| t.version + 1))
            .max(kv::get_be(&txn, &kv::m_key(stream, "trim")).await?.unwrap_or(0));
        txn.put(kv::m_key(stream, "trim"), floor.to_be_bytes())?;
        txn.commit().await?;
        Ok(tail_commit_info(tail))
    }

    /// Permanently deletes all events of `stream` with `version < floor` in
    /// bounded transactions. Retention removes the bytes, so point-in-time
    /// reads and future forks into purged versions are unavailable.
    ///
    /// Versions inherited by a live fork are never purged: the effective
    /// window is `(max live fork pin, floor)`. Streams with no live forks
    /// purge everything below `floor`. Returns the number of events deleted.
    pub async fn purge_below(&self, stream: &str, floor: Version) -> Result<u64> {
        const CHUNK: usize = 1024;
        let pin_floor = kv::ceiling(&*self.db, stream).await?.map_or(0, |c| c + 1);
        let mut next = pin_floor;
        let mut purged = 0u64;
        while next < floor {
            let mut it = self
                .db
                .scan_prefix(
                    kv::r_prefix(stream),
                    format!("{next:016}").into_bytes()..format!("{floor:016}").into_bytes(),
                )
                .await?;
            let mut keys = Vec::new();
            while keys.len() < CHUNK {
                let Some(kv) = it.next().await? else { break };
                keys.push(kv.key);
            }
            let Some(last) = keys.last() else { break };
            let last_version = kv::version_suffix(last)?;
            let deleted = keys.len() as u64;
            let txn = self.begin().await?;
            for key in keys {
                txn.delete(key)?;
            }
            txn.commit().await?;
            purged += deleted;
            next = last_version + 1;
        }
        Ok(purged)
    }

    /// Logically deletes `stream`: after this, reads see only the prefix
    /// pinned by live forks (or nothing), and [`list_streams`](Self::list_streams)
    /// omits it. The exclusive tail is removed while a shared prefix remains
    /// available to live forks. Stream ids are terminal after deletion.
    pub async fn delete_stream(&self, stream: &str) -> Result<()> {
        let txn = self.begin().await?;
        if kv::tail(&txn, stream).await?.is_some() {
            txn.put(kv::m_key(stream, "deleted"), [])?;
            txn.commit().await?;
        }
        Ok(())
    }

    /// All events of `stream` with version in `range`, in order. Trim
    /// floors, rollback windows, and deletion ceilings are applied. Raw
    /// per-stream view; see [`read_history`](Self::read_history) for the
    /// fork-resolved view.
    pub async fn read_stream(
        &self,
        stream: &str,
        range: impl std::ops::RangeBounds<Version> + Send,
    ) -> Result<Vec<Event>> {
        kv::read_events(&*self.db, stream, range).await
    }

    /// Fork-resolved history: the pinned parent prefix (chain by chain) plus
    /// the stream's own events, filtered by version range.
    pub async fn read_history(
        &self,
        stream: &str,
        range: impl std::ops::RangeBounds<Version> + Send,
    ) -> Result<Vec<Event>> {
        let (lo, hi) = bounds(range);
        resolve_history(&*self.db, stream, &mut Default::default(), lo, hi).await
    }

    /// The stream's last version, or `None` if it doesn't exist. O(1):
    /// served by the tail key.
    pub async fn stream_version(&self, stream: &str) -> Result<Option<Version>> {
        Ok(kv::tail(&*self.db, stream).await?.map(|t| t.version))
    }

    /// The stream's last (version, ts_ms), or `None` if it doesn't exist.
    /// O(1): served by the tail key.
    pub async fn check_tail(&self, stream: &str) -> Result<Option<(Version, i64)>> {
        Ok(kv::tail(&*self.db, stream)
            .await?
            .map(|t| (t.version, t.ts_ms)))
    }

    /// The last `n` events of the stream, in version order: reads the tail
    /// key, computes the start version, and scans only that window.
    pub async fn read_tail(&self, stream: &str, n: usize) -> Result<Vec<Event>> {
        let Some(t) = kv::tail(&*self.db, stream).await? else {
            return Ok(Vec::new());
        };
        let start = (t.version + 1).saturating_sub(n as u64);
        kv::read_events(&*self.db, stream, start..).await
    }

    /// Reads a page of events starting at `from_version`: up to
    /// `limit.max_count` events, cut short once cumulative payload size
    /// would exceed `limit.max_bytes` (the first event is always included).
    /// Returns the page and the cursor (next version to read), or `None`
    /// when the stream is drained.
    pub async fn read_page(
        &self,
        stream: &str,
        from_version: Version,
        limit: PageLimit,
    ) -> Result<(Vec<Event>, Option<Version>)> {
        let events = kv::read_events(&*self.db, stream, from_version..).await?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut drained = true;
        for e in events {
            if !out.is_empty()
                && (out.len() >= limit.max_count || bytes + e.data.len() > limit.max_bytes)
            {
                drained = false;
                break;
            }
            bytes += e.data.len();
            out.push(e);
        }
        let cursor = out.last().filter(|_| !drained).map(|last| last.version + 1);
        Ok((out, cursor))
    }

    /// Version of the first event with `ts_ms >= target`, via the `i/` time
    /// index; `None` if the stream has nothing at or after `target`.
    pub async fn seek_timestamp(&self, stream: &str, ts_ms: i64) -> Result<Option<Version>> {
        let prefix = format!("i/{stream}/").into_bytes();
        let mut it = self
            .db
            .scan_prefix(&prefix, format!("{ts_ms:016}").into_bytes()..)
            .await?;
        let Some(kv) = it.next().await? else {
            return Ok(None);
        };
        Ok(Some(kv::version_suffix(&kv.key)?))
    }

    /// Folds fork-resolved history over `range` into `init`. The generic
    /// rehydration primitive: application code supplies the state type and
    /// fold logic.
    pub async fn fold<T: Send>(
        &self,
        stream: &str,
        range: impl std::ops::RangeBounds<Version> + Send,
        mut init: T,
        apply: impl Fn(&mut T, &Event),
    ) -> Result<(T, Option<Version>)> {
        let (lo, hi) = bounds(range);
        let mut last = None;
        let events = resolve_history(&*self.db, stream, &mut Default::default(), lo, hi).await?;
        for e in &events {
            apply(&mut init, e);
            last = Some(e.version);
        }
        Ok((init, last))
    }

    /// Folds `stream` into an `A`, starting from the latest snapshot if any.
    /// Convenience adapter over [`fold`](Self::fold); `snapshot` bytes are
    /// opaque and may be a locator into external storage.
    pub async fn rehydrate<A: Aggregate>(&self, stream: &str) -> Result<(A, Option<Version>)> {
        match self.latest_snapshot(stream).await? {
            Some(s) => {
                let (a, tail) = self
                    .fold(stream, s.through_version + 1.., A::restore(&s.state)?, A::apply)
                    .await?;
                Ok((a, tail.or(Some(s.through_version))))
            }
            None => self.fold(stream, 0.., A::default(), A::apply).await,
        }
    }

    /// Folds `stream`'s full logical history, snapshots it with `build`, and
    /// publishes a [`CompactionRecord`] for listeners.
    ///
    /// Compaction is logical: events aren't deleted. What you get is fast
    /// rehydration plus an observable record.
    ///
    /// Call this inline or from a worker, gated on
    /// [`compaction_backlog`](Self::compaction_backlog). Publication is
    /// idempotent: the latest snapshot wins and partial attempts leave no
    /// visible record.
    pub async fn compact_with(
        &self,
        stream: &str,
        build: impl FnOnce(&[Event]) -> Result<Bytes>,
    ) -> Result<CompactionRecord> {
        self.compact_inner(stream, None, build).await
    }

    /// [`compact_with`](Self::compact_with) answering a claimed compaction job.
    pub async fn compact_job(
        &self,
        stream: &str,
        job_id: String,
        build: impl FnOnce(&[Event]) -> Result<Bytes>,
    ) -> Result<CompactionRecord> {
        self.compact_inner(stream, Some(job_id), build).await
    }

    /// Fold-based compaction for [`Aggregate`]s.
    pub async fn compact<A: Aggregate>(&self, stream: &str) -> Result<CompactionRecord> {
        self.compact_with(stream, |events| {
            let mut state = A::default();
            for e in events {
                state.apply(e);
            }
            state.snapshot()
        })
        .await
    }

    async fn compact_inner(
        &self,
        stream: &str,
        job_id: Option<String>,
        build: impl FnOnce(&[Event]) -> Result<Bytes>,
    ) -> Result<CompactionRecord> {
        let events = self.read_history(stream, 0..).await?;
        let state = build(&events)?;
        let Some(last) = events.last() else {
            return Err(Error::InvalidInput(format!("'{stream}' has no events to compact")));
        };
        let record = CompactionRecord {
            stream: stream.into(),
            through_version: last.version,
            events_compacted: events.len() as u64,
            job_id,
            ts_ms: crate::envelope::now_ms(),
        };
        // The folded bytes go straight into slog's own durable store, so
        // everything about this snapshot is verifiable in-store. Publish
        // atomically: pointer + announcement in one transaction.
        self.publish_snapshot(stream, record.clone(), state).await?;
        Ok(record)
    }

    /// Publishes an **already durable** snapshot for `stream` and announces
    /// the compaction — atomically, in one transaction.
    ///
    /// Application code produces and durably stores the snapshot state;
    /// slog publishes the pointer. Any data referenced by `state` must be
    /// durable before this call. Use content-addressed locators or checksums
    /// in `state` when readers need integrity for external bytes.
    ///
    /// If the call is never reached, external snapshot data remains the
    /// application's responsibility. The publication transaction is atomic.
    pub async fn publish_snapshot(
        &self,
        stream: &str,
        record: CompactionRecord,
        state: Bytes,
    ) -> Result<()> {
        let txn = self.begin().await?;
        let seq = kv::get_be(&txn, kv::SEQ_KEY).await?.unwrap_or(0) + 1;
        let mut snap_key = kv::snap_prefix(stream);
        snap_key.extend_from_slice(format!("{seq:016}").as_bytes());
        txn.put(
            snap_key,
            serde_json::to_vec(&SnapshotRecord {
                through_version: record.through_version,
                ts_ms: record.ts_ms,
                state,
            })?,
        )?;
        txn.put(
            format!("{COMPACTIONS_PREFIX}{}", ulid::Ulid::new()),
            serde_json::to_vec(&record)?,
        )?;
        // The pointer must be as durable as the data it points at: callers
        // made their blobs durable before publishing (data-before-pointer),
        // so publishing completes the contract with the durability barrier.
        if let Some(wal_seq) = commit_seq(txn, seq).await? {
            self.await_seq(wal_seq).await?;
        }
        Ok(())
    }

    /// Requests compaction of `stream`; returns the job id. Requests are
    /// fire-and-forget durable facts — claiming, timing out, and completing
    /// are all derived from the journal.
    pub async fn request_compaction(&self, stream: &str) -> Result<String> {
        let job_id = ulid::Ulid::new().to_string();
        self.write_job(JobEvent::requested(job_id.clone(), stream.into()))
            .await?;
        Ok(job_id)
    }

    /// Atomically claims a compaction job for `worker`: the job's records
    /// are read and the claim written in one serializable transaction, so
    /// concurrent claims cannot both win. Fails unless the job is
    /// [`JobStatus::Pending`].
    pub async fn claim_compaction(&self, job_id: &str, worker: &str) -> Result<()> {
        let txn = self.begin().await?;
        let events = job_events(&txn, job_id).await?;
        match crate::jobs::fold(events.iter()) {
            Some(JobStatus::Pending) => {
                txn.put(
                    job_key(),
                    serde_json::to_vec(&JobEvent::claimed(job_id.into(), worker.into()))?,
                )?;
                txn.commit().await?;
                Ok(())
            }
            other => Err(Error::InvalidInput(format!(
                "job '{job_id}' is not claimable: {other:?}"
            ))),
        }
    }

    /// Marks a job failed. Compaction compute is yours; so is reporting its
    /// failure.
    pub async fn fail_compaction(&self, job_id: &str, error: impl Into<String>) -> Result<()> {
        self.write_job(JobEvent::failed(job_id.into(), error.into()))
            .await
    }

    /// Derived status of a job: `None` if unknown, else the latest of its
    /// completed (a [`CompactionRecord`] with this `job_id`) or journaled
    /// state.
    pub async fn job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if self
            .compaction_records()
            .await?
            .iter()
            .any(|r| r.job_id.as_deref() == Some(job_id))
        {
            return Ok(Some(JobStatus::Completed));
        }
        let events = job_events(&*self.db, job_id).await?;
        Ok(crate::jobs::fold(events.iter()))
    }

    async fn write_job(&self, event: JobEvent) -> Result<()> {
        self.db.put(job_key(), serde_json::to_vec(&event)?).await?;
        Ok(())
    }

    /// Events not yet covered by any snapshot — the entire input a compaction
    /// policy needs (`if backlog > threshold { compact(...) }`).
    pub async fn compaction_backlog(&self, stream: &str) -> Result<u64> {
        let tip = self.stream_version(stream).await?.map_or(0, |v| v + 1);
        let snap = self
            .latest_snapshot(stream)
            .await?
            .map_or(0, |s| s.through_version + 1);
        Ok(tip.saturating_sub(snap))
    }

    /// All stream ids in the store — the enumeration input for a compaction
    /// sweeper (`for stream in list_streams() { if backlog > n { compact() } }`).
    /// Deleted streams (tombstoned) are omitted.
    pub async fn list_streams(&self) -> Result<Vec<String>> {
        let mut it = self.db.scan_prefix(b"m/", ..).await?;
        let mut streams = Vec::new();
        let mut deleted = HashSet::new();
        while let Some(kv) = it.next().await? {
            let key = String::from_utf8(kv.key.to_vec())
                .map_err(|e| Error::InvalidInput(format!("non-utf8 stream key: {e}")))?;
            let Some(rest) = key.strip_prefix("m/") else {
                continue;
            };
            if let Some(stream) = rest.strip_suffix("/tail") {
                streams.push(stream.into());
            } else if let Some(stream) = rest.strip_suffix("/deleted") {
                deleted.insert(stream.to_string());
            }
        }
        streams.retain(|s| !deleted.contains(s));
        Ok(streams)
    }

    /// Latest snapshot for `stream`, if any.
    pub async fn latest_snapshot(&self, stream: &str) -> Result<Option<SnapshotRecord>> {
        kv::latest_snapshot(&*self.db, stream).await
    }

    /// All compaction records written so far.
    pub async fn compaction_records(&self) -> Result<Vec<CompactionRecord>> {
        kv::KvRead::scan_prefix(&*self.db, COMPACTIONS_PREFIX.as_bytes().to_vec())
            .await?
            .into_iter()
            .map(|(_, value)| Ok(serde_json::from_slice(&value)?))
            .collect()
    }

    /// Starts a cross-stream transaction: queue appends/fences, then
    /// [`commit`](Self::commit) them atomically.
    pub fn transaction(&self) -> Transaction {
        Transaction { ops: Vec::new() }
    }

    /// Replays the transaction's ops into one serializable SlateDB
    /// transaction — per-stream version checks, fence checks, sequence and
    /// tail bookkeeping included — and commits once. Returns one
    /// `CommitInfo` per append op, in op order.
    pub async fn commit(&self, txn: Transaction) -> Result<Vec<CommitInfo>> {
        for op in &txn.ops {
            if let Op::Append { events, .. } = op {
                check_batch(events)?;
            }
        }
        let db_txn = self.begin().await?;
        let mut seq = kv::get_be(&db_txn, kv::SEQ_KEY).await?.unwrap_or(0);
        let mut infos = Vec::new();
        for op in txn.ops {
            match op {
                Op::Append {
                    stream,
                    expected,
                    events,
                } => infos.push(
                    replay_append(&db_txn, &mut seq, &stream, expected, None, None, events, true)
                        .await?,
                ),
                Op::Fence { stream, token } => {
                    replay_fence(&db_txn, &stream, token.as_deref())?
                }
            }
        }
        commit_seq(db_txn, seq).await?;
        Ok(infos)
    }

    /// Forces all prior writes to durable object storage.
    pub async fn flush(&self) -> Result<()> {
        Ok(self.db.flush().await?)
    }

    /// The global sequence counter: records with seq `<= N` are committed.
    /// With transaction commits the commit ack IS the durability boundary —
    /// kept for API parity ([`append_durable`](Self::append_durable) users
    /// can correlate `CommitInfo::start_sequence` against it).
    pub async fn durable_sequence(&self) -> Result<u64> {
        Ok(kv::get_be(&*self.db, kv::SEQ_KEY).await?.unwrap_or(0))
    }

    /// Flushes and closes the database.
    pub async fn close(&self) -> Result<()> {
        Ok(self.db.close().await?)
    }

    async fn begin(&self) -> Result<DbTransaction> {
        Ok(self.db.begin(IsolationLevel::SerializableSnapshot).await?)
    }

    /// Conditional append in one serializable transaction, committed without
    /// the durability wait (a [`CommitTicket`] for
    /// [`await_durable`](Self::await_durable)). Commit conflicts retry against
    /// a fresh snapshot; the expected-version check runs again against the
    /// current tail.
    async fn commit_appends(
        &self,
        stream: &str,
        expected: ExpectedVersion,
        from_version: Option<Version>,
        fence_token: Option<&str>,
        events: Vec<NewEvent>,
        index_ts: bool,
    ) -> Result<CommitTicket> {
        check_batch(&events)?;
        let mut attempt = 0;
        loop {
            let txn = self.begin().await?;
            let mut seq = kv::get_be(&txn, kv::SEQ_KEY).await?.unwrap_or(0);
            let info = replay_append(
                &txn,
                &mut seq,
                stream,
                expected,
                from_version,
                fence_token,
                events.clone(),
                index_ts,
            )
            .await?;
            match commit_seq(txn, seq).await {
                Ok(wal_seq) => {
                    return Ok(CommitTicket {
                        info,
                        seq: wal_seq.expect("append always writes"),
                    })
                }
                Err(e) if e.kind() == ErrorKind::Transaction && attempt < COMMIT_RETRIES => {
                    attempt += 1;
                }
                Err(e) => {
                    return Err(match e.kind() {
                        ErrorKind::Transaction => Error::VersionConflict {
                            stream: stream.into(),
                            expected,
                            actual: kv::tail(&*self.db, stream).await?.map(|t| t.version),
                        },
                        _ => Error::Backend(e),
                    })
                }
            }
        }
    }

    /// The shared durability barrier, on the backend's WAL-flush watermark.
    async fn await_seq(&self, seq: u64) -> Result<()> {
        let mut sub = self.db.subscribe();
        let status = sub
            .wait_for(|s| s.durable_seq >= seq || s.close_reason.is_some())
            .await
            .map_err(|_| {
                slatedb::Error::closed("watermark watcher closed".into(), slatedb::CloseReason::Clean)
            })?;
        if status.durable_seq < seq {
            return Err(slatedb::Error::closed(
                format!("closed before seq {seq} durable"),
                status.close_reason.unwrap_or(slatedb::CloseReason::Clean),
            )
            .into());
        }
        Ok(())
    }
}

/// slog's default SlateDB profile, tuned against the churn probe
/// (`slog-sqlite/examples/churn`, run over real S3):
///
/// - 10ms WAL flush tick (stock: 100ms). `await_durable` resolves on the
///   tick, so the tick is the sync latency floor: ~100ms → ~10ms locally,
///   and it stops being the dominating term over the network on S3.
/// - 4MB memtable freeze (stock: 64MB). WAL segments roll into compacted L0
///   SSTs continuously, keeping the next open's replay tail short:
///   fresh-VM open on a churned db went ~6.5s → ~1.6s on S3.
///
/// Stock SlateDB defaults remain available via `Some(Settings::default())`.
pub fn default_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(std::time::Duration::from_millis(10)),
        l0_sst_size_bytes: 4 * 1024 * 1024,
        ..Default::default()
    }
}

/// Stamps the global sequence counter `g/seq` and commits `txn` without the
/// durability wait; the returned WAL sequence is [`EventStore::await_seq`]'s
/// watermark target. `g/seq` is unmarked for conflict detection: it is a
/// shared counter, so tracking it would make independent streams collide.
/// Streams keep strict ordering through their (tracked) tail keys; the price
/// is that cross-stream `seq` values may duplicate under concurrent appends,
/// which all `seq` consumers (rollback watermarks, `durable_sequence` parity)
/// tolerate because they only ever compare within one stream.
async fn commit_seq(txn: DbTransaction, seq: u64) -> std::result::Result<Option<u64>, slatedb::Error> {
    txn.put(kv::SEQ_KEY, seq.to_be_bytes())?;
    txn.unmark_write([kv::SEQ_KEY])?;
    let handle = txn
        .commit_with_options(&WriteOptions {
            await_durable: false,
            ..Default::default()
        })
        .await?;
    Ok(handle.map(|h| h.seqnum()))
}

/// Replays one append op's checks and writes into `txn`, assigning versions,
/// global sequences (`seq` is the cursor for `g/seq`, bumped in place), and
/// monotonic per-stream timestamps; updates the stream tail key.
async fn replay_append(
    txn: &DbTransaction,
    seq: &mut u64,
    stream: &str,
    expected: ExpectedVersion,
    from_version: Option<Version>,
    fence_token: Option<&str>,
    events: Vec<NewEvent>,
    index_ts: bool,
) -> Result<CommitInfo> {
    let tail = kv::tail(txn, stream).await?;
    let current = tail.as_ref().map(|t| t.version);
    if let Some(token) = fence_token {
        if let Some(bytes) = txn.get(kv::m_key(stream, "fence")).await? {
            let current_token = String::from_utf8_lossy(&bytes).into_owned();
            if current_token != token {
                return Err(Error::FenceMismatch {
                    stream: stream.into(),
                    current_token,
                });
            }
        }
    }
    let ok = match expected {
        ExpectedVersion::Any => true,
        ExpectedVersion::NoStream => current.is_none(),
        ExpectedVersion::Exact(v) => current == Some(v),
    };
    if !ok {
        return Err(Error::VersionConflict {
            stream: stream.into(),
            expected,
            actual: current,
        });
    }
    if events.is_empty() {
        return Err(Error::InvalidInput("empty batch".into()));
    }
    let first = from_version.unwrap_or_else(|| current.map_or(0, |v| v + 1));
    let n = events.len() as u64;
    let last = first + n - 1;
    let start_seq = *seq + 1;
    // Timestamps never move backwards within a stream (may be equal).
    let mut last_ts = tail.as_ref().map_or(0, |t| t.ts_ms);
    for (i, e) in events.into_iter().enumerate() {
        let version = first + i as u64;
        let mut env = Envelope::new(e, version);
        env.seq = start_seq + i as u64;
        env.ts_ms = env.ts_ms.max(last_ts);
        last_ts = env.ts_ms;
        txn.put(kv::r_key(stream, version), env.encode()?.as_ref())?;
        if index_ts {
            txn.put(kv::ts_key(stream, env.ts_ms, version), [])?;
        }
    }
    *seq = start_seq + n - 1;
    let tail = TailRecord {
        version: current.map_or(last, |c: Version| c.max(last)),
        ts_ms: last_ts,
    };
    txn.put(kv::m_key(stream, "tail"), serde_json::to_vec(&tail)?)?;
    Ok(CommitInfo {
        first_version: first,
        last_version: last,
        start_sequence: start_seq,
    })
}

async fn job_events(r: &impl kv::KvRead, job_id: &str) -> Result<Vec<JobEvent>> {
    let mut out = Vec::new();
    for (_, value) in r.scan_prefix(JOBS_PREFIX.as_bytes().to_vec()).await? {
        let e: JobEvent = serde_json::from_slice(&value)?;
        if e.job_id() == job_id {
            out.push(e);
        }
    }
    Ok(out)
}

fn job_key() -> String {
    format!("{JOBS_PREFIX}{}", ulid::Ulid::new())
}

/// Bookkeeping `CommitInfo` for state-only ops (fence/trim): the stream's
/// current tail as first/last (0 when empty), no sequence.
fn tail_commit_info(tail: Option<TailRecord>) -> CommitInfo {
    let v = tail.map_or(0, |t| t.version);
    CommitInfo {
        first_version: v,
        last_version: v,
        start_sequence: 0,
    }
}

fn check_batch(events: &[NewEvent]) -> Result<()> {
    if events.len() > MAX_BATCH_RECORDS {
        return Err(Error::InvalidInput(format!(
            "batch of {} records exceeds max {MAX_BATCH_RECORDS}",
            events.len()
        )));
    }
    if let Some(e) = events.iter().find(|e| e.data.len() > MAX_RECORD_BYTES) {
        return Err(Error::InvalidInput(format!(
            "record data of {} bytes exceeds max {MAX_RECORD_BYTES}",
            e.data.len()
        )));
    }
    Ok(())
}

/// Sets or clears the stream fence key within `txn`.
fn replay_fence(txn: &DbTransaction, stream: &str, token: Option<&str>) -> Result<()> {
    let key = kv::m_key(stream, "fence");
    match token.filter(|t| !t.is_empty()) {
        Some(t) => txn.put(key, t.as_bytes())?,
        None => txn.delete(key)?,
    }
    Ok(())
}

pub(crate) fn bounds(range: impl std::ops::RangeBounds<Version>) -> (Version, Option<Version>) {
    use std::ops::Bound::*;
    let lo = match range.start_bound() {
        Included(&v) => v,
        Excluded(&v) => v + 1,
        Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Included(&v) => Some(v + 1),
        Excluded(&v) => Some(v),
        Unbounded => None,
    };
    (lo, hi)
}
