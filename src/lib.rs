//! # slog — event streams on SlateDB / object storage
//!
//! Durable, atomic, ordered event streams backed by object storage through
//! [SlateDB](https://slatedb.io). One process writes ([`EventStore`]); any
//! number of readers ([`EventStoreReader`]) can read from object storage.
//!
//! - **Streams are key prefixes.** A stream id maps to
//!   `r/{stream}/{version:016}`; per-stream metadata (tail, fence, trim,
//!   tombstone, and fork pins) lives under `m/{stream}/`.
//! - **Atomic conditional appends.** [`EventStore::append`] commits a batch
//!   iff the stream's version matches an [`ExpectedVersion`]
//!   (optimistic concurrency). Every mutation is one serializable SlateDB
//!   transaction; [`EventStore::transaction`] extends the same guarantees
//!   across streams.
//! - **Rehydration and tailing.** [`EventStore::rehydrate`] folds a stream
//!   into any [`Aggregate`]; [`EventStoreReader::follow_stream`] tails it
//!   live.
//! - **Forks.** [`EventStore::fork`] branches a stream at a pinned version
//!   with metadata only. [`EventStore::rehydrate`] and
//!   [`EventStore::read_history`] resolve the chain, and compaction flattens
//!   it.
//! - **Logical compaction.** [`EventStore::compact`] snapshots a stream's
//!   folded state (`snap/{stream}/`) and publishes a [`CompactionRecord`],
//!   which projections and caches can
//!   [`follow`](EventStoreReader::follow_compactions).
//! - **Range-bounded reads.** Event keys are version-ordered
//!   (`r/{stream}/{version:016}`), so reads push their ranges down to the
//!   store and fork-chain resolution intersects ranges per segment.
//! - **Retention.** [`EventStore::purge_below`] physically deletes events;
//!   live fork pins are honored.
//!
//! # Application state
//!
//! `slog` provides streams, versions, conditional appends, snapshot
//! registration, compaction events, and forks. Application code defines the
//! state and snapshot format. Snapshot bytes are opaque and may be a locator
//! into external storage such as LTX segments, runtime images, or filesystem
//! manifests. Use the following primitives to integrate application state:
//! [`EventStore::fold`] (custom rehydration), [`EventStore::compact_with`]
//! (custom snapshot production), and [`EventStore::publish_snapshot`]
//! (atomically publish snapshots produced by an external compactor; see its
//! data-before-pointer contract).
//!
//! ```no_run
//! use slog::{EventStore, ExpectedVersion, NewEvent};
//!
//! # #[tokio::main]
//! # async fn main() -> slog::Result<()> {
//! let store = EventStore::open_in_memory().await?; // or open(Config { path, object_store })
//! store.append("account-42", ExpectedVersion::NoStream, vec![
//!     NewEvent::json("opened", &serde_json::json!({"owner": "ada"}))?,
//! ]).await?;
//! store.flush().await?; // hard durability barrier (optional)
//! # Ok(())
//! # }
//! ```

mod aggregate;
mod envelope;
mod error;
pub mod fork;
pub mod jobs;
mod kv;
mod reader;
mod snapshot;
mod store;

pub use aggregate::Aggregate;
pub use envelope::{Event, ExpectedVersion, NewEvent, Version};
pub use error::Error;
pub use jobs::{JobEvent, JobStatus, JOBS_KEY};
pub use reader::{EventStoreReader, ReaderConfig};
pub use snapshot::{CompactionRecord, SnapshotRecord};
pub use store::{default_settings, CommitInfo, CommitTicket, Config, EventStore, PageLimit, Transaction};

/// Re-exports of the underlying crates, for configuration without adding
/// them yourself.
pub mod deps {
    pub use object_store;
    pub use slatedb;
}

pub type Result<T> = std::result::Result<T, Error>;
