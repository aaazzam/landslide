# slog

Durable, ordered event streams backed by object storage.

`slog` is a Rust event-store core built on [SlateDB](https://slatedb.io). It
provides atomic appends, optimistic concurrency, snapshots, forks, retention,
and read-only scale-out while leaving application state and snapshot formats
under application control.

> **Status:** early-stage software (`0.1.0`). The API and on-disk format may
> change while the project settles.

## Why slog?

Most event stores assume a long-running server owns the data path. `slog`
keeps the durable state in an object store and exposes a small Rust API over
that state:

- one serializable transaction per mutation;
- conditional, versioned appends for optimistic concurrency;
- one writer handle with any number of read-only readers;
- snapshots as opaque application-owned bytes;
- metadata-only forks with pinned, range-resolved history;
- logical compaction and opt-in physical retention;
- no service process or database cluster to operate.

The core is deliberately agnostic about the state being rebuilt. An account,
a filesystem image, a SQLite database, or a projection can use the same
stream primitives.

## At a glance

```text
                         object storage
                    ┌────────────────────┐
                    │ SlateDB namespace  │
                    └─────────┬──────────┘
                              │
             serializable    │    range scans / polling
             transactions     │
                              │
                 ┌────────────┴────────────┐
                 │                         │
          EventStore                  EventStoreReader
          writer handle                read-only handle
```

The writer commits through SlateDB transactions. Readers use SlateDB's
read-only view over the same object store and can be placed in separate
processes or machines. Reader visibility is bounded by the configured
manifest-poll interval.

## Features

### Atomic appends and concurrency

An append is a batch: all events commit together or none do. The expected
version is checked in the same serializable transaction as the write.

```rust
use slog::{EventStore, ExpectedVersion, NewEvent};

let commit = store
    .append(
        "account-42",
        ExpectedVersion::Exact(7),
        vec![NewEvent::json("deposited", &serde_json::json!({"cents": 500}))?],
    )
    .await?;
```

Use `ExpectedVersion::NoStream` for a create-if-empty operation,
`ExpectedVersion::Exact(version)` for compare-and-swap semantics, or
`ExpectedVersion::Any` when the caller does not need a version check.

Cross-stream appends and fence operations can share one transaction:

```rust
let mut transaction = store.transaction();
transaction
    .append("accounts", ExpectedVersion::Exact(7), account_events)
    .append("ledger", ExpectedVersion::Exact(19), ledger_events);

store.commit(transaction).await?;
```

### Explicit durability

The API separates commit visibility from the object-storage durability wait.
This makes group commit possible without hiding the durability boundary.

| Method | Returns when |
| --- | --- |
| `append` / `append_with_token` | The transaction has committed and the batch is visible to the store |
| `append_lazy` / `append_with_token_lazy` | A `CommitTicket` is available for a later durability wait |
| `await_durable(&ticket)` | The ticket's batch is durable in object storage |
| `append_durable` / `append_with_token_durable` | The batch is durable in object storage |
| `flush` | Prior writes have crossed the store's durability barrier |

Tickets on one store share a durability watermark. Waiting on the highest
ticket covers the earlier tickets as well.

### Rehydration, snapshots, and compaction

Implement `Aggregate` when a stream maps naturally to a typed state machine:

```rust
use bytes::Bytes;
use slog::{Aggregate, Event, Result};

#[derive(Default)]
struct Counter(u64);

impl Aggregate for Counter {
    fn apply(&mut self, event: &Event) {
        if event.event_type == "incremented" {
            self.0 += 1;
        }
    }

    fn snapshot(&self) -> Result<Bytes> {
        Ok(self.0.to_be_bytes().to_vec().into())
    }

    fn restore(bytes: &[u8]) -> Result<Self> {
        let value = u64::from_be_bytes(bytes.try_into().map_err(|_| {
            slog::Error::InvalidInput("counter snapshot must be 8 bytes".into())
        })?);
        Ok(Self(value))
    }
}

let (counter, through) = store.rehydrate::<Counter>("counter").await?;
```

`compact::<A>` publishes a snapshot and a `CompactionRecord`. Subsequent
rehydration starts from that snapshot and folds only the remaining events.
For application-defined state, use `fold`, `compact_with`, and
`publish_snapshot`. Snapshot bytes are opaque; they may be inline state or a
content-addressed locator into another object-store prefix.

Compaction can run inline or through the journaled job API:

```rust
let job_id = store.request_compaction("account-42").await?;
store.claim_compaction(&job_id, "worker-1").await?;
// Produce and publish the snapshot with compact_job(...).
```

Publishing an external snapshot follows a data-before-pointer contract: every
object referenced by the snapshot must be durable before
`publish_snapshot` commits its pointer.

### Forks and history branches

Forking records metadata and a pin; it does not copy events:

```rust
store.fork("main", 42, "experiment").await?;
```

`read_stream` returns the stream's local records. `read_history` and
`rehydrate` resolve the pinned parent chain and the branch's own events.
Parent appends after the fork point are not visible to the child. Compaction
can flatten a branch into a self-contained snapshot.

### Retention

- `trim_below` hides older events while preserving the stream and its version
  sequence.
- `purge_below` physically removes older event bytes in bounded transactions.
- Live fork pins protect the history still required by a branch.
- `delete_stream` is logical deletion; a deleted stream id is terminal.

Physical purge is irreversible for the affected history, so it is an explicit
operation rather than part of ordinary compaction.

## Storage model

The core uses ordered key prefixes inside a SlateDB namespace:

```text
r/{stream}/{version:016}          event envelope
m/{stream}/tail                   stream tail and timestamp
m/{stream}/fence                  writer fence token
m/{stream}/trim                   logical trim floor
m/{stream}/fork                   parent stream and pinned version
m/{stream}/forks/{child}          live fork pin
m/{stream}/rollback/{from}        rollback watermark
i/{stream}/{timestamp}/{version}  optional timestamp index
snap/{stream}/{sequence}          snapshot record
j/compactions/{ulid}              compaction announcement
g/seq                             global commit sequence
```

Version-ordered event keys let range bounds reach the storage layer. Fork
resolution intersects the requested range with each segment of the fork
chain, so a reader does not need to scan unrelated history.

## Quick start

To explore the repository locally:

```sh
git clone https://github.com/aaazzam/slog.git
cd slog
cargo test --workspace --all-targets
```

Add the core crate from GitHub:

```toml
[dependencies]
slog = { git = "https://github.com/aaazzam/slog" }
bytes = "1"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Then append and read a stream:

```rust
use slog::{EventStore, ExpectedVersion, NewEvent};

#[tokio::main]
async fn main() -> slog::Result<()> {
    let store = EventStore::open_in_memory().await?;

    let commit = store
        .append(
            "account-42",
            ExpectedVersion::NoStream,
            vec![NewEvent::json(
                "opened",
                &serde_json::json!({"owner": "ada"}),
            )?],
        )
        .await?;

    let history = store.read_history("account-42", ..).await?;
    println!(
        "committed versions {}..={} ({} event)",
        commit.first_version,
        commit.last_version,
        history.len()
    );
    Ok(())
}
```

The in-memory backend is useful for tests and examples. For a persistent
namespace, provide an `object_store::ObjectStore` and a path:

```rust
let store = slog::EventStore::open(slog::Config {
    path: "production/app".into(),
    object_store: bucket,
    settings: None,
})
.await?;
```

The default settings use a short WAL flush interval and small L0 freeze size
for responsive object-storage durability. Pass
`Some(slatedb::config::Settings::default())` when you want stock SlateDB
settings instead.

## Workspace crates

| Crate | Purpose |
| --- | --- |
| [`slog`](https://github.com/aaazzam/slog) | Core event streams, concurrency, snapshots, forks, readers, and retention |
| [`slog-fuse`](slog-fuse/) | Filesystem volumes, live replicas, FUSE mounts, and directory mirrors |
| [`slog-sqlite`](slog-sqlite/) | SQLite WAL capture, page-delta replication, checkpoints, and point-in-time restore |

### `slog-fuse`

`slog-fuse` maps a volume to a filesystem image. A `Volume` is the fenced
writer; a `Replica` is a read-only live view. The `Mirror` type materializes a
replica into a normal directory and needs no FUSE support, which is useful in
containers and restricted environments.

The optional FUSE adapter requires libfuse on Linux or macFUSE on macOS:

```sh
# Writable FUSE mount
cargo run -p slog-fuse --features fuse --bin slogfs -- mount my-volume /mnt/slog

# Read-only FUSE replica
cargo run -p slog-fuse --features fuse --bin slogfs -- follow my-volume /mnt/slog

# Directory mirror; no FUSE feature required
cargo run -p slog-fuse --bin slogfs -- mirror follow my-volume /tmp/slog-rootfs
```

The CLI uses a local directory at `/tmp/slog-bucket` by default. Set
`SLOG_BUCKET` for S3, or `SLOG_BUCKET_DIR` for another local directory;
`SLOG_PATH` selects the SlateDB namespace.

### `slog-sqlite`

`slog-sqlite` keeps a local SQLite database in WAL mode and streams committed
page post-images into a slog stream. Checkpoints seal page deltas into
compressed LTX segments and publish a manifest. A new process can reconstruct
the database from the manifest plus the remaining delta backlog.

The adapter supports:

- single-writer fencing per database name;
- incremental `sync` and coalesced checkpoints;
- optional physical purge of events covered by a checkpoint;
- point-in-time restore with `restore_at` while history is retained;
- reconstruction into a fresh local database file.

See [`slog-sqlite/examples/churn.rs`](slog-sqlite/examples/churn.rs) and the
integration tests for complete setups.

## Configuration and deployment notes

- `Config::path` is the SlateDB namespace inside the object store. Use a
  distinct path for each logical database.
- `ReaderConfig::options` controls manifest polling and reader caches. Lower
  `manifest_poll_interval` for lower tailing latency.
- `fence` is durable and token-aware appends enforce the token. Plain
  `append` is intentionally cooperative and does not require a fence token.
- `read_history` is the branch-resolved view; use `read_stream` when you need
  the stream's local physical records.
- Snapshot bytes are not interpreted by slog. If they reference external
  objects, publish only after those objects are durable.
- A single event payload is capped at 1 MiB and an append batch at 1,000
  records. Adapters chunk larger payloads before appending.
- Retention can make point-in-time reads impossible. Keep fork pins and purge
  floors aligned with the recovery window you need.

## Development

```sh
cargo test --workspace --all-targets
cargo run --example bank_account
cargo run --example fs_snapshots
```

The workspace also contains integration coverage for forks, lazy durability,
retention, concurrent writers, filesystem replicas, SQLite recovery, and
point-in-time restore. The S3 integration tests run when
`SLOG_TEST_BUCKET` is configured.

## License

MIT. See [LICENSE](LICENSE).
