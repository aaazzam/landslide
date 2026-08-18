//! Bank account: append a batch atomically, tail the stream live, then
//! compact the stream and watch a listener react to the compaction event.
//!
//! Run: cargo run --example bank_account

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use landslide::{
    Aggregate, Config, Event, EventStore, EventStoreReader, ExpectedVersion, NewEvent,
    ReaderConfig, Result,
};

#[derive(Serialize, Deserialize)]
enum AccountEvent {
    Opened { owner: String },
    Deposited { cents: u64 },
    Withdrawn { cents: u64 },
}

#[derive(Default, Serialize, Deserialize)]
struct Account {
    owner: String,
    balance: i64,
}

impl Aggregate for Account {
    fn apply(&mut self, event: &Event) {
        match event.json().expect("in-band event") {
            AccountEvent::Opened { owner } => self.owner = owner,
            AccountEvent::Deposited { cents } => self.balance += cents as i64,
            AccountEvent::Withdrawn { cents } => self.balance -= cents as i64,
        }
    }
    fn snapshot(&self) -> Result<Bytes> {
        Ok(serde_json::to_vec(self)?.into())
    }
    fn restore(state: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(state)?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Use an in-memory store for the example; production can use object storage.
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let store = EventStore::open(Config { path: "bank_account".into(), object_store: object_store.clone(), settings: None }).await?;
    // Tailing runs through the read-only reader, which can live in a separate process.
    let reader = EventStoreReader::open(ReaderConfig { path: "bank_account".into(), object_store, options: None }).await?;

    // One atomic batch: account opened + first deposit land together or not at all.
    // A second writer hitting ExpectedVersion::NoStream now gets a VersionConflict.
    let commit = store
        .append(
            "account-42",
            ExpectedVersion::NoStream,
            vec![
                NewEvent::json("opened", &AccountEvent::Opened { owner: "ada".into() })?,
                NewEvent::json("deposited", &AccountEvent::Deposited { cents: 10_000 })?,
            ],
        )
        .await?;
    println!("committed versions {}..={}", commit.first_version, commit.last_version);

    // Command-side rehydrate + optimistic-concurrency append.
    let (account, version) = store.rehydrate::<Account>("account-42").await?;
    println!("{} has {} cents", account.owner, account.balance);
    store
        .append(
            "account-42",
            ExpectedVersion::Exact(version.unwrap()),
            vec![NewEvent::json("withdrawn", &AccountEvent::Withdrawn { cents: 2_500 })?],
        )
        .await?;

    // Tail the stream via the reader: it polls object storage for new commits.
    let mut tail = reader.follow_stream("account-42", 0, Duration::from_millis(50));
    store.append(
        "account-42",
        ExpectedVersion::Exact(2),
        vec![NewEvent::json("deposited", &AccountEvent::Deposited { cents: 500 })?],
    )
    .await?;
    let tailed = async {
        while let Some(Ok(e)) = tail.recv().await {
            println!("tailed v{} {}", e.version, e.event_type);
            if e.version == 3 {
                return;
            }
        }
    };
    // DbReader discovers commits on its manifest poll (10 seconds by default).
    tokio::time::timeout(Duration::from_secs(30), tailed).await.expect("tail timed out");

    // Compact: write a snapshot and emit a compaction event. Listeners on the
    // $compactions stream react however they like.
    let record = store.compact::<Account>("account-42").await?;
    println!("compacted through v{} ({})", record.through_version, record.stream);

    // Rehydration now uses the published snapshot as its starting point.
    let (account, version) = store.rehydrate::<Account>("account-42").await?;
    println!("rehydrated at v{:?}: {} cents", version, account.balance);
    Ok(())
}
