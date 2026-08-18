//! Durability split from commit: `CommitTicket`s, group commit, ts-index opt-out.

use std::sync::Arc;

use slog::{Config, EventStore, ExpectedVersion, NewEvent};

fn ev(data: &str) -> NewEvent {
    NewEvent::new("test", data.to_string())
}

/// Lazy appends commit without waiting for object storage; awaiting their
/// tickets later is the group-commit primitive: one shared WAL flush lands
/// them all, and a fresh opener then sees every batch.
#[tokio::test]
async fn lazy_appends_share_one_durability_barrier() {
    let bucket = Arc::new(object_store::memory::InMemory::new());
    let config = || Config { path: "db".into(), object_store: bucket.clone(), settings: None };
    let store = EventStore::open(config()).await.unwrap();

    let mut tickets = Vec::new();
    for i in 0..20 {
        tickets.push(
            store
                .append_lazy(&format!("g{}", i % 4), ExpectedVersion::Any, vec![ev(&format!("e{i}"))])
                .await
                .unwrap(),
        );
    }
    for t in &tickets {
        store.await_durable(t).await.unwrap();
    }
    assert_eq!(tickets[3].info.first_version, tickets[3].info.last_version);
    drop(store);

    let store2 = EventStore::open(config()).await.unwrap();
    for s in 0..4 {
        assert_eq!(store2.read_stream(&format!("g{s}"), ..).await.unwrap().len(), 5);
    }
}

/// Applications that never seek by time do not need a second key per event.
#[tokio::test]
async fn unindexed_appends_skip_the_time_index() {
    let store = EventStore::open_in_memory().await.unwrap();
    let t = store
        .append_with_token_lazy("s", "w", ExpectedVersion::NoStream, vec![ev("a")], false)
        .await
        .unwrap();
    store.await_durable(&t).await.unwrap();
    let mut it = store.db().scan_prefix(b"i/" as &[u8], ..).await.unwrap();
    assert!(it.next().await.unwrap().is_none(), "unindexed append wrote ts keys");
    assert!(store.seek_timestamp("s", 0).await.unwrap().is_none());

    store.append("t", ExpectedVersion::NoStream, vec![ev("b")]).await.unwrap();
    let mut it = store.db().scan_prefix(b"i/" as &[u8], ..).await.unwrap();
    assert!(it.next().await.unwrap().is_some(), "default append lost its ts key");
}
