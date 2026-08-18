//! Durable-stream semantics: sequence assignment, timestamp monotonicity,
//! trim points, bounded reads, tombstoned streams.

use landslide::{EventStore, ExpectedVersion, NewEvent, Result};

fn rec(n: u64) -> NewEvent {
    NewEvent::new("r", format!("r{n}"))
}

async fn payloads(store: &EventStore, stream: &str) -> Vec<String> {
    store
        .read_stream(stream, ..)
        .await
        .unwrap()
        .iter()
        .map(|e| String::from_utf8(e.data.to_vec()).unwrap())
        .collect()
}

#[tokio::test]
async fn records_get_contiguous_per_stream_sequences() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    let c = store.append("s", ExpectedVersion::NoStream, vec![rec(1), rec(2)]).await?;
    assert_eq!((c.first_version, c.last_version), (0, 1));
    let c = store.append("s", ExpectedVersion::Exact(1), vec![rec(3)]).await?;
    assert_eq!((c.first_version, c.last_version), (2, 2));

    let events = store.read_stream("s", ..).await?;
    assert_eq!(
        events.iter().map(|e| e.version).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(events[0].global_seq < events[1].global_seq);
    assert!(events[1].global_seq < events[2].global_seq);
    Ok(())
}

#[tokio::test]
async fn timestamps_are_monotonic_in_stream_order() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    for i in 0..10u64 {
        store.append("s", ExpectedVersion::Any, vec![rec(i)]).await?;
    }
    let ts: Vec<i64> = store
        .read_stream("s", ..)
        .await?
        .iter()
        .map(|e| e.ts_ms)
        .collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]), "ts: {ts:?}");
    Ok(())
}

#[tokio::test]
async fn bounded_reads_slice_by_sequence_window() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("s", ExpectedVersion::NoStream, (0..10).map(rec).collect()).await?;
    let window: Vec<_> = payloads(&store, "s").await;
    assert_eq!(
        store
            .read_stream("s", 3..7)
            .await?
            .iter()
            .map(|e| String::from_utf8(e.data.to_vec()).unwrap())
            .collect::<Vec<_>>(),
        window[3..7]
    );
    Ok(())
}

#[tokio::test]
async fn trim_point_hides_older_records_and_stream_survives() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("s", ExpectedVersion::NoStream, (0..5).map(rec).collect()).await?;

    // Trim everything below v2: first two records gone, stream lives on.
    store.trim_below("s", 2).await?;
    assert_eq!(payloads(&store, "s").await, ["r2", "r3", "r4"]);

    // A stale (smaller) trim point changes nothing.
    store.trim_below("s", 1).await?;
    assert_eq!(payloads(&store, "s").await, ["r2", "r3", "r4"]);

    // Trimming past the tail empties the visible stream but doesn't delete it:
    // later appends continue numbering and are readable.
    store.trim_below("s", u64::MAX).await?;
    assert!(payloads(&store, "s").await.is_empty());
    let c = store.append("s", ExpectedVersion::Any, vec![rec(99)]).await?;
    assert_eq!(c.first_version, 5); // trims are state keys; no records consumed
    assert_eq!(payloads(&store, "s").await, ["r99"]);
    Ok(())
}

#[tokio::test]
async fn trim_points_do_not_touch_other_streams() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("a", ExpectedVersion::NoStream, (0..3).map(rec).collect()).await?;
    store.append("b", ExpectedVersion::NoStream, (0..3).map(rec).collect()).await?;
    store.trim_below("a", 3).await?;
    assert!(payloads(&store, "a").await.is_empty());
    assert_eq!(payloads(&store, "b").await, ["r0", "r1", "r2"]);
    Ok(())
}

#[tokio::test]
async fn tombstoned_stream_is_invisible_and_unlisted() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("live", ExpectedVersion::NoStream, (0..2).map(rec).collect()).await?;
    store.append("doomed", ExpectedVersion::NoStream, (0..2).map(rec).collect()).await?;

    store.delete_stream("doomed").await?;
    assert!(payloads(&store, "doomed").await.is_empty());
    assert_eq!(payloads(&store, "live").await, ["r0", "r1"]);
    assert_eq!(store.list_streams().await?, ["live"]);
    Ok(())
}
