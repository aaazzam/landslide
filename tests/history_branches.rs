//! Conformance tests for branch semantics: batched appends, range reads,
//! shadow appends (last-write-wins by transaction/global seq), forks,
//! trims, rollback windows, and refcounted branch deletion.

use slog::{Event, EventStore, ExpectedVersion, NewEvent, Result};

fn batch(tag: &str, vs: impl Iterator<Item = u64>) -> Vec<NewEvent> {
    vs.map(|v| NewEvent::new("e", format!("{tag}-{v}"))).collect()
}

fn data(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| String::from_utf8(e.data.to_vec()).unwrap())
        .collect()
}

async fn read_all(store: &EventStore, stream: &str) -> Vec<String> {
    data(&store.read_stream(stream, ..).await.unwrap())
}

async fn read_range(store: &EventStore, stream: &str, lo: u64, hi: u64) -> Vec<String> {
    data(&store.read_stream(stream, lo..hi).await.unwrap())
}

async fn read_history_range(store: &EventStore, stream: &str, lo: u64, hi: u64) -> Vec<String> {
    data(&store.read_history(stream, lo..hi).await.unwrap())
}

#[tokio::test]
async fn first_batch_is_selectable_by_range_and_in_full() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("b0", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    assert_eq!(read_range(&store, "b0", 0, 3).await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_all(&store, "b0").await, ["a-0", "a-1", "a-2"]);
    Ok(())
}

#[tokio::test]
async fn successive_batches_extend_the_branch() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("b0", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    store.append("b0", ExpectedVersion::Exact(2), batch("b", 3..5)).await?;
    assert_eq!(read_range(&store, "b0", 0, 3).await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_range(&store, "b0", 3, 5).await, ["b-3", "b-4"]);
    assert_eq!(
        read_all(&store, "b0").await,
        ["a-0", "a-1", "a-2", "b-3", "b-4"]
    );
    Ok(())
}

#[tokio::test]
async fn shadow_appends_are_last_write_wins() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("b0", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    store.append("b0", ExpectedVersion::Exact(2), batch("b", 3..5)).await?;
    // A later transaction rewrites versions 3..5: reads resolve to it.
    store.append_at("b0", 3, ExpectedVersion::Any, batch("c", 3..5)).await?;
    assert_eq!(read_range(&store, "b0", 0, 3).await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_range(&store, "b0", 3, 5).await, ["c-3", "c-4"]);
    assert_eq!(
        read_all(&store, "b0").await,
        ["a-0", "a-1", "a-2", "c-3", "c-4"]
    );
    Ok(())
}

#[tokio::test]
async fn forked_branches_diverge_and_read_independently() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("base", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    store.append("base", ExpectedVersion::Exact(2), batch("b", 3..5)).await?;

    // Branch at v2: shared prefix a-0..a-2, no marker record, own events from v3.
    store.fork("base", 2, "child").await?;
    store.append("child", ExpectedVersion::Exact(2), batch("c", 3..5)).await?;

    // Both branches read the shared prefix identically, then diverge.
    // (Fork-resolved reads go through read_history; read_stream is the raw view.)
    assert_eq!(read_range(&store, "base", 0, 3).await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_history_range(&store, "child", 0, 3).await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_range(&store, "base", 3, 5).await, ["b-3", "b-4"]);
    assert_eq!(read_history_range(&store, "child", 3, 5).await, ["c-3", "c-4"]);
    assert_eq!(
        read_all(&store, "base").await,
        ["a-0", "a-1", "a-2", "b-3", "b-4"]
    );
    Ok(())
}

#[tokio::test]
async fn shadowed_prefix_stays_consistent_through_a_fork() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("base", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    store.append("base", ExpectedVersion::Exact(2), batch("b", 3..5)).await?;
    // Rewrite the tail, then extend past it.
    store.append_at("base", 3, ExpectedVersion::Any, batch("c", 3..5)).await?;
    store.append("base", ExpectedVersion::Exact(4), batch("d", 5..6)).await?;

    // Fork after the rewrite: the child pins the shadowed (latest) prefix.
    store.fork("base", 4, "child").await?;
    store.append("child", ExpectedVersion::Exact(4), batch("e", 5..6)).await?;

    assert_eq!(
        read_all(&store, "base").await,
        ["a-0", "a-1", "a-2", "c-3", "c-4", "d-5"]
    );
    let child: Vec<String> = data(&store.read_history("child", ..).await?);
    assert_eq!(child, ["a-0", "a-1", "a-2", "c-3", "c-4", "e-5"]);
    Ok(())
}

#[tokio::test]
async fn shadow_appends_overwrite_and_rollback_windows_filter() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("b0", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    let legit = store.append("b0", ExpectedVersion::Exact(2), batch("b", 3..5)).await?;
    let legit_txn = legit.start_sequence + 1; // last global seq of the legit batch

    // A shadow append with a stale ExpectedVersion is rejected; nothing changes.
    store
        .append_at("b0", 3, ExpectedVersion::Exact(1), batch("s", 3..5))
        .await
        .unwrap_err();
    assert_eq!(read_range(&store, "b0", 3, 5).await, ["b-3", "b-4"]);

    // A shadow append with Any overwrites versions 3..5 in place.
    store.append_at("b0", 3, ExpectedVersion::Any, batch("z", 3..5)).await?;
    assert_eq!(read_range(&store, "b0", 3, 5).await, ["z-3", "z-4"]);

    // The rollback window (versions >= 3, seq > legit_txn) hides the
    // superseded writes. The older prefix remains visible.
    store.trim("b0", 3, legit_txn).await?;
    assert!(read_range(&store, "b0", 3, 5).await.is_empty());
    assert_eq!(read_range(&store, "b0", 0, 3).await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_all(&store, "b0").await, ["a-0", "a-1", "a-2"]);
    Ok(())
}

#[tokio::test]
async fn deleting_base_preserves_prefix_pinned_by_live_fork() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("base", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    store.append("base", ExpectedVersion::Exact(2), batch("b", 3..5)).await?;
    store.fork("base", 2, "child").await?;
    store.append("child", ExpectedVersion::Exact(2), batch("c", 3..5)).await?;

    // Delete base: its exclusive tail (b-3, b-4) goes, but the pinned prefix
    // (a-0..a-2) survives while "child" is alive — and the child is untouched.
    store.delete_stream("base").await?;
    assert_eq!(read_all(&store, "base").await, ["a-0", "a-1", "a-2"]);
    let child: Vec<String> = data(&store.read_history("child", ..).await?);
    assert_eq!(child, ["a-0", "a-1", "a-2", "c-3", "c-4"]);

    // Delete the child: the pin releases, and the base goes fully dark.
    store.delete_stream("child").await?;
    assert_eq!(read_all(&store, "base").await, Vec::<String>::new());
    assert_eq!(read_all(&store, "child").await, Vec::<String>::new());
    assert!(store.list_streams().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn deleting_fork_first_leaves_base_untouched() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("base", ExpectedVersion::NoStream, batch("a", 0..3)).await?;
    store.fork("base", 1, "child").await?;
    store.append("child", ExpectedVersion::Exact(1), batch("c", 2..3)).await?;

    store.delete_stream("child").await?;
    assert_eq!(read_all(&store, "base").await, ["a-0", "a-1", "a-2"]);
    assert_eq!(read_all(&store, "child").await, Vec::<String>::new());

    store.delete_stream("base").await?;
    assert_eq!(read_all(&store, "base").await, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn many_batches_stay_contiguous_and_atomic() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    let mut expected = Vec::new();
    let mut tail: Option<u64> = None;
    for (i, size) in [(3usize), (2), (5), (1), (4)].into_iter().enumerate() {
        let lo = expected.len() as u64;
        let expect = tail.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact);
        let commit = store
            .append("multi", expect, batch(&format!("t{i}"), lo..lo + size as u64))
            .await?;
        assert_eq!(commit.first_version, lo);
        assert_eq!(commit.last_version, lo + size as u64 - 1);
        tail = Some(commit.last_version);
        expected.extend((lo..lo + size as u64).map(|v| format!("t{i}-{v}")));
    }
    assert_eq!(read_all(&store, "multi").await, expected);
    Ok(())
}
