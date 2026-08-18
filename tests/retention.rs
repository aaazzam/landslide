//! Retention (`purge_below`) and range-pushdown correctness at fork boundaries.

use slog::{EventStore, ExpectedVersion, NewEvent};

async fn append_n(store: &EventStore, stream: &str, start: u64, n: u64) -> slog::Result<()> {
    let expected = if start == 0 {
        ExpectedVersion::NoStream
    } else {
        ExpectedVersion::Exact(start - 1)
    };
    let events = (start..start + n).map(|i| NewEvent::json("e", &i)).collect::<Result<_, _>>()?;
    store.append(stream, expected, events).await?;
    Ok(())
}

fn versions(events: &[slog::Event]) -> Vec<u64> {
    events.iter().map(|e| e.version).collect()
}

#[tokio::test]
async fn purge_below_deletes_sealed_history() {
    let store = EventStore::open_in_memory().await.unwrap();
    append_n(&store, "s", 0, 10).await.unwrap();

    let purged = store.purge_below("s", 6).await.unwrap();
    assert_eq!(purged, 6);
    assert_eq!(versions(&store.read_history("s", ..).await.unwrap()), [6, 7, 8, 9]);

    // Idempotent: nothing left below the floor on a second run.
    assert_eq!(store.purge_below("s", 6).await.unwrap(), 0);

    // Appends continue numbering after a purge.
    append_n(&store, "s", 10, 2).await.unwrap();
    assert_eq!(
        versions(&store.read_history("s", ..).await.unwrap()),
        [6, 7, 8, 9, 10, 11]
    );
}

#[tokio::test]
async fn purge_below_never_touches_live_fork_pins() {
    let store = EventStore::open_in_memory().await.unwrap();
    append_n(&store, "parent", 0, 8).await.unwrap();
    store.fork("parent", 2, "child").await.unwrap();

    // Window is (pin=2, floor=6): only versions 3..5 are purgeable.
    let purged = store.purge_below("parent", 6).await.unwrap();
    assert_eq!(purged, 3);
    assert_eq!(versions(&store.read_history("parent", ..).await.unwrap()), [0, 1, 2, 6, 7]);

    // The forked child still reads its full pinned prefix.
    assert_eq!(versions(&store.read_history("child", ..).await.unwrap()), [0, 1, 2]);

    // Purged versions are removed from the stream.
    assert!(store.read_history("parent", 3..6).await.unwrap().is_empty());
}

#[tokio::test]
async fn fold_ranges_are_exact_across_fork_boundaries() {
    let store = EventStore::open_in_memory().await.unwrap();
    append_n(&store, "parent", 0, 5).await.unwrap(); // versions 0..4
    store.fork("parent", 3, "child").await.unwrap();
    append_n(&store, "child", 4, 3).await.unwrap(); // versions 4..6
    append_n(&store, "parent", 5, 2).await.unwrap(); // parent moves on; child pinned at 3

    // Child history spans parent prefix (0..=3) + own (4..=6): 7 events.
    let (count, last) = store.fold("child", .., 0u64, |n, _| *n += 1).await.unwrap();
    assert_eq!((count, last), (7, Some(6)));

    // A range entirely within the parent prefix...
    let (n, last) = store.fold("child", 1..3, 0u64, |n, _| *n += 1).await.unwrap();
    assert_eq!((n, last), (2, Some(2)));

    // ...one crossing the fork point...
    let hist = store.read_history("child", 2..6).await.unwrap();
    assert_eq!(versions(&hist), [2, 3, 4, 5]);
    assert_eq!(hist.iter().map(|e| e.json::<u64>().unwrap()).collect::<Vec<_>>(), [2, 3, 4, 5]);

    // ...and one past any content.
    let (n, last) = store.fold("child", 50.., 0u64, |n, _| *n += 1).await.unwrap();
    assert_eq!((n, last), (0, None));
}
