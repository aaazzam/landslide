//! Cross-stream atomic transactions: `EventStore::transaction()` queues
//! appends/fences, `EventStore::commit` lands them in one serializable
//! SlateDB transaction — all ops or none.

use slog::{Error, EventStore, ExpectedVersion, NewEvent, Result};

fn ev(data: &str) -> NewEvent {
    NewEvent::new("e", data.to_string())
}

#[tokio::test]
async fn commit_lands_all_ops_atomically() -> Result<()> {
    let store = EventStore::open_in_memory().await?;

    let mut txn = store.transaction();
    txn.append("a", ExpectedVersion::NoStream, vec![ev("a-0"), ev("a-1")]);
    txn.append("b", ExpectedVersion::NoStream, vec![ev("b-0")]);
    txn.fence("c", Some("token-1".into()));
    let infos = store.commit(txn).await?;

    // One CommitInfo per append op, in op order, contiguous versions.
    assert_eq!(infos.len(), 2);
    assert_eq!((infos[0].first_version, infos[0].last_version), (0, 1));
    assert_eq!((infos[1].first_version, infos[1].last_version), (0, 0));

    assert_eq!(store.stream_version("a").await?, Some(1));
    assert_eq!(store.stream_version("b").await?, Some(0));

    // The fence op took effect: wrong-token writers are rejected.
    let err = store
        .append_with_token("c", "wrong", ExpectedVersion::Any, vec![ev("x")])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::FenceMismatch { .. }));
    store
        .append_with_token("c", "token-1", ExpectedVersion::NoStream, vec![ev("c-0")])
        .await?;
    Ok(())
}

#[tokio::test]
async fn failed_check_aborts_the_whole_transaction() -> Result<()> {
    let store = EventStore::open_in_memory().await?;
    store.append("x", ExpectedVersion::NoStream, vec![ev("x-0")]).await?;

    let mut txn = store.transaction();
    txn.append("y", ExpectedVersion::NoStream, vec![ev("y-0"), ev("y-1")]);
    // Stale expectation on "x" (tail is 0, not 5): must fail.
    txn.append("x", ExpectedVersion::Exact(5), vec![ev("x-1")]);
    let err = store.commit(txn).await.unwrap_err();
    assert!(matches!(err, Error::VersionConflict { actual: Some(0), .. }));

    // Nothing landed: the "y" append was rolled back with the rest.
    assert_eq!(store.stream_version("y").await?, None);
    assert!(store.read_stream("y", ..).await?.is_empty());
    assert_eq!(store.stream_version("x").await?, Some(0));
    Ok(())
}
