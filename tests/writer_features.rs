use std::sync::Arc;

use bytes::Bytes;
use slog::deps::slatedb;
use slog::{Config, Error, EventStore, ExpectedVersion, NewEvent, PageLimit};

fn ev(data: &str) -> NewEvent {
    NewEvent::new("test", data.to_string())
}

#[tokio::test]
async fn fence_coordinates_writers() {
    let store = EventStore::open_in_memory().await.unwrap();

    // Unfenced: any token works.
    store
        .append_with_token("s", "w0", ExpectedVersion::Any, vec![ev("a")])
        .await
        .unwrap();

    store.fence("s", Some("w1")).await.unwrap();

    // Wrong token: rejected, carrying the current token.
    let err = store
        .append_with_token("s", "w2", ExpectedVersion::Any, vec![ev("b")])
        .await
        .unwrap_err();
    match err {
        Error::FenceMismatch { stream, current_token } => {
            assert_eq!(stream, "s");
            assert_eq!(current_token, "w1");
        }
        other => panic!("expected FenceMismatch, got {other:?}"),
    }

    // Right token: succeeds.
    store
        .append_with_token("s", "w1", ExpectedVersion::Any, vec![ev("c")])
        .await
        .unwrap();

    // Plain append is cooperative: never checks the fence.
    store.append("s", ExpectedVersion::Any, vec![ev("d")]).await.unwrap();

    // Clearing the fence lets any token through again.
    store.fence("s", None).await.unwrap();
    store
        .append_with_token("s", "w2", ExpectedVersion::Any, vec![ev("e")])
        .await
        .unwrap();
}

#[tokio::test]
async fn append_durable_waits_for_watermark() {
    let store = EventStore::open_in_memory().await.unwrap();
    let events: Vec<_> = (0..5).map(|i| ev(&format!("payload-{i}"))).collect();
    let batch_len = events.len() as u64;
    let info = store
        .append_durable("s", ExpectedVersion::NoStream, events)
        .await
        .unwrap();
    // g/seq is the last committed sequence: the whole batch is at or below it.
    let durable = store.durable_sequence().await.unwrap();
    assert!(durable >= info.start_sequence + batch_len - 1);
}

#[tokio::test]
async fn timestamps_are_monotonic() {
    let store = EventStore::open_in_memory().await.unwrap();
    store.append("s", ExpectedVersion::NoStream, vec![ev("a"), ev("b")]).await.unwrap();
    store.append("s", ExpectedVersion::Any, vec![ev("c")]).await.unwrap();
    let events = store.read_stream("s", ..).await.unwrap();
    assert_eq!(events.len(), 3);
    assert!(events[0].ts_ms <= events[1].ts_ms);
    assert!(events[1].ts_ms <= events[2].ts_ms);
}

#[tokio::test]
async fn batch_caps_are_enforced() {
    let store = EventStore::open_in_memory().await.unwrap();

    let big_batch: Vec<_> = (0..1001).map(|_| ev("x")).collect();
    let err = store.append("s", ExpectedVersion::Any, big_batch).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");

    let big_record = NewEvent::new("test", Bytes::from(vec![0u8; 2 * 1024 * 1024]));
    let err = store
        .append_at("s", 0, ExpectedVersion::Any, vec![big_record])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");

    // A normal batch is fine.
    let ok: Vec<_> = (0..1000).map(|_| ev("x")).collect();
    store.append("s", ExpectedVersion::Any, ok).await.unwrap();
}

/// Streams are independent fate: concurrent appends on different streams
/// must not fail over shared bookkeeping (the sequence counter). Real
/// same-stream races still surface as VersionConflict.
#[tokio::test]
async fn concurrent_streams_dont_conflict_on_the_sequence_counter() {
    let store = Arc::new(EventStore::open_in_memory().await.unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for s in 0..8u32 {
        let (store, barrier) = (store.clone(), barrier.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut errs = 0;
            for i in 0..20 {
                errs += store
                    .append(&format!("stream-{s}"), ExpectedVersion::Any, vec![ev(&format!("{s}-{i}"))])
                    .await
                    .is_err() as usize;
            }
            errs
        }));
    }
    let mut errors = 0;
    for h in handles {
        errors += h.await.unwrap();
    }
    println!("CONFLICTS spurious={errors}");
    for s in 0..8 {
        assert_eq!(store.read_stream(&format!("stream-{s}"), ..).await.unwrap().len(), 20);
    }
    assert_eq!(errors, 0);
}

/// Bulk payloads must not pay base64+JSON on the wire; legacy JSON-encoded
/// envelopes still decode.
#[tokio::test]
async fn envelopes_store_binary_payloads_efficiently() {
    let store = EventStore::open_in_memory().await.unwrap();
    let data = Bytes::from(vec![7u8; 4096]);
    store.append("s", ExpectedVersion::NoStream, vec![NewEvent::new("txb", data.clone())]).await.unwrap();

    let mut it = store.db().scan_prefix(b"r/s/" as &[u8], ..).await.unwrap();
    let raw = it.next().await.unwrap().unwrap().value.len();
    println!("ENVELOPE payload=4096 raw={raw}");
    assert!(raw < 4096 + 256, "wire overhead: {raw} bytes for a 4096-byte payload");

    let e = &store.read_stream("s", ..).await.unwrap()[0];
    assert_eq!(e.data, data);

    store
        .db()
        .put(
            b"r/legacy/0000000000000009" as &[u8],
            br#"{"id":"01JQZ000000000000000000000","type":"tx","version":9,"seq":1,"data":"e30=","ts_ms":1}"#.as_ref(),
        )
        .await
        .unwrap();
    let legacy = &store.read_stream("legacy", ..).await.unwrap()[0];
    assert_eq!(legacy.version, 9);
    assert_eq!(legacy.data, Bytes::from_static(b"{}"));
}

#[tokio::test]
async fn read_tail_returns_last_n() {
    let store = EventStore::open_in_memory().await.unwrap();
    let events: Vec<_> = (0..5).map(|i| ev(&format!("e-{i}"))).collect();
    store.append("s", ExpectedVersion::NoStream, events).await.unwrap();

    let tail = store.read_tail("s", 2).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].version, 3);
    assert_eq!(tail[1].version, 4);

    let all = store.read_tail("s", 10).await.unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].version, 0);
}

#[tokio::test]
async fn read_page_limits_and_cursor() {
    let store = EventStore::open_in_memory().await.unwrap();
    // 10 events, each with a 10-byte payload.
    let events: Vec<_> = (0..10).map(|i| ev(&format!("{i:0>10}"))).collect();
    store.append("s", ExpectedVersion::NoStream, events).await.unwrap();

    // Count cap: 3 events plus cursor.
    let (page, cursor) = store
        .read_page("s", 0, PageLimit { max_count: 3, max_bytes: 1 << 20 })
        .await
        .unwrap();
    assert_eq!(page.len(), 3);
    assert_eq!(cursor, Some(3));

    // Drain the rest via the cursor: 7 events, no cursor.
    let (rest, cursor) = store
        .read_page("s", cursor.unwrap(), PageLimit { max_count: 100, max_bytes: 1 << 20 })
        .await
        .unwrap();
    assert_eq!(rest.len(), 7);
    assert_eq!(rest[0].version, 3);
    assert_eq!(cursor, None);

    // Byte cap: first event always included even though it alone exceeds it.
    let (page, cursor) = store
        .read_page("s", 0, PageLimit { max_count: 100, max_bytes: 1 })
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(cursor, Some(1));
}
