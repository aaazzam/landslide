use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::{
    Aggregate, Error, Event, EventStore, ExpectedVersion, NewEvent,
};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Tx {
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
        // Unknown event types do not affect the aggregate.
        let Ok(tx) = event.json::<Tx>() else { return };
        match tx {
            Tx::Opened { owner } => self.owner = owner,
            Tx::Deposited { cents } => self.balance += cents as i64,
            Tx::Withdrawn { cents } => self.balance -= cents as i64,
        }
    }
    fn snapshot(&self) -> slog::Result<Bytes> {
        Ok(serde_json::to_vec(self)?.into())
    }
    fn restore(state: &[u8]) -> slog::Result<Self> {
        Ok(serde_json::from_slice(state)?)
    }
}

fn tx(e: Tx) -> NewEvent {
    let t = match &e {
        Tx::Opened { .. } => "opened",
        Tx::Deposited { .. } => "deposited",
        Tx::Withdrawn { .. } => "withdrawn",
    };
    NewEvent::json(t, &e).unwrap()
}

#[tokio::test]
async fn append_and_replay() {
    let store = EventStore::open_in_memory().await.unwrap();
    let commit = store
        .append(
            "a-1",
            ExpectedVersion::NoStream,
            vec![
                tx(Tx::Opened { owner: "ada".into() }),
                tx(Tx::Deposited { cents: 100 }),
            ],
        )
        .await
        .unwrap();
    assert_eq!((commit.first_version, commit.last_version), (0, 1));

    let events = store.read_stream("a-1", ..).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].version, 1);
    assert!(events[1].global_seq > events[0].global_seq);
    assert_eq!(events[1].json::<Tx>().unwrap(), Tx::Deposited { cents: 100 });

    let (account, version) = store.rehydrate::<Account>("a-1").await.unwrap();
    assert_eq!((account.owner.as_str(), account.balance, version), ("ada", 100, Some(1)));
}

#[tokio::test]
async fn batches_are_atomic_and_conditional() {
    let store = EventStore::open_in_memory().await.unwrap();
    store
        .append("s", ExpectedVersion::NoStream, vec![tx(Tx::Deposited { cents: 1 })])
        .await
        .unwrap();

    // Wrong version: nothing lands.
    let err = store
        .append(
            "s",
            ExpectedVersion::Exact(5),
            vec![tx(Tx::Deposited { cents: 2 }), tx(Tx::Deposited { cents: 3 })],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionConflict { actual: Some(0), .. }));
    assert_eq!(store.stream_version("s").await.unwrap(), Some(0));

    // Retrying the original batch conflicts rather than creating duplicates.
    let err = store
        .append("s", ExpectedVersion::NoStream, vec![tx(Tx::Deposited { cents: 1 })])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionConflict { .. }));

    // Right version: whole batch lands with contiguous versions.
    let commit = store
        .append(
            "s",
            ExpectedVersion::Exact(0),
            vec![tx(Tx::Deposited { cents: 2 }), tx(Tx::Deposited { cents: 3 })],
        )
        .await
        .unwrap();
    assert_eq!((commit.first_version, commit.last_version), (1, 2));
    assert_eq!(store.stream_version("s").await.unwrap(), Some(2));
}

#[tokio::test]
async fn compact_and_rehydrate_from_snapshot() {
    let store = EventStore::open_in_memory().await.unwrap();
    store
        .append("a-9", ExpectedVersion::NoStream, vec![tx(Tx::Opened { owner: "ada".into() })])
        .await
        .unwrap();
    for _ in 0..10 {
        let (_, v) = store.rehydrate::<Account>("a-9").await.unwrap();
        store
            .append("a-9", ExpectedVersion::Exact(v.unwrap()), vec![tx(Tx::Deposited { cents: 10 })])
            .await
            .unwrap();
    }

    assert_eq!(store.compaction_backlog("a-9").await.unwrap(), 11);
    let record = store.compact::<Account>("a-9").await.unwrap();
    assert_eq!((record.stream.as_str(), record.through_version, record.events_compacted), ("a-9", 10, 11));
    assert_eq!(store.compaction_backlog("a-9").await.unwrap(), 0);

    let records = store.compaction_records().await.unwrap();
    assert_eq!(records.len(), 1);

    // Post-compaction rehydrate starts from the snapshot; more appends still work.
    let (account, version) = store.rehydrate::<Account>("a-9").await.unwrap();
    assert_eq!((account.balance, version), (100, Some(10)));
    store
        .append("a-9", ExpectedVersion::Exact(10), vec![tx(Tx::Withdrawn { cents: 40 })])
        .await
        .unwrap();
    let (account, _) = store.rehydrate::<Account>("a-9").await.unwrap();
    assert_eq!(account.balance, 60);
}

#[tokio::test]
async fn fork_shares_pinned_prefix_and_diverges() {
    let store = EventStore::open_in_memory().await.unwrap();
    store
        .append(
            "main",
            ExpectedVersion::NoStream,
            vec![
                tx(Tx::Opened { owner: "ada".into() }),
                tx(Tx::Deposited { cents: 100 }),
            ],
        )
        .await
        .unwrap();

    // Fork at v1: metadata only, no marker record; the child tail starts at v1.
    store.fork("main", 1, "experiment").await.unwrap();

    // Child rehydrates through the pinned prefix; its own tail is v1.
    let (account, version) = store.rehydrate::<Account>("experiment").await.unwrap();
    assert_eq!((account.balance, version), (100, Some(1)));
    store
        .append("experiment", ExpectedVersion::Exact(1), vec![tx(Tx::Withdrawn { cents: 30 })])
        .await
        .unwrap();

    // Parent keeps moving; the fork never sees it (pinned at v1).
    store
        .append("main", ExpectedVersion::Exact(1), vec![tx(Tx::Deposited { cents: 999 })])
        .await
        .unwrap();
    let history = store.read_history("experiment", 0..).await.unwrap();
    assert_eq!(history.len(), 3); // opened, deposited(100), withdrawn(30)
    assert_eq!(history[1].version, 1);
    // The raw stream holds only the child's own events: no $fork marker exists.
    let raw = store.read_stream("experiment", ..).await.unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!((raw[0].version, raw[0].event_type.as_str()), (2, "withdrawn"));
    let (experiment, _) = store.rehydrate::<Account>("experiment").await.unwrap();
    assert_eq!(experiment.balance, 70);

    // main is unaffected.
    let (main, _) = store.rehydrate::<Account>("main").await.unwrap();
    assert_eq!(main.balance, 1099);

    // Forking an existing or past-the-point stream fails.
    assert!(matches!(
        store.fork("main", 1, "experiment").await.unwrap_err(),
        Error::VersionConflict { .. }
    ));
    assert!(matches!(
        store.fork("main", 42, "too-late").await.unwrap_err(),
        Error::InvalidInput(_)
    ));
}

#[tokio::test]
async fn custom_engines_can_drive_snapshots() {
    let store = EventStore::open_in_memory().await.unwrap(); // production: use the application's bucket
    store
        .append(
            "db-7",
            ExpectedVersion::NoStream,
            vec![
                tx(Tx::Opened { owner: "ada".into() }),
                tx(Tx::Deposited { cents: 10 }),
                tx(Tx::Deposited { cents: 5 }),
            ],
        )
        .await
        .unwrap();

    // 1. In-band custom snapshot production: application code decides the bytes.
    let record = store
        .compact_with("db-7", |events| {
            Ok(format!("manifest:{{segments:4,through:v{}}}", events.len() - 1).into())
        })
        .await
        .unwrap();
    let snap = store.latest_snapshot("db-7").await.unwrap().unwrap();
    assert_eq!(String::from_utf8(snap.state.into()).unwrap(), "manifest:{segments:4,through:v2}");

    // 2. Externally produced snapshots (e.g. an LTX compaction job ran out of
    // band): register the result, listeners still get the compaction event.
    let later = slog::CompactionRecord {
        stream: "db-7".into(),
        through_version: record.through_version,
        events_compacted: 3,
        job_id: None,
        ts_ms: 0,
    };
    store
        .publish_snapshot("db-7", later, "ltx://bucket/db-7/0004.ltx".into())
        .await
        .unwrap();

    // 3. Custom rehydration with your own state type — no Aggregate impl.
    let (balance, version) = store
        .fold("db-7", 0.., 0i64, |bal, e| {
            if let Ok(Tx::Deposited { cents }) = e.json() {
                *bal += cents as i64;
            }
        })
        .await
        .unwrap();
    assert_eq!((balance, version), (15, Some(2)));
    assert_eq!(store.compaction_records().await.unwrap().len(), 2);
}

#[tokio::test]
async fn compaction_jobs_have_a_journaled_lifecycle() {
    use slog::JobStatus;
    let store = EventStore::open_in_memory().await.unwrap();
    store
        .append("w-1", ExpectedVersion::NoStream, vec![tx(Tx::Deposited { cents: 7 })])
        .await
        .unwrap();

    // Unknown job has no status; request makes it Pending.
    assert_eq!(store.job_status("nope").await.unwrap(), None);
    let job = store.request_compaction("w-1").await.unwrap();
    assert_eq!(store.job_status(&job).await.unwrap(), Some(JobStatus::Pending));

    // Exactly one claim sticks; the second is rejected.
    store.claim_compaction(&job, "worker-a").await.unwrap();
    assert_eq!(
        store.job_status(&job).await.unwrap(),
        Some(JobStatus::Claimed { worker: "worker-a".into() })
    );
    assert!(store.claim_compaction(&job, "worker-b").await.is_err());

    // Completing publishes the snapshot AND closes the job (correlated).
    let record = store
        .compact_job("w-1", job.clone(), |_| Ok(Bytes::from_static(b"img")))
        .await
        .unwrap();
    assert_eq!(record.job_id.as_deref(), Some(job.as_str()));
    assert_eq!(store.job_status(&job).await.unwrap(), Some(JobStatus::Completed));

    // Failure path.
    let job2 = store.request_compaction("w-1").await.unwrap();
    store.fail_compaction(&job2, "out of disk").await.unwrap();
    assert_eq!(
        store.job_status(&job2).await.unwrap(),
        Some(JobStatus::Failed { error: "out of disk".into() })
    );
}

#[tokio::test]
async fn followers_see_new_events_and_compactions() {
    use std::sync::Arc;

    // Followers live on EventStoreReader: a writer and a reader over the same
    // object store (DbReader replicates committed state — poll tolerant).
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let store = EventStore::open(slog::Config { path: "f".into(), object_store: object_store.clone(), settings: None })
        .await
        .unwrap();
    let reader =
        slog::EventStoreReader::open(slog::ReaderConfig { path: "f".into(), object_store, options: None })
            .await
            .unwrap();
    let mut events = reader.follow_stream("f-1", 0, Duration::from_millis(50));
    let mut compactions = reader.follow_compactions(Duration::from_millis(50));

    store
        .append("f-1", ExpectedVersion::Any, vec![tx(Tx::Opened { owner: "ada".into() })])
        .await
        .unwrap();
    store.compact::<Account>("f-1").await.unwrap();

    // DbReader replicates on a manifest poll (default 10s); timeouts are slack.
    let e = tokio::time::timeout(Duration::from_secs(30), events.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(e.event_type, "opened");

    let c = tokio::time::timeout(Duration::from_secs(30), compactions.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(c.stream, "f-1");
}
