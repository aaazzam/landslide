//! Compaction job lifecycle, journaled as events on the `$jobs` stream.
//!
//! Compaction is a process (requested → claimed → completed/failed), so its
//! state is recorded as a stream. [`JobStatus`] is derived by folding the
//! journal. `Completed` lives on `$compactions` in the correlated
//! [`CompactionRecord`], which also announces the published snapshot.

use serde::{Deserialize, Serialize};

use crate::envelope::now_ms;

pub const JOBS_KEY: &str = "$jobs";

/// One lifecycle transition. Completion is represented by the correlated
/// [`CompactionRecord`] so the snapshot and its job status are published
/// atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobEvent {
    Requested { job_id: String, stream: String, ts_ms: i64 },
    Claimed { job_id: String, worker: String, ts_ms: i64 },
    Failed { job_id: String, error: String, ts_ms: i64 },
}

impl JobEvent {
    pub fn requested(job_id: String, stream: String) -> Self {
        Self::Requested { job_id, stream, ts_ms: now_ms() }
    }
    pub fn claimed(job_id: String, worker: String) -> Self {
        Self::Claimed { job_id, worker, ts_ms: now_ms() }
    }
    pub fn failed(job_id: String, error: String) -> Self {
        Self::Failed { job_id, error, ts_ms: now_ms() }
    }
    pub fn job_id(&self) -> &str {
        match self {
            Self::Requested { job_id, .. } | Self::Claimed { job_id, .. } | Self::Failed { job_id, .. } => {
                job_id
            }
        }
    }
}

/// Derived state of a compaction job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    /// Requested and waiting for a worker.
    Pending,
    /// Claimed but not completed. A claim older than the chosen timeout can be
    /// retried; snapshot publication is atomic and the latest snapshot wins.
    Claimed { worker: String },
    /// A [`CompactionRecord`](crate::CompactionRecord) with this `job_id` was
    /// published on `$compactions`.
    Completed,
    Failed { error: String },
}

pub(crate) fn fold<'a>(events: impl IntoIterator<Item = &'a JobEvent>) -> Option<JobStatus> {
    let mut status = None;
    for e in events {
        status = Some(match e {
            JobEvent::Requested { .. } => JobStatus::Pending,
            JobEvent::Claimed { worker, .. } => JobStatus::Claimed { worker: worker.clone() },
            JobEvent::Failed { error, .. } => JobStatus::Failed { error: error.clone() },
        });
    }
    status
}
