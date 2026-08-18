use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::envelope::{b64_bytes, Version};

/// Folded state of a stream at `through_version`; rehydration resumes at +1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub through_version: Version,
    pub ts_ms: i64,
    #[serde(with = "b64_bytes")]
    pub state: Bytes,
}

/// Appended to `$compactions` by every successful `compact` — the hook for
/// listeners (rebuild projections, invalidate caches, schedule retention).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionRecord {
    pub stream: String,
    pub through_version: Version,
    pub events_compacted: u64,
    /// Correlates to a [`JobEvent`](crate::jobs::JobEvent) lifecycle, for
    /// compactions that answered a request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub ts_ms: i64,
}
