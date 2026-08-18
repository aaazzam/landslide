//! Metadata-only stream forks.
//!
//! `fork(parent, at_version, child)` records a [`ForkRef`] at
//! `m/{child}/fork`. No events are copied: the child shares the parent's
//! history through `at_version` (pinned; later parent appends are not
//! visible) and continues its own version numbering. Compaction flattens a
//! forked chain into a self-contained snapshot.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::kv::{self, KvRead};
use crate::{Error, Event, Result, Version};

/// Fork metadata stored at `m/{stream}/fork`: the branch point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkRef {
    pub stream: String,
    pub at_version: Version,
}

/// Logical history of `stream` over `[lo, hi)`: fork-chain prefixes pinned
/// at each fork point, then local events. The range is intersected with each
/// chain segment's window (parent: `≤ at_version`; own: the rest) and pushed
/// all the way down to the scans, so out-of-range history is never read.
/// Fork edges always point to an older stream, so cycles are impossible
/// through the API — the `visited` check only guards against hand-crafted
/// data.
pub(crate) async fn resolve_history<R: KvRead + Send>(
    r: &R,
    stream: &str,
    visited: &mut HashSet<String>,
    lo: Version,
    hi: Option<Version>,
) -> Result<Vec<Event>> {
    use std::ops::Bound::*;
    if hi.is_some_and(|hi| lo >= hi) {
        return Ok(Vec::new());
    }
    if !visited.insert(stream.to_string()) {
        return Err(Error::InvalidInput(format!("fork cycle at '{stream}'")));
    }
    let mut events = Vec::new();
    if let Some(f) = kv::fork_ref(r, stream).await? {
        // The parent's contribution lives at versions ≤ f.at_version.
        let parent_hi = hi.map_or(f.at_version + 1, |hi| hi.min(f.at_version + 1));
        if lo < parent_hi {
            events.append(
                &mut Box::pin(resolve_history(r, &f.stream, visited, lo, Some(parent_hi))).await?,
            );
        }
    }
    events
        .extend(kv::read_events(r, stream, (Included(lo), hi.map_or(Unbounded, Excluded))).await?);
    Ok(events)
}
