//! Key layout in the SlateDB store and the shared read primitives.
//!
//! ```text
//! r/{stream}/{version:016be}          event envelope
//! m/{stream}/tail                     TailRecord { version, ts_ms }
//! m/{stream}/fence                    token bytes
//! m/{stream}/trim                     u64be floor (hide v < floor)
//! m/{stream}/deleted                  deletion tombstone (empty)
//! m/{stream}/fork                     ForkRef { parent, at_version }
//! m/{stream}/forks/{child}            u64be at_version (pin registry)
//! m/{stream}/rollback/{from_v:016be}  u64be txn watermark
//! i/{stream}/{ts:016be}/{v:016be}     time index (empty value)
//! snap/{stream}/{seq:016be}           SnapshotRecord
//! j/compactions/{ulid}                CompactionRecord
//! g/seq                               u64be global sequence counter
//! ```

use serde::de::DeserializeOwned;

use crate::envelope::{Envelope, Event, Version};
use crate::fork::ForkRef;
use crate::{Error, Result, SnapshotRecord};

pub(crate) fn r_prefix(stream: &str) -> Vec<u8> {
    format!("r/{stream}/").into_bytes()
}

pub(crate) fn r_key(stream: &str, version: Version) -> Vec<u8> {
    format!("r/{stream}/{version:016}").into_bytes()
}

pub(crate) fn m_key(stream: &str, field: &str) -> Vec<u8> {
    format!("m/{stream}/{field}").into_bytes()
}

pub(crate) fn ts_key(stream: &str, ts_ms: i64, version: Version) -> Vec<u8> {
    format!("i/{stream}/{ts_ms:016}/{version:016}").into_bytes()
}

pub(crate) fn snap_prefix(stream: &str) -> Vec<u8> {
    format!("snap/{stream}/").into_bytes()
}

pub const COMPACTIONS_PREFIX: &str = "j/compactions/";
pub const SEQ_KEY: &[u8] = b"g/seq";

/// Minimal read surface implemented by Db, DbReader and DbTransaction.
pub(crate) trait KvRead: Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>>;
    async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        self.scan_prefix_range(prefix, ..).await
    }
    /// Scan with a suffix range pushed down to the store: only keys within
    /// the range are read, not filtered afterwards.
    async fn scan_prefix_range(
        &self,
        prefix: Vec<u8>,
        range: impl slatedb::ByteRangeBounds + Send,
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>>;
}

macro_rules! impl_kv_read {
    ($t:ty) => {
        impl KvRead for $t {
            async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
                Ok(self.get(key).await?)
            }
            async fn scan_prefix_range(
                &self,
                prefix: Vec<u8>,
                range: impl slatedb::ByteRangeBounds + Send,
            ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
                let mut it = self.scan_prefix(prefix, range).await?;
                let mut out = Vec::new();
                while let Some(kv) = it.next().await? {
                    out.push((kv.key, kv.value));
                }
                Ok(out)
            }
        }
    };
}

impl_kv_read!(slatedb::Db);
impl_kv_read!(slatedb::DbReader);
impl_kv_read!(slatedb::DbTransaction);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TailRecord {
    pub version: Version,
    pub ts_ms: i64,
}

// ---- read helpers -------------------------------------------------------

/// Raw stream records in `range`, with stream guards applied: trim floor,
/// deletion ceiling (pinned by live forks), rollback windows — and shadowing
/// resolved physically by the KV (overwrites).
///
/// The version range is pushed down to the scan (`{version:016}` suffix keys
/// sort numerically), so events outside `[max(lo, floor), hi)` are neither
/// read nor decoded — a snapshot reader never touches sealed history.
pub(crate) async fn read_events(
    r: &impl KvRead,
    stream: &str,
    range: impl std::ops::RangeBounds<Version> + Send,
) -> Result<Vec<Event>> {
    let (lo, hi) = crate::store::bounds(range);
    let floor = get_be(r, &m_key(stream, "trim")).await?.unwrap_or(0);
    let deleted = r.get(&m_key(stream, "deleted")).await?.is_some();
    let ceiling = if deleted { ceiling(r, stream).await? } else { None };
    let rollbacks = rollbacks(r, stream).await?;

    let lo = lo.max(floor);
    if hi.is_some_and(|hi| lo >= hi) {
        return Ok(Vec::new());
    }
    let sub_lo = std::ops::Bound::Included(format!("{lo:016}").into_bytes());
    let sub_hi = match hi {
        Some(hi) => std::ops::Bound::Excluded(format!("{hi:016}").into_bytes()),
        None => std::ops::Bound::Unbounded,
    };

    let mut out = Vec::new();
    for (_, value) in r.scan_prefix_range(r_prefix(stream), (sub_lo, sub_hi)).await? {
        let env = Envelope::decode(&value)?;
        if env.version < lo || hi.is_some_and(|hi| env.version >= hi) {
            continue;
        }
        // Deleted: only the prefix pinned by live forks survives (or nothing).
        if deleted && !ceiling.is_some_and(|c| env.version <= c) {
            continue;
        }
        if rollbacks
            .iter()
            .any(|(from, txn)| env.version >= *from && env.seq > *txn)
        {
            continue;
        }
        out.push(Event::from_parts(env));
    }
    Ok(out)
}

/// Highest fork pin held by a non-deleted child (ancestors stay readable).
pub(crate) async fn ceiling(r: &impl KvRead, stream: &str) -> Result<Option<Version>> {
    let mut ceiling = None;
    for (key, at) in r.scan_prefix(m_key(stream, "forks/")).await? {
        let child = std::str::from_utf8(&key).unwrap_or("").rsplit('/').next().unwrap_or("");
        if r.get(&m_key(child, "deleted")).await?.is_none() {
            let at = u64::from_be_bytes(at.as_ref().try_into().unwrap_or([0; 8]));
            ceiling = Some(ceiling.map_or(at, |c: Version| c.max(at)));
        }
    }
    Ok(ceiling)
}

/// (from_version, txn) windows: hide v >= from_version with global seq > txn.
async fn rollbacks(r: &impl KvRead, stream: &str) -> Result<Vec<(Version, u64)>> {
    r.scan_prefix(m_key(stream, "rollback/")).await?
        .into_iter()
        .map(|(k, v)| {
            Ok((
                version_suffix(&k)?,
                u64::from_be_bytes(v.as_ref().try_into().unwrap_or([0; 8])),
            ))
        })
        .collect()
}

/// Parses the trailing `{version:016}` component of a record/index key.
pub(crate) fn version_suffix(key: &[u8]) -> Result<Version> {
    std::str::from_utf8(&key[key.len() - 16..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::InvalidInput("bad versioned key".into()))
}

pub(crate) async fn tail(r: &impl KvRead, stream: &str) -> Result<Option<TailRecord>> {
    get_json(r, &m_key(stream, "tail")).await
}

pub(crate) async fn latest_snapshot(
    r: &impl KvRead,
    stream: &str,
) -> Result<Option<SnapshotRecord>> {
    match r.scan_prefix(snap_prefix(stream)).await?.last() {
        Some((_, value)) => Ok(Some(serde_json::from_slice(value)?)),
        None => Ok(None),
    }
}

pub(crate) async fn fork_ref(r: &impl KvRead, stream: &str) -> Result<Option<ForkRef>> {
    get_json(r, &m_key(stream, "fork")).await
}

pub(crate) async fn get_be(r: &impl KvRead, key: &[u8]) -> Result<Option<u64>> {
    Ok(r.get(key)
        .await?
        .map(|b| u64::from_be_bytes(b.as_ref().try_into().unwrap_or([0; 8]))))
}

pub(crate) async fn get_json<T: DeserializeOwned>(
    r: &impl KvRead,
    key: &[u8],
) -> Result<Option<T>> {
    Ok(r.get(key).await?.map(|b| serde_json::from_slice(&b)).transpose()?)
}
