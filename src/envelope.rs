use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Result;

/// Per-stream event version: contiguous, starting at 0.
pub type Version = u64;

/// Optimistic-concurrency expectation for [`EventStore::append`](crate::EventStore::append).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    Any,
    NoStream,
    Exact(Version),
}

/// An event to append.
#[derive(Debug, Clone)]
pub struct NewEvent {
    pub event_type: String,
    pub data: Bytes,
    pub metadata: Vec<(String, Bytes)>,
}

impl NewEvent {
    pub fn new(event_type: impl Into<String>, data: impl Into<Bytes>) -> Self {
        Self {
            event_type: event_type.into(),
            data: data.into(),
            metadata: Vec::new(),
        }
    }

    pub fn json<T: Serialize>(event_type: impl Into<String>, value: &T) -> Result<Self> {
        Ok(Self::new(event_type, Bytes::from(serde_json::to_vec(value)?)))
    }
}

/// Wire format stored as the value of each log record.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Envelope {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub version: Version,
    /// Global sequence counter value at commit. Monotone within a stream's
    /// commit order; may duplicate across concurrently committing streams.
    pub seq: u64,
    #[serde(with = "b64_bytes")]
    pub data: Bytes,
    // No skip_serializing_if: binary formats need every field present.
    #[serde(default, with = "b64_headers")]
    pub metadata: Vec<(String, Bytes)>,
    pub ts_ms: i64,
}

impl Envelope {
    pub(crate) fn new(e: NewEvent, version: Version) -> Self {
        Self {
            id: ulid::Ulid::new().to_string(),
            event_type: e.event_type,
            version,
            seq: 0, // assigned at commit
            data: e.data,
            metadata: e.metadata,
            ts_ms: now_ms(),
        }
    }

    /// Wire format: bincode (bulk payloads don't pay base64+JSON). Decoding
    /// falls back to JSON on a leading '{', so pre-bincode records still read.
    pub(crate) fn encode(&self) -> Result<Bytes> {
        Ok(Bytes::from(bincode::serialize(self)?))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.first() == Some(&b'{') {
            Ok(serde_json::from_slice(bytes)?)
        } else {
            Ok(bincode::deserialize(bytes)?)
        }
    }
}

/// A decoded event.
#[derive(Debug, Clone)]
pub struct Event {
    pub id: String,
    pub event_type: String,
    pub version: Version,
    /// Global sequence counter value at commit; see [`Envelope::seq`].
    pub global_seq: u64,
    pub data: Bytes,
    pub metadata: Vec<(String, Bytes)>,
    pub ts_ms: i64,
}

impl Event {
    pub(crate) fn from_parts(e: Envelope) -> Self {
        Self {
            id: e.id,
            event_type: e.event_type,
            version: e.version,
            global_seq: e.seq,
            data: e.data,
            metadata: e.metadata,
            ts_ms: e.ts_ms,
        }
    }

    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.data)?)
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) mod b64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Base64 in human-readable formats, raw bytes in binary ones.
    pub fn serialize<S: Serializer>(b: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&STANDARD.encode(b))
        } else {
            s.serialize_bytes(b)
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        if d.is_human_readable() {
            STANDARD
                .decode(String::deserialize(d)?)
                .map(Bytes::from)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Bytes::from(Vec::<u8>::deserialize(d)?))
        }
    }
}

pub(crate) mod b64_headers {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(h: &[(String, Bytes)], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            h.iter()
                .map(|(name, value)| (name, STANDARD.encode(value)))
                .collect::<Vec<(&String, String)>>()
                .serialize(s)
        } else {
            h.serialize(s)
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<(String, Bytes)>, D::Error> {
        if d.is_human_readable() {
            Vec::<(String, String)>::deserialize(d)?
                .into_iter()
                .map(|(name, value)| {
                    STANDARD
                        .decode(value)
                        .map(|v| (name, Bytes::from(v)))
                        .map_err(serde::de::Error::custom)
                })
                .collect()
        } else {
            Ok(Vec::<(String, Vec<u8>)>::deserialize(d)?
                .into_iter()
                .map(|(k, v)| (k, Bytes::from(v)))
                .collect())
        }
    }
}
