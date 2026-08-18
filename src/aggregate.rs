use crate::{Event, Result};

/// A state machine rebuilt by folding a stream's events in version order.
///
/// Optional — you can always fold [`EventStore::read_stream`](crate::EventStore::read_stream)
/// output yourself. Implementing it gets you
/// [`rehydrate`](crate::EventStore::rehydrate) and
/// [`compact`](crate::EventStore::compact) for free.
///
/// ```
/// # use landslide::{Aggregate, Event};
/// #[derive(Default)]
/// struct Counter(u64);
///
/// impl Aggregate for Counter {
///     fn apply(&mut self, event: &Event) {
///         if event.event_type == "incremented" {
///             self.0 += 1;
///         }
///     }
///     // Snapshots are opt-in; override `snapshot`/`restore` to enable
///     // snapshot-accelerated rehydration via `compact()`.
///     fn snapshot(&self) -> landslide::Result<bytes::Bytes> {
///         Ok(self.0.to_be_bytes().to_vec().into())
///     }
///     fn restore(state: &[u8]) -> landslide::Result<Self> {
///         Ok(Counter(u64::from_be_bytes(state.try_into().expect("8 bytes"))))
///     }
/// }
/// ```
pub trait Aggregate: Default + Send {
    /// Fold one event into the state. This method is infallible so older code
    /// can read streams containing newer event types; ignore unknown types.
    /// Validate events before appending them.
    fn apply(&mut self, event: &Event);

    /// Serialize folded state for a snapshot. Override with [`restore`](Self::restore).
    fn snapshot(&self) -> Result<bytes::Bytes> {
        Err(crate::Error::InvalidInput(
            "aggregate does not implement snapshot()".into(),
        ))
    }

    /// Rebuild folded state from snapshot bytes. Override with [`snapshot`](Self::snapshot).
    fn restore(_state: &[u8]) -> Result<Self> {
        Err(crate::Error::InvalidInput(
            "aggregate does not implement restore()".into(),
        ))
    }
}
