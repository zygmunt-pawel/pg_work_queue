//! Payload codec — pluggable serialization for `T: Serialize + DeserializeOwned`
//! into/from `BYTEA`.
//!
//! Default `JsonCodec`. Users may implement their own (`Codec` trait) to swap
//! in CBOR / bincode / etc.

use serde::{Serialize, de::DeserializeOwned};

/// Pluggable codec — serialize a typed payload to bytes and back.
///
/// Implementations must be `Send + Sync + 'static` so a `Worker<C>` can keep
/// a codec instance across tasks.
pub trait Codec: Send + Sync + 'static {
    /// Concrete error type returned by `encode` / `decode`. Must be a real
    /// `std::error::Error` so library glue can wrap via `Box<dyn Error>`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Serialize `value` into a byte buffer.
    ///
    /// # Errors
    /// Returns `Self::Error` when the underlying serializer rejects `value`
    /// (e.g. non-string map keys for JSON, NaN floats, etc.).
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Self::Error>;

    /// Deserialize `bytes` into `T`.
    ///
    /// # Errors
    /// Returns `Self::Error` when `bytes` is not a valid representation of
    /// `T` for this codec.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error>;
}

/// Default codec — `serde_json` over the wire.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    type Error = serde_json::Error;

    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(value)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        serde_json::from_slice(bytes)
    }
}
