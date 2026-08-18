use crate::{ExpectedVersion, Version};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("version conflict on '{stream}': expected {expected:?}, actual {actual:?}")]
    VersionConflict {
        stream: String,
        expected: ExpectedVersion,
        actual: Option<Version>,
    },
    #[error("fence mismatch on '{stream}': current token is '{current_token}'")]
    FenceMismatch { stream: String, current_token: String },
    #[error("backend: {0}")]
    Backend(#[from] slatedb::Error),
    #[error("encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("encoding: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),
    #[error("{0}")]
    InvalidInput(String),
}
