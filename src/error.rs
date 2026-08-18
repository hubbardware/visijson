use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("invalid JSON pointer: {0}")]
    InvalidPointer(String),
    #[error("type mismatch at {path}: {msg}")]
    TypeMismatch { path: String, msg: String },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("transaction retry limit exceeded after {0} attempts")]
    RetryLimit(u32),
    #[error("formula parse error in `{expr}`: {msg}")]
    FormulaParse { expr: String, msg: String },
    #[error("formula cycle detected involving {0}")]
    FormulaCycle(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("write-ahead log failed, store is fail-stopped for writes: {0}")]
    WalBroken(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;
