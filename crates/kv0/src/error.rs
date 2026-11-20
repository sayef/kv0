//! Error type shared by the key-value and tracking layers.

use std::fmt;

/// Result alias used throughout kv0.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by kv0.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The database or table does not exist and `create_if_missing` was false.
    NotFound(String),
    /// Stored data could not be decoded.
    Corruption(String),
    /// The operation is not allowed, for example a write to a read-only handle.
    NotSupported(String),
    /// An argument was invalid, or the handle was already closed.
    InvalidArgument(String),
    /// A concurrent writer committed a change that conflicts with this one.
    Conflict(String),
    /// Object store or file system failure.
    Io(String),
    /// Any other error reported by moraine, Arrow or Parquet.
    Storage(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::NotSupported(m) => write!(f, "not supported: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Conflict(m) => write!(f, "commit conflict: {m}"),
            Error::Io(m) => write!(f, "I/O error: {m}"),
            Error::Storage(e) => write!(f, "storage error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Error::Storage(Box::new(e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::InvalidArgument(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        if crate::storage::files::not_found(&e) {
            Error::NotFound(e.to_string())
        } else {
            Error::Storage(Box::new(e))
        }
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        if crate::storage::files::not_found(&e) {
            Error::NotFound(e.to_string())
        } else {
            Error::Io(e.to_string())
        }
    }
}
