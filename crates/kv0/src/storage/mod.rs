//! Locations, object-store options and the storage engines behind them.
//!
//! A location is anything Lance accepts: a local path, `file://`, `memory://`, `s3://`, `gs://`,
//! `az://` and so on. Credentials and endpoints are passed through to Lance's object store as
//! string options (for example `aws_endpoint`, `aws_region`, `allow_http`), and fall back to the
//! usual environment variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`).
//!
//! The key-value layer reaches storage only through [`oplog::OpLog`], the tracking layer only
//! through [`table::Table`] with [`pred::Pred`] filters; [`lance`] implements both.

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};

pub(crate) mod arrow_bridge;
#[cfg(feature = "delta")]
pub(crate) mod delta;
pub(crate) mod lance;
pub(crate) mod oplog;
pub(crate) mod pred;
pub(crate) mod table;
pub(crate) mod types;

pub(crate) use self::lance::{list_dir, remove_dir};

/// Object-store options passed through to Lance, such as `aws_endpoint` or `aws_region`.
pub type StorageOptions = HashMap<String, String>;

/// Normalises a location: URIs pass through unchanged, local paths become absolute.
pub fn normalize_uri(location: &str) -> Result<String> {
    if location.is_empty() {
        return Err(Error::InvalidArgument("empty location".into()));
    }
    if location.contains("://") {
        return Ok(location.trim_end_matches('/').to_string());
    }
    let path = Path::new(location);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::Io(e.to_string()))?
            .join(path)
    };
    Ok(abs.to_string_lossy().trim_end_matches('/').to_string())
}

/// Whether the location is on the local file system.
pub fn is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

/// Joins a child name onto a location.
pub fn join(uri: &str, child: &str) -> String {
    format!("{}/{}", uri.trim_end_matches('/'), child)
}

/// Options with empty values are dropped: object stores treat an empty endpoint or region as a
/// value, not as absent, and some panic on it.
pub(crate) fn non_empty(options: &StorageOptions) -> StorageOptions {
    options
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
