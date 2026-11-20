//! Locations, object-store options and the storage engine behind them.
//!
//! A location is a local path, `file://`, `memory://` or `s3://` (including S3-compatible
//! servers). Credentials and endpoints are passed to the object store as string options (for
//! example `aws_endpoint`, `aws_region`, `allow_http`), and fall back to the usual `AWS_*`
//! environment variables. Google Cloud Storage and Azure Blob Storage are not supported yet.
//!
//! The key-value layer reaches storage only through [`oplog::OpLog`], the tracking layer only
//! through [`table::Table`] with [`pred::Pred`] filters (a repository's tables through
//! [`table::Root`]); [`moraine`] implements both, as a thin adapter over the `morainedb` crate.

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};

pub(crate) mod faults;
pub(crate) mod files;
pub mod format;
pub(crate) mod moraine;
pub(crate) mod oplog;
pub(crate) mod pred;
pub(crate) mod table;
pub(crate) mod types;

pub use self::format::{detect, Engine, Format};

/// Object-store options, such as `aws_endpoint` or `aws_region`.
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

/// What a version cleanup removes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Cleanup {
    /// Data files removed by commits before this go now; the latest and tagged versions stay.
    pub before: chrono::DateTime<chrono::Utc>,
    /// Erasure (purge): also take the statistics of the removed files out of the log.
    pub erase: bool,
}

impl Cleanup {
    /// Removes the data of versions committed before `before`.
    pub(crate) fn before(before: chrono::DateTime<chrono::Utc>) -> Cleanup {
        Cleanup {
            before,
            erase: false,
        }
    }

    /// Removes the data of every version but the latest and tagged ones, and erases it from the
    /// log.
    pub(crate) fn erase() -> Cleanup {
        Cleanup {
            before: chrono::Utc::now() + chrono::Duration::seconds(1),
            erase: true,
        }
    }
}

/// A jittered exponential backoff for attempt `attempt` (from 1): about `base` ms doubled per
/// attempt, at most `max` ms, scaled by a random factor in [0.5, 1).
pub(crate) fn backoff(attempt: u32, base: u64, max: u64) -> std::time::Duration {
    use std::hash::{BuildHasher, Hasher};
    let ms = base.saturating_mul(1 << attempt.min(16)).min(max);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u32(attempt);
    let jitter = (h.finish() % 1000) as f64 / 1000.0;
    std::time::Duration::from_millis((ms as f64 * (0.5 + jitter / 2.0)) as u64)
}
