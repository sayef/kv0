//! Locations, object-store options and the storage engines behind them.
//!
//! A location is anything Lance accepts: a local path, `file://`, `memory://`, `s3://`, `gs://`,
//! `az://` and so on. Credentials and endpoints are passed through to Lance's object store as
//! string options (for example `aws_endpoint`, `aws_region`, `allow_http`), and fall back to the
//! usual environment variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`).
//!
//! The key-value layer reaches storage only through [`oplog::OpLog`], the tracking layer only
//! through [`table::Table`] with [`pred::Pred`] filters (a repository's tables through
//! [`table::Root`]); [`lance`] implements both, and so does `delta` with the `delta` feature.

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};

pub(crate) mod arrow_bridge;
#[cfg(feature = "delta")]
pub(crate) mod delta;
pub mod format;
pub(crate) mod lance;
pub(crate) mod oplog;
pub(crate) mod pred;
pub(crate) mod table;
pub(crate) mod types;

pub use self::format::{detect, Engine, Format};
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

/// What a version cleanup removes.
#[derive(Clone, Copy, Debug)]
// Lance reads only `before`.
#[cfg_attr(not(feature = "delta"), allow(dead_code))]
pub(crate) struct Cleanup {
    /// Versions committed before this are removed; the latest and tagged versions stay.
    pub before: chrono::DateTime<chrono::Utc>,
    /// Files that no version references are removed only once they are older than this: a
    /// writer may still be about to commit them. Engines that track every file they write
    /// (Lance) ignore it.
    pub orphan_grace: std::time::Duration,
    /// Erasure (purge): also remove every file a removed version referenced, however recently
    /// it was written or dropped.
    pub erase: bool,
}

impl Cleanup {
    /// Removes versions committed before `before`.
    pub(crate) fn before(
        before: chrono::DateTime<chrono::Utc>,
        orphan_grace: std::time::Duration,
    ) -> Cleanup {
        Cleanup {
            before,
            orphan_grace,
            erase: false,
        }
    }

    /// Removes every version but the latest and tagged ones, and the data only they hold.
    pub(crate) fn erase(orphan_grace: std::time::Duration) -> Cleanup {
        Cleanup {
            before: chrono::Utc::now() + chrono::Duration::seconds(1),
            orphan_grace,
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
