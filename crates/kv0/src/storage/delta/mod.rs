//! The Delta Lake engine: delta-rs for the transaction log, without DataFusion.
//!
//! delta-rs provides commits with optimistic concurrency, the log, time travel and a Parquet
//! writer. kv0 adds the rest here: scans ([`scan`]), reads by row ordinal ([`take`]), rewrites
//! ([`commit`]), tags and cleanup ([`table`]), and the tables of the tracking layer ([`rows`],
//! with predicates evaluated by [`filter`]).
//!
//! Rules every writer here follows:
//!
//! - Rewrites (compaction, merges) commit their removes and adds with `data_change = false`
//!   under an `Optimize` operation. Without DataFusion, delta-rs treats every file of a reader's
//!   snapshot as read, so a remove with `data_change = true` would fail every concurrent append.
//! - No metadata action after the version-0 commit: it conflicts with every concurrent writer.
//! - A writer's committed high-water mark is a Delta `txn` action (`appId` = `kv0-wal/<writer
//!   id>`, `version` = its highest sequence number), committed atomically with its data.

pub(crate) mod commit;
pub(crate) mod create;
pub(crate) mod faults;
pub(crate) mod filter;
pub(crate) mod footer_cache;
pub(crate) mod kv;
pub(crate) mod props;
pub(crate) mod rows;
pub(crate) mod scan;
pub(crate) mod table;
pub(crate) mod take;

use std::sync::Once;

use crate::error::Result;
use crate::storage::StorageOptions;

/// Registers the object-store handlers of the cloud backends kv0 was built with, once. Done
/// explicitly (not through the `deltalake` crate's load-time constructors) so it also works
/// in a Python extension module.
pub(crate) fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        #[cfg(feature = "aws")]
        deltalake_aws::register_handlers(None);
        #[cfg(feature = "gcp")]
        deltalake_gcp::register_handlers(None);
        #[cfg(feature = "azure")]
        deltalake_azure::register_handlers(None);
    });
}

/// Whether a missing object anywhere in `e`'s sources caused it.
pub(crate) fn not_found(e: &(dyn std::error::Error + 'static)) -> bool {
    use deltalake_core::ObjectStoreError;
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(e) = cur {
        if let Some(ObjectStoreError::NotFound { .. }) = e.downcast_ref::<ObjectStoreError>() {
            return true;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::NotFound {
                return true;
            }
        }
        cur = e.source();
    }
    false
}

/// Which engine holds (or will hold) the log at `uri`.
///
/// An existing location keeps its engine: a Delta log (`_delta_log/`) opens with Delta, a Lance
/// dataset (`_versions/`) with Lance. A new location uses Delta when `KV0_TEST_BACKEND=delta`
/// is set and Lance otherwise; the default switches to Delta once the Delta engine is complete.
/// (Detection of every layout with clear errors, and a format marker, come later.)
pub(crate) async fn chooses(uri: &str, options: &StorageOptions) -> Result<bool> {
    let wanted = std::env::var("KV0_TEST_BACKEND").is_ok_and(|v| v == "delta");
    if uri.starts_with("memory://") {
        return Ok(wanted);
    }
    Ok(match table::layout(uri, options).await? {
        table::Layout::Delta => true,
        table::Layout::Lance => false,
        table::Layout::None => wanted,
    })
}

/// Whether the tracking repository at `uri` uses Delta tables: an existing repository keeps
/// its engine (`runs/` with a Delta log: Delta; `runs.lance/`: Lance); a new one uses Delta
/// when `KV0_TEST_BACKEND=delta` is set.
pub(crate) async fn chooses_repo(uri: &str, options: &StorageOptions) -> Result<bool> {
    let wanted = std::env::var("KV0_TEST_BACKEND").is_ok_and(|v| v == "delta");
    if uri.starts_with("memory://") {
        return Ok(wanted);
    }
    if crate::storage::is_local(uri) {
        let dir = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
        return Ok(if dir.join("runs").join("_delta_log").is_dir() {
            true
        } else if dir.join("runs.lance").is_dir() {
            false
        } else {
            wanted
        });
    }
    let names = rows::list(uri, options, None).await?;
    Ok(if names.iter().any(|n| n == "runs") {
        true
    } else if names.iter().any(|n| n == "runs.lance") {
        false
    } else {
        wanted
    })
}
