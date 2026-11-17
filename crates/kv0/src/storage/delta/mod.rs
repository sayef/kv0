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
pub(crate) use super::faults;
pub(crate) mod filter;
pub(crate) mod footer_cache;
pub(crate) mod kv;
pub(crate) mod props;
pub(crate) mod rows;
pub(crate) mod scan;
pub(crate) mod table;
pub(crate) mod take;

use std::sync::Once;

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
