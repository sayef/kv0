//! Erasure: physically removing the data of deleted runs, records and keys.
//!
//! Deleting marks rows deleted (and, in the key-value store, writes a delete marker); the bytes
//! stay in data files until compaction rewrites them, and in older versions until version
//! cleanup removes those. `purge` does both at once for the data that is deleted now: it rewrites
//! every fragment that holds deleted rows, then removes every version older than the result,
//! except versions a tag keeps. A tag that keeps a version still holding purged data stops the
//! purge with an error naming it, unless `force` is set, in which case the tag is deleted.

use lance::dataset::cleanup::CleanupPolicyBuilder;
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::Dataset;

use crate::error::Result;

/// What [`crate::kv::Db::purge`] or [`crate::tracking::Repo::purge`] removed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// Deleted runs whose data was purged (tracking only).
    pub runs: usize,
    /// Fragments rewritten to drop deleted rows.
    pub fragments_rewritten: usize,
    /// Old versions removed.
    pub versions_removed: u64,
    /// Bytes of data, deletion, index and manifest files removed.
    pub bytes_removed: u64,
    /// Tags deleted because they kept purged data (only with `force`).
    pub tags_deleted: Vec<String>,
    /// Local view snapshots removed (key-value only).
    pub snapshots_removed: usize,
    /// Tags that, after the purge, keep a version holding data it removes: a write overwrote a
    /// tagged key while the purge ran (key-value only). Run purge again to handle them.
    pub tags_left: Vec<String>,
}

/// Rewrites every fragment of `ds` that has deleted rows. Returns the fragments rewritten.
pub(crate) async fn rewrite_deleted(ds: &mut Dataset) -> Result<usize> {
    if ds.fragments().iter().all(|f| f.deletion_file.is_none()) {
        return Ok(0);
    }
    let o = CompactionOptions {
        materialize_deletions: true,
        materialize_deletions_threshold: 0.0,
        ..CompactionOptions::default()
    };
    Ok(compact_files(ds, o, None).await?.fragments_removed)
}

/// Removes every version of `ds` but the latest and tagged ones. Returns (versions, bytes).
pub(crate) async fn remove_old_versions(ds: &Dataset) -> Result<(u64, u64)> {
    let policy = CleanupPolicyBuilder::default()
        .before_timestamp(chrono::Utc::now() + chrono::Duration::seconds(1))
        .error_if_tagged_old_versions(false)
        .build();
    let s = lance::dataset::cleanup::cleanup_old_versions(ds, policy).await?;
    Ok((s.old_versions, s.bytes_removed))
}

/// Tags of `ds` on versions older than its current one, as (name, version).
pub(crate) async fn old_tags(ds: &Dataset) -> Result<Vec<(String, u64)>> {
    let current = ds.version().version;
    let mut out: Vec<(String, u64)> = ds
        .tags()
        .list()
        .await?
        .into_iter()
        .map(|(n, c)| (n, c.version))
        .filter(|(_, v)| *v < current)
        .collect();
    out.sort();
    Ok(out)
}
