//! Erasure: physically removing the data of deleted runs, records and keys.
//!
//! Deleting marks rows deleted (and, in the key-value store, writes a delete marker); the bytes
//! stay in data files until compaction rewrites them, and in older versions until version
//! cleanup removes those. `purge` does both at once for the data that is deleted now: it rewrites
//! every fragment that holds deleted rows, then removes every version older than the result,
//! except versions a tag keeps. A tag that keeps a version still holding purged data stops the
//! purge with an error naming it, unless `force` is set, in which case the tag is deleted.

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
