//! The tracking layer's storage: versioned tables of Arrow record batches.
//!
//! A [`TableRef`] is a handle on one table at one version. Reads see that version; writes
//! commit a new version and move the handle to it. Rows live in files, each with an id that
//! stays the same for as long as the file is part of the table.

use std::collections::HashSet;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use super::oplog::FileId;
use super::pred::Pred;
use super::StorageOptions;
use crate::error::Result;

/// A table handle. Cheap to clone; a clone stays at its version while the original moves on.
pub(crate) type TableRef = Box<dyn Table>;

impl Clone for TableRef {
    fn clone(&self) -> Self {
        self.boxed()
    }
}

/// A data file of a table version.
#[derive(Clone, Debug)]
pub(crate) struct TableFile {
    pub id: FileId,
    /// Rows stored in the file, deleted ones included.
    pub rows: usize,
}

/// What a scan reads.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scan {
    /// Only rows matching this.
    pub filter: Option<Pred>,
    /// Only these columns (all when `None`).
    pub columns: Option<Vec<&'static str>>,
    /// Only rows of these files (all when `None`).
    pub files: Option<Vec<FileId>>,
    /// Keep the engine's read-ahead small, for scans that hold what they read in memory for a
    /// long time or run in the background. Latency-bound reads leave it off.
    pub bounded: bool,
}

impl Scan {
    pub fn all() -> Scan {
        Scan::default()
    }

    pub fn filter(pred: Pred) -> Scan {
        Scan {
            filter: Some(pred),
            ..Scan::default()
        }
    }

    pub fn columns(mut self, columns: &[&'static str]) -> Scan {
        self.columns = Some(columns.to_vec());
        self
    }

    pub fn files(mut self, files: Vec<FileId>) -> Scan {
        self.files = Some(files);
        self
    }

    pub fn bounded(mut self) -> Scan {
        self.bounded = true;
        self
    }
}

/// A stream of record batches written by [`Table::replace`].
pub(crate) type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Files carrying a file tag (see [`Table::replace`]).
#[derive(Clone, Debug, Default)]
pub(crate) struct Tagged {
    /// The tagged files of this version.
    pub files: HashSet<FileId>,
    /// Some file that carried the tag is gone (merged or rewritten without it).
    pub lost: bool,
}

/// Tags the files [`Table::replace`] writes, and keeps the tag on `keep`. Other files lose it.
#[derive(Clone, Debug)]
pub(crate) struct FileTag {
    pub key: &'static str,
    pub keep: HashSet<FileId>,
}

/// One version of a table in some storage engine.
pub(crate) trait Table: Send + Sync + 'static {
    /// A handle at the same version.
    fn boxed(&self) -> TableRef;

    fn version(&self) -> u64;

    /// The data files of this version.
    fn files(&self) -> Vec<TableFile>;

    /// Whether the table's schema has the column.
    fn has_column(&self, name: &str) -> bool;

    /// The latest committed version, read from storage; the handle stays where it is.
    fn latest_version(&self) -> BoxFuture<'_, Result<u64>>;

    /// Moves the handle to the latest version, when there is a newer one.
    fn latest(&mut self) -> BoxFuture<'_, Result<()>>;

    /// Version `version` of the same table.
    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<TableRef>>;

    /// The version a tag names, of the same table.
    fn checkout_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<TableRef>>;

    /// The rows `scan` asks for, in storage order.
    fn scan(&self, scan: Scan) -> BoxFuture<'_, Result<Vec<RecordBatch>>>;

    /// The rows `scan` asks for, in storage order, as they are read.
    fn scan_stream(&self, scan: Scan) -> BoxFuture<'_, Result<BatchStream>>;

    /// Rows matching `filter` (all rows when `None`).
    fn count(&self, filter: Option<Pred>) -> BoxFuture<'_, Result<usize>>;

    /// Appends `batches` in one commit.
    fn append(&mut self, batches: Vec<RecordBatch>) -> BoxFuture<'_, Result<()>>;

    /// Adds the nullable columns of `added`, null in every existing row.
    fn add_columns(&mut self, added: SchemaRef) -> BoxFuture<'_, Result<()>>;

    /// Removes the rows matching `pred`, in one commit.
    fn rewrite_where(&mut self, pred: Pred) -> BoxFuture<'_, Result<()>>;

    /// Merges files smaller than `target_rows` (the engine's default when `None`), rows
    /// unchanged, in one commit.
    fn merge_small(&mut self, target_rows: Option<usize>) -> BoxFuture<'_, Result<()>>;

    /// Replaces `remove` with files written from `source` (rows of `schema`), in one commit
    /// that keeps files other writers added meanwhile. With `tag`, the new files carry it.
    fn replace(
        &mut self,
        remove: Vec<FileId>,
        schema: SchemaRef,
        source: BatchStream,
        tag: Option<FileTag>,
    ) -> BoxFuture<'_, Result<()>>;

    /// Files carrying the file tag `key`.
    fn tagged(&self, key: &'static str) -> Tagged;

    /// Rewrites files still holding rows removed by [`Table::rewrite_where`], so their bytes
    /// leave the current version. Returns the files rewritten.
    fn compact_removed(&mut self) -> BoxFuture<'_, Result<usize>>;

    /// Builds lookup indexes on `columns`, or brings existing ones up to date.
    fn index<'a>(&'a mut self, columns: &'a [&'static str]) -> BoxFuture<'a, Result<()>>;

    /// Removes versions committed before `before` and the files only they use. The latest
    /// version and tagged versions are kept. Returns (versions, bytes) removed.
    fn cleanup(&self, before: chrono::DateTime<chrono::Utc>) -> BoxFuture<'_, Result<(u64, u64)>>;

    /// Tags and the versions they name, in no particular order.
    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>>;

    /// Tags `version`, failing when a concurrent cleanup removed it first.
    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>>;

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>>;
}

/// Files below `target` rows: only these can be merged, so only these count.
pub(crate) fn small_files(t: &dyn Table, target: usize) -> usize {
    t.files().iter().filter(|f| f.rows < target).count()
}

/// Tags of `t` on versions older than its current one, sorted by name.
pub(crate) async fn old_tags(t: &dyn Table) -> Result<Vec<(String, u64)>> {
    let current = t.version();
    let mut out: Vec<(String, u64)> = t
        .tags()
        .await?
        .into_iter()
        .filter(|(_, v)| *v < current)
        .collect();
    out.sort();
    Ok(out)
}

/// Opens the table at `uri`, creating it empty with `schema` when it does not exist and
/// `create` is set. `retention` applies to a table created here.
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    schema: SchemaRef,
    create: bool,
    retention: Option<std::time::Duration>,
) -> Result<TableRef> {
    super::lance::table::open(uri, options, schema, create, retention).await
}
