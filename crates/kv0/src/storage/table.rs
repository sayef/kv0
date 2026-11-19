//! The tracking layer's storage: versioned tables of Arrow record batches.
//!
//! A [`TableRef`] is a handle on one table at one version. Reads see that version; writes
//! commit a new version and move the handle to it. Rows live in files, each with an id that
//! stays the same for as long as the file is part of the table.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use super::oplog::{FileId, WriterMark};
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

/// Tags the files [`Table::replace`] writes. Files the replace keeps keep their own tags.
#[derive(Clone, Debug)]
pub(crate) struct FileTag {
    pub key: &'static str,
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

    /// Appends `batches` in one commit. With `mark`, the writer's mark is committed with the
    /// rows (a moraine transaction id), so a replayed append lands at most once.
    fn append(
        &mut self,
        batches: Vec<RecordBatch>,
        mark: Option<WriterMark>,
    ) -> BoxFuture<'_, Result<()>>;

    /// The highest sequence number `writer` committed to this table, as of this version.
    fn writer_mark<'a>(&'a self, writer: &'a str) -> BoxFuture<'a, Result<Option<u64>>>;

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

    /// Removes the data of versions committed before `c.before` (see
    /// [`crate::storage::Cleanup`]). The latest version and tagged versions are kept. Returns
    /// the data files removed (erased, with `c.erase`) and the bytes removed.
    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>>;

    /// moraine's upkeep at the end of `optimize` (checkpoint, log cleanup, vacuum), and moves
    /// the handle to the latest version.
    fn maintain(&mut self) -> BoxFuture<'_, Result<()>>;

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

/// A directory of tables: a tracking repository, one moraine database. Table names are paths
/// under it (`runs`, `records/<name>`); see [`super::moraine`] for where each is kept.
#[derive(Clone)]
pub(crate) struct Root {
    repo: Arc<super::moraine::Repo>,
}

impl Root {
    /// The repository at `uri`; a new one gets its format marker when `create` is set. `engine`
    /// must be moraine when given (see [`super::format::choose`]).
    pub(crate) async fn open(
        uri: &str,
        options: &StorageOptions,
        create: bool,
        engine: Option<super::Engine>,
    ) -> Result<Root> {
        use super::format::{choose, Kind};
        let (_, format) = choose(uri, options, Kind::Repository, engine).await?;
        let new = matches!(format, super::Format::Empty | super::Format::Unknown);
        let memory = uri
            .starts_with("memory://")
            .then(|| Arc::new(object_store::memory::InMemory::new()));
        let marker = (create && new).then_some(Kind::Repository);
        let repo = super::moraine::Repo::open(uri, options, memory, marker).await?;
        Ok(Root {
            repo: Arc::new(repo),
        })
    }

    /// Opens table `name`, creating it empty with `schema` (see `storage::types`) when it does
    /// not exist and `create` is set.
    pub(crate) async fn table(
        &self,
        name: &str,
        schema: SchemaRef,
        create: bool,
    ) -> Result<TableRef> {
        super::moraine::table::open(&self.repo, name, schema, create).await
    }

    /// Names of the tables in directory `dir` (only `records` holds any).
    pub(crate) async fn tables(&self, dir: &str) -> Result<Vec<String>> {
        if dir == crate::tracking::records::DIR {
            self.repo.record_tables().await
        } else {
            Ok(Vec::new())
        }
    }
}
