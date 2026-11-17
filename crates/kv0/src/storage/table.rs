//! The tracking layer's storage: versioned tables of Arrow record batches.
//!
//! A [`TableRef`] is a handle on one table at one version. Reads see that version; writes
//! commit a new version and move the handle to it. Rows live in files, each with an id that
//! stays the same for as long as the file is part of the table.

use std::collections::HashSet;
#[cfg(feature = "delta")]
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::FutureExt;

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

    /// Appends `batches` in one commit. With `mark`, an engine that keeps writer marks itself
    /// ([`Table::keeps_marks`]) commits the mark with the rows.
    fn append(
        &mut self,
        batches: Vec<RecordBatch>,
        mark: Option<WriterMark>,
    ) -> BoxFuture<'_, Result<()>>;

    /// Whether the engine keeps writer marks itself ([`Table::append`]); otherwise the caller
    /// writes them as rows.
    fn keeps_marks(&self) -> bool {
        false
    }

    /// The highest sequence number `writer` committed to this table, as of this version, when
    /// the engine keeps writer marks.
    fn writer_mark<'a>(&'a self, _writer: &'a str) -> BoxFuture<'a, Result<Option<u64>>> {
        async { Ok(None) }.boxed()
    }

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

    /// Removes versions committed before `c.before` and the files only they use (see
    /// [`crate::storage::Cleanup`]). The latest version and tagged versions are kept. Returns
    /// (versions, bytes) removed.
    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>>;

    /// Whether kv0 must run version cleanup itself (every few commits); `false` when the
    /// engine cleans up on its own (Lance's automatic cleanup, configured at creation).
    fn needs_cleanup_cadence(&self) -> bool {
        false
    }

    /// Tags and the versions they name, in no particular order.
    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>>;

    /// Tags `version`, failing when a concurrent cleanup removed it first.
    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>>;

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Files read and skipped, row groups read and skipped by scans of this handle, where the
    /// engine counts them.
    #[cfg(all(test, feature = "delta"))]
    fn pruning(&self) -> Option<[u64; 4]> {
        None
    }
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

/// A directory of tables in one engine: a tracking repository. Table names are paths under
/// it (`runs`, `records/<name>`); each engine places a table where it keeps it (Lance:
/// `<name>.lance`, Delta: `<name>/`).
#[derive(Clone)]
pub(crate) struct Root {
    uri: String,
    options: StorageOptions,
    engine: Engine,
}

#[derive(Clone)]
enum Engine {
    Lance,
    /// With the store every table shares, for `memory://` roots (each handle on a memory
    /// location otherwise gets a store of its own).
    #[cfg(feature = "delta")]
    Delta(Option<Arc<dyn deltalake_core::ObjectStore>>),
}

impl Root {
    /// The repository at `uri`, in the engine it was written with (a new one: see
    /// [`super::delta::chooses_repo`]).
    pub(crate) async fn open(uri: &str, options: &StorageOptions) -> Result<Root> {
        #[cfg(feature = "delta")]
        let engine = if super::delta::chooses_repo(uri, options).await? {
            Engine::Delta(uri.starts_with("memory://").then(|| {
                Arc::new(deltalake_core::logstore::object_store::memory::InMemory::new())
                    as Arc<dyn deltalake_core::ObjectStore>
            }))
        } else {
            Engine::Lance
        };
        #[cfg(not(feature = "delta"))]
        let engine = Engine::Lance;
        Ok(Root {
            uri: uri.to_string(),
            options: options.clone(),
            engine,
        })
    }

    /// Whether the tables are Delta tables.
    pub(crate) fn is_delta(&self) -> bool {
        !matches!(self.engine, Engine::Lance)
    }

    /// Where table `name` is kept.
    pub(crate) fn location(&self, name: &str) -> String {
        match self.engine {
            Engine::Lance => super::join(&self.uri, &format!("{name}.lance")),
            #[cfg(feature = "delta")]
            Engine::Delta(_) => super::join(&self.uri, name),
        }
    }

    /// The name of the table earlier builds kept every records table in (Lance:
    /// `records.lance`; Delta: `_legacy_records`, which a migration writes).
    pub(crate) fn legacy_records(&self) -> &'static str {
        match self.engine {
            Engine::Lance => "records",
            #[cfg(feature = "delta")]
            Engine::Delta(_) => "_legacy_records",
        }
    }

    /// Opens table `name`, creating it empty with `schema` (the stored layout of the Lance
    /// engine; see `storage::types`) when it does not exist and `create` is set. `retention`
    /// and `mark_ttl` (how long writer marks are kept, where the engine keeps them) apply to a
    /// table created here.
    pub(crate) async fn table(
        &self,
        name: &str,
        schema: SchemaRef,
        create: bool,
        retention: Option<std::time::Duration>,
        mark_ttl: std::time::Duration,
    ) -> Result<TableRef> {
        let uri = self.location(name);
        match &self.engine {
            Engine::Lance => {
                let _ = mark_ttl;
                super::lance::table::open(&uri, &self.options, schema, create, retention).await
            }
            #[cfg(feature = "delta")]
            Engine::Delta(store) => {
                super::delta::rows::open(
                    &uri,
                    &self.options,
                    store.clone(),
                    schema,
                    create,
                    retention,
                    mark_ttl,
                )
                .await
            }
        }
    }

    /// Names of the tables in directory `dir`.
    pub(crate) async fn tables(&self, dir: &str) -> Result<Vec<String>> {
        let uri = super::join(&self.uri, dir);
        match &self.engine {
            Engine::Lance => Ok(super::list_dir(&uri, &self.options)
                .await?
                .into_iter()
                .filter_map(|n| n.strip_suffix(".lance").map(str::to_string))
                .collect()),
            #[cfg(feature = "delta")]
            Engine::Delta(store) => {
                super::delta::rows::list(&uri, &self.options, store.as_ref()).await
            }
        }
    }

    /// Whether table `name` exists (one listing).
    pub(crate) async fn exists(&self, name: &str) -> Result<bool> {
        match &self.engine {
            Engine::Lance => {
                let file = format!("{name}.lance");
                let (dir, base) = match file.rsplit_once('/') {
                    Some((d, b)) => (super::join(&self.uri, d), b.to_string()),
                    None => (self.uri.clone(), file.clone()),
                };
                Ok(super::list_dir(&dir, &self.options).await?.contains(&base))
            }
            #[cfg(feature = "delta")]
            Engine::Delta(store) => {
                super::delta::rows::exists(&self.location(name), &self.options, store.as_ref())
                    .await
            }
        }
    }

    /// Deletes table `name` and every version of it.
    pub(crate) async fn remove(&self, name: &str) -> Result<()> {
        let uri = self.location(name);
        match &self.engine {
            Engine::Lance => super::remove_dir(&uri, &self.options).await,
            #[cfg(feature = "delta")]
            Engine::Delta(store) => {
                super::delta::rows::remove(&uri, &self.options, store.as_ref()).await
            }
        }
    }
}
