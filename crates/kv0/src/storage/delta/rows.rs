//! Tracking and records tables as Delta tables.
//!
//! - Columns are kv0's logical (signed) schema; Delta has no unsigned types. Batches cross
//!   between Arrow 58 (the tracking layer) and 59 (delta-rs) without copying
//!   ([`crate::storage::arrow_bridge`]).
//! - A file's id is its path. Files are read in the order they were written: each carries an
//!   `Add.tags["kv0.order"]` taken from a process-wide clock when it is written; a rewrite gives
//!   its output the order of what it replaces. Later rows win where readers pick one of
//!   several (overwritten metric steps), so scans keep this order.
//! - Scans prune files by their log statistics (`seq`, `series_id`, `step`), then row groups by
//!   Parquet statistics and, for records' `item_id`, Bloom filters, and filter rows with
//!   [`super::filter::eval`].
//! - Removing rows rewrites the files holding them (copy on write) with `data_change = false`,
//!   like every other rewrite (see [`super`]); the new file keeps the old one's tags.
//! - File tags ([`Table::tagged`]) are `Add.tags` entries, which checkpoints carry.
//! - A writer's mark is a `txn` action of the commit holding its rows.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::SchemaRef;
use deltalake_core::arrow::array::{new_null_array, ArrayRef, RecordBatch as RecordBatch59};
use deltalake_core::arrow::compute::{cast, filter_record_batch};
use deltalake_core::arrow::datatypes::{Schema as Schema59, SchemaRef as SchemaRef59};
use deltalake_core::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake_core::kernel::{Add, StructField, StructType};
use deltalake_core::logstore::object_store::ObjectStoreExt as _;
#[allow(deprecated)] // see `scan`
use deltalake_core::parquet::arrow::async_reader::ParquetObjectReader;
use deltalake_core::parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use deltalake_core::parquet::arrow::ProjectionMask;
use deltalake_core::{DeltaTable, ObjectStore, Path};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};

use super::commit::{self, FileWriter};
use super::create::{self, Spec};
use super::filter;
use super::footer_cache::FooterCache;
use super::{props, table};
use crate::error::{Error, Result};
use crate::storage::arrow_bridge::{from_59, to_59};
use crate::storage::oplog::{FileId, WriterMark};
use crate::storage::pred::Pred;
use crate::storage::table::{BatchStream, FileTag, Scan, Table, TableFile, TableRef, Tagged};
use crate::storage::types::{logical_schema, seq_from_i64, seq_to_i64};
use crate::storage::StorageOptions;

/// Rows per file a rewrite writes, and below which [`Table::merge_small`] merges by default.
const TARGET_ROWS: u64 = 1 << 20;

/// The file tag holding a file's place in the table's order.
const ORDER_TAG: &str = "kv0.order";

/// Rows per batch read.
const BATCH_ROWS: usize = 8192;

/// Attempts of a rewrite that lost a race with another rewrite of the same files.
const REWRITE_ATTEMPTS: u32 = 8;

/// Attempts of an append whose commit conflicted (a concurrent schema change).
const APPEND_ATTEMPTS: u32 = 4;

/// Records' lookups by `item_id` use a Bloom filter.
const BLOOM_COLUMN: &str = "item_id";

/// The next file order: nanoseconds since the epoch, strictly increasing within the process.
fn next_order() -> i64 {
    static LAST: AtomicI64 = AtomicI64::new(0);
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
    let mut last = LAST.load(Ordering::Relaxed);
    loop {
        let next = now.max(last + 1);
        match LAST.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(l) => last = l,
        }
    }
}

/// Opens the table at `uri` (see [`crate::storage::table::Root::table`]). `schema` is the table as
/// the Lance engine stores it; Delta stores its logical (signed) form.
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    store: Option<Arc<dyn ObjectStore>>,
    schema: SchemaRef,
    create: bool,
    retention: Option<Duration>,
    mark_ttl: Duration,
) -> Result<TableRef> {
    let logical = logical_schema(&schema);
    let arrow = to_59(&arrow_array::RecordBatch::new_empty(logical))?.schema();
    let kernel: StructType = arrow
        .as_ref()
        .try_into_kernel()
        .map_err(deltalake_core::DeltaTableError::from)?;
    let columns: Vec<StructField> = kernel.fields().cloned().collect();
    // Log statistics only for what scans prune files by; never strings or values.
    let stats: Vec<&str> = ["seq", "series_id", "step"]
        .into_iter()
        .filter(|c| arrow.field_with_name(c).is_ok())
        .collect();
    let spec = Spec {
        columns,
        config: props::table_config(retention, mark_ttl, &stats.join(",")),
    };
    let bloom = arrow
        .field_with_name(BLOOM_COLUMN)
        .is_ok()
        .then_some(BLOOM_COLUMN);
    let table = create::open(uri, options, None, create.then_some(&spec), store.as_ref()).await?;
    let shared = Arc::new(Shared {
        uri: uri.to_string(),
        footers: Arc::new(FooterCache::default()),
        bloom,
        pruning: Pruning::default(),
    });
    Ok(Box::new(RowTable::new(table, shared)?))
}

/// What every version of one handle shares.
struct Shared {
    uri: String,
    footers: Arc<FooterCache>,
    /// The column with a Bloom filter, if any.
    bloom: Option<&'static str>,
    pruning: Pruning,
}

/// Counts of what scans read and skipped (diagnostics and tests).
#[derive(Default)]
pub(crate) struct Pruning {
    pub files_read: AtomicU64,
    pub files_skipped: AtomicU64,
    pub row_groups_read: AtomicU64,
    pub row_groups_skipped: AtomicU64,
}

#[derive(Clone, Debug)]
struct FileEntry {
    path: String,
    size: u64,
    rows: u64,
    stats: Option<Arc<serde_json::Value>>,
    tags: HashMap<String, String>,
    order: i64,
}

impl FileEntry {
    fn may_match(&self, pred: &Pred) -> bool {
        match &self.stats {
            Some(doc) => filter::may_match(pred, &|c| filter::file_stats(doc, c)),
            None => true,
        }
    }

    /// Tags other than the order.
    fn kind(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = self
            .tags
            .iter()
            .filter(|(k, _)| k.as_str() != ORDER_TAG)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        v.sort();
        v
    }
}

/// A table version.
#[derive(Clone)]
pub(crate) struct RowTable {
    table: DeltaTable,
    shared: Arc<Shared>,
    /// Data files in table order.
    files: Arc<Vec<FileEntry>>,
    /// The table's schema (Arrow 59).
    schema: SchemaRef59,
    /// Files rewritten by [`Table::rewrite_where`] since [`Table::compact_removed`] last ran.
    rewritten: usize,
}

impl RowTable {
    fn new(table: DeltaTable, shared: Arc<Shared>) -> Result<Self> {
        RowTable::after(table, shared, &[])
    }

    /// The version `table`, reusing what `known` (an earlier version's files) already parsed.
    fn after(table: DeltaTable, shared: Arc<Shared>, known: &[FileEntry]) -> Result<Self> {
        let known: HashMap<&str, &FileEntry> = known.iter().map(|f| (f.path.as_str(), f)).collect();
        let snap = table.snapshot()?;
        let mut files = Vec::new();
        for f in snap.log_data().iter() {
            if f.deletion_vector_descriptor().is_some() {
                return Err(Error::NotSupported(format!(
                    "{} has deletion vectors, which kv0 does not implement",
                    shared.uri
                )));
            }
            let path = f.object_store_path().to_string();
            if let Some(e) = known.get(path.as_str()) {
                files.push((*e).clone());
                continue;
            }
            #[allow(deprecated)] // the only public way to a file's tags
            let tags: HashMap<String, String> = f
                .add_action()
                .tags
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v)))
                .collect();
            let order = tags
                .get(ORDER_TAG)
                .and_then(|o| o.parse().ok())
                .unwrap_or_else(|| f.modification_time().saturating_mul(1_000_000));
            files.push(FileEntry {
                path,
                size: f.size().max(0) as u64,
                rows: f.num_records().unwrap_or(0) as u64,
                stats: f
                    .stats()
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .map(Arc::new),
                tags,
                order,
            });
        }
        files.sort_by(|a, b| (a.order, &a.path).cmp(&(b.order, &b.path)));
        let schema = arrow_schema(&table)?;
        Ok(RowTable {
            table,
            shared,
            files: Arc::new(files),
            schema,
            rewritten: 0,
        })
    }

    /// This handle moved to `table`.
    fn moved(&mut self, table: DeltaTable) -> Result<()> {
        let rewritten = self.rewritten;
        *self = RowTable::after(table, self.shared.clone(), &self.files)?;
        self.rewritten = rewritten;
        Ok(())
    }

    fn at(&self, table: DeltaTable) -> Result<TableRef> {
        Ok(Box::new(RowTable::after(
            table,
            self.shared.clone(),
            &self.files,
        )?))
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        self.table.object_store()
    }

    async fn load_latest(&self) -> Result<DeltaTable> {
        let mut t = DeltaTable::new(self.table.log_store());
        t.load().await?;
        Ok(t)
    }

    /// Files a scan reads: of `files`, those whose log statistics allow a match.
    fn pick(&self, scan: &Scan) -> Vec<FileEntry> {
        let ids: Option<HashSet<&str>> = scan
            .files
            .as_ref()
            .map(|f| f.iter().map(|id| id.0.as_str()).collect());
        let mut out = Vec::new();
        for f in self.files.iter() {
            if ids
                .as_ref()
                .is_some_and(|ids| !ids.contains(f.path.as_str()))
            {
                continue;
            }
            if scan.filter.as_ref().is_some_and(|p| !f.may_match(p)) {
                self.shared
                    .pruning
                    .files_skipped
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            out.push(f.clone());
        }
        out
    }

    fn reader(&self, scan: &Scan) -> Reader {
        Reader {
            store: self.store(),
            shared: self.shared.clone(),
            schema: self.schema.clone(),
            pred: scan.filter.clone().map(Arc::new),
            columns: scan
                .columns
                .as_ref()
                .map(|c| Arc::new(c.iter().map(|s| s.to_string()).collect())),
        }
    }

    /// The batches of `scan`, in table order (up to `parallel` files read at once).
    fn batches(&self, scan: &Scan, parallel: usize) -> BoxStream<'static, Result<RecordBatch59>> {
        let files = self.pick(scan);
        let reader = self.reader(scan);
        if parallel <= 1 {
            return futures::stream::iter(files)
                .map(move |f| reader.file(f))
                .flatten()
                .boxed();
        }
        futures::stream::iter(files)
            .map(move |f| reader.file(f).try_collect::<Vec<_>>())
            .buffered(parallel)
            .map_ok(|v| futures::stream::iter(v.into_iter().map(Ok)))
            .try_flatten()
            .boxed()
    }

    /// Writes `batches` (Arrow 58, any column order, missing nullable columns allowed) into
    /// files of up to `target` rows.
    async fn write(
        &self,
        mut source: BoxStream<'_, Result<arrow_array::RecordBatch>>,
        target: Option<u64>,
    ) -> Result<Vec<commit::Written>> {
        let mut w = FileWriter::new(&self.table, props::rows_writer(self.shared.bloom), target)?;
        while let Some(b) = source.next().await {
            let b = b?;
            if b.num_rows() > 0 {
                w.write(conform(&to_59(&b)?, &self.schema)?).await?;
            }
        }
        w.finish().await
    }

    /// Commits a rewrite, retrying on a fresh version when another rewrite took some of the
    /// same files first: `plan` computes `(removed paths, adds)` for a version, or `None` when
    /// there is nothing to do. Returns the files removed.
    async fn rewrite<F>(&mut self, op: &str, mut plan: F) -> Result<usize>
    where
        F: for<'a> FnMut(&'a RowTable) -> BoxFuture<'a, Result<Option<(Vec<String>, Vec<Add>)>>>,
    {
        let mut attempt = 0;
        loop {
            let Some((removed, adds)) = plan(self).await? else {
                return Ok(0);
            };
            match commit::rewrite(&self.table, &removed, adds, op).await {
                Ok(t) => {
                    self.moved(t)?;
                    return Ok(removed.len());
                }
                Err(Error::Conflict(m)) if attempt + 1 < REWRITE_ATTEMPTS => {
                    attempt += 1;
                    tracing::debug!(uri = %self.shared.uri, attempt, error = %m, "kv0: rewrite conflicted; planning it again");
                    tokio::time::sleep(crate::storage::backoff(attempt, 100, 5_000)).await;
                    let t = self.load_latest().await?;
                    self.moved(t)?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Copy-on-write removal of the rows matching `pred` from this version's files: the
    /// removed paths and their replacements.
    async fn plan_delete(&self, pred: &Pred) -> Result<Option<(Vec<String>, Vec<Add>)>> {
        let candidates = self.pick(&Scan::filter(pred.clone()));
        let scan = Scan::filter(pred.clone());
        let reader = self.reader(&scan);
        // Which candidates hold a matching row: only the predicate's columns are read.
        let mut cols = Vec::new();
        filter::columns(pred, &mut cols);
        let probe = Reader {
            columns: Some(Arc::new(cols)),
            ..reader.clone()
        };
        let hits: Vec<bool> = futures::stream::iter(candidates.clone())
            .map(|f| {
                let mut s = probe.file(f);
                async move { Ok::<_, Error>(s.try_next().await?.is_some()) }
            })
            .buffered(super::scan::parallelism())
            .try_collect()
            .await?;
        let doomed: Vec<FileEntry> = candidates
            .into_iter()
            .zip(hits)
            .filter(|(_, hit)| *hit)
            .map(|(f, _)| f)
            .collect();
        if doomed.is_empty() {
            return Ok(None);
        }
        let mut removed = Vec::new();
        let mut adds = Vec::new();
        for f in doomed {
            // Every row of the file, those not matching kept.
            let all = Reader {
                pred: None,
                columns: None,
                ..reader.clone()
            };
            let p = pred.clone();
            let kept = all.file(f.clone()).map(move |b| {
                let b = b?;
                let m = filter::eval(&p, &b)?;
                let keep = deltalake_core::arrow::compute::not(&m)?;
                let b = filter_record_batch(&b, &keep)?;
                from_59(&b)
            });
            for w in self.write(kept.boxed(), None).await? {
                adds.push(with_tags(w.add, &f.tags));
            }
            removed.push(f.path);
        }
        Ok(Some((removed, adds)))
    }

    /// Merges runs of consecutive small files with the same tags, rows unchanged and in order.
    async fn plan_merge(&self, target: u64) -> Result<Option<(Vec<String>, Vec<Add>)>> {
        let mut groups: Vec<Vec<FileEntry>> = Vec::new();
        let mut cur: Vec<FileEntry> = Vec::new();
        for f in self.files.iter() {
            let fits = f.rows < target && cur.first().is_none_or(|c| c.kind() == f.kind());
            if !fits {
                groups.push(std::mem::take(&mut cur));
            }
            if f.rows < target {
                cur.push(f.clone());
            }
        }
        groups.push(cur);
        groups.retain(|g| g.len() > 1);
        if groups.is_empty() {
            return Ok(None);
        }
        let reader = self.reader(&Scan::all());
        let mut removed = Vec::new();
        let mut adds = Vec::new();
        for g in groups {
            let tags: HashMap<String, String> = g[0]
                .kind()
                .into_iter()
                .chain([(
                    ORDER_TAG.to_string(),
                    g.iter().map(|f| f.order).max().unwrap_or(0).to_string(),
                )])
                .collect();
            let r = reader.clone();
            let rows = futures::stream::iter(g.clone())
                .map(move |f| r.file(f))
                .flatten()
                .map(|b| from_59(&b?));
            for w in self.write(rows.boxed(), Some(target)).await? {
                adds.push(with_tags(w.add, &tags));
            }
            removed.extend(g.into_iter().map(|f| f.path));
        }
        Ok(Some((removed, adds)))
    }

    /// Commits an append, carrying `mark`; on a conflict (a concurrent schema change) the
    /// same files are committed again on the latest version.
    async fn commit_append(&mut self, adds: Vec<Add>, mark: Option<WriterMark>) -> Result<()> {
        let mut attempt = 0;
        loop {
            let txn = mark
                .as_ref()
                .map(|m| commit::mark(&m.writer, seq_to_i64(m.seq)));
            let err = match commit::append(&self.table, adds.clone(), txn).await {
                Ok(t) => return self.moved(t),
                Err(e) => e,
            };
            // A commit whose response was lost may have landed: its mark says so.
            let latest = self.load_latest().await?;
            if let Some(m) = &mark {
                let have = latest
                    .snapshot()?
                    .transaction_version(latest.log_store().as_ref(), commit::wal_app_id(&m.writer))
                    .await?;
                if have.is_some_and(|v| seq_from_i64(v) >= m.seq) {
                    tracing::debug!(uri = %self.shared.uri, error = %err, "kv0: the append was committed after all");
                    return self.moved(latest);
                }
            }
            attempt += 1;
            if !matches!(err, Error::Conflict(_)) || attempt >= APPEND_ATTEMPTS {
                return Err(err);
            }
            tokio::time::sleep(crate::storage::backoff(attempt, 50, 800)).await;
            self.moved(latest)?;
        }
    }
}

/// `add` carrying `tags` (and nothing else of the source's tags).
fn with_tags(mut add: Add, tags: &HashMap<String, String>) -> Add {
    add.tags = Some(
        tags.iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect(),
    );
    add
}

fn arrow_schema(table: &DeltaTable) -> Result<SchemaRef59> {
    let schema = table
        .snapshot()?
        .metadata()
        .parse_schema()
        .map_err(deltalake_core::DeltaTableError::from)?;
    let arrow: Schema59 = (&schema).try_into_arrow()?;
    Ok(Arc::new(arrow))
}

/// `b` as a batch of `schema`: columns by name, cast where only the type's details differ
/// (a list's item name), null where a nullable column is missing.
fn conform(b: &RecordBatch59, schema: &SchemaRef59) -> Result<RecordBatch59> {
    if b.schema().as_ref() == schema.as_ref() {
        return Ok(b.clone());
    }
    for f in b.schema().fields() {
        if schema.field_with_name(f.name()).is_err() {
            return Err(Error::InvalidArgument(format!(
                "column {} is not in the table",
                f.name()
            )));
        }
    }
    let cols = schema
        .fields()
        .iter()
        .map(|f| match b.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
            Some(c) => Ok(cast(c, f.data_type())?),
            None if f.is_nullable() => Ok(new_null_array(f.data_type(), b.num_rows())),
            None => Err(Error::InvalidArgument(format!(
                "column {} is missing and not nullable",
                f.name()
            ))),
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch59::try_new(schema.clone(), cols)?)
}

/// Reads files of one table version.
#[derive(Clone)]
struct Reader {
    store: Arc<dyn ObjectStore>,
    shared: Arc<Shared>,
    /// The table's schema: what every batch is read as.
    schema: SchemaRef59,
    pred: Option<Arc<Pred>>,
    /// Output columns, in this order (all when `None`).
    columns: Option<Arc<Vec<String>>>,
}

impl Reader {
    /// The output schema.
    fn out(&self) -> Result<SchemaRef59> {
        Ok(match &self.columns {
            None => self.schema.clone(),
            Some(cols) => {
                let idx = cols
                    .iter()
                    .map(|c| {
                        self.schema.index_of(c).map_err(|_| {
                            Error::InvalidArgument(format!("no column {c} in the table"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(self.schema.project(&idx)?)
            }
        })
    }

    /// The matching rows of `file`, projected, in file order.
    fn file(&self, file: FileEntry) -> BoxStream<'static, Result<RecordBatch59>> {
        let me = self.clone();
        let open = async move {
            let out = me.out()?;
            let p = &me.shared.pruning;
            let path = Path::from(file.path.as_str());
            let meta = me
                .shared
                .footers
                .get(me.store.clone(), &path, file.size)
                .await?;
            let md = meta.metadata().clone();
            let mut groups: Vec<usize> = (0..md.num_row_groups())
                .filter(|i| {
                    me.pred.as_ref().is_none_or(|pred| {
                        let rg = md.row_group(*i);
                        filter::may_match(pred, &|c| filter::row_group_stats(rg, c))
                    })
                })
                .collect();
            #[allow(deprecated)]
            let reader = ParquetObjectReader::new(me.store.clone(), path).with_file_size(file.size);
            let mut b = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta)
                .with_batch_size(BATCH_ROWS);
            // Bloom filters rule out row groups that cannot hold the value looked up.
            if let (Some(col), Some(pred)) = (me.shared.bloom, &me.pred) {
                if let Some(values) = filter::bloom_probe(pred, col) {
                    let idx = md
                        .file_metadata()
                        .schema_descr()
                        .columns()
                        .iter()
                        .position(|c| c.path().parts() == [col]);
                    if let Some(idx) = idx {
                        let mut kept = Vec::with_capacity(groups.len());
                        for g in groups {
                            let hit = match b.get_row_group_column_bloom_filter(g, idx).await? {
                                Some(sbbf) => values.iter().any(|v| sbbf.check(v.as_str())),
                                None => true,
                            };
                            if hit {
                                kept.push(g);
                            }
                        }
                        groups = kept;
                    }
                }
            }
            let skipped = md.num_row_groups() - groups.len();
            p.row_groups_skipped
                .fetch_add(skipped as u64, Ordering::Relaxed);
            p.row_groups_read
                .fetch_add(groups.len() as u64, Ordering::Relaxed);
            if groups.is_empty() {
                p.files_skipped.fetch_add(1, Ordering::Relaxed);
                return Ok::<_, Error>(futures::stream::empty().boxed());
            }
            p.files_read.fetch_add(1, Ordering::Relaxed);
            // The output columns and those the filter reads, by top-level name.
            let mut want: Vec<String> = out.fields().iter().map(|f| f.name().clone()).collect();
            if let Some(pred) = &me.pred {
                filter::columns(pred, &mut want);
            }
            let roots = md.file_metadata().schema_descr().root_schema().get_fields();
            let idx: Vec<usize> = roots
                .iter()
                .enumerate()
                .filter(|(_, f)| want.iter().any(|w| w == f.name()))
                .map(|(i, _)| i)
                .collect();
            let mask = ProjectionMask::roots(md.file_metadata().schema_descr(), idx);
            b = b.with_row_groups(groups).with_projection(mask);
            let pred = me.pred.clone();
            let stream = b.build()?.map_err(Error::from).and_then(move |batch| {
                let r = (|| {
                    let batch = match &pred {
                        Some(p) => filter_record_batch(&batch, &filter::eval(p, &batch)?)?,
                        None => batch,
                    };
                    project(&batch, &out)
                })();
                futures::future::ready(r)
            });
            Ok(stream
                .try_filter(|b| futures::future::ready(b.num_rows() > 0))
                .boxed())
        };
        futures::stream::once(open).try_flatten().boxed()
    }
}

/// `b`'s columns as `out` names and types them: a column the file lacks (written before the
/// table gained it) is null.
fn project(b: &RecordBatch59, out: &SchemaRef59) -> Result<RecordBatch59> {
    let cols = out
        .fields()
        .iter()
        .map(|f| match b.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
            Some(c) => cast(c, f.data_type()).map_err(|e| {
                Error::Corruption(format!(
                    "column {} is stored as {}, not {}: {e}",
                    f.name(),
                    c.data_type(),
                    f.data_type()
                ))
            }),
            None => Ok(new_null_array(f.data_type(), b.num_rows())),
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch59::try_new_with_options(
        out.clone(),
        cols,
        &deltalake_core::arrow::array::RecordBatchOptions::new().with_row_count(Some(b.num_rows())),
    )?)
}

fn parallel(scan: &Scan) -> usize {
    if scan.bounded {
        2
    } else {
        super::scan::parallelism()
    }
}

impl Table for RowTable {
    fn boxed(&self) -> TableRef {
        Box::new(self.clone())
    }

    fn version(&self) -> u64 {
        self.table.version().unwrap_or(0)
    }

    fn files(&self) -> Vec<TableFile> {
        self.files
            .iter()
            .map(|f| TableFile {
                id: FileId(f.path.clone()),
                rows: f.rows as usize,
            })
            .collect()
    }

    fn has_column(&self, name: &str) -> bool {
        self.schema.field_with_name(name).is_ok()
    }

    fn latest_version(&self) -> BoxFuture<'_, Result<u64>> {
        async move {
            match self.table.get_latest_version().await {
                Ok(v) => Ok(v),
                // This version's commit file was cleaned up: from the latest checkpoint.
                Err(_) => Ok(self.load_latest().await?.version().unwrap_or(0)),
            }
        }
        .boxed()
    }

    fn latest(&mut self) -> BoxFuture<'_, Result<()>> {
        async move {
            let mut t = self.table.clone();
            let moved = match t.get_latest_version().await {
                Ok(v) if self.table.version().is_some_and(|c| v <= c) => return Ok(()),
                Ok(_) => t.update_incremental(None).await,
                Err(e) => Err(e),
            };
            if moved.is_err() {
                t = self.load_latest().await?;
                if t.version() <= self.table.version() {
                    return Ok(());
                }
            }
            self.moved(t)
        }
        .boxed()
    }

    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<TableRef>> {
        async move {
            let mut t = DeltaTable::new(self.table.log_store());
            t.load_version(version).await?;
            self.at(t)
        }
        .boxed()
    }

    fn checkout_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<TableRef>> {
        async move {
            let v = table::tag_version(&self.table, tag).await?;
            self.checkout(v).await
        }
        .boxed()
    }

    fn scan(&self, scan: Scan) -> BoxFuture<'_, Result<Vec<arrow_array::RecordBatch>>> {
        async move {
            let n = parallel(&scan);
            self.batches(&scan, n)
                .map(|b| from_59(&b?))
                .try_collect()
                .await
        }
        .boxed()
    }

    fn scan_stream(&self, scan: Scan) -> BoxFuture<'_, Result<BatchStream>> {
        async move { Ok(self.batches(&scan, 1).map(|b| from_59(&b?)).boxed()) }.boxed()
    }

    fn count(&self, filter: Option<Pred>) -> BoxFuture<'_, Result<usize>> {
        async move {
            let Some(pred) = filter else {
                if self.files.iter().all(|f| f.stats.is_some()) {
                    return Ok(self.files.iter().map(|f| f.rows as usize).sum());
                }
                let n = self
                    .batches(&Scan::all().columns(&[]), parallel(&Scan::all()))
                    .try_fold(0, |n, b| futures::future::ready(Ok(n + b.num_rows())))
                    .await?;
                return Ok(n);
            };
            let scan = Scan::filter(pred).columns(&[]);
            self.batches(&scan, parallel(&scan))
                .try_fold(0, |n, b| futures::future::ready(Ok(n + b.num_rows())))
                .await
        }
        .boxed()
    }

    fn append(
        &mut self,
        batches: Vec<arrow_array::RecordBatch>,
        mark: Option<WriterMark>,
    ) -> BoxFuture<'_, Result<()>> {
        async move {
            let written = self
                .write(
                    futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
                    None,
                )
                .await?;
            if written.is_empty() {
                return Ok(());
            }
            let order = HashMap::from([(ORDER_TAG.to_string(), next_order().to_string())]);
            let adds = written
                .into_iter()
                .map(|w| with_tags(w.add, &order))
                .collect();
            self.commit_append(adds, mark).await
        }
        .boxed()
    }

    fn writer_mark<'a>(&'a self, writer: &'a str) -> BoxFuture<'a, Result<Option<u64>>> {
        async move {
            let v = self
                .table
                .snapshot()?
                .transaction_version(self.table.log_store().as_ref(), commit::wal_app_id(writer))
                .await?;
            Ok(v.map(seq_from_i64))
        }
        .boxed()
    }

    fn keeps_marks(&self) -> bool {
        true
    }

    /// `added` is the stored (Lance) layout; Delta stores its logical form. A metadata commit,
    /// so concurrent writers each retry once.
    fn add_columns(&mut self, added: SchemaRef) -> BoxFuture<'_, Result<()>> {
        async move {
            let logical = logical_schema(&added);
            let arrow = to_59(&arrow_array::RecordBatch::new_empty(logical))?.schema();
            let kernel: StructType = arrow
                .as_ref()
                .try_into_kernel()
                .map_err(deltalake_core::DeltaTableError::from)?;
            let fields: Vec<StructField> = kernel
                .fields()
                .filter(|f| !self.has_column(f.name()))
                .map(|f| StructField::new(f.name().clone(), f.data_type().clone(), true))
                .collect();
            if fields.is_empty() {
                return Ok(());
            }
            let t = self.table.clone().add_columns().with_fields(fields).await?;
            self.moved(t)
        }
        .boxed()
    }

    fn rewrite_where(&mut self, pred: Pred) -> BoxFuture<'_, Result<()>> {
        async move {
            let n = self
                .rewrite("delete", |t| {
                    let p = pred.clone();
                    async move { t.plan_delete(&p).await }.boxed()
                })
                .await?;
            self.rewritten += n;
            Ok(())
        }
        .boxed()
    }

    fn merge_small(&mut self, target_rows: Option<usize>) -> BoxFuture<'_, Result<()>> {
        async move {
            let target = target_rows.map_or(TARGET_ROWS, |t| t as u64);
            self.rewrite("merge", |t| t.plan_merge(target).boxed())
                .await?;
            Ok(())
        }
        .boxed()
    }

    fn replace(
        &mut self,
        remove: Vec<FileId>,
        _schema: SchemaRef,
        source: BatchStream,
        tag: Option<FileTag>,
    ) -> BoxFuture<'_, Result<()>> {
        async move {
            let removed: Vec<String> = remove.into_iter().map(|f| f.0).collect();
            let gone: HashSet<&str> = removed.iter().map(String::as_str).collect();
            let order = self
                .files
                .iter()
                .filter(|f| gone.contains(f.path.as_str()))
                .map(|f| f.order)
                .max()
                .unwrap_or_else(next_order);
            let mut tags = HashMap::from([(ORDER_TAG.to_string(), order.to_string())]);
            if let Some(t) = &tag {
                tags.insert(t.key.to_string(), "1".to_string());
            }
            let mut adds: Vec<Add> = self
                .write(source, Some(TARGET_ROWS))
                .await?
                .into_iter()
                .map(|w| with_tags(w.add, &tags))
                .collect();
            // Kept files that should carry the tag and do not: added again with it.
            if let Some(t) = &tag {
                let keep: HashSet<&str> = t.keep.iter().map(|f| f.0.as_str()).collect();
                #[allow(deprecated)]
                for f in self.table.snapshot()?.log_data().iter() {
                    let path = f.object_store_path().to_string();
                    if !keep.contains(path.as_str()) || gone.contains(path.as_str()) {
                        continue;
                    }
                    let mut add = f.add_action();
                    let tags = add.tags.get_or_insert_with(HashMap::new);
                    if tags.contains_key(t.key) {
                        continue;
                    }
                    tags.insert(t.key.to_string(), Some("1".to_string()));
                    add.path = path;
                    adds.push(add);
                }
            }
            if removed.is_empty() && adds.is_empty() {
                return Ok(());
            }
            let t = commit::rewrite(&self.table, &removed, adds, "replace").await?;
            self.moved(t)
        }
        .boxed()
    }

    fn tagged(&self, key: &'static str) -> Tagged {
        // Rewrites keep tags (or merge only files with the same ones), so none is ever lost.
        Tagged {
            files: self
                .files
                .iter()
                .filter(|f| f.tags.contains_key(key))
                .map(|f| FileId(f.path.clone()))
                .collect(),
            lost: false,
        }
    }

    /// Removing rows rewrote their files already; returns how many.
    fn compact_removed(&mut self) -> BoxFuture<'_, Result<usize>> {
        let n = std::mem::take(&mut self.rewritten);
        async move { Ok(n) }.boxed()
    }

    /// Delta tables have no separate indexes: sorted files, statistics and (for records) Bloom
    /// filters written with the data serve lookups.
    fn index<'a>(&'a mut self, _columns: &'a [&'static str]) -> BoxFuture<'a, Result<()>> {
        async { Ok(()) }.boxed()
    }

    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>> {
        table::cleanup(&self.table, c).boxed()
    }

    fn needs_cleanup_cadence(&self) -> bool {
        true
    }

    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>> {
        table::tags(&self.table).boxed()
    }

    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>> {
        table::create_tag(&self.table, tag, version).boxed()
    }

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>> {
        table::delete_tag(&self.table, tag).boxed()
    }

    #[cfg(test)]
    fn pruning(&self) -> Option<[u64; 4]> {
        let p = &self.shared.pruning;
        Some([
            p.files_read.load(Ordering::Relaxed),
            p.files_skipped.load(Ordering::Relaxed),
            p.row_groups_read.load(Ordering::Relaxed),
            p.row_groups_skipped.load(Ordering::Relaxed),
        ])
    }
}

/// Lists the tables directly under `uri` (directory names), on `store` when given.
pub(crate) async fn list(
    uri: &str,
    options: &StorageOptions,
    store: Option<&Arc<dyn ObjectStore>>,
) -> Result<Vec<String>> {
    if store.is_none() && crate::storage::is_local(uri) {
        let dir = uri.strip_prefix("file://").unwrap_or(uri);
        return match std::fs::read_dir(dir) {
            Ok(rd) => Ok(rd
                .flatten()
                .filter(|e| e.path().join("_delta_log").is_dir())
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(Error::Io(e.to_string())),
        };
    }
    let store = table::handle_in(uri, options, store)?.object_store();
    match store.list_with_delimiter(None).await {
        Ok(l) => Ok(l
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_string))
            .collect()),
        Err(deltalake_core::ObjectStoreError::NotFound { .. }) => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// Deletes the table at `uri`, every file under it.
pub(crate) async fn remove(
    uri: &str,
    options: &StorageOptions,
    store: Option<&Arc<dyn ObjectStore>>,
) -> Result<()> {
    if store.is_none() && crate::storage::is_local(uri) {
        let dir = uri.strip_prefix("file://").unwrap_or(uri);
        return match std::fs::remove_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Io(e.to_string())),
        };
    }
    let store = table::handle_in(uri, options, store)?.object_store();
    let paths: Vec<Path> = store
        .list(None)
        .map_ok(|m| m.location)
        .try_collect()
        .await?;
    for p in paths {
        match store.delete(&p).await {
            Ok(()) | Err(deltalake_core::ObjectStoreError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Whether `uri` holds a Delta table (on `store` when given).
pub(crate) async fn exists(
    uri: &str,
    options: &StorageOptions,
    store: Option<&Arc<dyn ObjectStore>>,
) -> Result<bool> {
    if store.is_none() && crate::storage::is_local(uri) {
        let dir = uri.strip_prefix("file://").unwrap_or(uri);
        return Ok(std::path::Path::new(dir).join("_delta_log").is_dir());
    }
    Ok(table::handle_in(uri, options, store)?
        .verify_deltatable_existence()
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Array as _, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use crate::storage::table::FileTag;
    use crate::tracking::schema::{METRICS, METRICS_LOGICAL};

    const TTL: Duration = Duration::from_secs(3600);

    fn run<T>(f: impl std::future::Future<Output = Result<T>> + Send + 'static) -> T
    where
        T: Send + 'static,
    {
        crate::rt::block_on(f).unwrap()
    }

    fn table(uri: &str, schema: SchemaRef) -> TableRef {
        let uri = uri.to_string();
        run(async move { open(&uri, &StorageOptions::new(), None, schema, true, None, TTL).await })
    }

    fn strings(t: &TableRef, scan: Scan, col: &'static str) -> Vec<String> {
        let t = t.clone();
        let batches = run(async move { t.scan(scan).await });
        let mut out = Vec::new();
        for b in &batches {
            let c = b.column_by_name(col).unwrap();
            let c = c.as_any().downcast_ref::<StringArray>().unwrap();
            out.extend(c.iter().map(|v| v.unwrap_or("<null>").to_string()));
        }
        out
    }

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("seq", DataType::UInt64, false),
        ]))
    }

    fn ids(ids: &[&str], seq0: i64) -> RecordBatch {
        RecordBatch::try_new(
            logical_schema(&id_schema()),
            vec![
                Arc::new(StringArray::from_iter_values(ids)),
                Arc::new(Int64Array::from_iter_values(
                    (0..ids.len() as i64).map(|i| seq0 + i),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn predicates_select_the_rows_they_describe() {
        let mut t = table("memory://rows-pred", id_schema());
        let b = ids(
            &[
                "__kv0_wal__/a",
                "xxkv0_wal__/b",
                "__kv0_wal__/c",
                "it's",
                "50%_off",
            ],
            1,
        );
        let mut t2 = t.clone();
        t = run(async move {
            t2.append(vec![b], None).await?;
            Ok(t2)
        });
        let rows = |p: Pred| {
            let mut v = strings(&t, Scan::filter(p), "run_id");
            v.sort();
            v
        };
        let prefix = |p: &str| Pred::StartsWith("run_id", p.to_string());
        assert_eq!(
            rows(prefix("__kv0_wal__/")),
            ["__kv0_wal__/a", "__kv0_wal__/c"]
        );
        assert_eq!(rows(prefix("50%")), ["50%_off"]);
        assert!(rows(prefix("50_")).is_empty());
        assert_eq!(rows(Pred::eq("run_id", "it's")), ["it's"]);
        assert!(rows(Pred::is_in("run_id", Vec::<String>::new())).is_empty());
        assert_eq!(
            rows(Pred::ge("seq", 2i64).and(Pred::lt("seq", 4i64))),
            ["__kv0_wal__/c", "xxkv0_wal__/b"]
        );
        assert_eq!(rows(Pred::And(Vec::new())).len(), 5);
        assert!(rows(Pred::Or(Vec::new())).is_empty());
        let n = {
            let t = t.clone();
            run(async move { t.count(Some(Pred::ge("seq", 3i64))).await })
        };
        assert_eq!(n, 3);
        // Logical (signed) types come back.
        let t3 = t.clone();
        let b = run(async move { t3.scan(Scan::all()).await });
        assert_eq!(
            b[0].schema().field_with_name("seq").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn rows_come_back_in_write_order_through_merges_and_deletes() {
        let mut t = table("memory://rows-order", id_schema());
        t = run(async move {
            for i in 0..6 {
                t.append(vec![ids(&[&format!("r{i}")], i)], None).await?;
            }
            // Merge, then delete from the merged file: order and rows as written.
            t.merge_small(None).await?;
            t.rewrite_where(Pred::eq("run_id", "r2")).await?;
            t.append(vec![ids(&["r6"], 6)], None).await?;
            Ok(t)
        });
        assert_eq!(
            strings(&t, Scan::all(), "run_id"),
            ["r0", "r1", "r3", "r4", "r5", "r6"]
        );
        assert_eq!(t.files().len(), 2);
        let mut t2 = t.clone();
        let n = run(async move { t2.compact_removed().await });
        assert_eq!(n, 1, "the file the delete rewrote");
    }

    #[test]
    fn rewrites_commit_without_data_change_and_keep_file_tags() {
        let d = tempfile::tempdir().unwrap();
        let uri = d.path().join("t").to_string_lossy().into_owned();
        let mut t = table(&uri, id_schema());
        let (t, log) = run(async move {
            t.append(vec![ids(&["a", "b"], 0)], None).await?;
            let src = futures::stream::iter([Ok(ids(&["c", "d"], 2))]).boxed();
            let old = t.files().into_iter().map(|f| f.id).collect();
            let tag = FileTag {
                key: "kv0.sorted",
                keep: HashSet::new(),
            };
            t.replace(old, logical_schema(&id_schema()), src, Some(tag))
                .await?;
            t.rewrite_where(Pred::eq("run_id", "c")).await?;
            let n = t.compact_removed().await?;
            assert_eq!(n, 1);
            let v = t.version();
            Ok((t, v))
        });
        assert_eq!(strings(&t, Scan::all(), "run_id"), ["d"]);
        assert_eq!(t.tagged("kv0.sorted").files.len(), 1);
        // Neither the replace nor the delete marked its removes and adds as data changes.
        for v in 2..=log {
            let p = d.path().join("t/_delta_log").join(format!("{v:020}.json"));
            let body = std::fs::read_to_string(p).unwrap();
            for line in body.lines() {
                let a: serde_json::Value = serde_json::from_str(line).unwrap();
                for k in ["add", "remove"] {
                    if let Some(x) = a.get(k) {
                        assert_eq!(x["dataChange"], false, "{line}");
                    }
                }
            }
        }
    }

    #[test]
    fn older_files_read_new_columns_as_null() {
        let d = tempfile::tempdir().unwrap();
        let uri = d.path().join("t").to_string_lossy().into_owned();
        let mut t = table(&uri, id_schema());
        // A handle opened before the schema change appends after it.
        let mut stale = t.clone();
        let t = run(async move {
            t.append(vec![ids(&["old"], 0)], None).await?;
            let added = Arc::new(Schema::new(vec![Field::new("note", DataType::Utf8, true)]));
            t.add_columns(added.clone()).await?;
            t.add_columns(added).await?; // again: nothing to do
            let schema = Arc::new(Schema::new(vec![
                Field::new("run_id", DataType::Utf8, false),
                Field::new("seq", DataType::Int64, false),
                Field::new("note", DataType::Utf8, true),
            ]));
            let b = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["new"])),
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![Some("hi")])),
                ],
            )?;
            t.append(vec![b], None).await?;
            stale.append(vec![ids(&["stale"], 2)], None).await?;
            t.latest().await?;
            Ok(t)
        });
        assert!(t.has_column("note"));
        assert_eq!(strings(&t, Scan::all(), "run_id"), ["old", "new", "stale"]);
        assert_eq!(strings(&t, Scan::all(), "note"), ["<null>", "hi", "<null>"]);
        assert_eq!(
            strings(&t, Scan::filter(Pred::IsNull("note")), "run_id"),
            ["old", "stale"]
        );
    }

    fn points(series: std::ops::Range<i64>, steps: i64) -> RecordBatch {
        let n = (series.end - series.start) * steps;
        let col = |v: Vec<i64>| Arc::new(Int64Array::from(v)) as arrow_array::ArrayRef;
        let ids: Vec<i64> = series
            .clone()
            .flat_map(|s| (0..steps).map(move |_| s))
            .collect();
        let st: Vec<i64> = series.flat_map(|_| 0..steps).collect();
        RecordBatch::try_new(
            METRICS_LOGICAL.clone(),
            vec![
                col(ids),
                col(st),
                Arc::new(Int64Array::new_null(n as usize)),
                Arc::new(
                    arrow_array::TimestampMicrosecondArray::from(vec![0; n as usize])
                        .with_timezone("UTC"),
                ),
                Arc::new(arrow_array::Float64Array::from(vec![1.0; n as usize])),
                Arc::new(arrow_array::BinaryArray::new_null(n as usize)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sorted_files_skip_by_statistics() {
        let mut t = table("memory://rows-prune", METRICS.clone());
        let t = run(async move {
            // One sorted file of 4 series × 40k points (several row groups), then small
            // unsorted files of other series.
            let src = futures::stream::iter((0..4).map(|s| Ok(points(s * 10..s * 10 + 1, 40_000))))
                .boxed();
            t.replace(Vec::new(), METRICS_LOGICAL.clone(), src, None)
                .await?;
            for s in 100..104 {
                t.append(vec![points(s..s + 1, 10)], None).await?;
            }
            Ok(t)
        });
        let before = t.pruning().unwrap();
        let filter = Pred::eq("series_id", 20i64)
            .and(Pred::ge("step", 100i64))
            .and(Pred::lt("step", 200i64));
        let t2 = t.clone();
        let got = run(async move { t2.count(Some(filter)).await });
        assert_eq!(got, 100);
        let after = t.pruning().unwrap();
        let d: Vec<u64> = (0..4).map(|i| after[i] - before[i]).collect();
        // [files read, files skipped, row groups read, row groups skipped]
        assert_eq!(d[0], 1, "{d:?}");
        assert_eq!(
            d[1], 4,
            "the unsorted files are skipped by their log statistics: {d:?}"
        );
        assert_eq!(d[2], 1, "{d:?}");
        assert!(d[3] >= 2, "{d:?}");
    }

    #[test]
    fn bloom_filters_skip_row_groups_without_the_item() {
        use crate::tracking::records::{RECORDS, RECORDS_LOGICAL};
        let mut t = table("memory://rows-bloom", RECORDS.clone());
        let t = run(async move {
            // Two row groups of interleaved item ids, so statistics cannot tell them apart.
            let n = 2 * props::ROW_GROUP_ROWS;
            let item = |i: usize| {
                format!(
                    "item-{}",
                    if i < n / 2 {
                        2 * i
                    } else {
                        2 * (i - n / 2) + 1
                    }
                )
            };
            let b = RecordBatch::try_new(
                RECORDS_LOGICAL.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..n as i64)),
                    Arc::new(StringArray::from_iter_values((0..n).map(|_| "t"))),
                    Arc::new(StringArray::new_null(n)),
                    Arc::new(StringArray::from_iter_values((0..n).map(item))),
                    Arc::new(StringArray::from_iter_values((0..n).map(|_| "{}"))),
                    Arc::new(
                        arrow_array::TimestampMicrosecondArray::from(vec![0; n])
                            .with_timezone("UTC"),
                    ),
                ],
            )?;
            t.append(vec![b], None).await?;
            Ok(t)
        });
        let before = t.pruning().unwrap();
        let t2 = t.clone();
        let n = run(async move { t2.count(Some(Pred::eq("item_id", "item-7"))).await });
        assert_eq!(n, 1);
        let after = t.pruning().unwrap();
        assert_eq!(after[2] - before[2], 1, "one row group read");
        assert_eq!(after[3] - before[3], 1, "one skipped by its Bloom filter");
    }
}
