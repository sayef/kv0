//! A tracking or records table as a moraine table (see [`super`]).

use std::sync::Arc;

use arrow_array::{Array as _, ArrayRef, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};

use super::pins::Pins;
use super::{error, wal_app_id, Repo};
use crate::error::{Error, Result};
use crate::storage::oplog::FileId;
use crate::storage::oplog::WriterMark;
use crate::storage::pred::{to_expr, Pred};
use crate::storage::table::{BatchStream, FileTag, Scan, Table, TableFile, TableRef, Tagged};
use crate::storage::types::{logical_schema, seq_to_i64};

/// Opens kv0's table `name` (see [`crate::storage::table::Root::table`]), creating it when it
/// does not exist and `create` is set. `schema` is the table as `tracking::schema` declares it.
pub(crate) async fn open(
    repo: &Arc<Repo>,
    name: &str,
    schema: SchemaRef,
    create: bool,
) -> Result<TableRef> {
    let moraine_name = super::table_name(name);
    let db = repo.database();
    let table = match db.open_table(&moraine_name).await {
        Ok(t) => t,
        Err(moraine::Error::TableNotFound(_)) if create => {
            let (stored, options) = definition(name, &schema)?;
            repo.name_table(name).await?;
            match db.create_table(&moraine_name, stored, options).await {
                Ok(t) => t,
                // Created meanwhile by another writer, and upgraded since (added columns).
                Err(moraine::Error::TableExistsWithDifferentSchema(_)) => {
                    db.open_table(&moraine_name).await.map_err(error)?
                }
                Err(e) => return Err(error(e)),
            }
        }
        Err(moraine::Error::TableNotFound(_)) => {
            return Err(Error::NotFound(format!(
                "table {name} of {} (create_if_missing is off)",
                repo.uri()
            )))
        }
        Err(e) => return Err(error(e)),
    };
    let snap = table.snapshot().await.map_err(error)?;
    Ok(Box::new(MoraineTable {
        moraine: moraine_name,
        repo: repo.clone(),
        table,
        snap,
    }))
}

/// The stored schema and options of kv0's table `name`.
fn definition(name: &str, schema: &SchemaRef) -> Result<(Arc<Schema>, moraine::TableOptions)> {
    let logical = logical_schema(schema);
    let mut fields: Vec<Field> = logical
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_data_type(delta_type(f.data_type())))
        .collect();
    let cluster: &[&str] = if name == "metrics" {
        // Points carry their run, so a run's series sit together and a run deletes alone.
        fields.insert(0, Field::new("run_id", DataType::Utf8, false));
        &["run_id", "series_id"]
    } else if name.starts_with("records/") {
        &["run_id", "item_id"]
    } else if fields.iter().any(|f| f.name() == "run_id") {
        &["run_id"]
    } else {
        &[]
    };
    Ok((
        Arc::new(Schema::new(fields)),
        moraine::TableOptions::default().cluster_by(cluster.iter().copied()),
    ))
}

/// `t` as moraine stores it: Delta Lake names a list's item field `element`.
fn delta_type(t: &DataType) -> DataType {
    match t {
        DataType::List(item) => DataType::List(Arc::new(
            Field::new("element", delta_type(item.data_type()), item.is_nullable())
                .with_metadata(item.metadata().clone()),
        )),
        other => other.clone(),
    }
}

/// `a` with the type `to`, where the two differ only in a list's item field name.
fn renamed(a: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    match (a.data_type(), to) {
        (DataType::List(_), DataType::List(field)) => {
            let list = a
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::Corruption("a list column is not a list".into()))?;
            let (_, offsets, values, nulls) = list.clone().into_parts();
            let values = renamed(&values, field.data_type())?;
            Ok(Arc::new(ListArray::try_new(
                field.clone(),
                offsets,
                values,
                nulls,
            )?))
        }
        _ => Ok(a.clone()),
    }
}

/// `batch` (kv0's logical columns) as moraine takes it for a table of schema `schema`: list
/// columns with the item field named as the table names it.
fn stored(schema: &Schema, b: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(b.num_columns());
    let mut cols = Vec::with_capacity(b.num_columns());
    for (f, c) in b.schema().fields().iter().zip(b.columns()) {
        match schema.field_with_name(f.name()) {
            Ok(t) if t.data_type() != f.data_type() => {
                cols.push(renamed(c, t.data_type())?);
                fields.push(f.as_ref().clone().with_data_type(t.data_type().clone()));
            }
            _ => {
                cols.push(c.clone());
                fields.push(f.as_ref().clone());
            }
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

/// A table version: the moraine table and the snapshot this handle reads.
#[derive(Clone)]
struct MoraineTable {
    /// The moraine table's name.
    moraine: String,
    repo: Arc<Repo>,
    table: moraine::Table,
    snap: moraine::TableSnapshot,
}

/// The compaction `merge_small` runs: every run of small adjacent files, whatever their age.
fn merge_policy() -> moraine::CompactionPolicy {
    moraine::CompactionPolicy::full().build_sidecars(false)
}

impl MoraineTable {
    /// `batch` (kv0's logical columns) as moraine takes it for this table.
    fn to_stored(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        stored(&self.snap.schema(), batch)
    }

    fn pins(&self) -> Pins<'_> {
        Pins {
            repo: &self.repo,
            table: &self.table,
            name: &self.moraine,
        }
    }

    /// Moves the snapshot to the newest committed version.
    async fn refresh(&mut self) -> Result<()> {
        self.snap = self.table.snapshot().await.map_err(error)?;
        Ok(())
    }

    /// Moves the snapshot to the one the handle holds after its own commit: no request.
    fn after_commit(&mut self) {
        self.snap = self.table.snapshot_in_hand();
    }

    /// A handle on this table at `snap`.
    fn at(&self, snap: moraine::TableSnapshot) -> TableRef {
        Box::new(MoraineTable {
            snap,
            ..self.clone()
        })
    }

    fn expr(&self, pred: &Pred) -> moraine::expr::Expr {
        to_expr(pred, &self.snap.schema())
    }

    /// moraine's scan for `scan`, at this handle's version. Chosen files that this version no
    /// longer holds are skipped; the others are read in the version's file order.
    fn moraine_scan(&self, scan: &Scan) -> moraine::Scan {
        let mut s = self.snap.scan();
        if let Some(ids) = &scan.files {
            let wanted: std::collections::HashSet<&str> =
                ids.iter().map(|f| f.0.as_str()).collect();
            s = s.files(
                self.snap
                    .files()
                    .into_iter()
                    .filter(|f| wanted.contains(f.path.as_str()))
                    .map(|f| f.path),
            );
        }
        if let Some(p) = &scan.filter {
            s = s.filter(self.expr(p));
        }
        if let Some(c) = &scan.columns {
            s = s.select(c.iter().copied());
        }
        s
    }

    async fn read(&self, scan: Scan) -> Result<Vec<RecordBatch>> {
        let batches = self.moraine_scan(&scan).collect().await.map_err(error)?;
        Ok(batches)
    }
}

impl Table for MoraineTable {
    fn boxed(&self) -> TableRef {
        Box::new(self.clone())
    }

    fn version(&self) -> u64 {
        self.snap.version()
    }

    fn files(&self) -> Vec<TableFile> {
        self.snap
            .files()
            .into_iter()
            .map(|f| TableFile {
                id: FileId(f.path),
                rows: f.rows.unwrap_or(0) as usize,
            })
            .collect()
    }

    fn has_column(&self, name: &str) -> bool {
        self.snap.schema().field_with_name(name).is_ok()
    }

    fn latest_version(&self) -> BoxFuture<'_, Result<u64>> {
        async move { self.table.refresh().await.map_err(error) }.boxed()
    }

    fn latest(&mut self) -> BoxFuture<'_, Result<()>> {
        self.refresh().boxed()
    }

    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<TableRef>> {
        async move {
            let snap = self.pins().at_version(&self.snap, version).await?;
            Ok(self.at(snap))
        }
        .boxed()
    }

    fn checkout_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<TableRef>> {
        async move {
            let snap = self.pins().snapshot(tag).await?;
            Ok(self.at(snap))
        }
        .boxed()
    }

    fn scan(&self, scan: Scan) -> BoxFuture<'_, Result<Vec<RecordBatch>>> {
        self.read(scan).boxed()
    }

    fn scan_stream(&self, scan: Scan) -> BoxFuture<'_, Result<BatchStream>> {
        async move {
            let s = self.moraine_scan(&scan).stream();
            Ok(s.map(|b| b.map_err(error)).boxed())
        }
        .boxed()
    }

    fn count(&self, filter: Option<Pred>) -> BoxFuture<'_, Result<usize>> {
        async move {
            let mut s = self.snap.scan();
            if let Some(p) = &filter {
                s = s.filter(self.expr(p));
            }
            // One column is enough to count rows.
            let first = self.snap.schema().field(0).name().clone();
            s.select([first]).count().await.map_err(error)
        }
        .boxed()
    }

    fn append(
        &mut self,
        batches: Vec<RecordBatch>,
        mark: Option<WriterMark>,
    ) -> BoxFuture<'_, Result<()>> {
        async move {
            let batches = batches
                .iter()
                .filter(|b| b.num_rows() > 0)
                .map(|b| self.to_stored(b))
                .collect::<Result<Vec<_>>>()?;
            match mark {
                // A replay of rows this writer already committed is skipped: the transaction
                // id says they are in.
                Some(m) => {
                    let outcome = self
                        .table
                        .txn(wal_app_id(&m.writer), seq_to_i64(m.seq))
                        .append_unchecked(batches)
                        .await
                        .map_err(error)?;
                    if !outcome.is_committed() {
                        // Committed earlier, perhaps by another process: the newest version.
                        return self.refresh().await;
                    }
                }
                None if batches.is_empty() => return Ok(()),
                None => {
                    self.table.append(batches).await.map_err(error)?;
                }
            }
            self.after_commit();
            Ok(())
        }
        .boxed()
    }

    fn writer_mark<'a>(&'a self, writer: &'a str) -> BoxFuture<'a, Result<Option<u64>>> {
        async move {
            Ok(self
                .snap
                .last_txn_version(&wal_app_id(writer))
                .map(crate::storage::types::seq_from_i64))
        }
        .boxed()
    }

    fn add_columns(&mut self, added: SchemaRef) -> BoxFuture<'_, Result<()>> {
        async move {
            let added = logical_schema(&added);
            for f in added.fields() {
                let f = f.as_ref().clone().with_data_type(delta_type(f.data_type()));
                self.table.add_column(f).await.map_err(error)?;
            }
            self.after_commit();
            Ok(())
        }
        .boxed()
    }

    fn rewrite_where(&mut self, pred: Pred) -> BoxFuture<'_, Result<()>> {
        async move {
            self.refresh().await?;
            let e = self.expr(&pred);
            // Copy-on-write: the files holding matching rows are rewritten at once.
            self.table.delete(e).await.map_err(error)?;
            self.after_commit();
            Ok(())
        }
        .boxed()
    }

    /// moraine's own compaction, which merges runs of adjacent small files with their rows in
    /// order; the target is moraine's (its target file size), not a row count.
    fn merge_small(&mut self, _target_rows: Option<usize>) -> BoxFuture<'_, Result<()>> {
        async move {
            self.table.compact(&merge_policy()).await.map_err(error)?;
            self.refresh().await
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
            // The rows are written as they are read (moraine D68). An error of the source is
            // kept as it is, so a caller still tells a missing file from other failures.
            let failed: Arc<parking_lot::Mutex<Option<Error>>> = Arc::default();
            let schema = self.snap.schema();
            let slot = failed.clone();
            let rows = source.filter_map(move |b| {
                let b = b.and_then(|b| match b.num_rows() {
                    0 => Ok(None),
                    _ => stored(&schema, &b).map(Some),
                });
                let out = match b {
                    Ok(b) => b.map(Ok),
                    Err(e) => {
                        let m = e.to_string();
                        *slot.lock() = Some(e);
                        Some(Err(moraine::Error::from(std::io::Error::other(format!(
                            "reading the rows of a replace: {m}"
                        )))))
                    }
                };
                futures::future::ready(out)
            });
            let mut r = moraine::Replace::new()
                .remove_all(remove.into_iter().map(|f| f.0))
                .add_stream(rows);
            // moraine keeps tags per file: the files kept carry theirs already.
            if let Some(t) = &tag {
                r = r.tag(t.key, "1");
            }
            let replaced = self.table.replace_files(r).await;
            if let Some(e) = failed.lock().take() {
                return Err(e);
            }
            replaced.map_err(error)?;
            self.after_commit();
            Ok(())
        }
        .boxed()
    }

    /// Files whose moraine tags hold `key`. moraine's compaction keeps a file's tags: it merges
    /// only files whose tags are equal, and the merged file carries them (moraine D68). A
    /// tagged file that a delete rewrote loses its tag unnoticed (`lost` stays false); the
    /// statistics fold finds its rows by their exact statistics instead.
    fn tagged(&self, key: &'static str) -> Tagged {
        Tagged {
            files: self
                .snap
                .files()
                .into_iter()
                .filter(|f| f.tags.contains_key(key))
                .map(|f| FileId(f.path))
                .collect(),
            lost: false,
        }
    }

    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>> {
        async move { super::erase::cleanup(&self.table, c).await }.boxed()
    }

    fn maintain(&mut self) -> BoxFuture<'_, Result<()>> {
        async move {
            super::maintain(&self.table).await?;
            self.refresh().await
        }
        .boxed()
    }

    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>> {
        async move { self.pins().list().await }.boxed()
    }

    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>> {
        async move { self.pins().create(tag, version).await }.boxed()
    }

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>> {
        async move { self.pins().delete(tag).await }.boxed()
    }
}
