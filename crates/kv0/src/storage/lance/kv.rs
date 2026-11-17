//! The key-value operation log as a Lance dataset.
//!
//! The dataset is an operation log: `seq: u64, op: u8, key: binary, value: binary?`, the stored
//! form of kv0's logical log (`seq: i64, op: i8`, see [`crate::storage::types`]). A flush
//! appends one fragment in one commit. Compaction replaces every fragment it read with the
//! materialised view in a single `Update` commit, which Lance allows to commit concurrently with
//! other writers' appends. A file is a fragment, named by its fragment id; a row address is
//! Lance's (`fragment id << 32 | offset`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, BinaryArray, Int64Array, Int8Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::transaction::{Operation, Transaction, TransactionBuilder, UpdateMode};
use lance::dataset::{CommitBuilder, Dataset, InsertBuilder, ProjectionRequest, WriteMode};
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;

use crate::error::{Error, Result};
use crate::kv::view::{self, Op, SnapChunk};
use crate::kv::VersionInfo;
use crate::storage::oplog::{Columns, FileId, Log, LogFile, OpLog, RawRow, Sink, WriterMark};
use crate::storage::types::{self, seq_from_i64, seq_to_i64};
use crate::storage::StorageOptions;

pub(crate) static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
    ]))
});

/// The log as kv0 reads and writes it.
static LOGICAL: LazyLock<SchemaRef> = LazyLock::new(|| types::logical_schema(&SCHEMA));

/// A logical batch of the log, as stored.
fn stored_batch(
    seq: Int64Array,
    op: Int8Array,
    key: BinaryArray,
    value: BinaryArray,
) -> Result<RecordBatch> {
    let b = RecordBatch::try_new(
        LOGICAL.clone(),
        vec![Arc::new(seq), Arc::new(op), Arc::new(key), Arc::new(value)],
    )?;
    types::to_stored(&b, &SCHEMA)
}

/// Rows per record batch when writing.
const WRITE_BATCH_ROWS: usize = 64 * 1024;

const ROW_ADDR: &str = "_rowaddr";

/// Opens the log at `uri` (see [`crate::storage::oplog::open`]).
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    version: Option<u64>,
    create: bool,
    retention: Option<std::time::Duration>,
) -> Result<Log> {
    let ds = match version {
        Some(v) => super::open(uri, options, Some(v)).await?,
        None => super::open_or_create(uri, options, SCHEMA.clone(), create, retention).await?,
    };
    Ok(LanceLog::wrap(ds, options.clone()))
}

/// A version of the log: a Lance dataset handle checked out at that version.
pub(crate) struct LanceLog {
    ds: Dataset,
    options: StorageOptions,
}

impl LanceLog {
    fn wrap(ds: Dataset, options: StorageOptions) -> Log {
        Arc::new(LanceLog { ds, options })
    }

    fn fragments(&self, ids: Option<&HashSet<FileId>>) -> Vec<lance::table::format::Fragment> {
        self.ds
            .fragments()
            .iter()
            .filter(|f| ids.is_none_or(|ids| ids.contains(&file_id(f.id))))
            .cloned()
            .collect()
    }
}

fn file_id(fragment: u64) -> FileId {
    FileId(fragment.to_string())
}

fn to_batches(ops: &[Op]) -> Result<Vec<RecordBatch>> {
    ops.chunks(WRITE_BATCH_ROWS)
        .map(|chunk| {
            let seq = Int64Array::from_iter_values(chunk.iter().map(|o| seq_to_i64(o.seq)));
            let op = Int8Array::from_iter_values(chunk.iter().map(|o| o.kind as i8));
            let key = BinaryArray::from_iter_values(chunk.iter().map(|o| &o.key[..]));
            let value: BinaryArray = chunk.iter().map(|o| o.value.as_deref()).collect();
            stored_batch(seq, op, key, value)
        })
        .collect()
}

fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Result<&'a T> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| Error::Corruption(format!("column {name} is missing or mistyped")))
}

/// The values at row addresses `addrs` of `ds`, in the same order.
async fn take_values(ds: &Dataset, addrs: &[u64]) -> Result<Vec<Bytes>> {
    if addrs.is_empty() {
        return Ok(Vec::new());
    }
    let b = ds
        .take_rows(
            addrs,
            ProjectionRequest::from_columns(["value"], ds.schema()),
        )
        .await?;
    let v = col::<BinaryArray>(&b, "value")?;
    if v.len() != addrs.len() {
        return Err(Error::Corruption(format!(
            "took {} values for {} row addresses",
            v.len(),
            addrs.len()
        )));
    }
    Ok((0..v.len())
        .map(|i| Bytes::copy_from_slice(v.value(i)))
        .collect())
}

/// Turns a snapshot chunk into a record batch, reading values kept in Lance from `src`.
async fn resolve(src: &Dataset, mut chunk: SnapChunk) -> Result<RecordBatch> {
    let seq = Int64Array::from_iter_values(chunk.seq.iter().map(|s| seq_to_i64(*s)));
    let op = Int8Array::from_iter_values(chunk.op.iter().map(|o| *o as i8));
    let key = chunk.key.finish();
    let mut value = chunk.value.finish();
    if !chunk.stored.is_empty() {
        let addrs: Vec<u64> = chunk.stored.iter().map(|s| s.1).collect();
        let taken = take_values(src, &addrs).await?;
        let mut at: HashMap<usize, Bytes> = chunk.stored.iter().map(|s| s.0).zip(taken).collect();
        value = (0..value.len())
            .map(|i| match at.remove(&i) {
                Some(v) => Some(v),
                None => (!value.is_null(i)).then(|| Bytes::copy_from_slice(value.value(i))),
            })
            .collect::<Vec<_>>()
            .iter()
            .map(|v| v.as_deref())
            .collect();
    }
    stored_batch(seq, op, key, value)
}

/// Replaces the fragments `ds` holds with fragments written from `chunks` (a view snapshot), in
/// one commit. Values kept in Lance are read from `ds` chunk by chunk while writing.
async fn replace(ds: Dataset, chunks: Vec<SnapChunk>, options: StorageOptions) -> Result<Dataset> {
    let removed: Vec<u64> = ds.fragments().iter().map(|f| f.id).collect();
    let ds = Arc::new(ds);
    let new_fragments = if chunks.is_empty() {
        Vec::new()
    } else {
        let src = ds.clone();
        let stream = futures::stream::iter(chunks).then(move |chunk| {
            let src = src.clone();
            async move { resolve(&src, chunk).await }
                .map_err(|e| DataFusionError::External(Box::new(e)))
        });
        let source: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(SCHEMA.clone(), stream));
        let params = super::write_params(&options, WriteMode::Append);
        let txn = InsertBuilder::new(ds.clone())
            .with_params(&params)
            .execute_uncommitted_stream(source)
            .await?;
        match txn.operation {
            Operation::Append { fragments } => fragments,
            other => {
                return Err(Error::Storage(
                    format!("unexpected uncommitted operation {other:?}").into(),
                ))
            }
        }
    };
    let op = Operation::Update {
        removed_fragment_ids: removed,
        updated_fragments: Vec::new(),
        new_fragments,
        fields_modified: Vec::new(),
        compacted_sstables: Vec::new(),
        fields_for_preserving_frag_bitmap: Vec::new(),
        update_mode: Some(UpdateMode::RewriteRows),
        inserted_rows_filter: None,
        updated_fragment_offsets: None,
    };
    let txn: Transaction = TransactionBuilder::new(ds.version().version, op).build();
    Ok(CommitBuilder::new(ds).execute(txn).await?)
}

impl OpLog for LanceLog {
    fn uri(&self) -> &str {
        self.ds.uri()
    }

    fn version(&self) -> u64 {
        self.ds.version().version
    }

    fn files(&self) -> Vec<LogFile> {
        self.ds
            .fragments()
            .iter()
            .map(|f| {
                let file = f.files.first().map_or("", |d| d.path.as_str());
                let rows = f.physical_rows.unwrap_or(0);
                // Which deletion file, not only whether there is one: more deletions write a
                // new one.
                let del = f
                    .deletion_file
                    .as_ref()
                    .map_or(String::new(), |d| format!("{}.{}", d.id, d.read_version));
                LogFile {
                    id: file_id(f.id),
                    rows,
                    // The first data file, which Lance names uniquely, the row count and the
                    // deletion file.
                    ident: format!("{file}|{rows}|{del}"),
                }
            })
            .collect()
    }

    fn small_files(&self) -> usize {
        // Only fragments below Lance's compaction target can be merged.
        let target = CompactionOptions::default().target_rows_per_fragment;
        self.ds
            .fragments()
            .iter()
            .filter(|f| f.physical_rows.unwrap_or(0) < target)
            .count()
    }

    fn scan<'a>(
        &'a self,
        files: Option<&'a HashSet<FileId>>,
        columns: Columns,
        sink: Sink<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let frags = self.fragments(files);
            if frags.is_empty() {
                return Ok(());
            }
            let mut scan = self.ds.scan();
            if files.is_some() {
                scan.with_fragments(frags);
            }
            if columns == Columns::Keys {
                scan.project(&["seq", "op", "key"])?;
            }
            scan.with_row_address();
            let mut stream = scan.try_into_stream().await?;
            while let Some(b) = stream.try_next().await? {
                let b = types::to_logical(&b, &SCHEMA)?;
                let seq = col::<Int64Array>(&b, "seq")?;
                let op = col::<Int8Array>(&b, "op")?;
                let key = col::<BinaryArray>(&b, "key")?;
                let value = match columns {
                    Columns::All => Some(col::<BinaryArray>(&b, "value")?),
                    Columns::Keys => None,
                };
                let addr = col::<UInt64Array>(&b, ROW_ADDR)?;
                for i in 0..b.num_rows() {
                    sink(RawRow {
                        seq: seq_from_i64(seq.value(i)),
                        op: op.value(i) as u8,
                        key: key.value(i),
                        value: value.and_then(|v| (!v.is_null(i)).then(|| v.value(i))),
                        addr: addr.value(i),
                    });
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn take<'a>(&'a self, addrs: &'a [u64]) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        take_values(&self.ds, addrs).boxed()
    }

    fn append(
        &self,
        mut ops: Vec<Op>,
        mark: Option<WriterMark>,
    ) -> BoxFuture<'static, Result<Log>> {
        let ds = self.ds.clone();
        let options = self.options.clone();
        async move {
            // The same fragment gets a mark row recording the writer's highest committed
            // sequence number, for idempotent WAL replay.
            if let Some(m) = mark {
                ops.push(view::mark(Bytes::from(m.writer.into_bytes()), m.seq));
            }
            let batches = to_batches(&ops)?;
            let params = super::write_params(&options, WriteMode::Append);
            let ds = InsertBuilder::new(Arc::new(ds))
                .with_params(&params)
                .execute(batches)
                .await?;
            Ok(LanceLog::wrap(ds, options))
        }
        .boxed()
    }

    fn rewrite(&self, chunks: Vec<SnapChunk>) -> BoxFuture<'static, Result<Log>> {
        let ds = self.ds.clone();
        let options = self.options.clone();
        async move {
            let ds = replace(ds, chunks, options.clone()).await?;
            Ok(LanceLog::wrap(ds, options))
        }
        .boxed()
    }

    fn merge_small(&self) -> BoxFuture<'static, Result<Log>> {
        let mut ds = self.ds.clone();
        let options = self.options.clone();
        async move {
            compact_files(&mut ds, CompactionOptions::default(), None).await?;
            Ok(LanceLog::wrap(ds, options))
        }
        .boxed()
    }

    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<Log>> {
        async move {
            let ds = self.ds.checkout_version(version).await?;
            Ok(LanceLog::wrap(ds, self.options.clone()))
        }
        .boxed()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<Option<Log>>> {
        async move {
            if self.ds.latest_version_id().await? <= self.version() {
                return Ok(None);
            }
            let mut ds = self.ds.clone();
            ds.checkout_latest().await?;
            Ok(Some(LanceLog::wrap(ds, self.options.clone())))
        }
        .boxed()
    }

    fn versions(&self) -> BoxFuture<'_, Result<Vec<VersionInfo>>> {
        async move {
            Ok(self
                .ds
                .versions()
                .await?
                .into_iter()
                .map(|v| VersionInfo {
                    version: v.version,
                    timestamp_micros: v.timestamp.timestamp_micros(),
                })
                .collect())
        }
        .boxed()
    }

    fn cleanup(&self, before: chrono::DateTime<chrono::Utc>) -> BoxFuture<'_, Result<(u64, u64)>> {
        super::cleanup(&self.ds, before).boxed()
    }

    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>> {
        async move {
            Ok(self
                .ds
                .tags()
                .list()
                .await?
                .into_iter()
                .map(|(name, c)| (name, c.version))
                .collect())
        }
        .boxed()
    }

    fn tag_version<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<u64>> {
        async move { Ok(self.ds.tags().get_version(tag).await?) }.boxed()
    }

    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>> {
        super::tag_version(&self.ds, tag, version).boxed()
    }

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>> {
        async move { Ok(self.ds.tags().delete(tag).await?) }.boxed()
    }
}
