//! The key-value operation log as a Delta table.
//!
//! Columns: `seq: long, op: byte, key: binary, value: binary?, vlen: integer?` (`vlen` is the
//! value's length, so a future open can skip large values without decoding them). The table is
//! unpartitioned; a flush appends exactly one file in one commit.
//!
//! A row address is `file ordinal << 32 | row ordinal`. File ordinals index a file table that
//! all versions of one handle share and that only grows: a file keeps its ordinal for the life
//! of the handle, and the view snapshot saves the table so a reopen keeps them too. Row
//! ordinals are positions in the file, known from the order rows were written in, so an
//! append needs no re-read to learn where its rows went.
//!
//! WAL marks are not rows: an append carries its writer's mark as a `txn` action.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow_array::Array as _;
use bytes::Bytes;
use deltalake_core::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Int32Array, Int64Array, Int8Array, RecordBatch,
};
use deltalake_core::arrow::datatypes::{DataType, Int64Type, Int8Type};
use deltalake_core::kernel::{DataType as DeltaType, PrimitiveType, StructField};
use deltalake_core::{DeltaTable, ObjectStore, Path};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;

use super::commit::{self, FileWriter};
use super::create::{self, Spec};
use super::footer_cache::FooterCache;
use super::scan::{self, ScanFile};
use super::{props, table, take};
use crate::error::{Error, Result};
use crate::kv::view::{Op, SnapChunk, OP_MARK, OP_PUT};
use crate::kv::VersionInfo;
use crate::storage::oplog::{Columns, FileId, Log, LogFile, OpLog, RawRow, Sink, WriterMark};
use crate::storage::types::{seq_from_i64, seq_to_i64};
use crate::storage::StorageOptions;

/// Files with fewer rows than this are merged by [`OpLog::merge_small`]; rewrites write files
/// of about this many rows.
const TARGET_ROWS: u64 = 1 << 20;

/// Statistics in the log cover only these columns: never keys or values.
const STATS_COLUMNS: &str = "seq,op";

static COLUMNS: LazyLock<Vec<StructField>> = LazyLock::new(|| {
    vec![
        StructField::new("seq", DeltaType::Primitive(PrimitiveType::Long), false),
        StructField::new("op", DeltaType::Primitive(PrimitiveType::Byte), false),
        StructField::new("key", DeltaType::Primitive(PrimitiveType::Binary), false),
        StructField::new("value", DeltaType::Primitive(PrimitiveType::Binary), true),
        StructField::new("vlen", DeltaType::Primitive(PrimitiveType::Integer), true),
    ]
});

/// Opens the log at `uri` (see [`crate::storage::oplog::open`]).
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    version: Option<u64>,
    create: bool,
    retention: Option<std::time::Duration>,
    mark_ttl: std::time::Duration,
) -> Result<Log> {
    let spec = Spec {
        columns: COLUMNS.clone(),
        config: props::table_config(retention, mark_ttl, STATS_COLUMNS),
    };
    let table = create::open(uri, options, version, create.then_some(&spec)).await?;
    let shared = Arc::new(Shared {
        uri: uri.to_string(),
        footers: Arc::new(FooterCache::default()),
        files: Mutex::new(FileTable::default()),
    });
    Ok(Arc::new(DeltaLog::new(table, shared, None)?))
}

/// What every version of one handle shares.
struct Shared {
    uri: String,
    footers: Arc<FooterCache>,
    files: Mutex<FileTable>,
}

/// File ordinals of row addresses: `(path, size)` by ordinal.
#[derive(Default)]
struct FileTable {
    ords: HashMap<String, u32>,
    files: Vec<(String, u64)>,
}

impl Shared {
    fn ord(&self, path: &str, size: u64) -> u32 {
        let mut t = self.files.lock();
        if let Some(o) = t.ords.get(path) {
            return *o;
        }
        let o = t.files.len() as u32;
        t.files.push((path.to_string(), size));
        t.ords.insert(path.to_string(), o);
        o
    }

    fn file(&self, ord: u32) -> Option<ScanFile> {
        self.files
            .lock()
            .files
            .get(ord as usize)
            .map(|(p, s)| ScanFile {
                ord,
                path: Path::from(p.as_str()),
                size: *s,
            })
    }
}

fn addr(ord: u32, row: u64) -> u64 {
    (u64::from(ord) << 32) | row
}

#[derive(Clone, Debug)]
struct FileEntry {
    path: String,
    size: u64,
    rows: u64,
}

/// A version of the log.
#[derive(Clone)]
pub(crate) struct DeltaLog {
    table: DeltaTable,
    shared: Arc<Shared>,
    files: Arc<Vec<FileEntry>>,
    /// `(key, seq, address)` of the puts the commit that made this version wrote, when this
    /// handle made it with an append.
    written: Option<Arc<Vec<(Bytes, u64, u64)>>>,
}

impl DeltaLog {
    fn new(
        table: DeltaTable,
        shared: Arc<Shared>,
        written: Option<Arc<Vec<(Bytes, u64, u64)>>>,
    ) -> Result<Self> {
        let snap = table.snapshot()?;
        let mut files = Vec::new();
        for f in snap.log_data().iter() {
            if f.deletion_vector_descriptor().is_some() {
                return Err(Error::NotSupported(format!(
                    "{} has deletion vectors, which kv0 does not implement",
                    shared.uri
                )));
            }
            files.push(FileEntry {
                path: f.object_store_path().to_string(),
                size: f.size().max(0) as u64,
                rows: f.num_records().unwrap_or(0) as u64,
            });
        }
        Ok(DeltaLog {
            table,
            shared,
            files: Arc::new(files),
            written,
        })
    }

    fn wrap(&self, table: DeltaTable, written: Option<Arc<Vec<(Bytes, u64, u64)>>>) -> Result<Log> {
        Ok(Arc::new(DeltaLog::new(
            table,
            self.shared.clone(),
            written,
        )?))
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        self.table.object_store()
    }

    fn scan_file(&self, f: &FileEntry) -> ScanFile {
        ScanFile {
            ord: self.shared.ord(&f.path, f.size),
            path: Path::from(f.path.as_str()),
            size: f.size,
        }
    }

    /// The values at `addrs`, read file by file (files in parallel).
    async fn take_values(&self, addrs: &[u64]) -> Result<Vec<Bytes>> {
        let mut by_file: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, a) in addrs.iter().enumerate() {
            by_file.entry((a >> 32) as u32).or_default().push(i);
        }
        let store = self.store();
        let reads = by_file.into_iter().map(|(ord, idx)| {
            let store = store.clone();
            let footers = self.shared.footers.clone();
            let file = self.shared.file(ord);
            let rows: Vec<u64> = idx.iter().map(|i| addrs[*i] & 0xffff_ffff).collect();
            async move {
                let file = file.ok_or_else(|| {
                    Error::Corruption(format!("row address names unknown file {ord}"))
                })?;
                let got = take::take(store, &footers, &file, &rows, "value").await?;
                Ok::<_, Error>((idx, got))
            }
        });
        let mut out = vec![Bytes::new(); addrs.len()];
        for (idx, got) in futures::future::try_join_all(reads).await? {
            for (i, v) in idx.into_iter().zip(got) {
                out[i] = v;
            }
        }
        Ok(out)
    }
}

fn int64(b: &RecordBatch, name: &str) -> Result<Int64Array> {
    col(b, name)?
        .as_primitive_opt::<Int64Type>()
        .cloned()
        .ok_or_else(|| mistyped(name))
}

fn int8(b: &RecordBatch, name: &str) -> Result<Int8Array> {
    col(b, name)?
        .as_primitive_opt::<Int8Type>()
        .cloned()
        .ok_or_else(|| mistyped(name))
}

fn binary(b: &RecordBatch, name: &str) -> Result<BinaryArray> {
    let c = col(b, name)?;
    match c.data_type() {
        DataType::Binary => Ok(c.as_binary::<i32>().clone()),
        _ => Err(mistyped(name)),
    }
}

fn col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    b.column_by_name(name)
        .ok_or_else(|| Error::Corruption(format!("column {name} is missing")))
}

fn mistyped(name: &str) -> Error {
    Error::Corruption(format!("column {name} is mistyped"))
}

/// A batch of the log from its columns.
fn batch(
    table: &DeltaTable,
    seq: Int64Array,
    op: Int8Array,
    key: BinaryArray,
    value: BinaryArray,
) -> Result<RecordBatch> {
    let vlen: Int32Array = (0..value.len())
        .map(|i| (!value.is_null(i)).then(|| value.value(i).len() as i32))
        .collect();
    let schema = table_arrow_schema(table)?;
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(seq),
            Arc::new(op),
            Arc::new(key),
            Arc::new(value),
            Arc::new(vlen),
        ],
    )?)
}

fn table_arrow_schema(table: &DeltaTable) -> Result<deltalake_core::arrow::datatypes::SchemaRef> {
    use deltalake_core::kernel::engine::arrow_conversion::TryIntoArrow;
    let schema = table
        .snapshot()?
        .metadata()
        .parse_schema()
        .map_err(deltalake_core::DeltaTableError::from)?;
    let arrow: deltalake_core::arrow::datatypes::Schema = (&schema).try_into_arrow()?;
    Ok(Arc::new(arrow))
}

impl OpLog for DeltaLog {
    fn uri(&self) -> &str {
        &self.shared.uri
    }

    fn version(&self) -> u64 {
        self.table.version().unwrap_or(0)
    }

    fn files(&self) -> Vec<LogFile> {
        self.files
            .iter()
            .map(|f| LogFile {
                id: FileId(f.path.clone()),
                rows: f.rows as usize,
                // Delta never rewrites a file in place: a path names its contents.
                ident: format!("{}|{}", f.path, f.size),
            })
            .collect()
    }

    fn small_files(&self) -> usize {
        self.files.iter().filter(|f| f.rows < TARGET_ROWS).count()
    }

    fn scan<'a>(
        &'a self,
        files: Option<&'a HashSet<FileId>>,
        columns: Columns,
        sink: Sink<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let picked: Vec<ScanFile> = self
                .files
                .iter()
                .filter(|f| files.is_none_or(|ids| ids.contains(&FileId(f.path.clone()))))
                .map(|f| self.scan_file(f))
                .collect();
            if picked.is_empty() {
                return Ok(());
            }
            let mut names = vec!["seq".to_string(), "op".into(), "key".into()];
            if columns == Columns::All {
                names.push("value".into());
            }
            scan::scan(
                self.store(),
                self.shared.footers.clone(),
                picked,
                Some(names),
                scan::parallelism(),
                |ord, first, b| {
                    let seq = int64(b, "seq")?;
                    let op = int8(b, "op")?;
                    let key = binary(b, "key")?;
                    let value = match columns {
                        Columns::All => Some(binary(b, "value")?),
                        Columns::Keys => None,
                    };
                    for i in 0..b.num_rows() {
                        sink(RawRow {
                            seq: seq_from_i64(seq.value(i)),
                            op: op.value(i) as u8,
                            key: key.value(i),
                            value: value
                                .as_ref()
                                .and_then(|v| (!v.is_null(i)).then(|| v.value(i))),
                            addr: addr(ord, first + i as u64),
                        });
                    }
                    Ok(())
                },
            )
            .await
        }
        .boxed()
    }

    fn take<'a>(&'a self, addrs: &'a [u64]) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        self.take_values(addrs).boxed()
    }

    fn append(&self, ops: Vec<Op>, mark: Option<WriterMark>) -> BoxFuture<'static, Result<Log>> {
        let this = self.clone();
        async move {
            let mut w = FileWriter::new(&this.table, props::kv_writer(), None)?;
            for chunk in ops.chunks(props::ROW_GROUP_ROWS) {
                let seq = Int64Array::from_iter_values(chunk.iter().map(|o| seq_to_i64(o.seq)));
                let op = Int8Array::from_iter_values(chunk.iter().map(|o| o.kind as i8));
                let key = BinaryArray::from_iter_values(chunk.iter().map(|o| &o.key[..]));
                let value: BinaryArray = chunk.iter().map(|o| o.value.as_deref()).collect();
                w.write(batch(&this.table, seq, op, key, value)?).await?;
            }
            let mut files = w.finish().await?;
            let txn = mark
                .as_ref()
                .map(|m| commit::mark(&m.writer, seq_to_i64(m.seq)));
            let adds: Vec<_> = files.iter().map(|f| f.add.clone()).collect();
            let committed = match commit::append(&this.table, adds, txn).await {
                Ok(t) => t,
                Err(e) => {
                    // A commit whose response was lost may have landed: its mark says so.
                    let Some(m) = &mark else { return Err(e) };
                    let mut latest = DeltaTable::new(this.table.log_store());
                    latest.load().await?;
                    let app = commit::wal_app_id(&m.writer);
                    let have = latest
                        .snapshot()?
                        .transaction_version(latest.log_store().as_ref(), app)
                        .await?;
                    if have.is_some_and(|v| seq_from_i64(v) >= m.seq) {
                        tracing::debug!(uri = %this.shared.uri, error = %e, "kv0: the append was committed after all");
                        return this.wrap(latest, None);
                    }
                    return Err(e);
                }
            };
            let written = match files.pop() {
                Some(f) => {
                    let ord = this.shared.ord(&f.path, f.add.size.max(0) as u64);
                    ops.iter()
                        .enumerate()
                        .filter(|(_, o)| o.kind == OP_PUT)
                        .map(|(i, o)| (o.key.clone(), o.seq, addr(ord, i as u64)))
                        .collect()
                }
                None => Vec::new(),
            };
            this.wrap(committed, Some(Arc::new(written)))
        }
        .boxed()
    }

    fn rewrite(&self, chunks: Vec<SnapChunk>) -> BoxFuture<'static, Result<Log>> {
        let this = self.clone();
        async move {
            let removed: Vec<String> = this.files.iter().map(|f| f.path.clone()).collect();
            let mut w = FileWriter::new(&this.table, props::kv_writer(), Some(TARGET_ROWS))?;
            for mut chunk in chunks {
                let key58 = chunk.key.finish();
                let value58 = chunk.value.finish();
                let stored: HashMap<usize, u64> = chunk.stored.iter().copied().collect();
                let wanted: Vec<u64> = chunk.stored.iter().map(|s| s.1).collect();
                let mut taken: HashMap<usize, Bytes> = chunk
                    .stored
                    .iter()
                    .map(|s| s.0)
                    .zip(this.take_values(&wanted).await?)
                    .collect();
                // Marks are `txn` actions here, never rows.
                let keep: Vec<usize> = (0..chunk.len())
                    .filter(|i| chunk.op[*i] != OP_MARK)
                    .collect();
                let seq =
                    Int64Array::from_iter_values(keep.iter().map(|i| seq_to_i64(chunk.seq[*i])));
                let op = Int8Array::from_iter_values(keep.iter().map(|i| chunk.op[*i] as i8));
                let key = BinaryArray::from_iter_values(keep.iter().map(|i| key58.value(*i)));
                let value: BinaryArray = keep
                    .iter()
                    .map(|i| {
                        if stored.contains_key(i) {
                            taken.remove(i)
                        } else {
                            (!value58.is_null(*i))
                                .then(|| Bytes::copy_from_slice(value58.value(*i)))
                        }
                    })
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|v| v.as_deref())
                    .collect();
                w.write(batch(&this.table, seq, op, key, value)?).await?;
            }
            let adds: Vec<_> = w.finish().await?.into_iter().map(|f| f.add).collect();
            if removed.is_empty() && adds.is_empty() {
                return Ok(Arc::new(this) as Log);
            }
            let t = commit::rewrite(&this.table, &removed, adds, "compact").await?;
            this.wrap(t, None)
        }
        .boxed()
    }

    fn merge_small(&self) -> BoxFuture<'static, Result<Log>> {
        let this = self.clone();
        async move {
            let small: Vec<FileEntry> = this
                .files
                .iter()
                .filter(|f| f.rows < TARGET_ROWS)
                .cloned()
                .collect();
            if small.len() < 2 {
                return Ok(Arc::new(this) as Log);
            }
            let mut w = FileWriter::new(&this.table, props::kv_writer(), Some(TARGET_ROWS))?;
            for f in &small {
                let mut s = scan::file_stream(
                    this.store(),
                    this.shared.footers.clone(),
                    this.scan_file(f),
                    None,
                );
                while let Some(b) = s.next().await {
                    w.write(b?.1).await?;
                }
            }
            let adds = w.finish().await?.into_iter().map(|f| f.add).collect();
            let removed: Vec<String> = small.into_iter().map(|f| f.path).collect();
            let t = commit::rewrite(&this.table, &removed, adds, "merge").await?;
            this.wrap(t, None)
        }
        .boxed()
    }

    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<Log>> {
        async move {
            let mut t = DeltaTable::new(self.table.log_store());
            t.load_version(version).await?;
            self.wrap(t, None)
        }
        .boxed()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<Option<Log>>> {
        async move {
            // Lists only the commits after this version, then reads just those, unless a
            // cleanup deleted this version's commit file: then from the latest checkpoint.
            let mut t = self.table.clone();
            let moved = match t.get_latest_version().await {
                Ok(v) if v <= self.version() => return Ok(None),
                Ok(_) => t.update_incremental(None).await,
                Err(e) => Err(e),
            };
            if let Err(e) = moved {
                tracing::debug!(uri = %self.shared.uri, error = %e, "kv0: reloading the log from its checkpoint");
                t = DeltaTable::new(self.table.log_store());
                t.load().await?;
                if t.version() <= self.table.version() {
                    return Ok(None);
                }
            }
            Ok(Some(self.wrap(t, None)?))
        }
        .boxed()
    }

    fn versions(&self) -> BoxFuture<'_, Result<Vec<VersionInfo>>> {
        table::versions(&self.table).boxed()
    }

    fn cleanup(&self, before: chrono::DateTime<chrono::Utc>) -> BoxFuture<'_, Result<(u64, u64)>> {
        table::cleanup(&self.table, before).boxed()
    }

    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>> {
        table::tags(&self.table).boxed()
    }

    fn tag_version<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<u64>> {
        table::tag_version(&self.table, tag).boxed()
    }

    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>> {
        table::create_tag(&self.table, tag, version).boxed()
    }

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>> {
        table::delete_tag(&self.table, tag).boxed()
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

    fn log_id(&self) -> String {
        self.table
            .snapshot()
            .map(|s| s.metadata().id().to_string())
            .unwrap_or_default()
    }

    fn addr_table(&self) -> Vec<String> {
        self.shared
            .files
            .lock()
            .files
            .iter()
            .map(|(p, s)| format!("{p}|{s}"))
            .collect()
    }

    fn restore_addr_table(&self, entries: Vec<String>) {
        let mut t = self.shared.files.lock();
        if !t.files.is_empty() {
            return;
        }
        for e in entries {
            let Some((p, s)) = e.rsplit_once('|') else {
                continue;
            };
            let Ok(size) = s.parse::<u64>() else { continue };
            let o = t.files.len() as u32;
            t.files.push((p.to_string(), size));
            t.ords.insert(p.to_string(), o);
        }
    }

    fn written(&self) -> Option<Arc<Vec<(Bytes, u64, u64)>>> {
        self.written.clone()
    }
}
