//! The key-value operation log as the moraine table `ops` of a database at the location.
//!
//! Columns: `seq: long, op: byte, key: binary, value: binary?, vlen: integer?`. The table has
//! no key and no cluster key, so moraine writes the rows of an append in the order given (it
//! sorts only by a cluster or primary key), one file per flush. Binary columns carry no
//! statistics in moraine's log, so neither keys nor values appear there, and a cluster key on
//! `key` would prune nothing: the view reads the log whole.
//!
//! A row address is `file ordinal << 32 | row ordinal`: file ordinals index a table of the
//! files this handle met, which only grows and which the view snapshot saves, keyed by the
//! table's id and the file's path; row ordinals are positions in a file. moraine's data files
//! are plain Parquet that never change, so kv0 scans them file by file itself, numbering the
//! rows; values are fetched by row ordinal through moraine (`TableSnapshot::take`).
//!
//! - An append carries its writer's mark as a moraine transaction id, `kv0-wal/<writer id>`
//!   at the writer's highest sequence number, so a replayed append lands at most once.
//! - A rewrite (compaction, purge) is one `replace_files` commit: every file of the version
//!   read goes, the rewritten rows come in, and files other writers appended meanwhile stay.
//! - A merge of small files is moraine's compaction (rows unchanged and in order).
//! - Tags are pins (see [`super::pins`]); versions are the latest one and the tagged ones.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, Int8Type};
use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt as _, TryStreamExt as _};
use object_store::ObjectStoreExt as _;
use parking_lot::Mutex;

use super::pins::Pins;
use super::{error, wal_app_id, Repo};
use crate::error::{Error, Result};
use crate::kv::view::{Op, SnapChunk, OP_MARK, OP_PUT};
use crate::kv::VersionInfo;
use crate::storage::files::footer_cache::FooterCache;
use crate::storage::files::scan::{self, ScanFile};
use crate::storage::format::Kind;
use crate::storage::oplog::{Columns, FileId, Log, LogFile, OpLog, RawRow, Sink, WriterMark};
use crate::storage::types::{seq_from_i64, seq_to_i64};
use crate::storage::StorageOptions;

/// The moraine table of a key-value database.
const TABLE: &str = "ops";

/// Files with fewer rows than this count as small ([`OpLog::small_files`]).
const TARGET_ROWS: u64 = 1 << 20;

/// Rows per batch of a rewrite.
const BATCH_ROWS: usize = 64 * 1024;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new("op", DataType::Int8, false),
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("vlen", DataType::Int32, true),
    ]))
}

/// Opens the log at `uri` (see [`crate::storage::oplog::open`]); `marker` writes the format
/// marker of a new database first.
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    version: Option<u64>,
    create: bool,
    marker: bool,
) -> Result<Log> {
    let memory = uri
        .starts_with("memory://")
        .then(|| Arc::new(object_store::memory::InMemory::new()));
    let repo = Arc::new(Repo::open(uri, options, memory, marker.then_some(Kind::Database)).await?);
    let db = repo.database();
    let table = match db.open_table(TABLE).await {
        Ok(t) => t,
        Err(moraine::Error::TableNotFound(_)) if create => {
            match db
                .create_table(TABLE, schema(), moraine::TableOptions::default())
                .await
            {
                Ok(t) => t,
                Err(moraine::Error::TableExistsWithDifferentSchema(_)) => {
                    db.open_table(TABLE).await.map_err(error)?
                }
                Err(e) => return Err(error(e)),
            }
        }
        Err(moraine::Error::TableNotFound(_)) => {
            return Err(Error::NotFound(format!(
                "{uri}: no key-value database (create_if_missing is off)"
            )))
        }
        Err(e) => return Err(error(e)),
    };
    let snap = table.snapshot().await.map_err(error)?;
    let shared = Arc::new(Shared {
        uri: uri.to_string(),
        repo,
        table,
        footers: Arc::new(FooterCache::default()),
        files: Mutex::new(FileTable::default()),
    });
    let log = MoraineLog::new(shared, snap, None);
    match version {
        Some(v) => log.checkout(v).await,
        None => Ok(Arc::new(log)),
    }
}

/// What every version of one handle shares.
struct Shared {
    uri: String,
    repo: Arc<Repo>,
    table: moraine::Table,
    footers: Arc<FooterCache>,
    files: Mutex<FileTable>,
}

/// File ordinals of row addresses: `(path, size)` by ordinal, and the ordinals by table id and
/// path (a table created again at the same location has another id).
#[derive(Default)]
struct FileTable {
    ords: HashMap<(String, String), u32>,
    files: Vec<(String, u64)>,
}

impl Shared {
    fn ord(&self, table_id: &str, path: &str, size: u64) -> u32 {
        let mut t = self.files.lock();
        let key = (table_id.to_string(), path.to_string());
        if let Some(o) = t.ords.get(&key) {
            return *o;
        }
        let o = t.files.len() as u32;
        t.files.push((path.to_string(), size));
        t.ords.insert(key, o);
        o
    }

    /// The path of the file with ordinal `ord`.
    fn path(&self, ord: u32) -> Option<String> {
        self.files
            .lock()
            .files
            .get(ord as usize)
            .map(|f| f.0.clone())
    }

    fn pins(&self) -> Pins<'_> {
        Pins {
            repo: &self.repo,
            table: &self.table,
            name: TABLE,
        }
    }
}

/// The location of data file `path` of the table.
fn data_path(repo: &Repo, path: &str) -> object_store::path::Path {
    repo.path(&format!("{TABLE}/data/{path}"))
}

fn addr(ord: u32, row: u64) -> u64 {
    (u64::from(ord) << 32) | row
}

/// A version of the log.
#[derive(Clone)]
pub(crate) struct MoraineLog {
    shared: Arc<Shared>,
    snap: moraine::TableSnapshot,
    files: Arc<Vec<moraine::SnapshotFile>>,
    /// `(key, seq, address)` of the puts the commit that made this version wrote, when this
    /// handle made it with an append.
    written: Option<Arc<Vec<(Bytes, u64, u64)>>>,
}

impl MoraineLog {
    fn new(
        shared: Arc<Shared>,
        snap: moraine::TableSnapshot,
        written: Option<Arc<Vec<(Bytes, u64, u64)>>>,
    ) -> Self {
        let files = Arc::new(snap.files());
        MoraineLog {
            shared,
            snap,
            files,
            written,
        }
    }

    fn wrap(
        &self,
        snap: moraine::TableSnapshot,
        written: Option<Arc<Vec<(Bytes, u64, u64)>>>,
    ) -> Log {
        Arc::new(MoraineLog::new(self.shared.clone(), snap, written))
    }

    fn ord(&self, f: &moraine::SnapshotFile) -> u32 {
        self.shared.ord(self.snap.table_id(), &f.path, f.size)
    }

    fn scan_file(&self, f: &moraine::SnapshotFile) -> ScanFile {
        ScanFile {
            ord: self.ord(f),
            path: data_path(&self.shared.repo, &f.path),
            size: f.size,
        }
    }

    /// The rows of one chunk of a rewrite, as batches of the table: values the chunk left in
    /// storage are read from this version, and marks are dropped (they are transaction ids
    /// here, never rows).
    async fn chunk_batches(&self, mut chunk: SnapChunk) -> Result<Vec<RecordBatch>> {
        let key58 = chunk.key.finish();
        let value58 = chunk.value.finish();
        let stored: HashSet<usize> = chunk.stored.iter().map(|s| s.0).collect();
        let wanted: Vec<u64> = chunk.stored.iter().map(|s| s.1).collect();
        let mut taken: HashMap<usize, Bytes> = chunk
            .stored
            .iter()
            .map(|s| s.0)
            .zip(self.take_values(&wanted).await?)
            .collect();
        let keep: Vec<usize> = (0..chunk.len())
            .filter(|i| chunk.op[*i] != OP_MARK)
            .collect();
        let mut batches = Vec::with_capacity(keep.len().div_ceil(BATCH_ROWS));
        for part in keep.chunks(BATCH_ROWS) {
            let seq = Int64Array::from_iter_values(part.iter().map(|i| seq_to_i64(chunk.seq[*i])));
            let op = Int8Array::from_iter_values(part.iter().map(|i| chunk.op[*i] as i8));
            let key = BinaryArray::from_iter_values(part.iter().map(|i| key58.value(*i)));
            let values: Vec<Option<Bytes>> = part
                .iter()
                .map(|i| {
                    if stored.contains(i) {
                        taken.remove(i)
                    } else {
                        (!value58.is_null(*i)).then(|| Bytes::copy_from_slice(value58.value(*i)))
                    }
                })
                .collect();
            let value: BinaryArray = values.iter().map(|v| v.as_deref()).collect();
            batches.push(MoraineLog::batch(seq, op, key, value)?);
        }
        Ok(batches)
    }

    /// The values at `addrs`, read file by file (files in parallel). A file this version no
    /// longer holds reads as missing ([`Error::NotFound`]), so the caller refreshes.
    async fn take_values(&self, addrs: &[u64]) -> Result<Vec<Bytes>> {
        let mut by_file: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, a) in addrs.iter().enumerate() {
            by_file.entry((a >> 32) as u32).or_default().push(i);
        }
        let reads = by_file.into_iter().map(|(ord, idx)| {
            let path = self.shared.path(ord);
            let rows: Vec<u64> = idx.iter().map(|i| addrs[*i] & 0xffff_ffff).collect();
            async move {
                let path = path.ok_or_else(|| {
                    Error::Corruption(format!("row address names unknown file {ord}"))
                })?;
                let got = self
                    .snap
                    .take(&path, &rows)
                    .select(["value"])
                    .collect()
                    .await
                    .map_err(|e| match e {
                        moraine::Error::FilesChanged(p) => Error::NotFound(format!(
                            "data file {p} is no longer part of version {}",
                            self.snap.version()
                        )),
                        e => error(e),
                    })?;
                let values = binary(&got, "value")?;
                // A null reads as empty.
                let got: Vec<Bytes> = (0..values.len())
                    .map(|i| Bytes::copy_from_slice(values.value(i)))
                    .collect();
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

    /// The rows of the log from its columns.
    fn batch(
        seq: Int64Array,
        op: Int8Array,
        key: BinaryArray,
        value: BinaryArray,
    ) -> Result<RecordBatch> {
        let vlen: Int32Array = (0..value.len())
            .map(|i| (!value.is_null(i)).then(|| value.value(i).len() as i32))
            .collect();
        Ok(RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(seq),
                Arc::new(op),
                Arc::new(key),
                Arc::new(value),
                Arc::new(vlen),
            ],
        )?)
    }
}

fn col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    b.column_by_name(name)
        .ok_or_else(|| Error::Corruption(format!("column {name} is missing")))
}

fn mistyped(name: &str) -> Error {
    Error::Corruption(format!("column {name} is mistyped"))
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

impl OpLog for MoraineLog {
    fn uri(&self) -> &str {
        &self.shared.uri
    }

    fn version(&self) -> u64 {
        self.snap.version()
    }

    fn files(&self) -> Vec<LogFile> {
        self.files
            .iter()
            .map(|f| LogFile {
                id: FileId(f.path.clone()),
                rows: f.rows.unwrap_or(0) as usize,
                // moraine never rewrites a file in place: a path names its contents.
                ident: format!("{}|{}", f.path, f.size),
            })
            .collect()
    }

    fn small_files(&self) -> usize {
        self.files
            .iter()
            .filter(|f| f.rows.unwrap_or(0) < TARGET_ROWS)
            .count()
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
                self.shared.repo.store().clone(),
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
            let table = &this.shared.table;
            let batches = if ops.is_empty() {
                Vec::new()
            } else {
                let seq = Int64Array::from_iter_values(ops.iter().map(|o| seq_to_i64(o.seq)));
                let op = Int8Array::from_iter_values(ops.iter().map(|o| o.kind as i8));
                let key = BinaryArray::from_iter_values(ops.iter().map(|o| &o.key[..]));
                let value: BinaryArray = ops.iter().map(|o| o.value.as_deref()).collect();
                vec![MoraineLog::batch(seq, op, key, value)?]
            };
            let committed = match &mark {
                Some(m) => {
                    let outcome = table
                        .txn(wal_app_id(&m.writer), seq_to_i64(m.seq))
                        .append_unchecked(batches)
                        .await
                        .map_err(error)?;
                    // Not committed by this call: committed before (a lost response, or a
                    // replay), and the newest version holds it.
                    outcome.is_committed().then(|| outcome.version())
                }
                None if batches.is_empty() => return Ok(Arc::new(this) as Log),
                None => Some(
                    table
                        .append_unchecked(batches)
                        .await
                        .map_err(error)?
                        .version(),
                ),
            };
            let Some(v) = committed else {
                let snap = table.snapshot().await.map_err(error)?;
                return Ok(this.wrap(snap, None));
            };
            // The handle's own commit advanced its snapshot: no request.
            let snap = table.snapshot_in_hand();
            let added: Vec<moraine::SnapshotFile> = snap
                .files()
                .into_iter()
                .filter(|f| f.added_in == Some(v))
                .collect();
            // Where the puts went, when the commit is one file of exactly these rows (moraine
            // keeps their order); otherwise the reader finds out by reading.
            let written = match added.as_slice() {
                [f] if f.rows == Some(ops.len() as u64) => {
                    let ord = this.shared.ord(snap.table_id(), &f.path, f.size);
                    Some(Arc::new(
                        ops.iter()
                            .enumerate()
                            .filter(|(_, o)| o.kind == OP_PUT)
                            .map(|(i, o)| (o.key.clone(), o.seq, addr(ord, i as u64)))
                            .collect(),
                    ))
                }
                [] if ops.is_empty() => Some(Arc::new(Vec::new())),
                _ => None,
            };
            Ok(this.wrap(snap, written))
        }
        .boxed()
    }

    fn rewrite(&self, chunks: Vec<SnapChunk>) -> BoxFuture<'static, Result<Log>> {
        let this = self.clone();
        async move {
            let removed: Vec<String> = this.files.iter().map(|f| f.path.clone()).collect();
            let has_rows = chunks
                .iter()
                .any(|c| (0..c.len()).any(|i| c.op[i] != OP_MARK));
            if removed.is_empty() && !has_rows {
                return Ok(Arc::new(this) as Log);
            }
            // The rows are written as they are built, one chunk at a time (moraine D68), so a
            // rewrite never holds every value at once. An error of kv0's own is kept as it is,
            // so a caller still tells a missing file from other failures.
            let failed: Arc<Mutex<Option<Error>>> = Arc::default();
            let slot = failed.clone();
            let source = this.clone();
            let rows = futures::stream::iter(chunks)
                .then(move |chunk| {
                    let source = source.clone();
                    async move { source.chunk_batches(chunk).await }
                })
                .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                .try_flatten()
                .map_err(move |e: Error| {
                    let m = e.to_string();
                    *slot.lock() = Some(e);
                    moraine::Error::from(std::io::Error::other(format!(
                        "building the rows of a rewrite: {m}"
                    )))
                });
            let table = &this.shared.table;
            let replaced = table
                .replace_files(moraine::Replace::new().remove_all(removed).add_stream(rows))
                .await;
            if let Some(e) = failed.lock().take() {
                return Err(e);
            }
            replaced.map_err(error)?;
            Ok(this.wrap(table.snapshot_in_hand(), None))
        }
        .boxed()
    }

    fn merge_small(&self) -> BoxFuture<'static, Result<Log>> {
        let this = self.clone();
        async move {
            if this.small_files() < 2 {
                return Ok(Arc::new(this) as Log);
            }
            let table = &this.shared.table;
            table
                .compact(&moraine::CompactionPolicy::full().build_sidecars(false))
                .await
                .map_err(error)?;
            let snap = table.snapshot().await.map_err(error)?;
            Ok(this.wrap(snap, None))
        }
        .boxed()
    }

    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<Log>> {
        async move {
            let snap = self.shared.pins().at_version(&self.snap, version).await?;
            Ok(self.wrap(snap, None))
        }
        .boxed()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<Option<Log>>> {
        async move {
            let table = &self.shared.table;
            let v = table.refresh().await.map_err(error)?;
            if v <= self.version() {
                return Ok(None);
            }
            Ok(Some(self.wrap(table.snapshot_in_hand(), None)))
        }
        .boxed()
    }

    /// The latest version and the tagged ones: moraine keeps no other old version readable.
    fn versions(&self) -> BoxFuture<'_, Result<Vec<VersionInfo>>> {
        async move {
            let table = &self.shared.table;
            let latest = table.refresh().await.map_err(error)?;
            let mut vs: Vec<u64> = table.pins().map_err(error)?.into_values().collect();
            vs.push(latest);
            vs.sort_unstable();
            vs.dedup();
            let store = self.shared.repo.store();
            let mut out = Vec::with_capacity(vs.len());
            for v in vs {
                let entry = self
                    .shared
                    .repo
                    .path(&format!("{TABLE}/data/_delta_log/{v:020}.json"));
                // The commit time is the log entry's; a cleaned-up entry's is unknown.
                let timestamp_micros = match store.head(&entry).await {
                    Ok(m) => m.last_modified.timestamp_micros(),
                    Err(object_store::Error::NotFound { .. }) => 0,
                    Err(e) => return Err(e.into()),
                };
                out.push(VersionInfo {
                    version: v,
                    timestamp_micros,
                });
            }
            Ok(out)
        }
        .boxed()
    }

    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>> {
        async move { super::erase::cleanup(&self.shared.table, c, self.shared.repo.is_local()).await }.boxed()
    }

    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>> {
        async move { self.shared.pins().list().await }.boxed()
    }

    fn tag_version<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<u64>> {
        async move { self.shared.pins().version(tag).await }.boxed()
    }

    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>> {
        async move { self.shared.pins().create(tag, version).await }.boxed()
    }

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>> {
        async move { self.shared.pins().delete(tag).await }.boxed()
    }

    fn writer_mark<'a>(&'a self, writer: &'a str) -> BoxFuture<'a, Result<Option<u64>>> {
        let v = self
            .snap
            .last_txn_version(&wal_app_id(writer))
            .map(seq_from_i64);
        futures::future::ready(Ok(v)).boxed()
    }

    /// The table's id (moraine D67): a database deleted and created again gets a new one.
    fn log_id(&self) -> String {
        self.snap.table_id().to_string()
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
        let id = self.snap.table_id().to_string();
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
            // Restored only for a log of the same id (`log_id`), which is this one.
            t.ords.insert((id.clone(), p.to_string()), o);
        }
    }

    fn written(&self) -> Option<Arc<Vec<(Bytes, u64, u64)>>> {
        self.written.clone()
    }
}
