//! Typed, columnar tables for an experiment tracker: runs, parameters and metric series.
//!
//! A [`Repo`] is a location holding four Lance datasets: `runs`, `params`, `series` and
//! `metrics`. Writes are buffered per [`Repo`] handle and appended in batches (one commit per
//! table per flush), on [`Repo::flush`], when [`RepoOptions::flush_rows`] points are buffered,
//! every [`RepoOptions::flush_interval`] and on close. Queries flush this handle's buffer first
//! and read the latest committed version, so they include this handle's writes and every other
//! writer's flushed writes.
//!
//! Runs and parameters are stored as change rows with a sequence number and resolved
//! newest-wins on read. Metric points are append-only; when a series has two points at the same
//! step, the later one wins.
//!
//! The async methods can run on any Tokio runtime. Writing ([`Run::track`], [`Run::set_params`])
//! only buffers and never blocks on I/O, unless the buffer is full.

mod json;
mod records;
pub mod schema;
mod stats;

pub use records::{At, NewRecord, Record, RecordQuery};
pub use stats::SeriesStats;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::{Dataset, InsertBuilder, NewColumnTransform, WriteMode};
use lance::index::DatasetIndexExt;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::IndexType;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::background::{self, Background};
use crate::clock::{now_nanos, Clock};
use crate::error::{Error, Result};
use crate::kv::view::Op;
use crate::kv::wal::{self, Wal};
pub use crate::kv::WalSync;
use crate::rt;
use crate::storage::{self, StorageOptions};

pub use json::canonical as canonical_context;

/// Options for [`Repo::open`].
#[derive(Clone, Debug)]
pub struct RepoOptions {
    /// Create the tables when they do not exist. Default `true`.
    pub create_if_missing: bool,
    /// Object-store options passed to Lance.
    pub storage_options: StorageOptions,
    /// Flush once this many metric points are buffered. Default 50 000.
    pub flush_rows: usize,
    /// Flush buffered writes in the background after this long. Default 5 s.
    pub flush_interval: Option<Duration>,
    /// Log every write to a local write-ahead log before acknowledging it (see
    /// [`crate::kv::Options::wal`]). Default `true`.
    pub wal: bool,
    /// When WAL writes reach the disk. Default [`WalSync::Batch`].
    pub wal_sync: WalSync,
    /// WAL root directory; `None` uses `KV0_WAL_DIR`, else the platform cache directory.
    pub wal_dir: Option<std::path::PathBuf>,
    /// Compact a table automatically after a flush once it has more than this many fragments
    /// (each flush adds one per table it writes). `0` disables. Default 32.
    pub max_fragments: usize,
    /// Fragments with at least this many rows count as settled: they never trigger automatic
    /// compaction, and compaction aims for this size. Default 1 000 000 (Lance's default).
    pub compaction_target_rows: usize,
    /// How often background maintenance prunes WAL mark rows (a filtered scan of every table).
    /// `optimize` always prunes. Default 1 h.
    pub mark_prune_interval: Duration,
    /// How long `optimize` keeps a writer's WAL mark rows (see the key-value layer's
    /// `mark_ttl`). Default 30 days.
    pub mark_ttl: Duration,
    /// Replay a WAL older than `mark_ttl` whose writer has no marks left (its writes may be
    /// stored twice). Default `false`: such a WAL fails the open.
    pub replay_stale_wal: bool,
    /// When creating the tables, enable Lance's automatic cleanup of versions older than this.
    /// `None` keeps every version. Default 7 days.
    pub version_retention: Option<Duration>,
    /// What to do when the repository still has the shared `records.lance` of builds before
    /// per-table records. Default [`LegacyRecords::Error`].
    pub legacy_records: LegacyRecords,
}

/// Handling of the shared `records.lance` that builds before per-table records wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LegacyRecords {
    /// Opening fails with a message saying how to migrate.
    #[default]
    Error,
    /// Open anyway: read it with [`Repo::legacy_records`], copy what is needed into tables,
    /// then remove it with [`Repo::drop_legacy_records`].
    Allow,
}

impl Default for RepoOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            storage_options: StorageOptions::new(),
            flush_rows: 50_000,
            flush_interval: Some(Duration::from_secs(5)),
            version_retention: Some(Duration::from_secs(7 * 24 * 3600)),
            wal: true,
            wal_sync: WalSync::Batch,
            wal_dir: None,
            mark_ttl: Duration::from_secs(30 * 24 * 3600),
            replay_stale_wal: false,
            max_fragments: 32,
            compaction_target_rows: 1_000_000,
            mark_prune_interval: Duration::from_secs(3600),
            legacy_records: LegacyRecords::Error,
        }
    }
}

/// Lifecycle state of a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Finished,
    Failed,
    Aborted,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Finished => "finished",
            RunStatus::Failed => "failed",
            RunStatus::Aborted => "aborted",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "running" => RunStatus::Running,
            "finished" => RunStatus::Finished,
            "failed" => RunStatus::Failed,
            "aborted" => RunStatus::Aborted,
            other => {
                return Err(Error::InvalidArgument(format!(
                    "unknown run status {other:?}"
                )))
            }
        })
    }
}

/// A run's current metadata. Timestamps are microseconds since the Unix epoch (UTC).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunInfo {
    pub run_id: String,
    pub name: Option<String>,
    pub experiment: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub status: RunStatus,
    pub archived: bool,
    pub created_at: i64,
    pub updated_at: i64,
    pub ended_at: Option<i64>,
    /// What the run is, such as `"training"` or `"eval"`.
    #[serde(default)]
    pub kind: Option<String>,
    /// What the run is about, as a JSON object (model, prompt, endpoint, commit, ...).
    #[serde(default)]
    pub target: Option<Value>,
}

/// Bits naming the fields of [`RunInfo`] a stored run row sets. Reads merge rows field by
/// field, newest first, so concurrent updates of different fields do not overwrite each other.
pub mod fields {
    pub const NAME: u32 = 1;
    pub const EXPERIMENT: u32 = 1 << 1;
    pub const DESCRIPTION: u32 = 1 << 2;
    pub const TAGS: u32 = 1 << 3;
    pub const STATUS: u32 = 1 << 4;
    pub const ARCHIVED: u32 = 1 << 5;
    pub const CREATED_AT: u32 = 1 << 6;
    pub const ENDED_AT: u32 = 1 << 7;
    pub const KIND: u32 = 1 << 8;
    pub const TARGET: u32 = 1 << 9;
    pub const ALL: u32 = (1 << 10) - 1;
    /// A tombstone: the run was deleted. Its rows, whatever their `seq`, are ignored.
    pub const DELETED: u32 = 1 << 10;
}

/// The fields that differ between `a` and `b`.
fn changed(a: &RunInfo, b: &RunInfo) -> u32 {
    use fields::*;
    let mut m = 0;
    for (bit, same) in [
        (NAME, a.name == b.name),
        (EXPERIMENT, a.experiment == b.experiment),
        (DESCRIPTION, a.description == b.description),
        (TAGS, a.tags == b.tags),
        (STATUS, a.status == b.status),
        (ARCHIVED, a.archived == b.archived),
        (CREATED_AT, a.created_at == b.created_at),
        (ENDED_AT, a.ended_at == b.ended_at),
        (KIND, a.kind == b.kind),
        (TARGET, a.target == b.target),
    ] {
        if !same {
            m |= bit;
        }
    }
    m
}

/// Copies the fields named by `mask` from `src` into `dst`.
fn take_fields(dst: &mut RunInfo, src: &RunInfo, mask: u32) {
    use fields::*;
    if mask & NAME != 0 {
        dst.name = src.name.clone();
    }
    if mask & EXPERIMENT != 0 {
        dst.experiment = src.experiment.clone();
    }
    if mask & DESCRIPTION != 0 {
        dst.description = src.description.clone();
    }
    if mask & TAGS != 0 {
        dst.tags = src.tags.clone();
    }
    if mask & STATUS != 0 {
        dst.status = src.status;
    }
    if mask & ARCHIVED != 0 {
        dst.archived = src.archived;
    }
    if mask & CREATED_AT != 0 {
        dst.created_at = src.created_at;
    }
    if mask & ENDED_AT != 0 {
        dst.ended_at = src.ended_at;
    }
    if mask & KIND != 0 {
        dst.kind = src.kind.clone();
    }
    if mask & TARGET != 0 {
        dst.target = src.target.clone();
    }
}

/// Merges stored rows into each run's current record: per field, the newest row that sets it;
/// `updated_at` from the newest row. Returns the record and its newest `seq` per run.
fn merge_runs(mut rows: Vec<(u64, RunInfo, u32)>) -> HashMap<String, (u64, RunInfo)> {
    let dead: HashSet<String> = rows
        .iter()
        .filter(|r| r.2 == fields::DELETED)
        .map(|r| r.1.run_id.clone())
        .collect();
    rows.retain(|r| !dead.contains(&r.1.run_id));
    rows.sort_by_key(|r| r.0);
    let mut out: HashMap<String, (u64, RunInfo)> = HashMap::new();
    for (seq, info, mask) in rows {
        match out.get_mut(&info.run_id) {
            None => {
                out.insert(info.run_id.clone(), (seq, info));
            }
            Some((s, cur)) => {
                take_fields(cur, &info, mask);
                cur.updated_at = cur.updated_at.max(info.updated_at);
                *s = seq;
            }
        }
    }
    out
}

/// Fields for [`Repo::create_run`].
#[derive(Clone, Debug, Default)]
pub struct NewRun {
    /// A run id; a time-ordered UUIDv7 is generated when `None`.
    pub run_id: Option<String>,
    pub name: Option<String>,
    pub experiment: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    /// Creation time in microseconds since the Unix epoch (UTC); now when `None`. For runs
    /// imported from elsewhere.
    pub created_at: Option<i64>,
    pub kind: Option<String>,
    pub target: Option<Value>,
}

/// A metric series: one metric name under one context in one run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeriesInfo {
    pub series_id: u64,
    pub run_id: String,
    pub name: String,
    pub context: Value,
    pub created_at: i64,
}

/// Optional fields for [`Run::track`].
#[derive(Clone, Debug, Default)]
pub struct Track {
    /// The step; one past the series' last step when `None`.
    pub step: Option<i64>,
    pub epoch: Option<i64>,
    /// The context, such as `{"subset": "train"}`. `None` is the empty context.
    pub context: Option<Value>,
    /// Microseconds since the Unix epoch; now when `None`.
    pub timestamp: Option<i64>,
}

/// A step range for metric reads: `start <= step < stop`; open ends when `None`.
#[derive(Clone, Copy, Debug, Default)]
pub struct StepRange {
    pub start: Option<i64>,
    pub stop: Option<i64>,
}

/// A time range for metric reads: `start <= timestamp < stop`, in microseconds since the Unix
/// epoch (UTC); open ends when `None`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TimeRange {
    pub start: Option<i64>,
    pub stop: Option<i64>,
}

/// Metric points ordered by step.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricData {
    pub steps: Vec<i64>,
    pub values: Vec<f64>,
    pub epochs: Vec<Option<i64>>,
    pub timestamps: Vec<i64>,
}

impl MetricData {
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

/// The id of the series `(run_id, name, context)`.
pub fn series_id(run_id: &str, name: &str, context: &Value) -> u64 {
    let ctx = json::canonical(context);
    let mut buf = Vec::with_capacity(run_id.len() + name.len() + ctx.len() + 2);
    buf.extend_from_slice(run_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(ctx.as_bytes());
    xxhash_rust::xxh3::xxh3_64(&buf)
}

fn now_micros() -> i64 {
    (now_nanos() / 1_000) as i64
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[derive(Default)]
struct Buffers {
    /// `(seq, record, fields it sets)`.
    runs: Vec<(u64, RunInfo, u32)>,
    params: Vec<(u64, String, String, Value)>,
    series: Vec<SeriesInfo>,
    series_ids: Vec<u64>,
    steps: Vec<i64>,
    epochs: Vec<Option<i64>>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    /// Series whose statistics must be computed from their points (a step written again, a
    /// replayed WAL, or `recompute_series_stats`).
    relogged: HashSet<u64>,
    clock: Clock,
    last_flush: Option<Instant>,
}

impl Buffers {
    fn is_empty(&self) -> bool {
        self.runs.is_empty()
            && self.params.is_empty()
            && self.series.is_empty()
            && self.series_ids.is_empty()
            && self.relogged.is_empty()
    }
}

struct Tables {
    runs: Dataset,
    params: Dataset,
    series: Dataset,
    metrics: Dataset,
}

/// State shared by a repository handle, its runs and the background flusher.
struct Core {
    uri: String,
    opts: RepoOptions,
    closed: AtomicBool,
    buffers: parking_lot::Mutex<Buffers>,
    /// Series this handle has written or read, with their run, so each is recorded once and
    /// a point can be traced to its run.
    known_series: parking_lot::Mutex<HashMap<u64, String>>,
    /// Runs this handle knows were deleted (tombstones read, or deleted here).
    deleted: parking_lot::Mutex<HashSet<String>>,
    /// Fragments of `runs` already read for tombstones.
    tombstones_read: parking_lot::Mutex<HashSet<u64>>,
    /// When a flush last checked for a newer version of `runs`.
    tombstones_checked: parking_lot::Mutex<Option<Instant>>,
    /// Held only by tasks on the kv0 runtime (see [`rt::spawn`]).
    tables: tokio::sync::Mutex<Tables>,
    /// Lock order: `tables`, then `buffers`, then `wal`.
    wal: parking_lot::Mutex<Option<Wal>>,
    /// Set by [`Repo::abandon`]: drop without flushing, as a crash would.
    abandoned: AtomicBool,
    /// A background flush is running.
    flushing: AtomicBool,
    /// Background maintenance is running.
    maintaining: AtomicBool,
    /// Tables compacted by background maintenance (diagnostics).
    compactions: AtomicU64,
    /// When marks were last pruned; `None` until the first maintenance after open.
    last_prune: parking_lot::Mutex<Option<Instant>>,
    /// Woken when background maintenance ends, for `close` and tests.
    maintained: tokio::sync::Notify,
    /// Test hook: the next maintenance panics.
    panic_next_maintenance: AtomicBool,
    /// This core, for spawning maintenance from `&self`.
    me: std::sync::OnceLock<Weak<Core>>,
    /// Records tables opened so far, by name.
    records: tokio::sync::Mutex<HashMap<String, Dataset>>,
}

/// Owns the [`Core`]; flushes it when the last handle is dropped.
struct RepoInner {
    core: Arc<Core>,
}

/// A tracking repository. Cheap to clone; clones share one handle and one write buffer.
#[derive(Clone)]
pub struct Repo {
    inner: Arc<RepoInner>,
}

impl std::fmt::Debug for Repo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Repo")
            .field("uri", &self.inner.core.uri)
            .finish()
    }
}

async fn open_table(
    root: &str,
    name: &str,
    schema: SchemaRef,
    opts: &RepoOptions,
) -> Result<Dataset> {
    storage::open_or_create(
        &storage::join(root, &format!("{name}.lance")),
        &opts.storage_options,
        schema,
        opts.create_if_missing,
        opts.version_retention,
    )
    .await
}

/// Adds the columns `runs` gained after its first release (as nulls, a metadata-only commit),
/// so rows written now fit. Another writer upgrading at the same time is harmless.
async fn upgrade(ds: &mut Dataset, added: &SchemaRef) -> Result<()> {
    let first = added.field(0).name().clone();
    if ds.schema().field(&first).is_some() {
        return Ok(());
    }
    let r = ds
        .add_columns(NewColumnTransform::AllNulls(added.clone()), None, None)
        .await;
    if let Err(e) = r {
        latest(ds).await?;
        if ds.schema().field(&first).is_none() {
            return Err(e.into());
        }
    }
    Ok(())
}

async fn append(ds: &mut Dataset, batch: RecordBatch, opts: &StorageOptions) -> Result<()> {
    append_all(ds, vec![batch], opts).await
}

/// Appends `batches` in one commit.
async fn append_all(
    ds: &mut Dataset,
    batches: Vec<RecordBatch>,
    opts: &StorageOptions,
) -> Result<()> {
    let params = storage::write_params(opts, WriteMode::Append);
    let new = InsertBuilder::new(Arc::new(ds.clone()))
        .with_params(&params)
        .execute(batches)
        .await?;
    *ds = new;
    Ok(())
}

async fn latest(ds: &mut Dataset) -> Result<()> {
    if ds.latest_version_id().await? > ds.version().version {
        ds.checkout_latest().await?;
    }
    Ok(())
}

async fn scan(ds: &Dataset, filter: Option<&str>) -> Result<Vec<RecordBatch>> {
    let mut s = ds.scan();
    if let Some(f) = filter {
        s.filter(f)?;
    }
    bounded(&mut s);
    Ok(s.try_into_stream().await?.try_collect().await?)
}

/// Keeps Lance's read-ahead of a scan small: by default it may buffer up to 2 GiB of I/O,
/// which a process keeps resident long after the scan.
fn bounded(s: &mut lance::dataset::scanner::Scanner) {
    s.io_buffer_size(SCAN_IO_BUFFER);
    s.fragment_readahead(2);
    s.batch_readahead(2);
}

/// Bytes of I/O a scan buffers ahead of decoding.
const SCAN_IO_BUFFER: u64 = 64 << 20;

fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Result<&'a T> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| Error::Corruption(format!("column {name} is missing or mistyped")))
}

fn opt_str(a: &StringArray, i: usize) -> Option<String> {
    (!a.is_null(i)).then(|| a.value(i).to_string())
}

fn ts_array(v: Vec<i64>) -> TimestampMicrosecondArray {
    TimestampMicrosecondArray::from(v).with_timezone("UTC")
}

fn runs_batch(rows: &[(u64, RunInfo, u32)]) -> Result<RecordBatch> {
    let mut tags = ListBuilder::new(StringBuilder::new());
    for (_, r, _) in rows {
        for t in &r.tags {
            tags.values().append_value(t);
        }
        tags.append(true);
    }
    let s = |f: fn(&RunInfo) -> Option<&str>| -> StringArray {
        rows.iter().map(|(_, r, _)| f(r)).collect()
    };
    let target: StringArray = rows
        .iter()
        .map(|(_, r, _)| r.target.as_ref().map(|t| t.to_string()))
        .collect();
    Ok(RecordBatch::try_new(
        schema::RUNS.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, r, _)| &r.run_id),
            )),
            Arc::new(s(|r| r.name.as_deref())),
            Arc::new(s(|r| r.experiment.as_deref())),
            Arc::new(s(|r| r.description.as_deref())),
            Arc::new(tags.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, r, _)| r.status.as_str()),
            )),
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|(_, r, _)| Some(r.archived)),
            )),
            Arc::new(ts_array(
                rows.iter().map(|(_, r, _)| r.created_at).collect(),
            )),
            Arc::new(ts_array(
                rows.iter().map(|(_, r, _)| r.updated_at).collect(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, r, _)| r.ended_at).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(s(|r| r.kind.as_deref())),
            Arc::new(target),
        ],
    )?)
}

fn read_runs(batches: &[RecordBatch]) -> Result<Vec<(u64, RunInfo, u32)>> {
    let mut out = Vec::new();
    for b in batches {
        let seq = col::<UInt64Array>(b, "seq")?;
        let id = col::<StringArray>(b, "run_id")?;
        let name = col::<StringArray>(b, "name")?;
        let exp = col::<StringArray>(b, "experiment")?;
        let desc = col::<StringArray>(b, "description")?;
        let tags = col::<arrow_array::ListArray>(b, "tags")?;
        let status = col::<StringArray>(b, "status")?;
        let archived = col::<BooleanArray>(b, "archived")?;
        let created = col::<TimestampMicrosecondArray>(b, "created_at")?;
        let updated = col::<TimestampMicrosecondArray>(b, "updated_at")?;
        let ended = col::<TimestampMicrosecondArray>(b, "ended_at")?;
        // Columns added later; absent or null in older rows.
        let mask = col::<UInt32Array>(b, "fields").ok();
        let kind = col::<StringArray>(b, "kind").ok();
        let target = col::<StringArray>(b, "target").ok();
        for i in 0..b.num_rows() {
            let t = tags.value(i);
            let t = t
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Corruption("tags are not strings".into()))?;
            out.push((
                seq.value(i),
                RunInfo {
                    run_id: id.value(i).to_string(),
                    name: opt_str(name, i),
                    experiment: opt_str(exp, i),
                    description: opt_str(desc, i),
                    tags: t.iter().flatten().map(str::to_string).collect(),
                    status: RunStatus::parse(status.value(i))?,
                    archived: archived.value(i),
                    created_at: created.value(i),
                    updated_at: updated.value(i),
                    ended_at: (!ended.is_null(i)).then(|| ended.value(i)),
                    kind: kind.and_then(|k| opt_str(k, i)),
                    target: match target.and_then(|t| opt_str(t, i)) {
                        Some(t) => Some(
                            serde_json::from_str(&t)
                                .map_err(|e| Error::Corruption(e.to_string()))?,
                        ),
                        None => None,
                    },
                },
                mask.filter(|m| !m.is_null(i))
                    .map_or(fields::ALL, |m| m.value(i)),
            ));
        }
    }
    Ok(out)
}

fn params_batch(rows: &[(u64, String, String, Value)]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema::PARAMS.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.2))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.3.to_string()),
            )),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.3.as_f64()))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.3.as_str()))),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r.3.as_bool()))),
        ],
    )?)
}

fn series_batch(rows: &[SeriesInfo]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema::SERIES.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.series_id),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| &r.run_id),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.name))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| json::canonical(&r.context)),
            )),
            Arc::new(ts_array(rows.iter().map(|r| r.created_at).collect())),
        ],
    )?)
}

fn metrics_batch(b: &Buffers) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema::METRICS.clone(),
        vec![
            Arc::new(UInt64Array::from(b.series_ids.clone())),
            Arc::new(Int64Array::from(b.steps.clone())),
            Arc::new(Int64Array::from(b.epochs.clone())),
            Arc::new(ts_array(b.timestamps.clone())),
            Arc::new(Float64Array::from(b.values.clone())),
            Arc::new(arrow_array::BinaryArray::new_null(b.series_ids.len())),
        ],
    )?)
}

/// Reserved metrics rows holding statistics deltas: `series_id` 0, the series in `step`.
fn deltas_batch(deltas: &[(u64, stats::Delta)]) -> Result<RecordBatch> {
    let n = deltas.len();
    let enc: Vec<Vec<u8>> = deltas.iter().map(|(_, d)| d.encode()).collect();
    Ok(RecordBatch::try_new(
        schema::METRICS.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0u64; n])),
            Arc::new(Int64Array::from_iter_values(
                deltas.iter().map(|(id, _)| *id as i64),
            )),
            Arc::new(Int64Array::new_null(n)),
            Arc::new(ts_array(vec![0; n])),
            Arc::new(Float64Array::from(vec![0.0; n])),
            Arc::new(arrow_array::BinaryArray::from_iter_values(enc.iter())),
        ],
    )?)
}

/// The filter that selects statistics rows.
const STATS_ROWS: &str = "series_id = 0 AND stats IS NOT NULL";

fn read_series_rows(batches: &[RecordBatch]) -> Result<Vec<SeriesInfo>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for b in batches {
        let id = col::<UInt64Array>(b, "series_id")?;
        let run = col::<StringArray>(b, "run_id")?;
        let name = col::<StringArray>(b, "name")?;
        let ctx = col::<StringArray>(b, "context")?;
        let created = col::<TimestampMicrosecondArray>(b, "created_at")?;
        for i in 0..b.num_rows() {
            if seen.insert(id.value(i)) {
                out.push(SeriesInfo {
                    series_id: id.value(i),
                    run_id: run.value(i).to_string(),
                    name: name.value(i).to_string(),
                    context: serde_json::from_str(ctx.value(i))
                        .map_err(|e| Error::Corruption(e.to_string()))?,
                    created_at: created.value(i),
                });
            }
        }
    }
    Ok(out)
}

/// How often a flush checks for runs deleted by other handles. A writer may commit writes to a
/// run deleted elsewhere for up to this long (plus one flush); reads always check.
const TOMBSTONE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Run id prefix of the WAL mark rows in `runs`, `params` and `series`.
const MARK_PREFIX: &str = "__kv0_wal__/";

fn writer_hash(writer: &str) -> i64 {
    xxhash_rust::xxh3::xxh3_64(writer.as_bytes()) as i64
}

/// Appends each non-empty buffer as one commit, clearing what was written. Series are written
/// before their points, so a reader never sees points of a series it cannot name.
///
/// With a WAL writer, each table's commit also holds a mark row: the writer has committed every
/// write of that table's kind up to sequence number `upto`.
async fn write_all(
    t: &mut Tables,
    left: &mut Buffers,
    o: &StorageOptions,
    mark: Option<(&str, u64)>,
    deltas: &[(u64, stats::Delta)],
) -> Result<()> {
    let mark_id = mark.map(|(w, _)| format!("{MARK_PREFIX}{w}"));
    if !left.runs.is_empty() {
        let mut rows = left.runs.clone();
        if let (Some(id), Some((_, upto))) = (&mark_id, mark) {
            let now = now_micros();
            rows.push((
                upto,
                RunInfo {
                    run_id: id.clone(),
                    name: None,
                    experiment: None,
                    description: None,
                    tags: Vec::new(),
                    status: RunStatus::Running,
                    archived: true,
                    created_at: now,
                    updated_at: now,
                    ended_at: None,
                    kind: None,
                    target: None,
                },
                fields::ALL,
            ));
        }
        append(&mut t.runs, runs_batch(&rows)?, o).await?;
        left.runs.clear();
    }
    if !left.params.is_empty() {
        let mut rows = left.params.clone();
        if let (Some(id), Some((_, upto))) = (&mark_id, mark) {
            rows.push((upto, id.clone(), String::new(), Value::Null));
        }
        append(&mut t.params, params_batch(&rows)?, o).await?;
        left.params.clear();
    }
    if !left.series.is_empty() {
        let mut rows = left.series.clone();
        if let (Some(id), Some((_, upto))) = (&mark_id, mark) {
            rows.push(SeriesInfo {
                series_id: 0,
                run_id: id.clone(),
                name: upto.to_string(),
                context: Value::Object(Default::default()),
                created_at: now_micros(),
            });
        }
        append(&mut t.series, series_batch(&rows)?, o).await?;
        left.series.clear();
    }
    if !left.series_ids.is_empty() || !deltas.is_empty() {
        let mut pts = Buffers {
            series_ids: left.series_ids.clone(),
            steps: left.steps.clone(),
            epochs: left.epochs.clone(),
            timestamps: left.timestamps.clone(),
            values: left.values.clone(),
            ..Default::default()
        };
        if let Some((w, upto)) = mark {
            pts.series_ids.push(0);
            pts.steps.push(upto as i64);
            pts.epochs.push(Some(writer_hash(w)));
            pts.timestamps.push(now_micros());
            pts.values.push(0.0);
        }
        // The statistics of these points in the same commit: atomic with them, no extra commit.
        let mut batches = vec![metrics_batch(&pts)?];
        if !deltas.is_empty() {
            batches.push(deltas_batch(deltas)?);
        }
        append_all(&mut t.metrics, batches, o).await?;
        left.relogged.clear();
        left.series_ids.clear();
        left.steps.clear();
        left.epochs.clear();
        left.timestamps.clear();
        left.values.clear();
    }
    Ok(())
}

/// Per-table WAL marks of `writer`: the highest committed sequence number of runs, params,
/// series and metric points.
async fn marks_of(t: &Tables, writer: &str) -> Result<[u64; 4]> {
    let id = quote(&format!("{MARK_PREFIX}{writer}"));
    let max_seq = |batches: Vec<RecordBatch>| -> Result<u64> {
        let mut m = 0;
        for b in &batches {
            let seq = col::<UInt64Array>(b, "seq")?;
            m = seq.values().iter().copied().fold(m, u64::max);
        }
        Ok(m)
    };
    let runs = max_seq(scan(&t.runs, Some(&format!("run_id = {id}"))).await?)?;
    let params = max_seq(scan(&t.params, Some(&format!("run_id = {id}"))).await?)?;
    let mut series = 0;
    for b in scan(&t.series, Some(&format!("run_id = {id}"))).await? {
        let name = col::<StringArray>(&b, "name")?;
        for v in name.iter().flatten() {
            series = series.max(v.parse().unwrap_or(0));
        }
    }
    let mut points = 0;
    let filter = format!("series_id = 0 AND epoch = {}", writer_hash(writer));
    for b in scan(&t.metrics, Some(&filter)).await? {
        let step = col::<Int64Array>(&b, "step")?;
        for v in step.values() {
            points = points.max(*v as u64);
        }
    }
    Ok([runs, params, series, points])
}

/// Fragments below the compaction target: only these can be merged, so only these count.
fn small_fragments(ds: &Dataset, target: usize) -> usize {
    ds.fragments()
        .iter()
        .filter(|f| f.physical_rows.unwrap_or(0) < target)
        .count()
}

impl Core {
    /// Background maintenance after a flush: compacts tables with more than `max_fragments`
    /// small fragments, and prunes mark rows once per `mark_prune_interval`. Works on clones of
    /// the tables without holding the `tables` lock; Lance lets these commits land alongside
    /// appends. Returns the number of tables compacted.
    async fn maintain(&self) -> Result<usize> {
        if self.panic_next_maintenance.swap(false, Ordering::AcqRel) {
            panic!("kv0: injected maintenance panic");
        }
        let o = &self.opts;
        let mut t = {
            let t = self.tables.lock().await;
            Tables {
                runs: t.runs.clone(),
                params: t.params.clone(),
                series: t.series.clone(),
                metrics: t.metrics.clone(),
            }
        };
        let due = {
            let mut last = self.last_prune.lock();
            let due = last.is_none_or(|at| at.elapsed() >= o.mark_prune_interval);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due {
            prune_marks(&mut t, o.mark_ttl).await?;
        }
        if o.max_fragments == 0 {
            return Ok(0);
        }
        let mut compacted = 0;
        let options = CompactionOptions {
            target_rows_per_fragment: o.compaction_target_rows,
            ..CompactionOptions::default()
        };
        // The records tables opened so far: clones, so appends keep going.
        let mut extra: Vec<Dataset> = self.records.lock().await.values().cloned().collect();
        for ds in [&mut t.runs, &mut t.params, &mut t.series, &mut t.metrics]
            .into_iter()
            .chain(extra.iter_mut())
        {
            if small_fragments(ds, o.compaction_target_rows) > o.max_fragments {
                latest(ds).await?;
                compact_files(ds, options.clone(), None).await?;
                compacted += 1;
            }
        }
        Ok(compacted)
    }

    /// Starts background maintenance unless one is already running.
    fn request_maintenance(&self) {
        let Some(core) = self.me.get().and_then(Weak::upgrade) else {
            return;
        };
        if core.closed.load(Ordering::Acquire) || core.maintaining.swap(true, Ordering::AcqRel) {
            return;
        }
        // Clears the flag however maintenance ends (error, panic or a dropped task), so one
        // failure cannot stop maintenance for the rest of the handle's life.
        struct Clear(Arc<Core>);
        impl Drop for Clear {
            fn drop(&mut self) {
                self.0.maintaining.store(false, Ordering::Release);
                self.0.maintained.notify_waiters();
            }
        }
        rt::detach(async move {
            let guard = Clear(core);
            let core = &guard.0;
            match AssertUnwindSafe(core.maintain()).catch_unwind().await {
                Ok(Ok(n)) => {
                    core.compactions.fetch_add(n as u64, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    tracing::debug!(uri = %core.uri, error = %e, "kv0: tracking maintenance skipped")
                }
                Err(_) => {
                    tracing::error!(uri = %core.uri, "kv0: tracking maintenance panicked; it will run again after the next flush")
                }
            }
        });
    }

    /// Waits until no background maintenance is running.
    async fn wait_for_maintenance(&self) {
        loop {
            let woken = self.maintained.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if !self.maintaining.load(Ordering::Acquire) {
                return;
            }
            woken.await;
        }
    }
}

/// The fewest rows that merge like `rows` do, now and with any row added later: per run, one
/// row per sequence number at which some field was last set, holding the merged record and
/// the mask of those fields. Each field keeps its own `seq`, so a row committed later with an
/// older `seq` (buffered in another handle, clock skew, a replayed WAL) still wins or loses
/// each field exactly as it would have against the unfolded rows.
fn fold_run_rows(mut rows: Vec<(u64, RunInfo, u32)>) -> Vec<(u64, RunInfo, u32)> {
    rows.sort_by_key(|r| r.0);
    // Per run: its first row's seq, and per field the seq of the row that last set it by its
    // mask (`None`: only the first row, taken whole, gives it a value).
    let mut last: HashMap<String, (u64, [Option<u64>; FIELD_COUNT])> = HashMap::new();
    for (seq, info, mask) in &rows {
        let (_, at) = last
            .entry(info.run_id.clone())
            .or_insert((*seq, [None; FIELD_COUNT]));
        for (bit, s) in at.iter_mut().enumerate() {
            if mask & (1 << bit) != 0 {
                *s = Some(*seq);
            }
        }
    }
    let merged = merge_runs(rows);
    let mut out = Vec::new();
    for (run, (first, at)) in last {
        let Some((_, info)) = merged.get(&run) else {
            continue;
        };
        let mut groups: BTreeMap<u64, u32> = BTreeMap::new();
        for (bit, s) in at.iter().enumerate() {
            if let Some(s) = s {
                *groups.entry(*s).or_default() |= 1 << bit;
            }
        }
        // Fields only the first row gives (taken whole, not by its mask) stay that way: the
        // first row is kept, with just its own mask, so a row that sorts before it later takes
        // those fields, as it would have before the fold.
        if at.iter().any(Option::is_none) {
            groups.entry(first).or_default();
        }
        for (seq, mask) in groups {
            out.push((seq, info.clone(), mask));
        }
    }
    out
}

/// Fields of a run record that rows set one by one (the bits of [`fields::ALL`]).
const FIELD_COUNT: usize = 10;

/// Rewrites `runs` with the fewest rows per run that merge the same (see [`fold_run_rows`]),
/// keeping tombstones and WAL mark rows as they are, in one commit that keeps rows
/// appended meanwhile. A run that changes often (a heartbeat every few seconds) otherwise
/// leaves thousands of rows that every read of the runs merges again. Returns the rows dropped.
async fn fold_runs(ds: &mut Dataset, opts: &StorageOptions) -> Result<usize> {
    use lance::dataset::transaction::{Operation, TransactionBuilder, UpdateMode};

    let fragments = ds.fragments().to_vec();
    let mut sc = ds.scan();
    sc.with_fragments(fragments.clone());
    let rows = read_runs(&sc.try_into_stream().await?.try_collect::<Vec<_>>().await?)?;
    let before = rows.len();
    let (kept, runs): (Vec<_>, Vec<_>) = rows
        .into_iter()
        .partition(|r| r.2 == fields::DELETED || r.1.run_id.starts_with(MARK_PREFIX));
    let dead: HashSet<&str> = kept
        .iter()
        .filter(|r| r.2 == fields::DELETED)
        .map(|r| r.1.run_id.as_str())
        .collect();
    // Rows of deleted runs (a writer racing the delete) are left for purge and expiry.
    let (live, of_dead): (Vec<_>, Vec<_>) = runs
        .into_iter()
        .partition(|r| !dead.contains(r.1.run_id.as_str()));
    let mut merged = fold_run_rows(live);
    merged.sort_by_key(|r| r.0);
    let mut out = kept;
    out.extend(of_dead);
    out.extend(merged);
    if out.len() == before {
        return Ok(0);
    }
    let removed: Vec<u64> = fragments.iter().map(|f| f.id).collect();
    let params = storage::write_params(opts, WriteMode::Append);
    let base = Arc::new(ds.clone());
    let txn = InsertBuilder::new(base.clone())
        .with_params(&params)
        .execute_uncommitted(vec![runs_batch(&out)?])
        .await?;
    let new_fragments = match txn.operation {
        Operation::Append { fragments } => fragments,
        other => {
            return Err(Error::Storage(
                format!("unexpected uncommitted operation {other:?}").into(),
            ))
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
    let txn = TransactionBuilder::new(base.version().version, op).build();
    *ds = lance::dataset::CommitBuilder::new(base)
        .execute(txn)
        .await?;
    Ok(before - out.len())
}

/// Points read into memory per pass when sorting the metrics table.
const SORT_BUCKET_ROWS: usize = 500_000;

/// Metadata key of `metrics` listing the fragments `optimize` wrote sorted (ranges `a-b`).
const SORTED_KEY: &str = "kv0.sorted_fragments";

fn parse_ranges(s: &str) -> HashSet<u64> {
    let mut out = HashSet::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (a, b) = part.split_once('-').unwrap_or((part, part));
        if let (Ok(a), Ok(b)) = (a.parse::<u64>(), b.parse::<u64>()) {
            out.extend(a..=b.min(a + 10_000_000));
        }
    }
    out
}

fn format_ranges(ids: &HashSet<u64>) -> String {
    let mut v: Vec<u64> = ids.iter().copied().collect();
    v.sort_unstable();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < v.len() {
        let mut j = i;
        while j + 1 < v.len() && v[j + 1] == v[j] + 1 {
            j += 1;
        }
        out.push(format!("{}-{}", v[i], v[j]));
        i = j + 1;
    }
    out.join(",")
}

/// Rewrites the fragments of `metrics` that `optimize` has not sorted yet, sorted by
/// `(series_id, step)`, in one commit, keeping only the latest point of each step and only
/// series in `live`, with one exact statistics delta per series. Fragments sorted before are
/// kept as they are, unless a new point may overwrite a step they hold (a step at or below the
/// series' last stored step), in which case every fragment is rewritten. Series are processed
/// in hash ranges of about [`SORT_BUCKET_ROWS`] points, so memory stays bounded; appends
/// committed meanwhile by other writers are kept. Returns whether anything was rewritten.
async fn sort_metrics(
    ds: &mut Dataset,
    opts: &StorageOptions,
    live: Arc<HashSet<u64>>,
) -> Result<bool> {
    use futures::{StreamExt, TryFutureExt};
    use lance::dataset::transaction::{Operation, TransactionBuilder, UpdateMode};
    use lance::deps::datafusion::error::DataFusionError;
    use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;

    let all: Vec<lance::table::format::Fragment> = ds.fragments().to_vec();
    let present: HashSet<u64> = all.iter().map(|f| f.id).collect();
    let recorded: HashSet<u64> = ds
        .metadata()
        .get(SORTED_KEY)
        .map(|s| parse_ranges(s))
        .unwrap_or_default();
    let sorted: HashSet<u64> = recorded.intersection(&present).copied().collect();
    let (old, new): (Vec<_>, Vec<_>) = all.into_iter().partition(|f| sorted.contains(&f.id));
    if new.is_empty() {
        return Ok(false);
    }
    // A sorted fragment gone (merged by background compaction, or emptied by a delete) may
    // have moved sorted points or their exact statistics into the fragments to rewrite: sort
    // everything rather than reason about the mix.
    let moved = recorded.len() != sorted.len() || {
        let mut sc = ds.scan();
        sc.with_fragments(new.clone());
        sc.project(&["stats"])?;
        sc.filter(STATS_ROWS)?;
        let mut exact = false;
        for b in sc.try_into_stream().await?.try_collect::<Vec<_>>().await? {
            let enc = col::<arrow_array::BinaryArray>(&b, "stats")?;
            for i in 0..b.num_rows() {
                // Statistics written by optimize (sequence 0), not by a flush.
                let d = stats::Delta::decode(enc.value(i))?;
                exact |= d.last_seq == 0 && !d.dirty && d.s.count > 0;
            }
        }
        exact
    };
    // Would a new point overwrite a step of a sorted fragment? Then sort everything.
    let full = old.is_empty() || moved || {
        let mut last: HashMap<u64, i64> = HashMap::new();
        let mut sc = ds.scan();
        sc.with_fragments(old.clone());
        sc.project(&["step", "stats"])?;
        sc.filter(STATS_ROWS)?;
        for b in sc.try_into_stream().await?.try_collect::<Vec<_>>().await? {
            let step = col::<Int64Array>(&b, "step")?;
            let enc = col::<arrow_array::BinaryArray>(&b, "stats")?;
            for i in 0..b.num_rows() {
                let d = stats::Delta::decode(enc.value(i))?;
                let e = last.entry(step.value(i) as u64).or_insert(i64::MIN);
                *e = (*e).max(d.s.last_step);
            }
        }
        let mut sc = ds.scan();
        sc.with_fragments(new.clone());
        sc.project(&["series_id", "step"])?;
        sc.filter("series_id <> 0")?;
        let mut overlap = false;
        for b in sc.try_into_stream().await?.try_collect::<Vec<_>>().await? {
            let id = col::<UInt64Array>(&b, "series_id")?;
            let step = col::<Int64Array>(&b, "step")?;
            for i in 0..b.num_rows() {
                if last.get(&id.value(i)).is_some_and(|l| step.value(i) <= *l) {
                    overlap = true;
                }
            }
        }
        overlap
    };
    let (keep, rewrite): (Vec<_>, Vec<_>) = if full {
        (Vec::new(), old.into_iter().chain(new).collect())
    } else {
        (old, new)
    };
    let removed: Vec<u64> = rewrite.iter().map(|f| f.id).collect();
    let rows: usize = rewrite.iter().map(|f| f.physical_rows.unwrap_or(0)).sum();
    let buckets = (rows / SORT_BUCKET_ROWS + 1) as u64;
    let width = u64::MAX / buckets;
    let src = Arc::new(ds.clone());
    let frags = Arc::new(rewrite);
    let stream = futures::stream::iter(0..buckets)
        .then(move |i| {
            let (src, live, frags) = (src.clone(), live.clone(), frags.clone());
            async move {
                let lo = i * width;
                let filter = if i + 1 == buckets {
                    format!("series_id >= {lo}")
                } else {
                    format!("series_id >= {lo} AND series_id < {}", lo + width)
                };
                let mut s = src.scan();
                s.with_fragments(frags.to_vec());
                s.filter(&filter)?;
                // One bucket at a time is the memory bound; keep Lance's read-ahead small too.
                bounded(&mut s);
                let batches: Vec<RecordBatch> = s.try_into_stream().await?.try_collect().await?;
                sorted_points(batches, &live)
            }
            .map_err(|e: Error| DataFusionError::External(Box::new(e)))
        })
        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
        .try_flatten();
    let source: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema::METRICS.clone(),
        stream,
    ));
    let params = storage::write_params(opts, WriteMode::Append);
    let base = Arc::new(ds.clone());
    let txn = InsertBuilder::new(base.clone())
        .with_params(&params)
        .execute_uncommitted_stream(source)
        .await?;
    let new_fragments = match txn.operation {
        Operation::Append { fragments } => fragments,
        other => {
            return Err(Error::Storage(
                format!("unexpected uncommitted operation {other:?}").into(),
            ))
        }
    };
    let added = new_fragments.len();
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
    let txn = TransactionBuilder::new(base.version().version, op).build();
    *ds = lance::dataset::CommitBuilder::new(base)
        .execute(txn)
        .await?;
    // This commit's fragments got the highest ids of its version; record them as sorted. A
    // crash before this leaves them unsorted, which only costs a rewrite next time.
    let mut ids: Vec<u64> = ds.fragments().iter().map(|f| f.id).collect();
    ids.sort_unstable();
    let mut sorted: HashSet<u64> = keep.iter().map(|f| f.id).collect();
    sorted.extend(ids.iter().rev().take(added));
    ds.update_metadata([(SORTED_KEY, format_ranges(&sorted).as_str())])
        .await?;
    Ok(true)
}

/// Points of `batches` sorted by `(series_id, step)`, keeping the last in scan order of points
/// that share a step (the one reads return) and only series in `live`, as metrics batches of
/// up to 64k rows; then one exact statistics delta per series, replacing the stored deltas.
fn sorted_points(batches: Vec<RecordBatch>, live: &HashSet<u64>) -> Result<Vec<RecordBatch>> {
    let rows = batches.iter().map(RecordBatch::num_rows).sum();
    let mut pts: Vec<(u64, i64, u64, Option<i64>, i64, f64)> = Vec::with_capacity(rows);
    for b in &batches {
        let id = col::<UInt64Array>(b, "series_id")?;
        let step = col::<Int64Array>(b, "step")?;
        let epoch = col::<Int64Array>(b, "epoch")?;
        let ts = col::<TimestampMicrosecondArray>(b, "timestamp")?;
        let value = col::<Float64Array>(b, "value")?;
        let stats = col::<arrow_array::BinaryArray>(b, "stats")?;
        for i in 0..b.num_rows() {
            let sid = id.value(i);
            // Statistics rows are recomputed below; points of deleted series are dropped.
            if (sid == 0 && !stats.is_null(i)) || (sid != 0 && !live.contains(&sid)) {
                continue;
            }
            let n = pts.len() as u64;
            pts.push((
                id.value(i),
                step.value(i),
                n,
                (!epoch.is_null(i)).then(|| epoch.value(i)),
                ts.value(i),
                value.value(i),
            ));
        }
    }
    drop(batches); // the points are copied out: release the decoded batches now
                   // WAL marks (series 0) are all kept; they are told apart by `epoch`.
    pts.sort_unstable_by_key(|p| (p.0, p.1, p.2));
    let mut out = Vec::new();
    let mut b = Buffers::default();
    // The kept points are sorted and unique per step, so the statistics are exact and can be
    // folded in passing, one series at a time.
    let mut deltas: Vec<(u64, stats::Delta)> = Vec::new();
    for (i, p) in pts.iter().enumerate() {
        let next = pts.get(i + 1);
        if p.0 != 0 && next.is_some_and(|q| q.0 == p.0 && q.1 == p.1) {
            continue;
        }
        if p.0 != 0 {
            let one = stats::Delta::of_point(p.1, p.5, p.4);
            match deltas.last_mut() {
                Some((id, d)) if *id == p.0 => d.merge(&one),
                _ => deltas.push((p.0, one)),
            }
        }
        b.series_ids.push(p.0);
        b.steps.push(p.1);
        b.epochs.push(p.3);
        b.timestamps.push(p.4);
        b.values.push(p.5);
        if b.series_ids.len() == 64 * 1024 {
            out.push(metrics_batch(&b)?);
            b = Buffers::default();
        }
    }
    if !b.series_ids.is_empty() {
        out.push(metrics_batch(&b)?);
    }
    drop(pts);
    deltas.sort_unstable_by_key(|d| d.0);
    for chunk in deltas.chunks(64 * 1024) {
        out.push(deltas_batch(chunk)?);
    }
    Ok(out)
}

/// Exact statistics of the series in `ids`, read from their points in `metrics`: a step
/// written twice counts once, with the value reads return.
async fn exact_from_points(
    metrics: &Dataset,
    ids: &HashSet<u64>,
) -> Result<HashMap<u64, SeriesStats>> {
    let mut out: HashMap<u64, SeriesStats> = HashMap::new();
    let list: Vec<u64> = ids.iter().copied().collect();
    for chunk in list.chunks(1000) {
        let filter = format!(
            "series_id IN ({})",
            chunk
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        let (mut i, mut st, mut v, mut ts) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for b in scan(metrics, Some(&filter)).await? {
            i.extend_from_slice(col::<UInt64Array>(&b, "series_id")?.values());
            st.extend_from_slice(col::<Int64Array>(&b, "step")?.values());
            ts.extend_from_slice(col::<TimestampMicrosecondArray>(&b, "timestamp")?.values());
            v.extend_from_slice(col::<Float64Array>(&b, "value")?.values());
        }
        out.extend(stats::exact(&i, &st, &v, &ts));
    }
    Ok(out)
}

/// Deletes WAL mark rows that are no longer needed: all but each writer's newest, and every
/// mark older than `ttl`. One delete per table, only when there is something to delete.
async fn prune_marks(t: &mut Tables, ttl: Duration) -> Result<()> {
    let cutoff = now_nanos().saturating_sub(ttl.as_nanos() as u64);
    let like = format!("run_id LIKE {}", quote(&format!("{MARK_PREFIX}%")));
    // runs and params carry the sequence number in `seq`.
    for ds in [&mut t.runs, &mut t.params] {
        latest(ds).await?;
        let mut best: HashMap<String, (u64, usize)> = HashMap::new();
        for b in scan(ds, Some(&like)).await? {
            let id = col::<StringArray>(&b, "run_id")?;
            let seq = col::<UInt64Array>(&b, "seq")?;
            for i in 0..b.num_rows() {
                if id.value(i).starts_with(MARK_PREFIX) {
                    let e = best.entry(id.value(i).to_string()).or_insert((0, 0));
                    e.0 = e.0.max(seq.value(i));
                    e.1 += 1;
                }
            }
        }
        let preds: Vec<String> = best
            .iter()
            .filter(|(_, (max, n))| *n > 1 || *max <= cutoff)
            .map(|(id, (max, _))| {
                if *max <= cutoff {
                    format!("run_id = {}", quote(id))
                } else {
                    format!("(run_id = {} AND seq < {max})", quote(id))
                }
            })
            .collect();
        if !preds.is_empty() {
            ds.delete(&preds.join(" OR ")).await?;
        }
    }
    // series: the sequence number is the `name`.
    latest(&mut t.series).await?;
    let mut best: HashMap<String, (u64, usize)> = HashMap::new();
    for b in scan(&t.series, Some(&like)).await? {
        let id = col::<StringArray>(&b, "run_id")?;
        let name = col::<StringArray>(&b, "name")?;
        for i in 0..b.num_rows() {
            if id.value(i).starts_with(MARK_PREFIX) {
                let e = best.entry(id.value(i).to_string()).or_insert((0, 0));
                e.0 = e.0.max(name.value(i).parse().unwrap_or(0));
                e.1 += 1;
            }
        }
    }
    let preds: Vec<String> = best
        .iter()
        .filter(|(_, (max, n))| *n > 1 || *max <= cutoff)
        .map(|(id, (max, _))| {
            if *max <= cutoff {
                format!("run_id = {}", quote(id))
            } else {
                format!(
                    "(run_id = {} AND name <> {})",
                    quote(id),
                    quote(&max.to_string())
                )
            }
        })
        .collect();
    if !preds.is_empty() {
        t.series.delete(&preds.join(" OR ")).await?;
    }
    // metrics: series_id 0, the writer's hash in `epoch`, the sequence number in `step`.
    latest(&mut t.metrics).await?;
    let mut best: HashMap<i64, (i64, usize)> = HashMap::new();
    for b in scan(&t.metrics, Some("series_id = 0")).await? {
        let epoch = col::<Int64Array>(&b, "epoch")?;
        let step = col::<Int64Array>(&b, "step")?;
        for i in 0..b.num_rows() {
            if !epoch.is_null(i) {
                let e = best.entry(epoch.value(i)).or_insert((0, 0));
                e.0 = e.0.max(step.value(i));
                e.1 += 1;
            }
        }
    }
    let preds: Vec<String> = best
        .iter()
        .filter(|(_, (max, n))| *n > 1 || (*max as u64) <= cutoff)
        .map(|(h, (max, _))| {
            if (*max as u64) <= cutoff {
                format!("(series_id = 0 AND epoch = {h})")
            } else {
                format!("(series_id = 0 AND epoch = {h} AND step < {max})")
            }
        })
        .collect();
    if !preds.is_empty() {
        t.metrics.delete(&preds.join(" OR ")).await?;
    }
    Ok(())
}

// ---- WAL records: one per write call, kind 20..23 so they never mix with key-value records.

const W_RUN: u8 = 20;
const W_PARAM: u8 = 21;
const W_SERIES: u8 = 22;
const W_POINT: u8 = 23;

fn run_record(seq: u64, info: &RunInfo, mask: u32) -> Op {
    let mut v = serde_json::to_value(info).unwrap_or_default();
    if let Value::Object(m) = &mut v {
        m.insert("__fields".into(), mask.into());
    }
    Op {
        seq,
        kind: W_RUN,
        key: Bytes::new(),
        value: Some(Bytes::from(serde_json::to_vec(&v).unwrap_or_default())),
    }
}

fn param_record(seq: u64, run_id: &str, key: &str, value: &Value) -> Op {
    Op {
        seq,
        kind: W_PARAM,
        key: Bytes::copy_from_slice(run_id.as_bytes()),
        value: Some(Bytes::from(
            serde_json::to_vec(&(key, value)).unwrap_or_default(),
        )),
    }
}

fn series_record(seq: u64, s: &SeriesInfo) -> Op {
    Op {
        seq,
        kind: W_SERIES,
        key: Bytes::new(),
        value: Some(Bytes::from(serde_json::to_vec(s).unwrap_or_default())),
    }
}

fn point_record(seq: u64, sid: u64, step: i64, epoch: Option<i64>, ts: i64, value: f64) -> Op {
    let mut v = Vec::with_capacity(33);
    v.extend_from_slice(&step.to_le_bytes());
    v.push(epoch.is_some() as u8);
    v.extend_from_slice(&epoch.unwrap_or(0).to_le_bytes());
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&value.to_le_bytes());
    Op {
        seq,
        kind: W_POINT,
        key: Bytes::copy_from_slice(&sid.to_le_bytes()),
        value: Some(Bytes::from(v)),
    }
}

/// Puts a replayed WAL record back into the buffers. Returns its table index (as in
/// [`marks_of`]), or `None` for a record that is not tracking data.
fn restore_record(b: &mut Buffers, op: &Op) -> Result<Option<usize>> {
    let bad = || Error::Corruption("a tracking WAL record could not be decoded".into());
    let v = op.value.as_deref().unwrap_or_default();
    match op.kind {
        W_RUN => {
            let mut v: Value = serde_json::from_slice(v).map_err(|_| bad())?;
            // Records written before per-field updates set every field.
            let mask = v
                .as_object_mut()
                .and_then(|m| m.remove("__fields"))
                .and_then(|m| m.as_u64())
                .map_or(fields::ALL, |m| m as u32);
            b.runs
                .push((op.seq, serde_json::from_value(v).map_err(|_| bad())?, mask));
            Ok(Some(0))
        }
        W_PARAM => {
            let (k, val): (String, Value) = serde_json::from_slice(v).map_err(|_| bad())?;
            let run = String::from_utf8(op.key.to_vec()).map_err(|_| bad())?;
            b.params.push((op.seq, run, k, val));
            Ok(Some(1))
        }
        W_SERIES => {
            b.series.push(serde_json::from_slice(v).map_err(|_| bad())?);
            Ok(Some(2))
        }
        W_POINT => {
            if v.len() != 33 || op.key.len() != 8 {
                return Err(bad());
            }
            let i64_at = |i: usize| i64::from_le_bytes(v[i..i + 8].try_into().unwrap());
            b.series_ids
                .push(u64::from_le_bytes(op.key[..].try_into().unwrap()));
            b.steps.push(i64_at(0));
            b.epochs.push((v[8] == 1).then(|| i64_at(9)));
            b.timestamps.push(i64_at(17));
            b.values
                .push(f64::from_le_bytes(v[25..33].try_into().unwrap()));
            Ok(Some(3))
        }
        _ => Ok(None),
    }
}

/// Puts unwritten rows back in front of rows buffered since.
fn prepend(b: &mut Buffers, mut left: Buffers) {
    fn front<T>(dst: &mut Vec<T>, mut src: Vec<T>) {
        src.append(dst);
        *dst = src;
    }
    front(&mut b.runs, std::mem::take(&mut left.runs));
    front(&mut b.params, std::mem::take(&mut left.params));
    front(&mut b.series, std::mem::take(&mut left.series));
    front(&mut b.series_ids, std::mem::take(&mut left.series_ids));
    front(&mut b.steps, std::mem::take(&mut left.steps));
    front(&mut b.epochs, std::mem::take(&mut left.epochs));
    front(&mut b.timestamps, std::mem::take(&mut left.timestamps));
    front(&mut b.values, std::mem::take(&mut left.values));
    b.relogged.extend(left.relogged.drain());
}

impl Core {
    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::InvalidArgument("the repository is closed".into()))
        } else {
            Ok(())
        }
    }

    fn push_run(&self, info: RunInfo, mask: u32) -> Result<()> {
        let mut b = self.buffers.lock();
        let seq = b.clock.next();
        self.log(&[run_record(seq, &info, mask)])?;
        b.runs.push((seq, info, mask));
        Ok(())
    }

    /// Appends records to the WAL; called with `buffers` locked so the WAL order matches.
    fn log(&self, records: &[Op]) -> Result<()> {
        match self.wal.lock().as_mut() {
            Some(w) => w.append(records),
            None => Ok(()),
        }
    }

    async fn flush(&self) -> Result<()> {
        let mut t = self.tables.lock().await;
        let (mut taken, mark) = {
            let mut b = self.buffers.lock();
            b.last_flush = Some(Instant::now());
            if b.is_empty() {
                return Ok(());
            }
            // Seal the WAL segment with the buffers taken: every record up to `upto` is in them.
            let mut w = self.wal.lock();
            let mark = match w.as_mut() {
                Some(w) => {
                    w.rotate()?;
                    Some((w.writer_id().to_string(), b.clock.last()))
                }
                None => None,
            };
            (
                Buffers {
                    runs: std::mem::take(&mut b.runs),
                    params: std::mem::take(&mut b.params),
                    series: std::mem::take(&mut b.series),
                    series_ids: std::mem::take(&mut b.series_ids),
                    steps: std::mem::take(&mut b.steps),
                    epochs: std::mem::take(&mut b.epochs),
                    timestamps: std::mem::take(&mut b.timestamps),
                    values: std::mem::take(&mut b.values),
                    relogged: std::mem::take(&mut b.relogged),
                    ..Default::default()
                },
                mark,
            )
        };
        // Writes to runs deleted elsewhere are dropped here (their WAL records with the segment).
        let r = match self.drop_deleted(&mut t, &mut taken).await {
            Ok(()) => {
                let seq = mark
                    .as_ref()
                    .map_or_else(|| self.buffers.lock().clock.last(), |m| m.1);
                let deltas: Vec<(u64, stats::Delta)> = stats::of_points(
                    &taken.series_ids,
                    &taken.steps,
                    &taken.values,
                    &taken.timestamps,
                    seq,
                    &taken.relogged,
                )
                .into_iter()
                .collect();
                let before = t.runs.version().version;
                let r = write_all(
                    &mut t,
                    &mut taken,
                    &self.opts.storage_options,
                    mark.as_ref().map(|(w, s)| (w.as_str(), *s)),
                    &deltas,
                )
                .await;
                // Exactly one version later: the new fragments are this flush's own, which
                // hold no tombstones, so later flushes need not read them.
                if r.is_ok() && t.runs.version().version == before + 1 {
                    let mut seen = self.tombstones_read.lock();
                    let ours: Vec<u64> = t
                        .runs
                        .fragments()
                        .iter()
                        .map(|f| f.id)
                        .filter(|id| !seen.contains(id))
                        .collect();
                    if ours.len() <= 1 {
                        seen.extend(ours);
                    }
                }
                r
            }
            Err(e) => Err(e),
        };
        match &r {
            Ok(()) => {
                if let Some(w) = self.wal.lock().as_mut() {
                    w.committed();
                }
            }
            Err(_) => prepend(&mut self.buffers.lock(), taken),
        }
        drop(t);
        if r.is_ok() {
            // After the WAL is truncated and without the tables lock: keep commits cheap and
            // mark rows few in the background.
            self.request_maintenance();
        }
        r
    }

    /// Runs deleted so far, reading only `runs` fragments not read before. With `fresh`,
    /// first checks for a newer version of `runs`; otherwise (flushes) at most once per
    /// [`TOMBSTONE_CHECK_INTERVAL`], so a flush costs no object-store listing.
    async fn tombstones(&self, runs: &mut Dataset, fresh: bool) -> Result<HashSet<String>> {
        let due = fresh || {
            let mut last = self.tombstones_checked.lock();
            let due = last.is_none_or(|at| at.elapsed() >= TOMBSTONE_CHECK_INTERVAL);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due {
            latest(runs).await?;
        }
        let new: Vec<lance::table::format::Fragment> = {
            let seen = self.tombstones_read.lock();
            runs.fragments()
                .iter()
                .filter(|f| !seen.contains(&f.id))
                .cloned()
                .collect()
        };
        if !new.is_empty() {
            let mut sc = runs.scan();
            sc.with_fragments(new.clone());
            sc.project(&["run_id", "fields"])?;
            sc.filter(&format!("fields = {}", fields::DELETED))?;
            let batches: Vec<RecordBatch> = sc.try_into_stream().await?.try_collect().await?;
            let mut dead = self.deleted.lock();
            for b in &batches {
                let id = col::<StringArray>(b, "run_id")?;
                for i in 0..b.num_rows() {
                    dead.insert(id.value(i).to_string());
                }
            }
            self.tombstones_read.lock().extend(new.iter().map(|f| f.id));
        }
        Ok(self.deleted.lock().clone())
    }

    /// Removes from `b` every write to a deleted run, so a writer that keeps going after its
    /// run was deleted (or a WAL replayed after it) does not bring the run back.
    async fn drop_deleted(&self, t: &mut Tables, b: &mut Buffers) -> Result<()> {
        let dead = self.tombstones(&mut t.runs, false).await?;
        // The run of each point's series: known to this handle, new in this flush, or stored.
        let mut run_of: HashMap<u64, String> = b
            .series
            .iter()
            .map(|s| (s.series_id, s.run_id.clone()))
            .collect();
        let mut unknown: HashSet<u64> = HashSet::new();
        {
            let known = self.known_series.lock();
            for sid in &b.series_ids {
                if run_of.contains_key(sid) {
                    continue;
                }
                match known.get(sid) {
                    Some(r) => {
                        run_of.insert(*sid, r.clone());
                    }
                    None => {
                        unknown.insert(*sid);
                    }
                }
            }
        }
        if !unknown.is_empty() {
            // Only after a WAL replay: series committed before the crash.
            latest(&mut t.series).await?;
            let list: Vec<u64> = unknown.iter().copied().collect();
            for chunk in list.chunks(1000) {
                let ids = chunk
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                for s in read_series_rows(
                    &scan(&t.series, Some(&format!("series_id IN ({ids})"))).await?,
                )? {
                    run_of.insert(s.series_id, s.run_id.clone());
                    self.known_series.lock().insert(s.series_id, s.run_id);
                }
            }
        }
        // A point whose series is nowhere belongs to a run deleted with its series.
        let gone = |sid: &u64| run_of.get(sid).is_none_or(|r| dead.contains(r));
        if dead.is_empty() && !b.series_ids.iter().any(gone) {
            return Ok(());
        }
        let mut dropped: HashSet<String> = HashSet::new();
        b.runs.retain(|r| {
            let keep = !dead.contains(&r.1.run_id);
            if !keep {
                dropped.insert(r.1.run_id.clone());
            }
            keep
        });
        b.params.retain(|p| !dead.contains(&p.1));
        b.series.retain(|s| !dead.contains(&s.run_id));
        if b.series_ids.iter().any(gone) {
            let keep: Vec<bool> = b.series_ids.iter().map(|s| !gone(s)).collect();
            let mut i = 0;
            b.steps.retain(|_| {
                i += 1;
                keep[i - 1]
            });
            let mut i = 0;
            b.epochs.retain(|_| {
                i += 1;
                keep[i - 1]
            });
            let mut i = 0;
            b.timestamps.retain(|_| {
                i += 1;
                keep[i - 1]
            });
            let mut i = 0;
            b.values.retain(|_| {
                i += 1;
                keep[i - 1]
            });
            b.series_ids.retain(|s| !gone(s));
        }
        b.relogged.retain(|s| !gone(s));
        let mut known = self.known_series.lock();
        known.retain(|_, r| !dead.contains(r));
        if !dropped.is_empty() {
            tracing::warn!(uri = %self.uri, runs = ?dropped, "kv0: dropped writes to deleted runs");
        }
        Ok(())
    }

    /// Fails when `run_id` is known to be deleted.
    fn check_run(&self, run_id: &str) -> Result<()> {
        if self.deleted.lock().contains(run_id) {
            return Err(Error::NotFound(format!("run {run_id} was deleted")));
        }
        Ok(())
    }

    /// Flushes from synchronous code: the background thread, `Drop` and a full buffer.
    fn flush_blocking(self: &Arc<Self>) -> Result<()> {
        let core = self.clone();
        rt::block_on(async move { core.flush().await })
    }

    async fn runs(&self) -> Result<Vec<RunInfo>> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        latest(&mut t.runs).await?;
        let rows: Vec<_> = read_runs(&scan(&t.runs, None).await?)?
            .into_iter()
            .filter(|r| !r.1.run_id.starts_with(MARK_PREFIX))
            .collect();
        let mut out: Vec<RunInfo> = merge_runs(rows).into_values().map(|(_, r)| r).collect();
        out.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then(b.run_id.cmp(&a.run_id))
        });
        Ok(out)
    }

    async fn run(&self, run_id: &str) -> Result<Option<RunInfo>> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        latest(&mut t.runs).await?;
        let rows = read_runs(&scan(&t.runs, Some(&format!("run_id = {}", quote(run_id)))).await?)?;
        Ok(merge_runs(rows).remove(run_id).map(|r| r.1))
    }

    async fn params(&self, run_id: &str) -> Result<Value> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        if self.tombstones(&mut t.runs, true).await?.contains(run_id) {
            return Ok(Value::Object(Default::default()));
        }
        latest(&mut t.params).await?;
        let batches = scan(&t.params, Some(&format!("run_id = {}", quote(run_id)))).await?;
        let mut current: BTreeMap<String, (u64, Value)> = BTreeMap::new();
        for b in &batches {
            let seq = col::<UInt64Array>(b, "seq")?;
            let key = col::<StringArray>(b, "key")?;
            let value = col::<StringArray>(b, "value")?;
            for i in 0..b.num_rows() {
                let s = seq.value(i);
                if current.get(key.value(i)).is_some_and(|(old, _)| *old >= s) {
                    continue;
                }
                let v: Value = serde_json::from_str(value.value(i))
                    .map_err(|e| Error::Corruption(e.to_string()))?;
                current.insert(key.value(i).to_string(), (s, v));
            }
        }
        Ok(json::unflatten(
            current.into_iter().map(|(k, (_, v))| (k, v)),
        ))
    }

    async fn series(&self, run_id: &str) -> Result<Vec<SeriesInfo>> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        if self.tombstones(&mut t.runs, true).await?.contains(run_id) {
            return Ok(Vec::new());
        }
        latest(&mut t.series).await?;
        let rows = read_series_rows(
            &scan(&t.series, Some(&format!("run_id = {}", quote(run_id)))).await?,
        )?;
        self.known_series
            .lock()
            .extend(rows.iter().map(|s| (s.series_id, s.run_id.clone())));
        Ok(rows)
    }

    async fn all_params(&self) -> Result<HashMap<String, Value>> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        let dead = self.tombstones(&mut t.runs, true).await?;
        latest(&mut t.params).await?;
        let batches = scan(&t.params, None).await?;
        let mut current: HashMap<String, BTreeMap<String, (u64, Value)>> = HashMap::new();
        for b in &batches {
            let seq = col::<UInt64Array>(b, "seq")?;
            let run = col::<StringArray>(b, "run_id")?;
            let key = col::<StringArray>(b, "key")?;
            let value = col::<StringArray>(b, "value")?;
            for i in 0..b.num_rows() {
                if run.value(i).starts_with(MARK_PREFIX) || dead.contains(run.value(i)) {
                    continue;
                }
                let m = current.entry(run.value(i).to_string()).or_default();
                let s = seq.value(i);
                if m.get(key.value(i)).is_some_and(|(old, _)| *old >= s) {
                    continue;
                }
                let v: Value = serde_json::from_str(value.value(i))
                    .map_err(|e| Error::Corruption(e.to_string()))?;
                m.insert(key.value(i).to_string(), (s, v));
            }
        }
        Ok(current
            .into_iter()
            .map(|(run, m)| {
                (
                    run,
                    json::unflatten(m.into_iter().map(|(k, (_, v))| (k, v))),
                )
            })
            .collect())
    }

    async fn all_series(&self) -> Result<Vec<SeriesInfo>> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        let dead = self.tombstones(&mut t.runs, true).await?;
        latest(&mut t.series).await?;
        let rows: Vec<SeriesInfo> = read_series_rows(&scan(&t.series, None).await?)?
            .into_iter()
            .filter(|s| !s.run_id.starts_with(MARK_PREFIX) && !dead.contains(&s.run_id))
            .collect();
        self.known_series
            .lock()
            .extend(rows.iter().map(|s| (s.series_id, s.run_id.clone())));
        Ok(rows)
    }

    /// Statistics of the series in `ids` (all when `None`): the deltas stored with the points,
    /// merged; series with a dirty delta are computed from their points.
    async fn series_stats(&self, ids: Option<&[u64]>) -> Result<HashMap<u64, SeriesStats>> {
        self.flush().await?;
        let ds = {
            let mut t = self.tables.lock().await;
            latest(&mut t.metrics).await?;
            t.metrics.clone()
        };
        let mut merged: HashMap<u64, stats::Delta> = HashMap::new();
        let chunks: Vec<Option<String>> = match ids {
            None => vec![None],
            Some([]) => return Ok(HashMap::new()),
            Some(ids) => ids
                .chunks(1000)
                .map(|c| {
                    Some(
                        c.iter()
                            .map(|i| (*i as i64).to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                    )
                })
                .collect(),
        };
        for c in chunks {
            let filter = match c {
                None => STATS_ROWS.to_string(),
                Some(list) => format!("{STATS_ROWS} AND step IN ({list})"),
            };
            let mut sc = ds.scan();
            sc.project(&["step", "stats"])?;
            sc.filter(&filter)?;
            let batches: Vec<RecordBatch> = sc.try_into_stream().await?.try_collect().await?;
            for b in &batches {
                let step = col::<Int64Array>(b, "step")?;
                let enc = col::<arrow_array::BinaryArray>(b, "stats")?;
                for i in 0..b.num_rows() {
                    let d = stats::Delta::decode(enc.value(i))?;
                    match merged.entry(step.value(i) as u64) {
                        std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().merge(&d),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(d);
                        }
                    }
                }
            }
        }
        let dirty: HashSet<u64> = merged
            .iter()
            .filter(|(_, d)| d.dirty)
            .map(|(id, _)| *id)
            .collect();
        let exact = exact_from_points(&ds, &dirty).await?;
        Ok(merged
            .into_iter()
            .filter_map(|(id, d)| {
                if d.dirty {
                    exact.get(&id).map(|s| (id, *s))
                } else {
                    Some((id, d.s))
                }
            })
            .collect())
    }

    /// Makes the statistics of `ids` (every series when `None`) be computed from their points
    /// until the next `optimize` stores them exactly.
    async fn recompute_stats(&self, ids: Option<Vec<u64>>) -> Result<usize> {
        let ids: HashSet<u64> = match ids {
            Some(ids) => ids.into_iter().collect(),
            None => self
                .all_series()
                .await?
                .into_iter()
                .map(|s| s.series_id)
                .collect(),
        };
        let n = ids.len();
        self.buffers.lock().relogged.extend(ids);
        self.flush().await?;
        Ok(n)
    }

    /// Records table `table` at its latest version; created when `create`, else `None` when
    /// it does not exist.
    async fn record_ds(&self, table: &str, create: bool) -> Result<Option<Dataset>> {
        let mut g = self.records.lock().await;
        if let Some(ds) = g.get_mut(table) {
            latest(ds).await?;
            return Ok(Some(ds.clone()));
        }
        let create = create && self.opts.create_if_missing;
        match records::open(
            &self.uri,
            table,
            &self.opts.storage_options,
            create,
            self.opts.version_retention,
        )
        .await
        {
            Ok(ds) => {
                g.insert(table.to_string(), ds.clone());
                Ok(Some(ds))
            }
            Err(Error::NotFound(_)) if !create => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Keeps `ds` as the newest known version of `table`.
    async fn keep_record_ds(&self, table: &str, ds: Dataset) {
        let mut g = self.records.lock().await;
        if g.get(table)
            .is_none_or(|old| old.version().version <= ds.version().version)
        {
            g.insert(table.to_string(), ds);
        }
    }

    /// Every records table, at its latest version.
    async fn all_record_ds(&self) -> Result<Vec<(String, Dataset)>> {
        let mut names: HashSet<String> = records::tables(&self.uri, &self.opts.storage_options)
            .await?
            .into_iter()
            .collect();
        names.extend(self.records.lock().await.keys().cloned());
        let mut names: Vec<String> = names.into_iter().collect();
        names.sort();
        let mut out = Vec::new();
        for n in names {
            if let Some(ds) = self.record_ds(&n, false).await? {
                out.push((n, ds));
            }
        }
        Ok(out)
    }

    async fn append_records(&self, table: &str, rows: Vec<NewRecord>) -> Result<u64> {
        let mut ds = self.record_ds(table, true).await?.ok_or_else(|| {
            Error::NotFound(format!(
                "records table {table:?} (create_if_missing is off)"
            ))
        })?;
        if rows.is_empty() {
            return Ok(ds.version().version);
        }
        let seqs: Vec<u64> = {
            let mut b = self.buffers.lock();
            rows.iter().map(|_| b.clock.next()).collect()
        };
        let batch = records::batch(table, &rows, &seqs, now_micros())?;
        let v = records::append(&mut ds, batch, &self.opts.storage_options).await?;
        self.keep_record_ds(table, ds).await;
        Ok(v)
    }

    async fn read_records(&self, table: &str, q: RecordQuery) -> Result<Vec<Record>> {
        let Some(ds) = self.record_ds(table, false).await? else {
            return match q.at {
                At::Latest => Ok(Vec::new()),
                _ => Err(Error::NotFound(format!("records table {table:?}"))),
            };
        };
        let ds = match &q.at {
            At::Latest => ds,
            At::Version(v) => ds.checkout_version(*v).await?,
            At::Tag(t) => ds.checkout_version(t.as_str()).await?,
        };
        records::read(&ds, &q).await
    }

    /// A records table that must exist.
    async fn existing_record_ds(&self, table: &str) -> Result<Dataset> {
        self.record_ds(table, false)
            .await?
            .ok_or_else(|| Error::NotFound(format!("records table {table:?}")))
    }

    async fn delete_runs(&self, run_ids: &[String]) -> Result<usize> {
        if run_ids.is_empty() {
            return Ok(0);
        }
        self.flush().await?;
        let mut t = self.tables.lock().await;
        let t = &mut *t;
        for ds in [&mut t.runs, &mut t.params, &mut t.series, &mut t.metrics] {
            latest(ds).await?;
        }
        let list = run_ids
            .iter()
            .map(|r| quote(r))
            .collect::<Vec<_>>()
            .join(", ");
        let pred = format!("run_id IN ({list})");
        let existing: HashSet<String> = merge_runs(read_runs(&scan(&t.runs, Some(&pred)).await?)?)
            .into_keys()
            .collect();
        // Tombstones first: from their commit on, readers hide the runs and writers (here or
        // elsewhere, and WAL replays) drop further writes to them.
        let rows: Vec<(u64, RunInfo, u32)> = {
            let mut b = self.buffers.lock();
            let now = now_micros();
            run_ids
                .iter()
                .map(|id| {
                    (
                        b.clock.next(),
                        RunInfo {
                            run_id: id.clone(),
                            name: None,
                            experiment: None,
                            description: None,
                            tags: Vec::new(),
                            status: RunStatus::Aborted,
                            archived: true,
                            created_at: now,
                            updated_at: now,
                            ended_at: None,
                            kind: None,
                            target: None,
                        },
                        fields::DELETED,
                    )
                })
                .collect()
        };
        append(&mut t.runs, runs_batch(&rows)?, &self.opts.storage_options).await?;
        self.deleted.lock().extend(run_ids.iter().cloned());
        let series: Vec<u64> = read_series_rows(&scan(&t.series, Some(&pred)).await?)?
            .into_iter()
            .map(|s| s.series_id)
            .collect();
        // Points and their statistics first: a crash part-way leaves runs whose points are
        // gone, never points without a series to name them. Running it again finishes it.
        for chunk in series.chunks(1000) {
            let ids = chunk
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let steps = chunk
                .iter()
                .map(|i| (*i as i64).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            t.metrics
                .delete(&format!(
                    "series_id IN ({ids}) OR ({STATS_ROWS} AND step IN ({steps}))"
                ))
                .await?;
        }
        t.series.delete(&pred).await?;
        t.params.delete(&pred).await?;
        for (name, mut ds) in self.all_record_ds().await? {
            if ds.count_rows(Some(pred.clone())).await? > 0 {
                ds.delete(&pred).await?;
                self.keep_record_ds(&name, ds).await;
            }
        }
        t.runs
            .delete(&format!(
                "{pred} AND (fields IS NULL OR fields <> {})",
                fields::DELETED
            ))
            .await?;
        {
            let mut known = self.known_series.lock();
            for s in &series {
                known.remove(s);
            }
        }
        Ok(existing.len())
    }

    async fn read_many(
        &self,
        series_ids: &[u64],
        range: StepRange,
        time: TimeRange,
    ) -> Result<HashMap<u64, MetricData>> {
        if series_ids.is_empty() {
            return Ok(HashMap::new());
        }
        self.flush().await?;
        let ds = {
            let mut t = self.tables.lock().await;
            latest(&mut t.metrics).await?;
            t.metrics.clone()
        };
        let ids: Vec<String> = series_ids.iter().map(u64::to_string).collect();
        let mut filter = if ids.len() == 1 {
            format!("series_id = {}", ids[0])
        } else {
            format!("series_id IN ({})", ids.join(", "))
        };
        if let Some(s) = range.start {
            filter.push_str(&format!(" AND step >= {s}"));
        }
        if let Some(s) = range.stop {
            filter.push_str(&format!(" AND step < {s}"));
        }
        // The time range applies after the later point of each step is chosen, so an overwritten
        // point is never returned because its replacement falls outside the range. To stay
        // bounded, first find the steps with a point in the range, then read only those steps
        // (with every later point of them) and filter after choosing.
        if time.start.is_some() || time.stop.is_some() {
            let mut f = filter.clone();
            if let Some(t) = time.start {
                f.push_str(&format!(" AND timestamp >= to_timestamp_micros({t})"));
            }
            if let Some(t) = time.stop {
                f.push_str(&format!(" AND timestamp < to_timestamp_micros({t})"));
            }
            let mut sc = ds.scan();
            sc.project(&["series_id", "step"])?;
            sc.filter(&f)?;
            let hits: Vec<RecordBatch> = sc.try_into_stream().await?.try_collect().await?;
            let mut span: HashMap<u64, (i64, i64)> = HashMap::new();
            for b in &hits {
                let id = col::<UInt64Array>(b, "series_id")?;
                let step = col::<Int64Array>(b, "step")?;
                for i in 0..b.num_rows() {
                    let e = span
                        .entry(id.value(i))
                        .or_insert((step.value(i), step.value(i)));
                    e.0 = e.0.min(step.value(i));
                    e.1 = e.1.max(step.value(i));
                }
            }
            if span.is_empty() {
                return Ok(HashMap::new());
            }
            filter = span
                .iter()
                .map(|(id, (lo, hi))| {
                    format!("(series_id = {id} AND step >= {lo} AND step <= {hi})")
                })
                .collect::<Vec<_>>()
                .join(" OR ");
        }
        // Chart reads keep Lance's default read-ahead: they are latency-bound on object stores
        // and return what they read.
        let batches: Vec<RecordBatch> = {
            let mut s = ds.scan();
            s.filter(&filter)?;
            s.try_into_stream().await?.try_collect().await?
        };
        // (step, arrival order, ...) per series, so the later of two points at one step wins.
        type Point = (i64, usize, Option<i64>, i64, f64);
        let mut rows: HashMap<u64, Vec<Point>> = HashMap::new();
        let mut n = 0usize;
        for b in &batches {
            let id = col::<UInt64Array>(b, "series_id")?;
            let step = col::<Int64Array>(b, "step")?;
            let epoch = col::<Int64Array>(b, "epoch")?;
            let ts = col::<TimestampMicrosecondArray>(b, "timestamp")?;
            let value = col::<Float64Array>(b, "value")?;
            for i in 0..b.num_rows() {
                rows.entry(id.value(i)).or_default().push((
                    step.value(i),
                    n,
                    (!epoch.is_null(i)).then(|| epoch.value(i)),
                    ts.value(i),
                    value.value(i),
                ));
                n += 1;
            }
        }
        Ok(rows
            .into_iter()
            .map(|(id, mut pts)| {
                pts.sort_unstable_by_key(|p| (p.0, p.1));
                let mut d = MetricData::default();
                for (i, p) in pts.iter().enumerate() {
                    if pts.get(i + 1).is_some_and(|q| q.0 == p.0) {
                        continue;
                    }
                    if time.start.is_some_and(|t| p.3 < t) || time.stop.is_some_and(|t| p.3 >= t) {
                        continue;
                    }
                    d.steps.push(p.0);
                    d.epochs.push(p.2);
                    d.timestamps.push(p.3);
                    d.values.push(p.4);
                }
                (id, d)
            })
            .collect())
    }

    /// Physically removes the data of deleted runs (see [`Repo::purge`]).
    async fn purge(&self, force: bool) -> Result<crate::PurgeReport> {
        use crate::purge;
        self.flush().await?;
        let mut t = self.tables.lock().await;
        let t = &mut *t;
        for ds in [&mut t.runs, &mut t.params, &mut t.series, &mut t.metrics] {
            latest(ds).await?;
        }
        let mut recs = self.all_record_ds().await?;
        // Every run with a tombstone, read in full.
        let dead: Vec<String> =
            read_runs(&scan(&t.runs, Some(&format!("fields = {}", fields::DELETED))).await?)?
                .into_iter()
                .map(|r| r.1.run_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
        let mut report = crate::PurgeReport {
            runs: dead.len(),
            ..Default::default()
        };
        let pred = if dead.is_empty() {
            None
        } else {
            let list = dead.iter().map(|r| quote(r)).collect::<Vec<_>>().join(", ");
            Some(format!("run_id IN ({list})"))
        };
        // Records tables are tagged one by one, so a tag on a table without rows of a deleted
        // run (an evaluation dataset, say) does not stand in the way.
        let mut pinned: Vec<(String, String, u64)> = Vec::new();
        {
            let mut tables: Vec<(String, &Dataset, bool)> = vec![
                ("runs".into(), &t.runs, true),
                ("params".into(), &t.params, true),
                ("series".into(), &t.series, true),
                ("metrics".into(), &t.metrics, false),
            ];
            for (name, ds) in &recs {
                tables.push((format!("records table {name:?}"), ds, true));
            }
            for (name, ds, by_run) in tables {
                for (tag, v) in purge::old_tags(ds).await? {
                    let holds = match (&pred, by_run) {
                        (_, false) => true,
                        (None, true) => false,
                        (Some(pred), true) => {
                            let old = ds.checkout_version(v).await?;
                            // In runs, the tombstones themselves hold no data.
                            let filter = if name == "runs" {
                                format!(
                                    "{pred} AND (fields IS NULL OR fields <> {})",
                                    fields::DELETED
                                )
                            } else {
                                pred.clone()
                            };
                            old.count_rows(Some(filter)).await? > 0
                        }
                    };
                    if holds {
                        pinned.push((name.clone(), tag, v));
                    }
                }
            }
        }
        if !pinned.is_empty() {
            if !force {
                let names = pinned
                    .iter()
                    .map(|(t, n, v)| format!("{n:?} on {t} (version {v})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::InvalidArgument(format!(
                    "purge: tag {names} keeps a version that still holds deleted data; delete the tag or purge with force"
                )));
            }
            for (table, tag, _) in &pinned {
                let ds: &Dataset = match table.as_str() {
                    "runs" => &t.runs,
                    "params" => &t.params,
                    "series" => &t.series,
                    "metrics" => &t.metrics,
                    other => recs
                        .iter()
                        .find(|(n, _)| format!("records table {n:?}") == other)
                        .map(|(_, d)| d)
                        .expect("listed"),
                };
                ds.tags().delete(tag).await?;
                report.tags_deleted.push(format!("{tag} ({table})"));
            }
        }
        // Only now change anything: a refused purge leaves the repository as it was. First the
        // rows a writer racing a delete committed after it.
        if let Some(pred) = &pred {
            let series: Vec<u64> = read_series_rows(&scan(&t.series, Some(pred)).await?)?
                .into_iter()
                .map(|s| s.series_id)
                .collect();
            for chunk in series.chunks(1000) {
                let ids = chunk
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let steps = chunk
                    .iter()
                    .map(|i| (*i as i64).to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                t.metrics
                    .delete(&format!(
                        "series_id IN ({ids}) OR ({STATS_ROWS} AND step IN ({steps}))"
                    ))
                    .await?;
            }
            t.series.delete(pred).await?;
            t.params.delete(pred).await?;
            for (_, ds) in recs.iter_mut() {
                if ds.count_rows(Some(pred.clone())).await? > 0 {
                    ds.delete(pred).await?;
                }
            }
            t.runs
                .delete(&format!(
                    "{pred} AND (fields IS NULL OR fields <> {})",
                    fields::DELETED
                ))
                .await?;
        }
        // Tags on versions that may still hold deleted data. Runs, params, series and records
        // are checked by run id; tracking never tags metrics, so a tag there counts as holding.
        for ds in [&mut t.runs, &mut t.params, &mut t.series, &mut t.metrics]
            .into_iter()
            .chain(recs.iter_mut().map(|(_, d)| d))
        {
            report.fragments_rewritten += purge::rewrite_deleted(ds).await?;
            let (v, b) = purge::remove_old_versions(ds).await?;
            report.versions_removed += v;
            report.bytes_removed += b;
        }
        Ok(report)
    }

    /// Removes tombstones older than `mark_ttl`, with any rows of their runs written after the
    /// delete (a writer racing it). After `mark_ttl` no write-ahead log can be replayed
    /// without `replay_stale_wal`, so nothing can bring those runs back.
    async fn expire_tombstones(&self, t: &mut Tables) -> Result<()> {
        latest(&mut t.runs).await?;
        let cutoff = now_nanos().saturating_sub(self.opts.mark_ttl.as_nanos() as u64);
        let old: Vec<String> = read_runs(
            &scan(
                &t.runs,
                Some(&format!("fields = {} AND seq < {cutoff}", fields::DELETED)),
            )
            .await?,
        )?
        .into_iter()
        .map(|r| r.1.run_id)
        .collect();
        if old.is_empty() {
            return Ok(());
        }
        let list = old.iter().map(|r| quote(r)).collect::<Vec<_>>().join(", ");
        let pred = format!("run_id IN ({list})");
        // The series first: the sorted rewrite then drops their points.
        for ds in [&mut t.series, &mut t.params, &mut t.runs] {
            latest(ds).await?;
            ds.delete(&pred).await?;
        }
        let mut dead = self.deleted.lock();
        for r in &old {
            dead.remove(r);
        }
        Ok(())
    }

    async fn optimize(&self) -> Result<()> {
        self.flush().await?;
        let mut t = self.tables.lock().await;
        let t = &mut *t;
        prune_marks(t, self.opts.mark_ttl).await?;
        self.expire_tombstones(t).await?;
        latest(&mut t.runs).await?;
        fold_runs(&mut t.runs, &self.opts.storage_options).await?;
        for ds in [&mut t.runs, &mut t.params, &mut t.series] {
            latest(ds).await?;
            compact_files(ds, CompactionOptions::default(), None).await?;
        }
        // Points sorted by (series, step), the shape every read has, with overwritten steps
        // dropped, points of deleted series dropped, and each series' statistics replaced by
        // one exact delta; this rewrite also merges the small fragments. The series are read
        // after the points' version: a series is always committed before its points.
        latest(&mut t.metrics).await?;
        let dead = self.tombstones(&mut t.runs, true).await?;
        latest(&mut t.series).await?;
        let live: HashSet<u64> = read_series_rows(&scan(&t.series, None).await?)?
            .into_iter()
            .filter(|s| !dead.contains(&s.run_id))
            .map(|s| s.series_id)
            .collect();
        // Records tables: drop superseded rows, then index item and run ids.
        for (name, mut ds) in self.all_record_ds().await? {
            let before = ds.version().version;
            records::fold(&mut ds, &self.opts.storage_options).await?;
            records::index(&mut ds).await?;
            if ds.version().version != before {
                self.keep_record_ds(&name, ds).await;
            }
        }
        if !sort_metrics(&mut t.metrics, &self.opts.storage_options, Arc::new(live)).await? {
            return Ok(()); // nothing new: no rewrite, no index work
        }
        if t.metrics.count_rows(None).await? > 0 {
            let has_index = t
                .metrics
                .load_indices()
                .await?
                .iter()
                .any(|i| i.name == "series_id_idx");
            if has_index {
                // Indexes only the new fragments.
                t.metrics
                    .optimize_indices(&lance_index::optimize::OptimizeOptions::default())
                    .await?;
            } else {
                t.metrics
                    .create_index(
                        &["series_id"],
                        IndexType::BTree,
                        Some("series_id_idx".into()),
                        &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
                        true,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

impl Repo {
    /// Opens (or creates) a repository at `location`: a local path or any URI Lance supports.
    pub async fn open(location: &str, opts: RepoOptions) -> Result<Repo> {
        let uri = storage::normalize_uri(location)?;
        {
            let (u, o) = (uri.clone(), opts.storage_options.clone());
            let moved = rt::spawn(async move { records::migrate_paths(&u, &o).await }).await?;
            if !moved.is_empty() {
                tracing::warn!(uri = %uri, tables = ?moved, "kv0: moved records tables to their current directory names");
            }
        }
        if opts.legacy_records == LegacyRecords::Error {
            let (u, o) = (uri.clone(), opts.storage_options.clone());
            let names = rt::spawn(async move { storage::list_dir(&u, &o).await }).await?;
            if names.iter().any(|n| n == records::LEGACY) {
                return Err(Error::InvalidArgument(format!(
                    "{uri} has records in the shared {} of an earlier build, which this build \
                     does not read. Open with legacy_records Allow, read them with \
                     legacy_records(), append them to tables, then drop_legacy_records()",
                    records::LEGACY
                )));
            }
        }
        let tables = {
            let (uri, opts) = (uri.clone(), opts.clone());
            rt::spawn(async move {
                let mut runs = open_table(&uri, "runs", schema::RUNS.clone(), &opts).await?;
                upgrade(&mut runs, &schema::RUNS_ADDED).await?;
                let mut metrics =
                    open_table(&uri, "metrics", schema::METRICS.clone(), &opts).await?;
                upgrade(&mut metrics, &schema::METRICS_ADDED).await?;
                Ok::<_, Error>(Tables {
                    runs,
                    params: open_table(&uri, "params", schema::PARAMS.clone(), &opts).await?,
                    series: open_table(&uri, "series", schema::SERIES.clone(), &opts).await?,
                    metrics,
                })
            })
            .await?
        };
        // Claim WALs of writers that are no longer running; replay what they did not commit.
        let mut buffers = Buffers::default();
        let mut known: HashMap<u64, String> = HashMap::new();
        let mut dirty: HashSet<u64> = HashSet::new();
        let mut log = None;
        let mut orphans = Vec::new();
        if opts.wal && !uri.starts_with("memory://") {
            let root = opts.wal_dir.clone().unwrap_or_else(wal::default_root);
            let (mut w, found) = Wal::open(&root, &format!("repo:{uri}"), opts.wal_sync)?;
            for o in &found {
                if o.ops.is_empty() {
                    continue;
                }
                let marks = {
                    let snapshot = Tables {
                        runs: tables.runs.clone(),
                        params: tables.params.clone(),
                        series: tables.series.clone(),
                        metrics: tables.metrics.clone(),
                    };
                    let id = o.writer_id.clone();
                    rt::spawn(async move { marks_of(&snapshot, &id).await }).await?
                };
                // Ambiguous only if an operation is old enough that its mark may have been
                // pruned (see the key-value layer).
                let cutoff = now_nanos().saturating_sub(opts.mark_ttl.as_nanos() as u64);
                if marks == [0; 4]
                    && o.ops.iter().any(|op| op.seq <= cutoff)
                    && !opts.replay_stale_wal
                {
                    return Err(Error::InvalidArgument(format!(
                        "the write-ahead log in {} is older than mark_ttl and its writer's marks have expired, so kv0 cannot tell which of its writes were committed. Open with replay_stale_wal=True to replay it (writes may be stored twice), or delete the directory to discard them",
                        o.dir.display()
                    )));
                }
                let mut keep: Vec<Op> = Vec::new();
                for op in &o.ops {
                    let mut probe = Buffers::default();
                    if let Some(table) = restore_record(&mut probe, op)? {
                        if op.seq > marks[table] {
                            keep.push(op.clone());
                        }
                    }
                }
                keep.sort_by_key(|op| op.seq);
                if keep.is_empty() {
                    continue;
                }
                // Every series with replayed points: its statistics are computed from its
                // points until the next optimize, as a replayed point may overwrite a step.
                for op in &keep {
                    if op.kind == W_POINT && op.key.len() == 8 {
                        dirty.insert(u64::from_le_bytes(op.key[..].try_into().expect("8 bytes")));
                    }
                }
                w.append(&keep)?;
                for op in &keep {
                    buffers.clock.observe(op.seq);
                    restore_record(&mut buffers, op)?;
                }
                known.extend(
                    buffers
                        .series
                        .iter()
                        .map(|s| (s.series_id, s.run_id.clone())),
                );
            }
            // A replayed point may overwrite a step committed before the crash.
            buffers.relogged.extend(dirty.iter().copied());
            if !buffers.is_empty() {
                w.rotate()?;
            }
            orphans = found;
            log = Some(w);
        }
        let batch_sync = log.is_some() && opts.wal_sync == WalSync::Batch;
        let core = Arc::new(Core {
            uri,
            closed: AtomicBool::new(false),
            buffers: parking_lot::Mutex::new(buffers),
            known_series: parking_lot::Mutex::new(known),
            tables: tokio::sync::Mutex::new(tables),
            wal: parking_lot::Mutex::new(log),
            abandoned: AtomicBool::new(false),
            flushing: AtomicBool::new(false),
            maintaining: AtomicBool::new(false),
            compactions: AtomicU64::new(0),
            last_prune: parking_lot::Mutex::new(None),
            maintained: tokio::sync::Notify::new(),
            panic_next_maintenance: AtomicBool::new(false),
            me: std::sync::OnceLock::new(),
            records: tokio::sync::Mutex::new(HashMap::new()),
            deleted: parking_lot::Mutex::new(HashSet::new()),
            tombstones_read: parking_lot::Mutex::new(HashSet::new()),
            tombstones_checked: parking_lot::Mutex::new(None),
            opts,
        });
        let _ = core.me.set(Arc::downgrade(&core));
        for o in orphans {
            o.remove();
        }
        if !core.buffers.lock().is_empty() {
            let c = core.clone();
            if let Err(e) = rt::spawn(async move { c.flush().await }).await {
                tracing::warn!(uri = %core.uri, error = %e, "kv0: committing replayed tracking writes failed");
            }
        }
        if core.opts.flush_interval.is_some() || batch_sync {
            let weak: Weak<dyn Background> = Arc::downgrade(&core) as Weak<dyn Background>;
            background::register(weak);
        }
        Ok(Repo {
            inner: Arc::new(RepoInner { core }),
        })
    }

    fn core(&self) -> Arc<Core> {
        self.inner.core.clone()
    }

    pub fn uri(&self) -> &str {
        &self.inner.core.uri
    }

    /// Creates a run in state [`RunStatus::Running`].
    pub fn create_run(&self, new: NewRun) -> Result<Run> {
        self.inner.core.check_open()?;
        let now = now_micros();
        let info = RunInfo {
            run_id: new
                .run_id
                .unwrap_or_else(|| uuid::Uuid::now_v7().simple().to_string()),
            name: new.name,
            experiment: new.experiment,
            description: new.description,
            tags: new.tags,
            status: RunStatus::Running,
            archived: false,
            created_at: new.created_at.unwrap_or(now),
            updated_at: now,
            ended_at: None,
            kind: new.kind,
            target: new.target,
        };
        self.inner.core.push_run(info.clone(), fields::ALL)?;
        Ok(Run {
            repo: self.clone(),
            info: Arc::new(parking_lot::Mutex::new(info)),
            steps: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    /// Opens an existing run for further writes, continuing each series after its last step.
    pub async fn resume_run(&self, run_id: &str) -> Result<Run> {
        let info = self
            .run(run_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("run {run_id}")))?;
        let series = self.series(run_id).await?;
        let ids: Vec<u64> = series.iter().map(|s| s.series_id).collect();
        let mut steps = HashMap::new();
        for (id, d) in self.read_many(&ids, StepRange::default()).await? {
            if let Some(last) = d.steps.last() {
                steps.insert(id, *last);
            }
        }
        Ok(Run {
            repo: self.clone(),
            info: Arc::new(parking_lot::Mutex::new(info)),
            steps: Arc::new(parking_lot::Mutex::new(steps)),
        })
    }

    /// Tables compacted by background maintenance so far (diagnostics and tests).
    #[doc(hidden)]
    pub fn background_compactions(&self) -> u64 {
        self.inner.core.compactions.load(Ordering::Relaxed)
    }

    /// Makes the next background maintenance panic (tests).
    #[doc(hidden)]
    pub fn inject_maintenance_panic(&self) {
        self.inner
            .core
            .panic_next_maintenance
            .store(true, Ordering::Release);
    }

    /// Waits until no background maintenance is running (tests).
    #[doc(hidden)]
    pub async fn wait_for_maintenance(&self) {
        self.inner.core.wait_for_maintenance().await
    }

    /// Commits buffered writes.
    pub async fn flush(&self) -> Result<()> {
        let core = self.core();
        rt::spawn(async move { core.flush().await }).await
    }

    /// Every run, newest first.
    pub async fn runs(&self) -> Result<Vec<RunInfo>> {
        let core = self.core();
        rt::spawn(async move { core.runs().await }).await
    }

    /// One run's current metadata.
    pub async fn run(&self, run_id: &str) -> Result<Option<RunInfo>> {
        let (core, id) = (self.core(), run_id.to_string());
        rt::spawn(async move { core.run(&id).await }).await
    }

    /// A run's parameters as a nested JSON object.
    pub async fn params(&self, run_id: &str) -> Result<Value> {
        let (core, id) = (self.core(), run_id.to_string());
        rt::spawn(async move { core.params(&id).await }).await
    }

    /// The metric series of a run.
    pub async fn series(&self, run_id: &str) -> Result<Vec<SeriesInfo>> {
        let (core, id) = (self.core(), run_id.to_string());
        rt::spawn(async move { core.series(&id).await }).await
    }

    /// Every run's parameters, by run id, from one scan.
    pub async fn all_params(&self) -> Result<HashMap<String, Value>> {
        let core = self.core();
        rt::spawn(async move { core.all_params().await }).await
    }

    /// Every metric series of every run, from one scan.
    pub async fn all_series(&self) -> Result<Vec<SeriesInfo>> {
        let core = self.core();
        rt::spawn(async move { core.all_series().await }).await
    }

    /// Changes a run's metadata without reading its points: reads the current record, applies
    /// `f` and records only the fields `f` changed (other writers' changes to other fields are
    /// kept). Returns the record as written.
    pub async fn update_run(&self, run_id: &str, f: impl FnOnce(&mut RunInfo)) -> Result<RunInfo> {
        let cur = self
            .run(run_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("run {run_id}")))?;
        let mut next = cur.clone();
        f(&mut next);
        next.run_id = cur.run_id.clone();
        next.updated_at = now_micros();
        self.inner.core.check_open()?;
        self.inner
            .core
            .push_run(next.clone(), changed(&cur, &next))?;
        Ok(next)
    }

    /// The newest committed version of each table, `(runs, params, series, metrics)`, read from
    /// storage without scanning: a cheap check for other writers' commits.
    pub async fn latest_versions(&self) -> Result<(u64, u64, u64, u64)> {
        let core = self.core();
        rt::spawn(async move {
            let t = {
                let t = core.tables.lock().await;
                [
                    t.runs.clone(),
                    t.params.clone(),
                    t.series.clone(),
                    t.metrics.clone(),
                ]
            };
            Ok::<_, Error>((
                t[0].latest_version_id().await?,
                t[1].latest_version_id().await?,
                t[2].latest_version_id().await?,
                t[3].latest_version_id().await?,
            ))
        })
        .await
    }

    /// Deletes runs and everything recorded for them from all four tables. Space is reclaimed
    /// by [`Repo::cleanup_old_versions`]. Returns the number of runs that existed.
    pub async fn delete_runs(&self, run_ids: &[String]) -> Result<usize> {
        self.inner.core.check_open()?;
        let (core, ids) = (self.core(), run_ids.to_vec());
        rt::spawn(async move { core.delete_runs(&ids).await }).await
    }

    /// Physically removes the data of deleted runs: rows written after the delete by a racing
    /// writer are deleted, every fragment holding deleted rows is rewritten without them, and
    /// every version but the latest and tagged ones is removed. Fails, naming the tag, when a
    /// tag keeps a version that still holds data of a deleted run; with `force` such tags are
    /// deleted instead. Readers holding an older version must refresh (see the operations
    /// guide); concurrent writes are kept.
    pub async fn purge(&self, force: bool) -> Result<crate::PurgeReport> {
        self.inner.core.check_open()?;
        let core = self.core();
        rt::spawn(async move { core.purge(force).await }).await
    }

    /// Stored totals (count, first and last step, last value, minimum, maximum, ...) of the
    /// series in `ids`, without reading points.
    pub async fn series_stats(&self, ids: &[u64]) -> Result<HashMap<u64, SeriesStats>> {
        let (core, ids) = (self.core(), ids.to_vec());
        rt::spawn(async move { core.series_stats(Some(&ids)).await }).await
    }

    /// Stored totals of every series.
    pub async fn all_series_stats(&self) -> Result<HashMap<u64, SeriesStats>> {
        let core = self.core();
        rt::spawn(async move { core.series_stats(None).await }).await
    }

    /// Recomputes stored totals from points, for the series in `ids` or all series (`None`):
    /// after concurrent writers to one series, or for repositories written before totals were
    /// kept. Returns the number of series recomputed.
    pub async fn recompute_series_stats(&self, ids: Option<Vec<u64>>) -> Result<usize> {
        self.inner.core.check_open()?;
        let core = self.core();
        rt::spawn(async move { core.recompute_stats(ids).await }).await
    }

    /// Appends rows to records table `table` (created on first use) in one commit. A row whose
    /// `(run_id, item_id)` is already present supersedes the older one. Returns the table's
    /// version that holds them, which [`At::Version`] reads back exactly.
    pub async fn append_records(&self, table: &str, rows: Vec<NewRecord>) -> Result<u64> {
        self.inner.core.check_open()?;
        let (core, table) = (self.core(), table.to_string());
        rt::spawn(async move { core.append_records(&table, rows).await }).await
    }

    /// The newest row per `(run_id, item_id)` of records table `table` that matches `q`, in
    /// append order; empty when the table does not exist.
    pub async fn records(&self, table: &str, q: RecordQuery) -> Result<Vec<Record>> {
        let (core, table) = (self.core(), table.to_string());
        rt::spawn(async move { core.read_records(&table, q).await }).await
    }

    /// Every row of the shared `records.lance` of earlier builds (of table `table`, or all),
    /// in append order, for a one-time migration; empty when there is none.
    pub async fn legacy_records(&self, table: Option<&str>) -> Result<Vec<Record>> {
        let (uri, o, table) = (
            self.inner.core.uri.clone(),
            self.inner.core.opts.storage_options.clone(),
            table.map(str::to_string),
        );
        rt::spawn(async move { records::read_legacy(&uri, &o, table.as_deref()).await }).await
    }

    /// Whether the shared `records.lance` of earlier builds exists; one directory listing, no
    /// rows read.
    pub async fn has_legacy_records(&self) -> Result<bool> {
        let (uri, o) = (
            self.inner.core.uri.clone(),
            self.inner.core.opts.storage_options.clone(),
        );
        rt::spawn(async move {
            Ok(storage::list_dir(&uri, &o)
                .await?
                .iter()
                .any(|n| n == records::LEGACY))
        })
        .await
    }

    /// Deletes the shared `records.lance` of earlier builds, after a migration. Returns
    /// whether there was one.
    pub async fn drop_legacy_records(&self) -> Result<bool> {
        let (uri, o) = (
            self.inner.core.uri.clone(),
            self.inner.core.opts.storage_options.clone(),
        );
        rt::spawn(async move { records::drop_legacy(&uri, &o).await }).await
    }

    /// Names of the records tables.
    pub async fn record_tables(&self) -> Result<Vec<String>> {
        let core = self.core();
        rt::spawn(async move {
            Ok(core
                .all_record_ds()
                .await?
                .into_iter()
                .map(|(n, _)| n)
                .collect())
        })
        .await
    }

    /// The latest committed version of records table `table`.
    pub async fn records_version(&self, table: &str) -> Result<u64> {
        let (core, table) = (self.core(), table.to_string());
        rt::spawn(async move { Ok(core.existing_record_ds(&table).await?.version().version) }).await
    }

    /// Names a version of records table `table` (the latest when `None`), so it can be read
    /// back with [`At::Tag`] and is kept by version cleanup. The tag keeps this table only.
    pub async fn tag_records(&self, table: &str, tag: &str, version: Option<u64>) -> Result<()> {
        let (core, table, tag) = (self.core(), table.to_string(), tag.to_string());
        rt::spawn(async move {
            let ds = core.existing_record_ds(&table).await?;
            let v = version.unwrap_or(ds.version().version);
            storage::tag_version(&ds, &tag, v).await
        })
        .await
    }

    /// Tags of records table `table` and the versions they name, sorted by name.
    pub async fn record_tags(&self, table: &str) -> Result<Vec<(String, u64)>> {
        let (core, table) = (self.core(), table.to_string());
        rt::spawn(async move {
            let Some(ds) = core.record_ds(&table, false).await? else {
                return Ok(Vec::new());
            };
            let mut out: Vec<(String, u64)> = ds
                .tags()
                .list()
                .await?
                .into_iter()
                .map(|(name, c)| (name, c.version))
                .collect();
            out.sort();
            Ok(out)
        })
        .await
    }

    /// Removes a tag of records table `table` (the version it named becomes eligible for
    /// cleanup).
    pub async fn delete_record_tag(&self, table: &str, tag: &str) -> Result<()> {
        let (core, table, tag) = (self.core(), table.to_string(), tag.to_string());
        rt::spawn(async move {
            let ds = core.existing_record_ds(&table).await?;
            Ok(ds.tags().delete(&tag).await?)
        })
        .await
    }

    /// Points of one series in a step range.
    pub async fn read_metric(
        &self,
        run_id: &str,
        name: &str,
        context: &Value,
        range: StepRange,
    ) -> Result<MetricData> {
        let id = series_id(run_id, name, context);
        let (core, run) = (self.core(), run_id.to_string());
        let deleted = rt::spawn(async move {
            let mut t = core.tables.lock().await;
            Ok::<_, Error>(core.tombstones(&mut t.runs, true).await?.contains(&run))
        })
        .await?;
        if deleted {
            return Ok(MetricData::default());
        }
        Ok(self
            .read_many(&[id], range)
            .await?
            .remove(&id)
            .unwrap_or_default())
    }

    /// Points of several series in a step range, keyed by series id.
    pub async fn read_many(
        &self,
        series_ids: &[u64],
        range: StepRange,
    ) -> Result<HashMap<u64, MetricData>> {
        self.read_many_in(series_ids, range, TimeRange::default())
            .await
    }

    /// Points of several series in a step range and a time range, keyed by series id.
    pub async fn read_many_in(
        &self,
        series_ids: &[u64],
        range: StepRange,
        time: TimeRange,
    ) -> Result<HashMap<u64, MetricData>> {
        let (core, ids) = (self.core(), series_ids.to_vec());
        rt::spawn(async move { core.read_many(&ids, range, time).await }).await
    }

    /// Compacts small files in every table and (re)builds the metrics index on `series_id`.
    pub async fn optimize(&self) -> Result<()> {
        let core = self.core();
        rt::spawn(async move { core.optimize().await }).await
    }

    /// Deletes versions of every table older than `older_than` (the latest is always kept).
    /// Returns the bytes removed.
    pub async fn cleanup_old_versions(&self, older_than: Duration) -> Result<u64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| Error::InvalidArgument(e.to_string()))?;
        let core = self.core();
        rt::spawn(async move {
            let mut t = core.tables.lock().await;
            let t = &mut *t;
            let mut removed = 0;
            let mut recs = core.all_record_ds().await?;
            for ds in [&mut t.runs, &mut t.params, &mut t.series, &mut t.metrics]
                .into_iter()
                .chain(recs.iter_mut().map(|(_, d)| d))
            {
                latest(ds).await?;
                // Tagged versions are kept, and do not stop the cleanup of others.
                let policy = lance::dataset::cleanup::CleanupPolicyBuilder::default()
                    .before_timestamp(cutoff)
                    .error_if_tagged_old_versions(false)
                    .build();
                removed += lance::dataset::cleanup::cleanup_old_versions(ds, policy)
                    .await?
                    .bytes_removed;
            }
            Ok(removed)
        })
        .await
    }

    /// Stored points and WAL mark rows of the metrics table, statistics rows not included
    /// (diagnostics and tests).
    #[doc(hidden)]
    pub async fn metric_rows(&self) -> Result<usize> {
        let core = self.core();
        rt::spawn(async move {
            core.flush().await?;
            let mut t = core.tables.lock().await;
            latest(&mut t.metrics).await?;
            Ok(t.metrics
                .count_rows(Some("stats IS NULL".to_string()))
                .await?)
        })
        .await
    }

    /// Table versions, for diagnostics: `(runs, params, series, metrics)`.
    pub async fn versions(&self) -> (u64, u64, u64, u64) {
        let core = self.core();
        rt::spawn(async move {
            let t = core.tables.lock().await;
            (
                t.runs.version().version,
                t.params.version().version,
                t.series.version().version,
                t.metrics.version().version,
            )
        })
        .await
    }

    /// Flushes and closes the handle.
    pub async fn close(&self) -> Result<()> {
        if self.inner.core.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let r = self.flush().await;
        self.inner.core.closed.store(true, Ordering::Release);
        // No maintenance starts after `closed`; let a running one finish so nothing writes to
        // the tables once `close` returns.
        self.inner.core.wait_for_maintenance().await;
        if r.is_ok() {
            if let Some(w) = self.inner.core.wal.lock().take() {
                w.remove();
            }
        }
        r
    }

    /// Drops the handle without flushing, as a crash would; buffered writes stay in the WAL.
    #[doc(hidden)]
    pub fn abandon(self) {
        let core = &self.inner.core;
        core.abandoned.store(true, Ordering::Release);
        core.closed.store(true, Ordering::Release);
        core.wal.lock().take();
    }
}

impl Background for Core {
    fn tick(&self, _now: Instant) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        if let Some(w) = self.wal.lock().as_mut() {
            w.sync_if_due();
        }
        let Some(every) = self.opts.flush_interval else {
            return false;
        };
        let b = self.buffers.lock();
        !b.is_empty() && b.last_flush.is_none_or(|t| t.elapsed() >= every)
    }

    fn flush_now(self: Arc<Self>) {
        if let Err(e) = self.flush_blocking() {
            tracing::warn!(uri = %self.uri, error = %e, "kv0: tracking flush failed");
        }
    }

    fn flushing(&self) -> &AtomicBool {
        &self.flushing
    }
}

impl Drop for RepoInner {
    fn drop(&mut self) {
        let core = &self.core;
        if core.closed.load(Ordering::Acquire) || core.abandoned.load(Ordering::Acquire) {
            return;
        }
        match core.flush_blocking() {
            Ok(()) => {
                if let Some(w) = core.wal.lock().take() {
                    w.remove();
                }
            }
            // The writes stay in the WAL and are replayed by the next open on this machine.
            Err(e) => {
                tracing::warn!(uri = %core.uri, error = %e, "kv0: tracking flush on drop failed")
            }
        }
    }
}

/// A run being written. Cheap to clone.
#[derive(Clone)]
pub struct Run {
    repo: Repo,
    info: Arc<parking_lot::Mutex<RunInfo>>,
    steps: Arc<parking_lot::Mutex<HashMap<u64, i64>>>,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run").field("run_id", &self.id()).finish()
    }
}

impl Run {
    pub fn id(&self) -> String {
        self.info.lock().run_id.clone()
    }

    /// The run's metadata as this handle knows it.
    pub fn info(&self) -> RunInfo {
        self.info.lock().clone()
    }

    /// Buffers one metric point.
    pub fn track(&self, name: &str, value: f64, t: Track) -> Result<()> {
        self.repo.inner.core.check_open()?;
        let run_id = self.id();
        self.repo.inner.core.check_run(&run_id)?;
        let context = t.context.unwrap_or(Value::Object(Default::default()));
        if !context.is_object() {
            return Err(Error::InvalidArgument(
                "a context must be a JSON object".into(),
            ));
        }
        let sid = series_id(&run_id, name, &context);
        let relog;
        let step = {
            let mut steps = self.steps.lock();
            let step = match (t.step, steps.get(&sid)) {
                (Some(s), _) => s,
                (None, None) => 0,
                (None, Some(last)) => last.checked_add(1).ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "series {name:?} is at the largest step; pass `step` explicitly"
                    ))
                })?,
            };
            // A step at or below one already written may overwrite it: the statistics of the
            // series are then computed from its points.
            relog = steps.get(&sid).is_some_and(|last| step <= *last);
            let last = steps.entry(sid).or_insert(step);
            *last = (*last).max(step);
            step
        };
        let new_series = self
            .repo
            .inner
            .core
            .known_series
            .lock()
            .insert(sid, run_id.clone())
            .is_none();
        let core = &self.repo.inner.core;
        let full = {
            let mut b = core.buffers.lock();
            let ts = t.timestamp.unwrap_or_else(now_micros);
            let series = new_series.then(|| SeriesInfo {
                series_id: sid,
                run_id,
                name: name.to_string(),
                context,
                created_at: now_micros(),
            });
            let mut records = Vec::with_capacity(2);
            if let Some(s) = &series {
                let seq = b.clock.next();
                records.push(series_record(seq, s));
            }
            let seq = b.clock.next();
            records.push(point_record(seq, sid, step, t.epoch, ts, value));
            if let Err(e) = core.log(&records) {
                if new_series {
                    core.known_series.lock().remove(&sid);
                }
                return Err(e);
            }
            if let Some(s) = series {
                b.series.push(s);
            }
            if relog {
                b.relogged.insert(sid);
            }
            b.series_ids.push(sid);
            b.steps.push(step);
            b.epochs.push(t.epoch);
            b.timestamps.push(ts);
            b.values.push(value);
            b.series_ids.len() >= core.opts.flush_rows
        };
        if full {
            self.repo.inner.core.flush_blocking()?;
        }
        Ok(())
    }

    /// Sets parameters from a JSON object, merged into the existing ones by dotted key.
    pub fn set_params(&self, params: &Value) -> Result<()> {
        self.repo.inner.core.check_open()?;
        if !params.is_object() {
            return Err(Error::InvalidArgument(
                "parameters must be a JSON object".into(),
            ));
        }
        let mut flat = Vec::new();
        json::flatten("", params, &mut flat);
        self.push_params(flat)
    }

    /// Sets one parameter; `key` may be dotted (`"optimizer.lr"`).
    pub fn set_param(&self, key: &str, value: Value) -> Result<()> {
        self.repo.inner.core.check_open()?;
        let mut flat = Vec::new();
        json::flatten(key, &value, &mut flat);
        self.push_params(flat)
    }

    fn push_params(&self, flat: Vec<(String, Value)>) -> Result<()> {
        let run_id = self.id();
        let core = &self.repo.inner.core;
        core.check_run(&run_id)?;
        let mut b = core.buffers.lock();
        let rows: Vec<_> = flat
            .into_iter()
            .map(|(k, v)| (b.clock.next(), run_id.clone(), k, v))
            .collect();
        let records: Vec<Op> = rows
            .iter()
            .map(|(seq, r, k, v)| param_record(*seq, r, k, v))
            .collect();
        core.log(&records)?;
        b.params.extend(rows);
        Ok(())
    }

    /// Updates metadata with `f` and records only the fields it changed, so a concurrent
    /// change of other fields (from another process) is kept.
    pub fn update(&self, f: impl FnOnce(&mut RunInfo)) -> Result<()> {
        self.repo.inner.core.check_open()?;
        // Change a copy; keep it only once the change is recorded.
        let mut i = self.info.lock();
        self.repo.inner.core.check_run(&i.run_id)?;
        let mut next = i.clone();
        f(&mut next);
        next.run_id = i.run_id.clone();
        next.updated_at = now_micros();
        self.repo
            .inner
            .core
            .push_run(next.clone(), changed(&i, &next))?;
        *i = next;
        Ok(())
    }

    /// Marks the run ended with `status` now and flushes.
    pub async fn finish(&self, status: RunStatus) -> Result<()> {
        self.finish_at(status, None).await
    }

    /// Marks the run ended with `status` at `ended_at` (microseconds since the Unix epoch, UTC;
    /// now when `None`) and flushes.
    pub async fn finish_at(&self, status: RunStatus, ended_at: Option<i64>) -> Result<()> {
        self.update(|i| {
            i.status = status;
            i.ended_at = Some(ended_at.unwrap_or_else(now_micros));
        })?;
        self.repo.flush().await?;
        // The flush finds out when the run was deleted elsewhere, and drops its writes.
        self.repo.inner.core.check_run(&self.id())
    }
}

const _: () = assert!(fields::ALL == (1 << FIELD_COUNT) - 1);

#[cfg(test)]
mod fold_tests {
    use super::*;

    /// A small deterministic generator, so failures reproduce.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn row(rng: &mut Rng, run: &str, seq: u64, mask: u32) -> (u64, RunInfo, u32) {
        let v = rng.below(1000) as i64;
        (
            seq,
            RunInfo {
                run_id: run.into(),
                name: Some(format!("n{v}")),
                experiment: Some(format!("e{}", v + 1)),
                description: Some(format!("d{}", v + 2)),
                tags: vec![format!("t{}", v + 3)],
                status: [RunStatus::Running, RunStatus::Finished, RunStatus::Failed]
                    [rng.below(3) as usize],
                archived: rng.below(2) == 1,
                created_at: v + 4,
                updated_at: v,
                ended_at: Some(v + 5),
                kind: Some(format!("k{}", v + 6)),
                target: Some(serde_json::json!({ "v": v + 7 })),
            },
            mask,
        )
    }

    #[test]
    fn repeated_folds_with_late_and_skewed_rows_merge_like_no_folds() {
        // Several optimize rounds, each folding what is stored so far, with rows committed in
        // between whose sequence numbers may be older than anything stored (buffered writes,
        // clock skew, replayed WALs).
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        for case in 0..3000 {
            let mut seqs: Vec<u64> = (1..=60).collect();
            for i in (1..seqs.len()).rev() {
                seqs.swap(i, rng.below(i as u64 + 1) as usize);
            }
            let mut seqs = seqs.into_iter();
            let mut all: Vec<(u64, RunInfo, u32)> = Vec::new();
            let mut stored: Vec<(u64, RunInfo, u32)> = Vec::new();
            for _round in 0..4 {
                for _ in 0..1 + rng.below(6) {
                    let run = ["a", "b", "c"][rng.below(3) as usize];
                    let mask = if rng.below(5) == 0 {
                        fields::ALL
                    } else {
                        (rng.below(fields::ALL as u64) as u32 + 1) & fields::ALL
                    };
                    let r = row(&mut rng, run, seqs.next().expect("enough"), mask);
                    all.push(r.clone());
                    stored.push(r);
                }
                stored = fold_run_rows(stored);
            }
            let strip = |m: HashMap<String, (u64, RunInfo)>| {
                let mut v: Vec<RunInfo> = m.into_values().map(|x| x.1).collect();
                v.sort_by(|a, b| a.run_id.cmp(&b.run_id));
                v
            };
            assert_eq!(
                strip(merge_runs(stored)),
                strip(merge_runs(all)),
                "case {case}"
            );
        }
    }

    #[test]
    fn folding_never_changes_what_rows_merge_to_even_with_rows_added_later() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for case in 0..2000 {
            let runs = ["a", "b"];
            let mut seqs: Vec<u64> = (1..=40).collect();
            // Distinct sequence numbers in random order.
            for i in (1..seqs.len()).rev() {
                seqs.swap(i, rng.below(i as u64 + 1) as usize);
            }
            let n = 2 + rng.below(12) as usize;
            let late = rng.below(5) as usize;
            let mut rows = Vec::new();
            for (i, seq) in seqs.iter().take(n + late).enumerate() {
                let run = runs[rng.below(2) as usize];
                // Creation rows set every field; others a random subset.
                let mask = if rng.below(4) == 0 {
                    fields::ALL
                } else {
                    (rng.below(fields::ALL as u64) as u32 + 1) & fields::ALL
                };
                rows.push((i < n, row(&mut rng, run, *seq, mask)));
            }
            let (early, later): (Vec<_>, Vec<_>) = rows.into_iter().partition(|r| r.0);
            let early: Vec<_> = early.into_iter().map(|r| r.1).collect();
            let later: Vec<_> = later.into_iter().map(|r| r.1).collect();
            let unfolded = merge_runs(early.iter().cloned().chain(later.iter().cloned()).collect());
            let folded_rows = fold_run_rows(early.clone());
            assert!(folded_rows.len() <= early.len(), "case {case}");
            let folded = merge_runs(folded_rows.into_iter().chain(later).collect());
            let strip = |m: HashMap<String, (u64, RunInfo)>| {
                let mut v: Vec<RunInfo> = m.into_values().map(|x| x.1).collect();
                v.sort_by(|a, b| a.run_id.cmp(&b.run_id));
                v
            };
            assert_eq!(strip(folded), strip(unfolded), "case {case}");
        }
    }
}
