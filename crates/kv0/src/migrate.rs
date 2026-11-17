//! Converting locations written with the Lance engine (kv0 0.1.0-beta.2 and earlier) to Delta
//! Lake: `kv0 migrate <from> <to>`, or [`migrate`] from code.
//!
//! What it does:
//!
//! - **Key-value database:** for every tagged version (oldest first) and then the latest one,
//!   the rows that reproduce that version (live values, and delete markers younger than
//!   `tombstone_grace`) are written as one commit replacing the previous one, and the tags are
//!   created on the resulting Delta versions. Writers' WAL marks become `txn` actions.
//! - **Tracking repository:** `runs`, `params`, `series`, `metrics`, every records table and
//!   the shared `records.lance` of older builds (as `_legacy_records`) are copied the same way,
//!   table by table, tags included. WAL mark rows are dropped and each writer's mark becomes a
//!   `txn` action on its table; statistics rows stay. `optimize` runs at the end.
//! - Every copied version is checked: row counts and a checksum of the rows (of the live
//!   entries, for a database) must match between the source and the new version.
//! - `<to>/_kv0/migration.json` records the source, the version map of every table and tag,
//!   row counts and checksums. Then the format marker `_kv0/format.json` is written: until
//!   then, the target does not count as a Delta location.
//!
//! Version numbers restart: the Delta versions are the commits the migration made. Untagged
//! history is not carried over.
//!
//! Writers must be stopped first. The migration refuses a source that was committed to less
//! than `quiet_period` ago, and fails if the source changes while it runs. A failed or
//! interrupted migration is restarted with `resume`: tables already verified are kept, the
//! rest is written again.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

use crate::error::{Error, Result};
use crate::kv::view::View;
use crate::storage::format::{self, Kind};
use crate::storage::oplog::{Log, WriterMark};
use crate::storage::table::{Root, Scan, TableRef};
use crate::storage::types::seq_from_i64;
use crate::storage::{self, StorageOptions};
use crate::tracking::{records, schema};

/// Options of [`migrate`].
#[derive(Clone, Debug)]
pub struct MigrateOptions {
    /// Object-store options of the source (and of the target, unless
    /// `target_storage_options` is set).
    pub storage_options: StorageOptions,
    /// Object-store options of the target, when it is on another store.
    pub target_storage_options: Option<StorageOptions>,
    /// Carry over tags, each with the data of its version. Default `true`.
    pub keep_tags: bool,
    /// Only report what would be migrated. Default `false`.
    pub dry_run: bool,
    /// Continue a migration that stopped: tables already verified are kept. Default `false`.
    pub resume: bool,
    /// Delete the Lance data once everything is verified. Default `false`.
    pub remove_source: bool,
    /// Allow a key-value database to be migrated in place (both formats then share the
    /// directory until the source is removed). Repositories may always be. Default `false`.
    pub force_in_place: bool,
    /// Run `optimize` on a migrated repository. Default `true`.
    pub optimize: bool,
    /// Refuse a source committed to more recently than this (a writer is probably still
    /// running). Default 10 s.
    pub quiet_period: Duration,
    /// Delete markers younger than this are kept (a database's slower writers). Default 1 h.
    pub tombstone_grace: Duration,
    /// `version_retention` of the new tables. Default 7 days.
    pub version_retention: Option<Duration>,
    /// How long the carried-over writer marks are kept. Default 30 days.
    pub mark_ttl: Duration,
}

impl Default for MigrateOptions {
    fn default() -> Self {
        MigrateOptions {
            storage_options: StorageOptions::new(),
            target_storage_options: None,
            keep_tags: true,
            dry_run: false,
            resume: false,
            remove_source: false,
            force_in_place: false,
            optimize: true,
            quiet_period: Duration::from_secs(10),
            tombstone_grace: Duration::from_secs(3600),
            version_retention: Some(Duration::from_secs(7 * 24 * 3600)),
            mark_ttl: Duration::from_secs(30 * 24 * 3600),
        }
    }
}

/// What a migration did: the content of `<to>/_kv0/migration.json`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Report {
    /// `database` or `repository`.
    pub kind: String,
    pub source: String,
    pub target: String,
    pub in_place: bool,
    /// `running`, `complete` or `dry-run`.
    pub status: String,
    pub created_by: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    /// One entry per table (a database is one table, named `.`).
    pub tables: Vec<TableReport>,
    /// Writer marks of tracking metrics whose writer could not be named (it wrote points but
    /// nothing else); their writers' WALs must not be replayed.
    pub unmapped_marks: usize,
    /// Whether `optimize` ran on the migrated repository (it is not needed for correctness;
    /// when it fails, the migration still stands and `optimize` can run later).
    #[serde(default)]
    pub optimized: bool,
    pub source_removed: bool,
}

/// One table of a [`Report`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TableReport {
    /// The target table (`.` for a database; `runs`, `records/<name>`, `_legacy_records`, …).
    pub name: String,
    /// The source table (`records` for the shared legacy records).
    pub source_name: String,
    pub source_version: u64,
    pub target_version: u64,
    /// Rows of the latest version: live entries of a database, rows without WAL marks of a
    /// table.
    pub rows: u64,
    pub checksum: String,
    pub tags: Vec<TagReport>,
    /// Writer marks carried over.
    pub marks: usize,
    /// Copied and verified.
    pub done: bool,
}

/// A tag of a [`TableReport`], with the version it names before and after.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TagReport {
    pub name: String,
    pub source_version: u64,
    pub target_version: u64,
    pub rows: u64,
    pub checksum: String,
}

/// Where the report lives, relative to the target.
const REPORT: &str = "_kv0/migration.json";

/// Rows per file of a migrated table (one file per commit, so later rows stay later).
const SEGMENT_ROWS: usize = 1 << 20;

/// Converts the Lance database or repository at `from` to Delta Lake at `to` (which may equal
/// `from` for a repository). See the module documentation.
pub fn migrate(from: &str, to: &str, opts: &MigrateOptions) -> Result<Report> {
    let from = storage::normalize_uri(from)?;
    let to = storage::normalize_uri(to)?;
    let opts = opts.clone();
    crate::rt::block_on(async move { run(&from, &to, &opts).await })
}

async fn run(from: &str, to: &str, opts: &MigrateOptions) -> Result<Report> {
    if from.starts_with("memory://") || to.starts_with("memory://") {
        return Err(Error::InvalidArgument(
            "memory:// locations cannot be migrated (each handle has its own)".into(),
        ));
    }
    let src_opts = &opts.storage_options;
    let dst_opts = opts.target_storage_options.as_ref().unwrap_or(src_opts);
    let in_place = from == to;
    let kind = source_kind(from, src_opts).await?;
    if in_place && kind == Kind::Database && !opts.force_in_place {
        return Err(Error::InvalidArgument(format!(
            "{from}: a key-value database is migrated to a new location; pass force_in_place (--force-in-place) to write Delta Lake next to the Lance data"
        )));
    }
    let earlier = read_report(to, dst_opts).await?;
    let mut report = match earlier {
        Some(r) if r.status == "complete" => {
            return Err(Error::InvalidArgument(format!(
                "{to} was migrated already ({} by {})",
                r.finished_at.as_deref().unwrap_or("?"),
                r.created_by
            )));
        }
        Some(r) if r.status == "running" && !opts.resume && !opts.dry_run => {
            return Err(Error::InvalidArgument(format!(
                "a migration to {to} stopped before it finished; run it again with resume (--resume)"
            )));
        }
        Some(r) if r.status == "running" && r.source == from => r,
        Some(r) if r.status == "running" => {
            return Err(Error::InvalidArgument(format!(
                "{to} holds an unfinished migration of {}, not of {from}",
                r.source
            )));
        }
        _ => {
            if !in_place && !opts.dry_run {
                let found = format::detect_uri(to, dst_opts).await?;
                if found != format::Format::Empty {
                    return Err(Error::InvalidArgument(format!(
                        "{to} is not empty ({found:?}); migrate to a new location"
                    )));
                }
            }
            if in_place && !opts.dry_run && delta_present(to, dst_opts, kind).await? {
                return Err(Error::InvalidArgument(format!(
                    "{to} already holds Delta Lake tables that no migration recorded"
                )));
            }
            Report {
                kind: kind_name(kind).into(),
                source: from.to_string(),
                target: to.to_string(),
                in_place,
                status: "running".into(),
                created_by: format!("kv0 {}", env!("CARGO_PKG_VERSION")),
                started_at: chrono::Utc::now().to_rfc3339(),
                finished_at: None,
                tables: Vec::new(),
                unmapped_marks: 0,
                optimized: false,
                source_removed: false,
            }
        }
    };
    if opts.dry_run {
        report.status = "dry-run".into();
        report.tables = match kind {
            Kind::Database => vec![dry_kv(from, src_opts, opts).await?],
            Kind::Repository => dry_repo(from, src_opts, opts).await?,
        };
        return Ok(report);
    }
    // Refuse a source still being written to, before anything is written.
    if let Some(at) = last_commit(from, src_opts, kind).await? {
        refuse_recent(from, at, opts.quiet_period)?;
    }
    write_report(to, dst_opts, &report).await?;
    match kind {
        Kind::Database => kv(from, to, src_opts, dst_opts, opts, &mut report).await?,
        Kind::Repository => repo(from, to, src_opts, dst_opts, opts, &mut report).await?,
    }
    format::write_marker(to, dst_opts, kind).await?;
    report.status = "complete".into();
    report.finished_at = Some(chrono::Utc::now().to_rfc3339());
    write_report(to, dst_opts, &report).await?;
    if kind == Kind::Repository && opts.optimize {
        match optimize(to, dst_opts).await {
            Ok(()) => {
                report.optimized = true;
                write_report(to, dst_opts, &report).await?;
            }
            Err(e) => {
                tracing::warn!(error = %e, "kv0 migrate: optimize failed; the migration stands, run optimize later");
            }
        }
    }
    if opts.remove_source {
        remove_source(from, src_opts, kind, in_place).await?;
        report.source_removed = true;
        write_report(to, dst_opts, &report).await?;
    }
    Ok(report)
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Database => "database",
        Kind::Repository => "repository",
    }
}

/// Lance tables of a repository; any one makes a location a Lance repository.
const LANCE_TABLES: [&str; 5] = [
    "runs.lance",
    "params.lance",
    "series.lance",
    "metrics.lance",
    "records.lance",
];

/// What the Lance source holds. A location already converted (with the marker) fails.
async fn source_kind(uri: &str, options: &StorageOptions) -> Result<Kind> {
    let names = format::list(uri, options).await?;
    let has = |n: &str| names.iter().any(|x| x == n);
    if has("_kv0") {
        if let Some(m) = format::read_marker(uri, options).await? {
            if m.engine == "delta" {
                return Err(Error::InvalidArgument(format!(
                    "{uri} is a Delta Lake location already"
                )));
            }
        }
    }
    if has("_versions") {
        Ok(Kind::Database)
    } else if LANCE_TABLES.iter().any(|t| has(t)) {
        Ok(Kind::Repository)
    } else if names.is_empty() {
        Err(Error::NotFound(format!("nothing to migrate at {uri}")))
    } else {
        Err(Error::InvalidArgument(format!(
            "{uri} holds no kv0 data in the Lance format"
        )))
    }
}

/// Whether Delta tables of `kind` already exist at `uri` (an in-place target).
async fn delta_present(uri: &str, options: &StorageOptions, kind: Kind) -> Result<bool> {
    let names = format::list(uri, options).await?;
    let has = |n: &str| names.iter().any(|x| x == n);
    Ok(match kind {
        Kind::Database => has("_delta_log"),
        Kind::Repository => ["runs", "params", "series", "metrics", "_legacy_records"]
            .iter()
            .any(|t| has(t)),
    })
}

// ---------------------------------------------------------------------------------------------
// Key-value databases

async fn dry_kv(
    from: &str,
    options: &StorageOptions,
    opts: &MigrateOptions,
) -> Result<TableReport> {
    let src = storage::lance::kv::open(from, options, None, false, None).await?;
    let (rows, checksum) = kv_digest(&src).await?;
    let mut tags = Vec::new();
    if opts.keep_tags {
        for (name, v) in sorted_tags(src.tags().await?) {
            let (rows, checksum) = kv_digest(&src.checkout(v).await?).await?;
            tags.push(TagReport {
                name,
                source_version: v,
                target_version: 0,
                rows,
                checksum,
            });
        }
    }
    Ok(TableReport {
        name: ".".into(),
        source_name: ".".into(),
        source_version: src.version(),
        target_version: 0,
        rows,
        checksum,
        tags,
        marks: 0,
        done: false,
    })
}

async fn kv(
    from: &str,
    to: &str,
    src_opts: &StorageOptions,
    dst_opts: &StorageOptions,
    opts: &MigrateOptions,
    report: &mut Report,
) -> Result<()> {
    let src = storage::lance::kv::open(from, src_opts, None, false, None).await?;
    let start = src.version();
    // A database is one commit sequence: a stopped attempt is written again from scratch.
    if !report.tables.is_empty() || delta_present(to, dst_opts, Kind::Database).await? {
        clear_delta_kv(to, dst_opts).await?;
        report.tables.clear();
    }
    let mut dst = storage::delta::kv::open(
        to,
        dst_opts,
        None,
        true,
        opts.version_retention,
        opts.mark_ttl,
    )
    .await?;
    let tags = if opts.keep_tags {
        sorted_tags(src.tags().await?)
    } else {
        Vec::new()
    };
    let keep_after =
        crate::clock::now_nanos().saturating_sub(opts.tombstone_grace.as_nanos() as u64);
    let mut versions: Vec<u64> = tags.iter().map(|t| t.1).collect();
    versions.push(start);
    versions.dedup();
    let mut at: HashMap<u64, u64> = HashMap::new();
    let mut marks = Vec::new();
    for v in versions {
        let state = src.checkout(v).await?;
        let mut view = View::default();
        view.lazy_threshold = 4096;
        crate::kv::store::load_into(state.as_ref(), &mut view).await?;
        let chunks = view.snapshot(
            keep_after,
            u64::MAX,
            crate::kv::store::REWRITE_BATCH_ROWS,
            crate::kv::store::REWRITE_BATCH_BYTES,
        );
        dst = dst.rewrite_from(chunks, state).await?;
        at.insert(v, dst.version());
        if v == start {
            marks = view
                .marks
                .iter()
                .map(|(w, s)| WriterMark {
                    writer: String::from_utf8_lossy(w).into_owned(),
                    seq: *s,
                })
                .collect();
        }
    }
    let n_marks = marks.len();
    dst = dst.adopt_marks(marks).await?;
    for (name, v) in &tags {
        dst.create_tag(name, at[v]).await?;
    }
    // Verify every version against its source.
    let mut tag_reports = Vec::new();
    for (name, v) in &tags {
        let (rows, checksum) = kv_digest(&src.checkout(*v).await?).await?;
        let got = kv_digest(&dst.checkout(at[v]).await?).await?;
        mismatch(&format!("tag {name}"), (rows, &checksum), (got.0, &got.1))?;
        tag_reports.push(TagReport {
            name: name.clone(),
            source_version: *v,
            target_version: at[v],
            rows,
            checksum,
        });
    }
    let (rows, checksum) = kv_digest(&src).await?;
    let latest = dst.version();
    let got = kv_digest(&dst).await?;
    mismatch("the latest version", (rows, &checksum), (got.0, &got.1))?;
    for m in &dst_marks(&src).await? {
        if dst.writer_mark(&m.writer).await? != Some(m.seq) {
            return Err(Error::Corruption(format!(
                "the WAL mark of writer {} was not carried over",
                m.writer
            )));
        }
    }
    unchanged(from, start, || async {
        Ok(storage::lance::kv::open(from, src_opts, None, false, None)
            .await?
            .version())
    })
    .await?;
    report.tables = vec![TableReport {
        name: ".".into(),
        source_name: ".".into(),
        source_version: start,
        target_version: latest,
        rows,
        checksum,
        tags: tag_reports,
        marks: n_marks,
        done: true,
    }];
    Ok(())
}

/// The writer marks of a Lance log's latest version.
async fn dst_marks(src: &Log) -> Result<Vec<WriterMark>> {
    let mut view = View::default();
    view.lazy_threshold = 1;
    src.scan(None, crate::storage::oplog::Columns::Keys, &mut |r| {
        if r.op == crate::kv::view::OP_MARK {
            view.apply_parts(r.seq, r.op, r.key, Some(&r.seq.to_le_bytes()), None)
        }
    })
    .await?;
    Ok(view
        .marks
        .iter()
        .map(|(w, s)| WriterMark {
            writer: String::from_utf8_lossy(w).into_owned(),
            seq: *s,
        })
        .collect())
}

/// Live entries of a log version and a checksum of them (keys and values, in key order).
async fn kv_digest(log: &Log) -> Result<(u64, String)> {
    let mut view = View::default();
    view.lazy_threshold = 4096;
    crate::kv::store::load_into(log.as_ref(), &mut view).await?;
    let mut h = Xxh3::new();
    let mut rows = 0u64;
    for chunk in view.snapshot(u64::MAX, u64::MAX, 4096, 64 << 20) {
        let mut chunk = chunk;
        let keys = chunk.key.finish();
        let values = chunk.value.finish();
        let wanted: Vec<u64> = chunk.stored.iter().map(|s| s.1).collect();
        let mut taken: HashMap<usize, bytes::Bytes> = chunk
            .stored
            .iter()
            .map(|s| s.0)
            .zip(log.take(&wanted).await?)
            .collect();
        for i in 0..keys.len() {
            let key = keys.value(i);
            h.update(&(key.len() as u64).to_le_bytes());
            h.update(key);
            match taken.remove(&i) {
                Some(v) => {
                    h.update(&(v.len() as u64).to_le_bytes());
                    h.update(&v);
                }
                None => {
                    let v = values.value(i);
                    h.update(&(v.len() as u64).to_le_bytes());
                    h.update(v);
                }
            }
            rows += 1;
        }
    }
    Ok((rows, format!("{:016x}", h.digest())))
}

/// Deletes what an earlier attempt wrote of a Delta database at `uri`: its log, its data files
/// (at the top level, where Lance keeps none) and its tags.
async fn clear_delta_kv(uri: &str, options: &StorageOptions) -> Result<()> {
    use deltalake_core::logstore::object_store::ObjectStoreExt;
    use futures::TryStreamExt;
    let store = storage::delta::table::handle(uri, options)?.object_store();
    let all: Vec<_> = store.list(None).try_collect().await?;
    for m in all {
        let p = m.location.as_ref();
        let top_parquet = !p.contains('/') && p.ends_with(".parquet");
        if p.starts_with("_delta_log/") || p.starts_with("_kv0/tags/") || top_parquet {
            match store.delete(&m.location).await {
                Ok(()) | Err(deltalake_core::ObjectStoreError::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Tracking repositories

/// The tables of a Lance repository: (source name, target name, schema).
async fn repo_tables(src: &Root) -> Result<Vec<(String, String, SchemaRef)>> {
    let mut out: Vec<(String, String, SchemaRef)> = [
        ("runs", schema::RUNS.clone()),
        ("params", schema::PARAMS.clone()),
        ("series", schema::SERIES.clone()),
        ("metrics", schema::METRICS.clone()),
    ]
    .into_iter()
    .map(|(n, s)| (n.to_string(), n.to_string(), s))
    .collect();
    let mut names = src.tables("records").await?;
    names.sort();
    for n in names {
        let t = format!("records/{n}");
        out.push((t.clone(), t, records::RECORDS.clone()));
    }
    if src.exists("records").await? {
        out.push((
            "records".into(),
            "_legacy_records".into(),
            records::RECORDS.clone(),
        ));
    }
    Ok(out)
}

/// Opens a source table, or `None` when it does not exist (an older repository may lack one).
async fn open_source(src: &Root, name: &str, schema: &SchemaRef) -> Result<Option<TableRef>> {
    if !src.exists(name).await? {
        return Ok(None);
    }
    Ok(Some(
        src.table(name, schema.clone(), false, None, Duration::ZERO)
            .await?,
    ))
}

async fn dry_repo(
    from: &str,
    options: &StorageOptions,
    opts: &MigrateOptions,
) -> Result<Vec<TableReport>> {
    let src = Root::lance_at(from, options);
    let mut out = Vec::new();
    for (name, target, schema) in repo_tables(&src).await? {
        let Some(t) = open_source(&src, &name, &schema).await? else {
            continue;
        };
        let kind = TableKind::of(&name);
        let d = table_digest(t.as_ref(), kind, None).await?;
        let mut tags = Vec::new();
        if opts.keep_tags {
            for (tag, v) in sorted_tags(t.tags().await?) {
                let d = table_digest(t.checkout(v).await?.as_ref(), kind, None).await?;
                tags.push(TagReport {
                    name: tag,
                    source_version: v,
                    target_version: 0,
                    rows: d.rows,
                    checksum: d.checksum(),
                });
            }
        }
        out.push(TableReport {
            name: target,
            source_name: name,
            source_version: t.version(),
            target_version: 0,
            rows: d.rows,
            checksum: d.checksum(),
            tags,
            marks: d.marks.len(),
            done: false,
        });
    }
    Ok(out)
}

async fn repo(
    from: &str,
    to: &str,
    src_opts: &StorageOptions,
    dst_opts: &StorageOptions,
    opts: &MigrateOptions,
    report: &mut Report,
) -> Result<()> {
    // Older builds named records tables differently; Lance repositories take the current
    // names when opened (as kv0 0.1.0-beta.2 itself does).
    let moved = records::migrate_paths(from, src_opts).await?;
    if !moved.is_empty() {
        tracing::info!(tables = ?moved, "kv0 migrate: renamed records tables to their current names first");
    }
    let src = Root::lance_at(from, src_opts);
    let dst = Root::delta_at(to, dst_opts);
    let tables = repo_tables(&src).await?;
    let mut sources = Vec::new();
    for (name, target, schema) in &tables {
        if let Some(t) = open_source(&src, name, schema).await? {
            sources.push((name.clone(), target.clone(), schema.clone(), t));
        }
    }
    // Writer names, for the metrics marks that only carry a hash of them.
    let mut writers: HashMap<i64, String> = HashMap::new();
    for (name, target, schema, t) in sources {
        let kind = TableKind::of(&name);
        let done = report
            .tables
            .iter()
            .find(|r| r.name == target && r.done && r.source_version == t.version())
            .cloned();
        if let Some(done) = done {
            // Verified by the earlier attempt; check it is still what it recorded.
            let d = dst
                .table(&target, schema.clone(), false, None, opts.mark_ttl)
                .await?;
            let got = table_digest(
                d.checkout(done.target_version).await?.as_ref(),
                kind,
                Some(&columns_of(t.as_ref())),
            )
            .await?;
            mismatch(
                &target,
                (done.rows, &done.checksum),
                (got.rows, &got.checksum()),
            )?;
            for w in table_digest(t.as_ref(), kind, None).await?.writers {
                writers.insert(crate::tracking::writer_hash(&w), w);
            }
            continue;
        }
        report.tables.retain(|r| r.name != target);
        if dst.exists(&target).await? {
            dst.remove(&target).await?;
        }
        let r = copy_table(
            t.as_ref(),
            &dst,
            &target,
            &name,
            &schema,
            kind,
            opts,
            &mut writers,
        )
        .await?;
        report.unmapped_marks += r.1;
        report.tables.push(r.0);
        write_report(to, dst_opts, report).await?;
    }
    // Nothing may have been written to the source meanwhile.
    for r in &report.tables {
        let schema = tables
            .iter()
            .find(|t| t.1 == r.name)
            .map(|t| t.2.clone())
            .unwrap_or_else(|| records::RECORDS.clone());
        let source_name = r.source_name.clone();
        let src = &src;
        unchanged(
            &format!("{from} ({source_name})"),
            r.source_version,
            || async move {
                match open_source(src, &source_name, &schema).await? {
                    Some(t) => t.latest_version().await,
                    None => Ok(0),
                }
            },
        )
        .await?;
    }
    Ok(())
}

/// Copies one table, every tagged version first, and verifies each. Returns its report and
/// the number of writer marks that could not be named.
#[allow(clippy::too_many_arguments)]
async fn copy_table(
    src: &dyn crate::storage::table::Table,
    dst_root: &Root,
    target: &str,
    source_name: &str,
    schema: &SchemaRef,
    kind: TableKind,
    opts: &MigrateOptions,
    writers: &mut HashMap<i64, String>,
) -> Result<(TableReport, usize)> {
    let mut dst = dst_root
        .table(
            target,
            schema.clone(),
            true,
            opts.version_retention,
            opts.mark_ttl,
        )
        .await?;
    let start = src.version();
    let tags = if opts.keep_tags {
        sorted_tags(src.tags().await?)
    } else {
        Vec::new()
    };
    let mut versions: Vec<u64> = tags.iter().map(|t| t.1).collect();
    versions.push(start);
    versions.dedup();
    let cols = columns_of(src);
    let mut at: HashMap<u64, (u64, Digest)> = HashMap::new();
    let mut latest_marks = Vec::new();
    for v in versions {
        let state = src.checkout(v).await?;
        let d = copy_state(state.as_ref(), &mut dst, schema, kind).await?;
        if v == start {
            latest_marks = d.marks.clone();
            for w in &d.writers {
                writers.insert(crate::tracking::writer_hash(w), w.clone());
            }
        }
        at.insert(v, (dst.version(), d));
    }
    // Marks of the latest version, by writer (metrics name writers by a hash).
    let mut unmapped = 0;
    let mut marks: BTreeMap<String, u64> = BTreeMap::new();
    for (w, seq) in latest_marks {
        let name = match w {
            MarkOf::Name(n) => n,
            MarkOf::Hash(h) => match writers.get(&h) {
                Some(n) => n.clone(),
                None => {
                    unmapped += 1;
                    continue;
                }
            },
        };
        let m = marks.entry(name).or_default();
        *m = (*m).max(seq);
    }
    let n_marks = marks.len();
    dst.adopt_marks(
        marks
            .into_iter()
            .map(|(writer, seq)| WriterMark { writer, seq })
            .collect(),
    )
    .await?;
    for (name, v) in &tags {
        dst.create_tag(name, at[v].0).await?;
    }
    // Verify every copied version.
    let mut tag_reports = Vec::new();
    for (name, v) in &tags {
        let (tv, d) = &at[v];
        let got = table_digest(dst.checkout(*tv).await?.as_ref(), kind, Some(&cols)).await?;
        mismatch(
            &format!("{target} at tag {name}"),
            (d.rows, &d.checksum()),
            (got.rows, &got.checksum()),
        )?;
        tag_reports.push(TagReport {
            name: name.clone(),
            source_version: *v,
            target_version: *tv,
            rows: d.rows,
            checksum: d.checksum(),
        });
    }
    let (tv, d) = &at[&start];
    let got = table_digest(dst.checkout(*tv).await?.as_ref(), kind, Some(&cols)).await?;
    mismatch(target, (d.rows, &d.checksum()), (got.rows, &got.checksum()))?;
    Ok((
        TableReport {
            name: target.to_string(),
            source_name: source_name.to_string(),
            source_version: start,
            target_version: dst.version(),
            rows: d.rows,
            checksum: d.checksum(),
            tags: tag_reports,
            marks: n_marks,
            done: true,
        },
        unmapped,
    ))
}

/// Replaces everything in `dst` with the rows of `state` (WAL marks dropped): the first
/// segment in one commit with the removal, the rest appended, so rows keep their order.
async fn copy_state(
    state: &dyn crate::storage::table::Table,
    dst: &mut TableRef,
    schema: &SchemaRef,
    kind: TableKind,
) -> Result<Digest> {
    let mut d = Digest::default();
    let cols = columns_of(state);
    let mut stream = state.scan_stream(Scan::all()).await?;
    let mut first = true;
    let mut seg: Vec<RecordBatch> = Vec::new();
    let mut seg_rows = 0;
    loop {
        let next = stream.next().await.transpose()?;
        let end = next.is_none();
        if let Some(b) = next {
            let b = kind.data_rows(&b, &mut d)?;
            d.add(&b, &cols)?;
            seg_rows += b.num_rows();
            if b.num_rows() > 0 {
                seg.push(b);
            }
            if seg_rows < SEGMENT_ROWS {
                continue;
            }
        } else if !first && seg.is_empty() {
            break;
        }
        let batches = std::mem::take(&mut seg);
        seg_rows = 0;
        if first {
            let remove = dst.files().into_iter().map(|f| f.id).collect();
            dst.replace(
                remove,
                schema.clone(),
                futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
                None,
            )
            .await?;
            first = false;
        } else if !batches.is_empty() {
            dst.append(batches, None).await?;
        }
        if end {
            break;
        }
    }
    Ok(d)
}

fn columns_of(t: &dyn crate::storage::table::Table) -> Vec<String> {
    let mut cols: Vec<String> = [
        "seq",
        "run_id",
        "name",
        "experiment",
        "description",
        "tags",
        "status",
        "archived",
        "created_at",
        "updated_at",
        "ended_at",
        "fields",
        "kind",
        "target",
        "key",
        "value",
        "num",
        "text",
        "flag",
        "series_id",
        "context",
        "step",
        "epoch",
        "timestamp",
        "stats",
        "table_name",
        "item_id",
        "data",
    ]
    .into_iter()
    .filter(|c| t.has_column(c))
    .map(str::to_string)
    .collect();
    cols.sort();
    cols.dedup();
    cols
}

/// Which table, for telling WAL mark rows from data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableKind {
    /// `runs` and `params`: mark rows have a `__kv0_wal__/<writer>` run id, the mark in `seq`.
    RunsOrParams,
    /// `series`: the same run id, the mark in `name`.
    Series,
    /// `metrics`: series id 0 without statistics, the mark in `step`, a hash of the writer in
    /// `epoch`.
    Metrics,
    /// Records tables: no marks.
    Records,
}

impl TableKind {
    fn of(name: &str) -> TableKind {
        match name {
            "runs" | "params" => TableKind::RunsOrParams,
            "series" => TableKind::Series,
            "metrics" => TableKind::Metrics,
            _ => TableKind::Records,
        }
    }

    /// `b` without its WAL mark rows, which are recorded in `d`.
    fn data_rows(self, b: &RecordBatch, d: &mut Digest) -> Result<RecordBatch> {
        let n = b.num_rows();
        let keep: Vec<bool> = match self {
            TableKind::Records => return Ok(b.clone()),
            TableKind::RunsOrParams | TableKind::Series => {
                let ids = string_col(b, "run_id")?;
                (0..n)
                    .map(|i| {
                        let id = ids.value(i);
                        let Some(w) = id.strip_prefix(crate::tracking::MARK_PREFIX) else {
                            return true;
                        };
                        let seq = if self == TableKind::Series {
                            string_col(b, "name")
                                .ok()
                                .and_then(|c| c.value(i).parse().ok())
                                .unwrap_or(0)
                        } else {
                            b.column_by_name("seq")
                                .map(|c| seq_from_i64(c.as_primitive::<Int64Type>().value(i)))
                                .unwrap_or(0)
                        };
                        d.writers.push(w.to_string());
                        d.marks.push((MarkOf::Name(w.to_string()), seq));
                        false
                    })
                    .collect()
            }
            TableKind::Metrics => {
                let sid = int_col(b, "series_id")?;
                let stats = b.column_by_name("stats");
                let step = int_col(b, "step")?;
                let epoch = b.column_by_name("epoch");
                (0..n)
                    .map(|i| {
                        let mark = sid.value(i) == 0 && stats.is_none_or(|s| s.is_null(i));
                        if mark {
                            let h = epoch
                                .filter(|e| !e.is_null(i))
                                .map(|e| e.as_primitive::<Int64Type>().value(i))
                                .unwrap_or(0);
                            d.marks.push((MarkOf::Hash(h), step.value(i) as u64));
                        }
                        !mark
                    })
                    .collect()
            }
        };
        if keep.iter().all(|k| *k) {
            return Ok(b.clone());
        }
        Ok(arrow_select::filter::filter_record_batch(
            b,
            &BooleanArray::from(keep),
        )?)
    }
}

fn string_col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a arrow_array::StringArray> {
    b.column_by_name(name)
        .and_then(|c| c.as_string_opt::<i32>())
        .ok_or_else(|| Error::Corruption(format!("column {name} missing or not a string")))
}

fn int_col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a arrow_array::Int64Array> {
    b.column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<Int64Type>())
        .ok_or_else(|| Error::Corruption(format!("column {name} missing or not an integer")))
}

/// Whose WAL mark a mark row holds.
#[derive(Clone, Debug)]
enum MarkOf {
    Name(String),
    Hash(i64),
}

/// Rows of a table version (WAL marks excluded) and an order-independent checksum of them.
#[derive(Clone, Debug, Default)]
struct Digest {
    rows: u64,
    sum: u64,
    xor: u64,
    marks: Vec<(MarkOf, u64)>,
    writers: Vec<String>,
}

impl Digest {
    fn checksum(&self) -> String {
        format!("{:016x}{:016x}", self.sum, self.xor)
    }

    /// Adds the rows of `b`, hashing the columns `cols` (missing ones count as absent).
    fn add(&mut self, b: &RecordBatch, cols: &[String]) -> Result<()> {
        let arrays: Vec<Option<&dyn Array>> = cols
            .iter()
            .map(|c| b.column_by_name(c).map(|a| a.as_ref()))
            .collect();
        for i in 0..b.num_rows() {
            let mut h = Xxh3::new();
            for (c, a) in cols.iter().zip(&arrays) {
                h.update(c.as_bytes());
                match a {
                    Some(a) => hash_value(*a, i, &mut h)?,
                    None => h.update(&[2]),
                }
            }
            let x = h.digest();
            self.rows += 1;
            self.sum = self.sum.wrapping_add(x);
            self.xor ^= x;
        }
        Ok(())
    }
}

/// Rows and checksum of a table version over `cols` (the source's columns; by default the
/// table's own), WAL marks excluded and recorded.
async fn table_digest(
    t: &dyn crate::storage::table::Table,
    kind: TableKind,
    cols: Option<&[String]>,
) -> Result<Digest> {
    let own = columns_of(t);
    let cols = cols.unwrap_or(&own);
    let mut d = Digest::default();
    let mut stream = t.scan_stream(Scan::all()).await?;
    while let Some(b) = stream.next().await.transpose()? {
        let b = kind.data_rows(&b, &mut d)?;
        d.add(&b, cols)?;
    }
    Ok(d)
}

/// Hashes value `i` of `a` by its logical content (a null and an absent column differ).
fn hash_value(a: &dyn Array, i: usize, h: &mut Xxh3) -> Result<()> {
    if a.is_null(i) {
        h.update(&[0]);
        return Ok(());
    }
    h.update(&[1]);
    macro_rules! prim {
        ($t:ty) => {
            h.update(&a.as_primitive::<$t>().value(i).to_le_bytes())
        };
    }
    let bytes = |h: &mut Xxh3, v: &[u8]| {
        h.update(&(v.len() as u64).to_le_bytes());
        h.update(v);
    };
    match a.data_type() {
        DataType::Boolean => h.update(&[a.as_boolean().value(i) as u8]),
        DataType::Int8 => prim!(Int8Type),
        DataType::Int16 => prim!(Int16Type),
        DataType::Int32 => prim!(Int32Type),
        DataType::Int64 => prim!(Int64Type),
        DataType::UInt8 => prim!(UInt8Type),
        DataType::UInt16 => prim!(UInt16Type),
        DataType::UInt32 => prim!(UInt32Type),
        DataType::UInt64 => prim!(UInt64Type),
        DataType::Float32 => prim!(Float32Type),
        DataType::Float64 => prim!(Float64Type),
        DataType::Timestamp(TimeUnit::Second, _) => prim!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => prim!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => prim!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => prim!(TimestampNanosecondType),
        DataType::Utf8 => bytes(h, a.as_string::<i32>().value(i).as_bytes()),
        DataType::LargeUtf8 => bytes(h, a.as_string::<i64>().value(i).as_bytes()),
        DataType::Utf8View => bytes(h, a.as_string_view().value(i).as_bytes()),
        DataType::Binary => bytes(h, a.as_binary::<i32>().value(i)),
        DataType::LargeBinary => bytes(h, a.as_binary::<i64>().value(i)),
        DataType::BinaryView => bytes(h, a.as_binary_view().value(i)),
        DataType::List(_) => {
            let v = a.as_list::<i32>().value(i);
            h.update(&(v.len() as u64).to_le_bytes());
            for j in 0..v.len() {
                hash_value(v.as_ref(), j, h)?;
            }
        }
        DataType::LargeList(_) => {
            let v = a.as_list::<i64>().value(i);
            h.update(&(v.len() as u64).to_le_bytes());
            for j in 0..v.len() {
                hash_value(v.as_ref(), j, h)?;
            }
        }
        DataType::Struct(_) => {
            for c in a.as_struct().columns() {
                hash_value(c.as_ref(), i, h)?;
            }
        }
        other => {
            return Err(Error::NotSupported(format!(
                "cannot compare values of type {other} in a migration"
            )))
        }
    }
    Ok(())
}

async fn optimize(to: &str, options: &StorageOptions) -> Result<()> {
    let repo = crate::tracking::Repo::open(
        to,
        crate::tracking::RepoOptions {
            storage_options: options.clone(),
            wal: false,
            flush_interval: None,
            legacy_records: crate::tracking::LegacyRecords::Allow,
            engine: Some(storage::Engine::Delta),
            ..crate::tracking::RepoOptions::default()
        },
    )
    .await?;
    let r = repo.optimize().await;
    repo.close().await?;
    r.map(|_| ())
}

async fn remove_source(
    from: &str,
    options: &StorageOptions,
    kind: Kind,
    in_place: bool,
) -> Result<()> {
    match kind {
        Kind::Database if !in_place => storage::remove_dir(from, options).await,
        Kind::Database => {
            for d in [
                "_versions",
                "data",
                "_refs",
                "_transactions",
                "_indices",
                "_deletions",
            ] {
                remove_if_there(&storage::join(from, d), options).await?;
            }
            Ok(())
        }
        Kind::Repository => {
            let src = Root::lance_at(from, options);
            for (name, _, _) in repo_tables(&src).await? {
                remove_if_there(&src.location(&name), options).await?;
            }
            Ok(())
        }
    }
}

async fn remove_if_there(uri: &str, options: &StorageOptions) -> Result<()> {
    if storage::is_local(uri)
        && !std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri)).exists()
    {
        return Ok(());
    }
    storage::remove_dir(uri, options).await
}

// ---------------------------------------------------------------------------------------------
// Shared

fn sorted_tags(mut tags: Vec<(String, u64)>) -> Vec<(String, u64)> {
    tags.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
    tags
}

fn mismatch(what: &str, want: (u64, &str), got: (u64, &str)) -> Result<()> {
    if want == got {
        return Ok(());
    }
    Err(Error::Corruption(format!(
        "verification failed for {what}: the source has {} rows (checksum {}), the migrated table {} (checksum {})",
        want.0, want.1, got.0, got.1
    )))
}

/// When the source was last committed to (microseconds since the epoch).
async fn last_commit(from: &str, options: &StorageOptions, kind: Kind) -> Result<Option<i64>> {
    match kind {
        Kind::Database => {
            let src = storage::lance::kv::open(from, options, None, false, None).await?;
            Ok(src.versions().await?.last().map(|v| v.timestamp_micros))
        }
        Kind::Repository => {
            let src = Root::lance_at(from, options);
            let mut newest = None;
            for (name, _, schema) in repo_tables(&src).await? {
                if let Some(t) = open_source(&src, &name, &schema).await? {
                    newest = newest.max(t.last_commit_micros().await?);
                }
            }
            Ok(newest)
        }
    }
}

fn refuse_recent(uri: &str, at_micros: i64, quiet: Duration) -> Result<()> {
    let age = chrono::Utc::now().timestamp_micros() - at_micros;
    if age >= 0 && (age as u128) < quiet.as_micros() {
        return Err(Error::InvalidArgument(format!(
            "{uri} was written {:.1} s ago: stop every writer first (or lower quiet_period, --quiet-period)",
            age as f64 / 1e6
        )));
    }
    Ok(())
}

/// Fails when the source moved past `version` while the migration ran.
async fn unchanged<F, Fut>(what: &str, version: u64, latest: F) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let now = latest().await?;
    if now != version {
        return Err(Error::Conflict(format!(
            "{what} was written to during the migration (version {version} became {now}): stop every writer and run the migration again with resume"
        )));
    }
    Ok(())
}

async fn read_report(uri: &str, options: &StorageOptions) -> Result<Option<Report>> {
    use deltalake_core::logstore::object_store::ObjectStoreExt;
    let store = storage::delta::table::handle(uri, options)?.object_store();
    match store.get(&deltalake_core::Path::from(REPORT)).await {
        Ok(r) => {
            let b = r.bytes().await?;
            Ok(Some(serde_json::from_slice(&b).map_err(|e| {
                Error::Corruption(format!("{uri}/{REPORT}: {e}"))
            })?))
        }
        Err(deltalake_core::ObjectStoreError::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn write_report(uri: &str, options: &StorageOptions, report: &Report) -> Result<()> {
    use deltalake_core::logstore::object_store::ObjectStoreExt;
    if storage::is_local(uri) {
        let dir = uri.strip_prefix("file://").unwrap_or(uri);
        std::fs::create_dir_all(dir).map_err(|e| Error::Io(format!("{dir}: {e}")))?;
    }
    let store = storage::delta::table::handle(uri, options)?.object_store();
    let body = serde_json::to_vec_pretty(report)?;
    store
        .put(&deltalake_core::Path::from(REPORT), body.into())
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Command line

const USAGE: &str = "usage: kv0 migrate <from> <to> [options]

Converts a kv0 key-value database or tracking repository written with the Lance engine
(kv0 0.1.0-beta.2 and earlier) to Delta Lake. Stop every writer first.

options:
  --storage-option KEY=VALUE         object-store option of the source (and target), repeatable
  --target-storage-option KEY=VALUE  object-store option of the target, repeatable
  --no-tags                          do not carry over tags (default: every tag, with its data)
  --dry-run                          report what would be migrated; write nothing
  --resume                           continue a migration that stopped
  --remove-source                    delete the Lance data once everything is verified
  --force-in-place                   allow <to> = <from> for a key-value database
  --no-optimize                      skip `optimize` on a migrated repository
  --quiet-period SECONDS             refuse a source written to more recently (default 10)
  --json                             print the full report as JSON";

/// Runs the `kv0` command line with `args` (without the program name) and returns the exit
/// code. Messages go to standard output and standard error.
pub fn cli(args: &[String]) -> i32 {
    match parse(args) {
        Ok(None) => {
            println!("{USAGE}");
            0
        }
        Ok(Some((from, to, opts, json))) => match migrate(&from, &to, &opts) {
            Ok(r) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&r).unwrap_or_default());
                } else {
                    print_summary(&r);
                }
                0
            }
            Err(e) => {
                eprintln!("kv0 migrate: {e}");
                1
            }
        },
        Err(e) => {
            eprintln!("kv0: {e}\n\n{USAGE}");
            2
        }
    }
}

type Parsed = (String, String, MigrateOptions, bool);

fn parse(args: &[String]) -> std::result::Result<Option<Parsed>, String> {
    let mut it = args.iter();
    match it.next().map(String::as_str) {
        None | Some("-h" | "--help" | "help") => return Ok(None),
        Some("migrate") => {}
        Some(other) => return Err(format!("unknown command {other:?}")),
    }
    let mut opts = MigrateOptions::default();
    let mut target: Option<StorageOptions> = None;
    let mut json = false;
    let mut places = Vec::new();
    let kv = |s: Option<&String>| -> std::result::Result<(String, String), String> {
        let s = s.ok_or("a KEY=VALUE argument is missing")?;
        let (k, v) = s
            .split_once('=')
            .ok_or_else(|| format!("{s:?} is not KEY=VALUE"))?;
        Ok((k.to_string(), v.to_string()))
    };
    while let Some(a) = it.next() {
        match a.as_str() {
            "-h" | "--help" => return Ok(None),
            "--storage-option" => {
                let (k, v) = kv(it.next())?;
                opts.storage_options.insert(k, v);
            }
            "--target-storage-option" => {
                let (k, v) = kv(it.next())?;
                target.get_or_insert_with(StorageOptions::new).insert(k, v);
            }
            "--keep-tags" => opts.keep_tags = true,
            "--no-tags" => opts.keep_tags = false,
            "--dry-run" => opts.dry_run = true,
            "--resume" => opts.resume = true,
            "--remove-source" => opts.remove_source = true,
            "--force-in-place" => opts.force_in_place = true,
            "--no-optimize" => opts.optimize = false,
            "--json" => json = true,
            "--quiet-period" => {
                let s = it
                    .next()
                    .ok_or("--quiet-period needs a number of seconds")?;
                let secs: f64 = s
                    .parse()
                    .map_err(|_| format!("--quiet-period {s:?} is not a number"))?;
                opts.quiet_period = Duration::from_secs_f64(secs.max(0.0));
            }
            s if s.starts_with("--") => return Err(format!("unknown option {s}")),
            s => places.push(s.to_string()),
        }
    }
    opts.target_storage_options = target;
    match places.as_slice() {
        [from, to] => Ok(Some((from.clone(), to.clone(), opts, json))),
        _ => Err("expected <from> and <to>".into()),
    }
}

fn print_summary(r: &Report) {
    let verb = if r.status == "dry-run" {
        "would migrate"
    } else {
        "migrated"
    };
    println!("{verb} the {} {} -> {}", r.kind, r.source, r.target);
    for t in &r.tables {
        println!(
            "  {:<24} {:>10} rows  version {} -> {}  {} tags, {} writer marks",
            t.name,
            t.rows,
            t.source_version,
            t.target_version,
            t.tags.len(),
            t.marks
        );
        for g in &t.tags {
            println!(
                "    tag {:<20} version {} -> {}  {} rows",
                g.name, g.source_version, g.target_version, g.rows
            );
        }
    }
    if r.unmapped_marks > 0 {
        println!(
            "  {} metric writer marks could not be named: do not replay those writers' WALs",
            r.unmapped_marks
        );
    }
    if r.status != "dry-run" {
        if r.kind == "repository" && !r.optimized {
            println!("  optimize did not run (or failed); run it on the repository later");
        }
        println!("report: {}/{REPORT}", r.target);
    }
    if r.source_removed {
        println!("the Lance data was removed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_string).collect()
    }

    #[test]
    fn the_command_line_parses() {
        assert!(parse(&args("")).unwrap().is_none());
        assert!(parse(&args("migrate --help")).unwrap().is_none());
        let (from, to, o, json) = parse(&args(
            "migrate a b --storage-option aws_region=x --target-storage-option allow_http=true --no-tags --dry-run --resume --remove-source --force-in-place --no-optimize --quiet-period 0 --json",
        ))
        .unwrap()
        .unwrap();
        assert_eq!((from.as_str(), to.as_str(), json), ("a", "b", true));
        assert_eq!(o.storage_options["aws_region"], "x");
        assert_eq!(o.target_storage_options.unwrap()["allow_http"], "true");
        assert!(!o.keep_tags && o.dry_run && o.resume && o.remove_source && o.force_in_place);
        assert!(!o.optimize && o.quiet_period.is_zero());
        assert!(parse(&args("migrate a")).is_err());
        assert!(parse(&args("migrate a b --bogus")).is_err());
        assert!(parse(&args("migrate a b --storage-option novalue")).is_err());
        assert!(parse(&args("frobnicate a b")).is_err());
    }
}
