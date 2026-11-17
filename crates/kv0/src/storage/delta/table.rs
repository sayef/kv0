//! Locations, versions, tags and cleanup of a Delta table.
//!
//! - Versions are the commit files in `_delta_log/`, listed once; a version's time is its commit
//!   file's modification time.
//! - Tags are sidecar files `_kv0/tags/<name>.json` holding `{"version": v, "created_at_us": t}`
//!   (names escaped as records tables are, so no store percent-encodes them). Tagging writes a
//!   checkpoint at the version, so it stays loadable after older commit files are deleted.
//!   `_kv0/` starts with `_`, so delta-rs never treats it as data.
//! - Cleanup removes the data files only old versions use (with delta-rs's vacuum, as a dry run
//!   so it commits nothing), then the commit files before the oldest version kept, after making
//!   sure a checkpoint exists there.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use deltalake_core::logstore::object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use deltalake_core::logstore::LogStoreRef;
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::{
    DeltaTable, DeltaTableBuilder, DeltaTableError, ObjectStore, ObjectStoreError, Path,
};
use futures::TryStreamExt;
use url::Url;

use crate::error::{Error, Result};
use crate::kv::VersionInfo;
use crate::storage::{is_local, non_empty, Cleanup, StorageOptions};

/// The table URL of a location (a local path becomes a `file://` directory URL; nothing is
/// created).
pub(crate) fn url(uri: &str) -> Result<Url> {
    // object_store reads `memory:///<path>` only; kv0 names memory locations `memory://<name>`.
    let memory;
    let uri = match uri.strip_prefix("memory://") {
        Some(rest) if !rest.starts_with('/') => {
            memory = format!("memory:///{rest}");
            memory.as_str()
        }
        _ => uri,
    };
    let url = if uri.contains("://") {
        Url::parse(uri).map_err(|e| Error::InvalidArgument(format!("{uri}: {e}")))?
    } else {
        Url::from_directory_path(uri)
            .map_err(|_| Error::InvalidArgument(format!("{uri} is not an absolute path")))?
    };
    Ok(deltalake_core::table::normalize_table_url(&url))
}

/// An unloaded table handle at `uri`: its log store, without reading anything.
pub(crate) fn handle(uri: &str, options: &StorageOptions) -> Result<DeltaTable> {
    handle_in(uri, options, None)
}

/// [`handle`], on `store` (rooted at the store's root) when given: the tables of one
/// `memory://` repository share one in-memory store.
pub(crate) fn handle_in(
    uri: &str,
    options: &StorageOptions,
    store: Option<&Arc<dyn ObjectStore>>,
) -> Result<DeltaTable> {
    super::init();
    let url = url(uri)?;
    let builder = || -> Result<DeltaTableBuilder> {
        Ok(DeltaTableBuilder::from_url(url.clone())?.with_storage_options(non_empty(options)))
    };
    let store = match store {
        Some(s) => Some(s.clone()),
        // The test hooks wrap the store delta-rs would build.
        None if super::faults::armed() => {
            Some(builder()?.build()?.log_store().root_object_store(None))
        }
        None => None,
    };
    let mut b = builder()?;
    if let Some(s) = store {
        b = b.with_storage_backend(super::faults::wrap(s), url);
    }
    Ok(b.build()?)
}

/// What a location holds.
pub(crate) enum Layout {
    Delta,
    Lance,
    None,
}

/// Whether `uri` holds a Delta table, a Lance dataset or neither (one listing).
pub(crate) async fn layout(uri: &str, options: &StorageOptions) -> Result<Layout> {
    if is_local(uri) {
        let dir = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
        return Ok(if dir.join("_delta_log").is_dir() {
            Layout::Delta
        } else if dir.join("_versions").is_dir() {
            Layout::Lance
        } else {
            Layout::None
        });
    }
    let store = handle(uri, options)?.object_store();
    let listed = match store.list_with_delimiter(None).await {
        Ok(l) => l,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(Layout::None),
        Err(e) => return Err(e.into()),
    };
    let has = |name: &str| {
        listed
            .common_prefixes
            .iter()
            .any(|p| p.filename() == Some(name))
    };
    Ok(if has("_delta_log") {
        Layout::Delta
    } else if has("_versions") {
        Layout::Lance
    } else {
        Layout::None
    })
}

/// A file of `_delta_log/`.
enum LogEntry {
    Commit,
    Checkpoint,
    Other,
}

fn parse_log_name(name: &str) -> Option<(u64, LogEntry)> {
    let (v, rest) = name.split_at_checked(20)?;
    let v: u64 = v.parse().ok()?;
    let kind = match rest {
        ".json" => LogEntry::Commit,
        r if r.starts_with(".checkpoint.") && r.ends_with(".parquet") => LogEntry::Checkpoint,
        _ => LogEntry::Other,
    };
    Some((v, kind))
}

struct LogFiles {
    /// Commit files by version, with their modification times.
    commits: BTreeMap<u64, DateTime<Utc>>,
    /// Versions with a checkpoint.
    checkpoints: HashMap<u64, DateTime<Utc>>,
    /// Every versioned file (commits, checkpoints, checksums), with its version.
    all: Vec<(u64, Path)>,
}

async fn log_files(store: &dyn ObjectStore) -> Result<LogFiles> {
    let mut out = LogFiles {
        commits: BTreeMap::new(),
        checkpoints: HashMap::new(),
        all: Vec::new(),
    };
    let prefix = Path::from("_delta_log");
    let mut list = store.list(Some(&prefix));
    while let Some(m) = list.try_next().await? {
        let Some(name) = m.location.filename() else {
            continue;
        };
        let Some((v, kind)) = parse_log_name(name) else {
            continue;
        };
        match kind {
            LogEntry::Commit => {
                out.commits.insert(v, m.last_modified);
            }
            LogEntry::Checkpoint => {
                out.checkpoints.insert(v, m.last_modified);
            }
            LogEntry::Other => {}
        }
        out.all.push((v, m.location.clone()));
    }
    Ok(out)
}

/// Every version still loadable, oldest first: those with a commit file, and tagged versions
/// kept only as a checkpoint.
pub(crate) async fn versions(table: &DeltaTable) -> Result<Vec<VersionInfo>> {
    let files = log_files(table.object_store().as_ref()).await?;
    let oldest_commit = files.commits.keys().next().copied().unwrap_or(0);
    let mut out: BTreeMap<u64, DateTime<Utc>> = files.commits;
    for (v, t) in files.checkpoints {
        if v < oldest_commit {
            out.entry(v).or_insert(t);
        }
    }
    Ok(out
        .into_iter()
        .map(|(version, t)| VersionInfo {
            version,
            timestamp_micros: t.timestamp_micros(),
        })
        .collect())
}

fn tag_path(tag: &str) -> Path {
    Path::from(format!(
        "_kv0/tags/{}.json",
        crate::tracking::records::encode(tag)
    ))
}

fn not_found(e: &ObjectStoreError) -> bool {
    matches!(e, ObjectStoreError::NotFound { .. })
}

/// Tags and the versions they name.
pub(crate) async fn tags(table: &DeltaTable) -> Result<Vec<(String, u64)>> {
    let store = table.object_store();
    let prefix = Path::from("_kv0/tags");
    let found: Vec<_> = match store.list(Some(&prefix)).try_collect().await {
        Ok(f) => f,
        Err(e) if not_found(&e) => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    let mut out = Vec::new();
    for m in found {
        let Some(name) = m
            .location
            .filename()
            .and_then(|f| f.strip_suffix(".json"))
            .and_then(crate::tracking::records::decode)
        else {
            continue;
        };
        match read_tag(store.as_ref(), &m.location).await {
            Ok(v) => out.push((name, v)),
            // Deleted while listing.
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

async fn read_tag(store: &dyn ObjectStore, path: &Path) -> Result<u64> {
    let body = match store.get(path).await {
        Ok(r) => r.bytes().await?,
        Err(e) if not_found(&e) => return Err(Error::NotFound(format!("tag file {path}"))),
        Err(e) => return Err(e.into()),
    };
    let v: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| Error::Corruption(format!("tag file {path}: {e}")))?;
    v.get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::Corruption(format!("tag file {path} has no version")))
}

/// The version `tag` names.
pub(crate) async fn tag_version(table: &DeltaTable, tag: &str) -> Result<u64> {
    read_tag(table.object_store().as_ref(), &tag_path(tag))
        .await
        .map_err(|e| match e {
            Error::NotFound(_) => Error::NotFound(format!("tag {tag:?} does not exist")),
            e => e,
        })
}

/// Whether `e` says log files that a load or a commit read were deleted meanwhile, by a
/// cleanup or purge running at the same time: loading the latest version again gets past it.
pub(crate) fn log_cut(e: &DeltaTableError) -> bool {
    if matches!(e, DeltaTableError::InvalidVersion(_)) {
        return true;
    }
    let s = e.to_string();
    s.contains("No files in log segment")
        || s.contains("Gap between checkpoint")
        || s.contains("LogSegment end version")
        || s.contains("Path does not exist")
        || (s.contains("_delta_log") && (s.contains("FileNotFound") || s.contains("not found")))
}

/// Loads `version` (the latest when `None`) on a fresh handle, again (a few times) when a
/// concurrent cleanup deleted log files while they were read.
pub(crate) async fn load_at(log_store: LogStoreRef, version: Option<u64>) -> Result<DeltaTable> {
    let attempts = 6;
    let mut attempt = 0;
    loop {
        let mut t = DeltaTable::new(log_store.clone());
        let r = match version {
            Some(v) => t.load_version(v).await,
            None => t.load().await,
        };
        match r {
            Ok(()) => return Ok(t),
            Err(e) if log_cut(&e) && attempt + 1 < attempts => {
                attempt += 1;
                tracing::debug!(error = %e, attempt, "kv0: log files went away while loading; again");
                tokio::time::sleep(crate::storage::backoff(attempt, 20, 500)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Loads `version` of the table on a separate handle.
async fn load(table: &DeltaTable, version: u64) -> Result<DeltaTable> {
    load_at(table.log_store(), Some(version)).await
}

/// Writes a checkpoint at `version` unless one exists.
async fn ensure_checkpoint(table: &DeltaTable, version: u64, have: bool) -> Result<()> {
    if have {
        return Ok(());
    }
    let t = load(table, version).await?;
    deltalake_core::checkpoints::create_checkpoint(&t, None).await?;
    Ok(())
}

/// Tags `version`. Fails when the tag exists, or when a concurrent cleanup removed the version
/// first (the tag is then deleted again).
pub(crate) async fn create_tag(table: &DeltaTable, tag: &str, version: u64) -> Result<()> {
    if let Err(e) = load(table, version).await {
        return Err(Error::NotFound(format!("version {version}: {e}")));
    }
    let store = table.object_store();
    let path = tag_path(tag);
    let body = serde_json::json!({
        "version": version,
        "created_at_us": Utc::now().timestamp_micros(),
    });
    let put = store
        .put_opts(
            &path,
            PutPayload::from(serde_json::to_vec(&body)?),
            PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            },
        )
        .await;
    match put {
        Ok(_) => {}
        Err(ObjectStoreError::AlreadyExists { .. }) => {
            return Err(Error::InvalidArgument(format!(
                "tag {tag:?} already exists"
            )))
        }
        Err(e) => return Err(e.into()),
    }
    let files = log_files(store.as_ref()).await?;
    let checked = async {
        ensure_checkpoint(table, version, files.checkpoints.contains_key(&version)).await?;
        load(table, version).await
    }
    .await;
    if let Err(e) = checked {
        let _ = store.delete(&path).await;
        return Err(Error::NotFound(format!(
            "version {version} was removed while it was being tagged: {e}"
        )));
    }
    Ok(())
}

pub(crate) async fn delete_tag(table: &DeltaTable, tag: &str) -> Result<()> {
    let store = table.object_store();
    let path = tag_path(tag);
    // A missing tag is an error, as for a missing version.
    if let Err(e) = store.head(&path).await {
        return Err(if not_found(&e) {
            Error::NotFound(format!("tag {tag:?} does not exist"))
        } else {
            e.into()
        });
    }
    match store.delete(&path).await {
        Ok(()) => Ok(()),
        Err(e) if not_found(&e) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Paths of the data files `version` of the table references.
async fn file_paths(table: &DeltaTable, version: Option<u64>) -> Result<HashSet<Path>> {
    let t = match version {
        Some(v) => load(table, v).await?,
        None => table.clone(),
    };
    Ok(t.snapshot()?
        .log_data()
        .iter()
        .map(|f| f.object_store_path())
        .collect())
}

/// Paths of the data files that the commit files of `versions` add or remove: every file
/// those versions ever referenced.
async fn committed_paths(
    store: &dyn ObjectStore,
    versions: impl Iterator<Item = u64>,
) -> Result<HashSet<Path>> {
    let mut out = HashSet::new();
    for v in versions {
        let path = Path::from(format!("_delta_log/{v:020}.json"));
        let body = match store.get(&path).await {
            Ok(r) => r.bytes().await?,
            // Removed by a concurrent cleanup: its files are that cleanup's to delete.
            Err(e) if not_found(&e) => continue,
            Err(e) => return Err(e.into()),
        };
        for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let action: serde_json::Value = serde_json::from_slice(line)
                .map_err(|e| Error::Corruption(format!("{path}: {e}")))?;
            for kind in ["add", "remove"] {
                let Some(p) = action
                    .get(kind)
                    .and_then(|a| a.get("path"))
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                // Absolute paths point outside the table; kv0 never writes them.
                if p.contains("://") {
                    continue;
                }
                if let Ok(p) = Path::from_url_path(p) {
                    out.insert(p);
                }
            }
        }
    }
    Ok(out)
}

/// Deletes `paths` (missing ones are fine). Returns the bytes removed, as far as `sizes` knows
/// them.
async fn delete_all(
    store: &dyn ObjectStore,
    paths: &HashSet<Path>,
    sizes: &HashMap<Path, u64>,
) -> Result<u64> {
    let mut bytes = 0;
    for path in paths {
        match store.delete(path).await {
            Ok(()) => bytes += sizes.get(path).copied().unwrap_or(0),
            Err(e) if not_found(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(bytes)
}

/// Removes versions committed before `c.before` and the data files only they use. The latest
/// version and tagged versions stay loadable. Returns (versions, bytes) removed.
///
/// - Data files: delta-rs's vacuum, run as a dry run (so it commits nothing) in two modes: Lite
///   for files removed before `c.before`, Full for files no version references (a crashed
///   writer's uncommitted files) once they are older than `c.orphan_grace`. Both keep the files
///   of tagged versions. With `c.erase`, also every file the removed versions' commits added or
///   removed, however recent: those are known to be committed, so no writer is about to use
///   them.
/// - Log: the commit, checkpoint and checksum files below the oldest version kept, after
///   writing a checkpoint there and at every tagged version below it. A tagged version keeps
///   its checkpoint, from which it loads on its own.
///
/// Tags are listed before anything is deleted and again before the log is, so a tag created
/// meanwhile protects its version (its checkpoint is written by the tagging itself). A version
/// tagged after its data files were deleted fails the tag's own check only if its log is gone
/// too; that window is the time between the two listings.
pub(crate) async fn cleanup(table: &DeltaTable, c: Cleanup) -> Result<(u64, u64)> {
    // Another cleanup or purge running at the same time may delete log files this one is
    // reading; it then starts over from the latest version.
    let mut attempt = 0;
    loop {
        match cleanup_once(table, c).await {
            Err(e) if cut(&e) && attempt < 4 => {
                attempt += 1;
                tracing::debug!(error = %e, attempt, "kv0: a concurrent cleanup moved the log; cleaning up again");
                tokio::time::sleep(crate::storage::backoff(attempt, 20, 500)).await;
            }
            r => return r,
        }
    }
}

/// [`log_cut`] for an error already converted.
fn cut(e: &Error) -> bool {
    let s = e.to_string();
    s.contains("No files in log segment")
        || s.contains("Gap between checkpoint")
        || s.contains("LogSegment end version")
        || s.contains("Invalid table version")
        || s.contains("Path does not exist")
        || (s.contains("_delta_log") && (s.contains("FileNotFound") || s.contains("not found")))
}

async fn cleanup_once(table: &DeltaTable, c: Cleanup) -> Result<(u64, u64)> {
    let latest = load_at(table.log_store(), None).await?;
    let latest_version = latest.version().unwrap_or(0);
    let store = latest.object_store();
    let tagged: Vec<u64> = tags(&latest).await?.into_iter().map(|t| t.1).collect();
    let now = Utc::now();
    let age = (now - c.before).max(chrono::Duration::zero());
    let grace = chrono::Duration::from_std(c.orphan_grace)
        .map_err(|e| Error::InvalidArgument(format!("orphan_grace: {e}")))?;

    let mut doomed: HashSet<Path> = HashSet::new();
    for (mode, retention) in [(VacuumMode::Lite, age), (VacuumMode::Full, age.max(grace))] {
        let (_, m) = latest
            .clone()
            .vacuum()
            .with_mode(mode)
            .with_retention_period(retention)
            .with_enforce_retention_duration(false)
            .with_keep_versions(&tagged)
            .with_dry_run(true)
            .await?;
        doomed.extend(m.files_deleted.iter().map(|f| Path::from(f.as_str())));
    }
    let files = log_files(store.as_ref()).await?;
    if c.erase {
        // Only commits up to the version loaded: a newer one may add files that are live.
        let mut gone = committed_paths(
            store.as_ref(),
            files
                .commits
                .keys()
                .copied()
                .filter(|v| *v <= latest_version),
        )
        .await?;
        for p in file_paths(&latest, None).await? {
            gone.remove(&p);
        }
        for &v in &tagged {
            for p in file_paths(&latest, Some(v)).await? {
                gone.remove(&p);
            }
        }
        doomed.extend(gone);
    }
    let mut bytes = 0u64;
    if !doomed.is_empty() {
        let sizes: HashMap<Path, u64> = store
            .list_with_delimiter(None)
            .await?
            .objects
            .into_iter()
            .map(|m| (m.location, m.size))
            .collect();
        bytes = delete_all(store.as_ref(), &doomed, &sizes).await?;
    }

    // Commit files: everything before the oldest version committed at or after `c.before`
    // (the latest at most), once a checkpoint there makes them unnecessary.
    let Some(&newest) = files.commits.keys().next_back() else {
        return Ok((0, bytes));
    };
    let keep_from = files
        .commits
        .iter()
        .find(|(_, t)| **t >= c.before)
        .map_or(newest, |(v, _)| *v);
    if !files.all.iter().any(|(v, _)| *v < keep_from) {
        return Ok((0, bytes));
    }
    // Listed again right before the log goes: a tag created since the first listing keeps
    // its checkpoint too.
    let tagged_set: HashSet<u64> = tags(&latest)
        .await?
        .into_iter()
        .map(|t| t.1)
        .chain(tagged.iter().copied())
        .collect();
    for &t in &tagged_set {
        if t < keep_from {
            ensure_checkpoint(&latest, t, files.checkpoints.contains_key(&t)).await?;
        }
    }
    // Written last and always, so `_last_checkpoint` (which readers list the log from) names
    // `keep_from` and not an older checkpoint whose following commits are about to go.
    ensure_checkpoint(&latest, keep_from, false).await?;
    let mut versions = 0u64;
    for (v, path) in &files.all {
        if *v >= keep_from {
            continue;
        }
        let kind = parse_log_name(path.filename().unwrap_or_default()).map(|k| k.1);
        if tagged_set.contains(v) && matches!(kind, Some(LogEntry::Checkpoint)) {
            continue;
        }
        match store.delete(path).await {
            Ok(()) => {
                if matches!(kind, Some(LogEntry::Commit)) && !tagged_set.contains(v) {
                    versions += 1;
                }
            }
            Err(e) if not_found(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok((versions, bytes))
}
