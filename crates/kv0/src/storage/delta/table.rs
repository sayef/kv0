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

use chrono::{DateTime, Utc};
use deltalake_core::logstore::object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::{DeltaTable, DeltaTableBuilder, ObjectStore, ObjectStoreError, Path};
use futures::TryStreamExt;
use url::Url;

use crate::error::{Error, Result};
use crate::kv::VersionInfo;
use crate::storage::{is_local, non_empty, StorageOptions};

/// Unreferenced files younger than this are never deleted: a writer may still be about to
/// commit them.
const INFLIGHT_GRACE: chrono::Duration = chrono::Duration::hours(1);

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
    super::init();
    Ok(DeltaTableBuilder::from_url(url(uri)?)?
        .with_storage_options(non_empty(options))
        .build()?)
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

/// Loads `version` of the table on a separate handle.
async fn load(table: &DeltaTable, version: u64) -> Result<DeltaTable> {
    let mut t = DeltaTable::new(table.log_store());
    t.load_version(version).await?;
    Ok(t)
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

/// Removes versions committed before `before` and the data files only they use. The latest
/// version and tagged versions stay loadable. Returns (versions, bytes) removed.
pub(crate) async fn cleanup(table: &DeltaTable, before: DateTime<Utc>) -> Result<(u64, u64)> {
    let mut latest = DeltaTable::new(table.log_store());
    latest.load().await?;
    let store = latest.object_store();
    // Listed right before deleting anything, so a tag created meanwhile protects its version.
    let tagged: Vec<u64> = tags(&latest).await?.into_iter().map(|t| t.1).collect();
    let tagged_set: HashSet<u64> = tagged.iter().copied().collect();
    let now = Utc::now();
    let age = (now - before).max(chrono::Duration::zero());

    // Data files: those removed before `before` (and not used by a tagged version), then
    // files no version references that are older than the in-flight grace period.
    let mut doomed: HashSet<String> = HashSet::new();
    for (mode, retention) in [
        (VacuumMode::Lite, age),
        (VacuumMode::Full, age.max(INFLIGHT_GRACE)),
    ] {
        let (_, m) = latest
            .clone()
            .vacuum()
            .with_mode(mode)
            .with_retention_period(retention)
            .with_enforce_retention_duration(false)
            .with_keep_versions(&tagged)
            .with_dry_run(true)
            .await?;
        doomed.extend(m.files_deleted);
    }
    let mut bytes = 0u64;
    if !doomed.is_empty() {
        let sizes: HashMap<String, u64> = store
            .list_with_delimiter(None)
            .await?
            .objects
            .into_iter()
            .map(|m| (m.location.to_string(), m.size))
            .collect();
        for f in &doomed {
            let path = Path::from(f.as_str());
            match store.delete(&path).await {
                Ok(()) => bytes += sizes.get(path.as_ref()).copied().unwrap_or(0),
                Err(e) if not_found(&e) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }

    // Commit files: everything before the oldest version committed at or after `before`
    // (the latest at most), once a checkpoint there makes them unnecessary. Tagged versions
    // keep their checkpoint.
    let files = log_files(store.as_ref()).await?;
    let Some(&newest) = files.commits.keys().next_back() else {
        return Ok((0, bytes));
    };
    let keep_from = files
        .commits
        .iter()
        .find(|(_, t)| **t >= before)
        .map_or(newest, |(v, _)| *v);
    if !files.all.iter().any(|(v, _)| *v < keep_from) {
        return Ok((0, bytes));
    }
    ensure_checkpoint(
        &latest,
        keep_from,
        files.checkpoints.contains_key(&keep_from),
    )
    .await?;
    for &t in &tagged {
        if t < keep_from {
            ensure_checkpoint(&latest, t, files.checkpoints.contains_key(&t)).await?;
        }
    }
    let mut versions = 0u64;
    for (v, path) in &files.all {
        if *v >= keep_from {
            continue;
        }
        let name = path.filename().unwrap_or_default();
        let tag_checkpoint = tagged_set.contains(v)
            && matches!(parse_log_name(name), Some((_, LogEntry::Checkpoint)));
        if tag_checkpoint {
            continue;
        }
        match store.delete(path).await {
            Ok(()) => {
                if matches!(parse_log_name(name), Some((_, LogEntry::Commit)))
                    && !tagged_set.contains(v)
                {
                    versions += 1;
                }
            }
            Err(e) if not_found(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok((versions, bytes))
}
