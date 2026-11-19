//! The moraine engine for the tracking layer: a repository is one moraine database, each kv0
//! table one moraine table.
//!
//! - Tables keep kv0's logical (signed) columns. moraine stores Delta Lake tables, so a list's
//!   item field is called `element` there; batches are renamed on the way in.
//! - Every table is keyless and clustered by `run_id` (moraine sorts every file it writes by the
//!   cluster key, stably, so rows of one key keep their append order). `metrics` rows carry
//!   `run_id` too and are clustered by (`run_id`, `series_id`): a series is a few pages of a few
//!   files, and deleting a run needs no join.
//! - A writer's mark is a moraine transaction id (`kv0-wal/<writer id>`, its highest sequence
//!   number) on the commit holding its rows, so a replayed write lands at most once.
//! - Every handle opens the database with [`moraine::Consistency::Strong`]; kv0 decides when a
//!   handle moves to a newer version, as on the other engines.
//! - moraine table names are `[a-z0-9][a-z0-9_-]{0,63}`. A records table `records/<dir name>`
//!   becomes `rec-<base32 of the dir name>` (shortened with a hash when long); its directory name
//!   is kept in `_kv0/records/<table name>`, written before the table.
//! - A key-value database is one moraine database with one table, `ops` (see [`kv`]).
//! - Tags are moraine pins (see [`pins`]). moraine keeps no other old versions readable: a
//!   handle reads the latest version, or a tagged one.
//! - Maintenance is moraine's (compaction, checkpoints, log cleanup, vacuum on server time);
//!   a version cleanup or a purge that asks files to go sooner than moraine's grace period runs
//!   a moraine purge (see [`erase`]).

pub(crate) mod erase;
pub(crate) mod kv;
pub(crate) mod pins;
pub(crate) mod table;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions};

use super::format::{Kind, Marker, FORMAT_VERSION, MARKER};
use super::{is_local, non_empty, Engine, StorageOptions};
use crate::error::{Error, Result};

/// Where the directory names of records tables are kept.
const NAMES: &str = "_kv0/records";

/// The prefix of a records table's moraine name.
const RECORDS_PREFIX: &str = "rec-";

/// Characters of an encoded name kept before the hash, when the name is too long.
const KEPT: usize = 43;

/// The application id of a writer's marks.
pub(crate) fn wal_app_id(writer: &str) -> String {
    format!("kv0-wal/{writer}")
}

pub(crate) fn error(e: moraine::Error) -> Error {
    use moraine::Error as M;
    match e {
        M::TableNotFound(_) | M::PinNotFound(_) | M::VersionNotAvailable(_) => {
            Error::NotFound(e.to_string())
        }
        M::InvalidSchema(_)
        | M::InvalidBatch(_)
        | M::InvalidQuery(_)
        | M::ColumnNotFound(_)
        | M::InvalidConfig(_) => Error::InvalidArgument(e.to_string()),
        M::UnsupportedTableFeature(_) | M::ConditionalWritesUnsupported(_) => {
            Error::NotSupported(e.to_string())
        }
        // Another writer or maintainer got there first; trying again later succeeds.
        M::TooManyConflicts { .. }
        | M::TableExistsWithDifferentSchema(_)
        | M::FilesChanged(_)
        | M::Fenced { .. }
        | M::LeaseHeld { .. } => Error::Conflict(e.to_string()),
        // Named as a missing data file, which callers that plan again on the newest version
        // (optimize) recognise.
        M::SnapshotExpired => Error::NotFound(format!("{e} (a data file, .parquet, is missing)")),
        M::Corrupt { .. } | M::BlobCorrupt(_) => Error::Corruption(e.to_string()),
        M::Storage(ref s) if s.is_not_found() => Error::NotFound(e.to_string()),
        // Transient: the store was slow or unreachable, the outcome is unknown, or the table's
        // commit task stopped before it took the write (nothing was written).
        M::Storage(_)
        | M::Timeout(_)
        | M::Throttled { .. }
        | M::OutcomeUnknown { .. }
        | M::CommitDeadlineExceeded { .. }
        | M::Cancelled => Error::Io(e.to_string()),
        other => Error::Storage(Box::new(other)),
    }
}

/// One pass of moraine's upkeep for a kv0 table: checkpoint, log cleanup and vacuum (each
/// under its moraine lease). Compaction is kv0's call (`merge_small`, `optimize`), and kv0
/// builds no sidecars and queues no writes.
pub(crate) async fn maintain(table: &moraine::Table) -> Result<moraine::MaintenanceReport> {
    let opts = moraine::MaintainOptions::default()
        .build_indexes(None)
        .ingest(false)
        .compaction(None);
    table.maintain(&opts).await.map_err(error)
}

/// The repository at one location: its database, and the store kv0 keeps its own files in.
pub(crate) struct Repo {
    uri: String,
    db: moraine::Database,
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl Repo {
    /// Opens the repository at `uri`; `memory` is the store of a `memory://` repository, which
    /// every table of one handle shares. Creates the format marker when `marker` is set.
    pub(crate) async fn open(
        uri: &str,
        options: &StorageOptions,
        memory: Option<Arc<InMemory>>,
        marker: Option<Kind>,
    ) -> Result<Repo> {
        let options = store_options(options);
        let (store, prefix) = match memory {
            Some(m) => (m as Arc<dyn ObjectStore>, Path::from("kv0")),
            None => store_for(uri, &options)?,
        };
        if let Some(kind) = marker {
            write_marker(store.as_ref(), &prefix, kind).await?;
        }
        let armed = super::faults::armed();
        let builder = if uri.starts_with("memory://") || armed {
            // The test hooks wrap the store moraine would build; on local disk, the wrapped
            // store keeps moraine's create-if-absent support for local files.
            let inner: Arc<dyn ObjectStore> = if is_local(uri) {
                Arc::new(moraine::store::LocalCasStore::new(Arc::new(
                    object_store::local::LocalFileSystem::new(),
                )))
            } else {
                store.clone()
            };
            moraine::Database::builder_for_store(super::faults::wrap(inner), prefix.as_ref())
        } else {
            moraine::Database::builder(uri).storage_options(options)
        };
        let db = builder
            .consistency(moraine::Consistency::Strong)
            .open()
            .await
            .map_err(error)?;
        Ok(Repo {
            uri: uri.to_string(),
            db,
            store,
            prefix,
        })
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    /// Whether the repository is on local disk or in memory.
    pub(crate) fn is_local(&self) -> bool {
        is_local(&self.uri) || self.uri.starts_with("memory://")
    }

    pub(crate) fn database(&self) -> &moraine::Database {
        &self.db
    }

    /// The store kv0's own files (and moraine's data files, for direct reads) are reached in.
    pub(crate) fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The location of `rel` under the repository.
    pub(crate) fn path(&self, rel: &str) -> Path {
        rel.split('/')
            .filter(|p| !p.is_empty())
            .fold(self.prefix.clone(), |p, part| p.join(part))
    }

    /// Records the directory name of a records table before its table is created.
    pub(crate) async fn name_table(&self, kv0_name: &str) -> Result<()> {
        let Some(dir) = kv0_name.strip_prefix("records/") else {
            return Ok(());
        };
        let name = table_name(kv0_name);
        let put = self
            .store
            .put_opts(
                &self.path(&format!("{NAMES}/{name}")),
                Bytes::copy_from_slice(dir.as_bytes()).into(),
                PutOptions::from(PutMode::Create),
            )
            .await;
        match put {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Directory names of the records tables (the names under `records/`).
    pub(crate) async fn record_tables(&self) -> Result<Vec<String>> {
        let named: Vec<object_store::ObjectMeta> = self
            .store
            .list(Some(&self.path(NAMES)))
            .try_collect()
            .await?;
        if named.is_empty() {
            return Ok(Vec::new());
        }
        let tables = self.table_names().await?;
        let mut out = Vec::new();
        for m in named {
            let Some(name) = m.location.filename().map(str::to_string) else {
                continue;
            };
            if !tables.contains(&name) {
                // Named, but its creation has not finished (or failed).
                continue;
            }
            match decode_records_name(&name) {
                Some(dir) => out.push(dir),
                None => {
                    let body = self.store.get(&m.location).await?.bytes().await?;
                    out.push(
                        String::from_utf8(body.to_vec())
                            .map_err(|e| Error::Corruption(format!("{}: {e}", m.location)))?,
                    );
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// moraine tables at the location.
    async fn table_names(&self) -> Result<std::collections::HashSet<String>> {
        let listed = self.store.list_with_delimiter(Some(&self.prefix)).await?;
        Ok(listed
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_string))
            .filter(|n| !n.starts_with('_'))
            .collect())
    }
}

/// kv0's storage options as moraine's object store reads them: empty values dropped.
fn store_options(options: &StorageOptions) -> StorageOptions {
    non_empty(options)
}

/// The store and prefix of a location, for kv0's own files there.
pub(crate) fn store_for(
    uri: &str,
    options: &StorageOptions,
) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let bad = |e: String| Error::InvalidArgument(format!("{uri}: {e}"));
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
        for (k, v) in options {
            let key: object_store::aws::AmazonS3ConfigKey = k
                .parse()
                .map_err(|e: object_store::Error| bad(e.to_string()))?;
            b = b.with_config(key, v);
        }
        let store = b.build().map_err(|e| bad(e.to_string()))?;
        return Ok((Arc::new(store), Path::from(prefix)));
    }
    if !is_local(uri) {
        return Err(Error::NotSupported(format!(
            "{uri}: the moraine engine reaches local files, S3 and memory:// only (GCS and Azure \
             come later)"
        )));
    }
    let dir = uri.strip_prefix("file://").unwrap_or(uri);
    std::fs::create_dir_all(dir).map_err(|e| Error::Io(format!("{dir}: {e}")))?;
    let prefix = Path::from_filesystem_path(dir).map_err(|e| bad(e.to_string()))?;
    Ok((
        Arc::new(object_store::local::LocalFileSystem::new()),
        prefix,
    ))
}

/// Writes the format marker of a new moraine location, unless one exists.
async fn write_marker(store: &dyn ObjectStore, prefix: &Path, kind: Kind) -> Result<()> {
    let body = serde_json::to_vec(&Marker {
        engine: Engine::Moraine.name().into(),
        version: FORMAT_VERSION,
        kind: Some(kind.name().into()),
        created_by: Some(format!("kv0 {}", env!("CARGO_PKG_VERSION"))),
    })?;
    let path = MARKER
        .split('/')
        .fold(prefix.clone(), |p, part| p.join(part));
    let mut attempt = 0;
    loop {
        let put = store
            .put_opts(
                &path,
                Bytes::from(body.clone()).into(),
                PutOptions::from(PutMode::Create),
            )
            .await;
        match put {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => return Ok(()),
            Err(e) if attempt < 3 => {
                attempt += 1;
                tracing::debug!(attempt, error = %e, "kv0: writing the format marker failed; again");
                tokio::time::sleep(super::backoff(attempt, 50, 1_000)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// The moraine table holding kv0's table `kv0_name` (`runs`, `records/<dir name>`, ...).
pub(crate) fn table_name(kv0_name: &str) -> String {
    match kv0_name.strip_prefix("records/") {
        Some(dir) => {
            let enc = base32(dir.as_bytes());
            if RECORDS_PREFIX.len() + enc.len() <= 64 {
                format!("{RECORDS_PREFIX}{enc}")
            } else {
                let h = xxhash_rust::xxh3::xxh3_64(dir.as_bytes());
                format!("{RECORDS_PREFIX}{}-{h:016x}", &enc[..KEPT])
            }
        }
        None => kv0_name.to_string(),
    }
}

/// The directory name of the records table `name`, when it is not shortened.
fn decode_records_name(name: &str) -> Option<String> {
    let enc = name.strip_prefix(RECORDS_PREFIX)?;
    if enc.contains('-') {
        return None;
    }
    String::from_utf8(unbase32(enc)?).ok()
}

const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Lowercase base32 (RFC 4648 alphabet), without padding.
pub(super) fn base32(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let (mut buf, mut bits) = (0u32, 0u32);
    for &b in bytes {
        buf = (buf << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buf >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buf << (5 - bits)) & 31) as usize] as char);
    }
    out
}

pub(super) fn unbase32(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let (mut buf, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = ALPHABET.iter().position(|&a| a == c)? as u32;
        buf = (buf << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_names_are_valid_moraine_names_and_round_trip() {
        let valid = |n: &str| {
            n.len() <= 64
                && n.bytes()
                    .next()
                    .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                && n.bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-')
        };
        for dir in ["evals", "Ünïcode~2F名前", "a", "x".repeat(37).as_str()] {
            let n = table_name(&format!("records/{dir}"));
            assert!(valid(&n), "{n}");
            assert_eq!(decode_records_name(&n).as_deref(), Some(dir));
        }
        let long = "y".repeat(200);
        let n = table_name(&format!("records/{long}"));
        assert!(valid(&n), "{n}");
        assert_eq!(n.len(), 64);
        assert_eq!(decode_records_name(&n), None);
        assert_ne!(n, table_name(&format!("records/{long}z")));
        assert_eq!(table_name("metrics"), "metrics");
    }

    #[test]
    fn base32_round_trips() {
        for len in 0..12 {
            let b: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(unbase32(&base32(&b)).unwrap(), b);
        }
        assert_eq!(base32(b"foobar"), "mzxw6ytboi");
    }
}
