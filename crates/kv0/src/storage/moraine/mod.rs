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
//! - Maintenance (compaction, version cleanup), tags and reads of older versions are not
//!   implemented yet: they wait for moraine's maintenance and version pins.

pub(crate) mod table;

use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
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
        M::TableNotFound(_) => Error::NotFound(e.to_string()),
        M::InvalidSchema(_)
        | M::InvalidBatch(_)
        | M::InvalidQuery(_)
        | M::ColumnNotFound(_)
        | M::InvalidConfig(_) => Error::InvalidArgument(e.to_string()),
        M::UnsupportedTableFeature(_) | M::ConditionalWritesUnsupported(_) => {
            Error::NotSupported(e.to_string())
        }
        M::TooManyConflicts { .. } | M::TableExistsWithDifferentSchema(_) => {
            Error::Conflict(e.to_string())
        }
        M::CorruptLog(_) => Error::Corruption(e.to_string()),
        M::Storage(ref s) if matches!(s, object_store::Error::NotFound { .. }) => {
            Error::NotFound(e.to_string())
        }
        M::Storage(_)
        | M::Timeout(_)
        | M::Throttled { .. }
        | M::OutcomeUnknown { .. }
        | M::CommitDeadlineExceeded { .. } => Error::Io(e.to_string()),
        other => Error::Storage(Box::new(other)),
    }
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

    pub(crate) fn database(&self) -> &moraine::Database {
        &self.db
    }

    fn path(&self, rel: &str) -> Path {
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

    /// Whether the table exists: its first log entry does.
    pub(crate) async fn exists(&self, kv0_name: &str) -> Result<bool> {
        let first = format!("{}/data/_delta_log/{:020}.json", table_name(kv0_name), 0);
        match self.store.head(&self.path(&first)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes a table and every file of it.
    pub(crate) async fn remove(&self, kv0_name: &str) -> Result<()> {
        let name = table_name(kv0_name);
        let dir = self.path(&name);
        let files = self.store.list(Some(&dir)).map_ok(|m| m.location).boxed();
        self.store
            .delete_stream(files)
            .try_collect::<Vec<_>>()
            .await?;
        if kv0_name.starts_with("records/") {
            match self
                .store
                .delete(&self.path(&format!("{NAMES}/{name}")))
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// kv0's storage options as moraine's object store reads them: empty values dropped.
fn store_options(options: &StorageOptions) -> StorageOptions {
    non_empty(options)
}

/// The store and prefix of a location, for kv0's own files there.
fn store_for(uri: &str, options: &StorageOptions) -> Result<(Arc<dyn ObjectStore>, Path)> {
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
fn base32(bytes: &[u8]) -> String {
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

fn unbase32(s: &str) -> Option<Vec<u8>> {
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

    /// A prefix match finds exactly the rows starting with the prefix, wildcards and the escape
    /// character included, through moraine's file and page pruning.
    #[test]
    fn prefix_scans_match_literally() {
        use arrow_array_59::{Array as _, RecordBatch, StringArray};
        use arrow_schema_59::{DataType, Field, Schema};

        use crate::storage::pred::{to_expr, Pred};

        let ids = ["a_b1", "axb2", "a%c", "abc", r"a\d", "a_", "b_b1"];
        let got = crate::rt::block_on(async move {
            let db = moraine::Database::builder_for_store(Arc::new(InMemory::new()), "t")
                .consistency(moraine::Consistency::Strong)
                .open()
                .await
                .map_err(error)?;
            let schema = Arc::new(Schema::new(vec![Field::new(
                "run_id",
                DataType::Utf8,
                false,
            )]));
            let t = db
                .create_table(
                    "runs",
                    schema.clone(),
                    moraine::TableOptions {
                        cluster_by: vec!["run_id".into()],
                        ..Default::default()
                    },
                )
                .await
                .map_err(error)?;
            // One file per row, so file pruning has something to skip.
            for id in ids {
                let b = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec![id]))],
                )?;
                t.append(vec![b]).await.map_err(error)?;
            }
            let mut out = Vec::new();
            for prefix in ["a_", "a%", r"a\", "ab", "", "c"] {
                let e = to_expr(&Pred::StartsWith("run_id", prefix.into()), &schema);
                let mut found: Vec<String> = Vec::new();
                for b in t.scan().filter(e).collect().await.map_err(error)? {
                    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    found.extend((0..c.len()).map(|i| c.value(i).to_string()));
                }
                found.sort();
                out.push(found);
            }
            Ok::<_, Error>(out)
        })
        .unwrap();
        let want = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(got[0], want(&["a_", "a_b1"]));
        assert_eq!(got[1], want(&["a%c"]));
        assert_eq!(got[2], want(&[r"a\d"]));
        assert_eq!(got[3], want(&["abc"]));
        let mut all = want(&ids);
        all.sort();
        assert_eq!(got[4], all);
        assert!(got[5].is_empty());
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
