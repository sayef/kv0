//! What a location holds, and which engine opens it.
//!
//! A location written by the Delta Lake engine carries a marker, `_kv0/format.json`, written
//! once when it is created:
//!
//! ```json
//! {"engine":"delta","version":1,"kind":"database","created_by":"kv0 0.1.0-beta.3"}
//! ```
//!
//! Each of its tables also records `kv0.format = delta/1` in its version-0 metadata. Locations
//! from before the marker are recognised by their layout: a Delta log (`_delta_log/`) or a
//! Lance dataset (`_versions/`) for a key-value database, `runs/` or `runs.lance/` (and the
//! other tables) for a tracking repository. Detection costs one listing of the location, plus
//! one read when the marker is there.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::{is_local, StorageOptions};

/// The storage engine of a location.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Engine {
    /// Lance datasets: the format of kv0 0.1.0-beta.2 and earlier.
    Lance,
    /// Delta Lake tables (delta-rs).
    Delta,
}

impl Engine {
    /// The engine's name, as `KV0_ENGINE` and the marker spell it.
    pub fn name(self) -> &'static str {
        match self {
            Engine::Lance => "lance",
            Engine::Delta => "delta",
        }
    }

    /// The engine for new locations named by the environment: `KV0_ENGINE` (`lance` or
    /// `delta`), else `KV0_TEST_BACKEND` (what kv0's own test suites set). `None` when neither
    /// is set; an unknown name is an error.
    pub fn from_env() -> Result<Option<Engine>> {
        for var in ["KV0_ENGINE", "KV0_TEST_BACKEND"] {
            if let Ok(v) = std::env::var(var) {
                if !v.is_empty() {
                    return v
                        .parse()
                        .map(Some)
                        .map_err(|e: Error| Error::InvalidArgument(format!("{var}: {e}")));
                }
            }
        }
        Ok(None)
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Engine {
    type Err = Error;

    fn from_str(s: &str) -> Result<Engine> {
        match s.to_ascii_lowercase().as_str() {
            "lance" => Ok(Engine::Lance),
            "delta" => Ok(Engine::Delta),
            _ => Err(Error::InvalidArgument(format!(
                "unknown storage engine {s:?} (expected \"lance\" or \"delta\")"
            ))),
        }
    }
}

/// What a location holds, as [`detect`] finds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// Nothing: the location does not exist or is empty. `memory://` locations always are,
    /// since each handle has its own.
    Empty,
    /// A key-value database ([`crate::kv::Db`]).
    Database(Engine),
    /// A tracking repository ([`crate::tracking::Repo`]).
    Repository(Engine),
    /// Something kv0 did not write.
    Unknown,
}

/// The format version this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;

/// Where the marker lives, relative to the location.
pub(crate) const MARKER: &str = "_kv0/format.json";

/// The body of `_kv0/format.json`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Marker {
    pub engine: String,
    pub version: u32,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
}

/// The kinds of location, as the marker names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Database,
    Repository,
}

impl Kind {
    #[cfg_attr(not(feature = "delta"), allow(dead_code))]
    fn name(self) -> &'static str {
        match self {
            Kind::Database => "database",
            Kind::Repository => "repository",
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Kind::Database => "a key-value database",
            Kind::Repository => "a tracking repository",
        }
    }
}

/// Tables of a Lance repository; any one of them makes a location a Lance repository.
const LANCE_TABLES: [&str; 5] = [
    "runs.lance",
    "params.lance",
    "series.lance",
    "metrics.lance",
    "records.lance",
];
/// The same for a Delta repository written before the marker existed.
const DELTA_TABLES: [&str; 4] = ["runs", "params", "series", "metrics"];

/// Finds what `location` holds (one listing, and one read when it carries a marker). A
/// marker from a newer kv0 (a format version above [`FORMAT_VERSION`]) is an error.
pub fn detect(location: &str, options: &StorageOptions) -> Result<Format> {
    let uri = crate::storage::normalize_uri(location)?;
    let o = options.clone();
    crate::rt::block_on(async move { detect_uri(&uri, &o).await })
}

/// [`detect`] for a normalised location.
pub(crate) async fn detect_uri(uri: &str, options: &StorageOptions) -> Result<Format> {
    if uri.starts_with("memory://") {
        return Ok(Format::Empty);
    }
    let names = list(uri, options).await?;
    let has = |n: &str| names.iter().any(|x| x == n);
    if has("_kv0") {
        if let Some(m) = read_marker(uri, options).await? {
            let engine: Engine = m.engine.parse().map_err(|_| {
                Error::NotSupported(format!(
                    "{uri} was written by {} with the storage engine {:?}, which this kv0 does not know",
                    m.created_by.as_deref().unwrap_or("another kv0"),
                    m.engine
                ))
            })?;
            if m.version > FORMAT_VERSION {
                return Err(Error::NotSupported(format!(
                    "{uri} was written by {} in {} format version {}; this kv0 ({}) reads version {} and earlier. Upgrade kv0",
                    m.created_by.as_deref().unwrap_or("a newer kv0"),
                    engine,
                    m.version,
                    env!("CARGO_PKG_VERSION"),
                    FORMAT_VERSION
                )));
            }
            match m.kind.as_deref() {
                Some("database") => return Ok(Format::Database(engine)),
                Some("repository") => return Ok(Format::Repository(engine)),
                // A marker without a kind: the layout decides below.
                _ => {}
            }
        }
    }
    Ok(if has("_delta_log") {
        Format::Database(Engine::Delta)
    } else if has("_versions") {
        Format::Database(Engine::Lance)
    } else if LANCE_TABLES.iter().any(|t| has(t)) {
        Format::Repository(Engine::Lance)
    } else if DELTA_TABLES.iter().any(|t| has(t)) {
        Format::Repository(Engine::Delta)
    } else if names.is_empty() || names.iter().all(|n| n == "_kv0") {
        Format::Empty
    } else {
        Format::Unknown
    })
}

/// The engine that opens the location `uri` of `kind`, or why none can.
///
/// - An existing location keeps the engine it was written with. When `wanted` names another
///   engine, the open fails: a Lance location asked to open with Delta points to `kv0 migrate`.
/// - A new (empty or unrecognised) location uses `wanted`, else the engine `KV0_ENGINE` names,
///   else Lance.
/// - A database location opened as a repository (or the reverse) fails.
/// - A build without the `delta` feature refuses Delta locations.
pub(crate) async fn choose(
    uri: &str,
    options: &StorageOptions,
    kind: Kind,
    wanted: Option<Engine>,
) -> Result<(Engine, Format)> {
    let format = detect_uri(uri, options).await?;
    let found = match (format, kind) {
        (Format::Database(e), Kind::Database) | (Format::Repository(e), Kind::Repository) => {
            Some(e)
        }
        (Format::Database(_), Kind::Repository) | (Format::Repository(_), Kind::Database) => {
            let other = if kind == Kind::Database {
                Kind::Repository
            } else {
                Kind::Database
            };
            return Err(Error::InvalidArgument(format!(
                "{uri} holds {}, not {}",
                other.noun(),
                kind.noun()
            )));
        }
        (Format::Empty | Format::Unknown, _) => None,
    };
    let engine = match (found, wanted) {
        (Some(Engine::Lance), Some(Engine::Delta)) => {
            return Err(Error::NotSupported(format!(
                "{uri} holds {} in the Lance format (kv0 0.1.0-beta.2 and earlier), not Delta Lake. \
                 Convert it with `kv0 migrate {uri} <new location>` (cargo install kv0 --features lance-migrate); \
                 see docs/guides/migration.md",
                kind.noun()
            )));
        }
        (Some(Engine::Delta), Some(Engine::Lance)) => {
            return Err(Error::NotSupported(format!(
                "{uri} holds {} in the Delta Lake format; the Lance engine cannot open it",
                kind.noun()
            )));
        }
        (Some(e), _) => e,
        (None, Some(e)) => e,
        (None, None) => Engine::from_env()?.unwrap_or(Engine::Lance),
    };
    if engine == Engine::Delta && !cfg!(feature = "delta") {
        return Err(Error::NotSupported(if found.is_some() {
            format!(
                "{uri} holds {} in the Delta Lake format; this build of kv0 has only the Lance engine (enable the `delta` feature)",
                kind.noun()
            )
        } else {
            "the Delta Lake engine was chosen, but this build of kv0 has only the Lance engine (enable the `delta` feature)".to_string()
        }));
    }
    Ok((engine, format))
}

/// Writes the marker of a new Delta location of `kind`, unless one exists. Done before the
/// first table is created, so a concurrent opener (or one after a crash in between) already
/// sees the engine. `memory://` locations get none: nothing else can see them.
#[cfg(feature = "delta")]
pub(crate) async fn write_marker(uri: &str, options: &StorageOptions, kind: Kind) -> Result<()> {
    use deltalake_core::logstore::object_store::{PutMode, PutOptions};
    use deltalake_core::{ObjectStoreError, Path};
    if uri.starts_with("memory://") {
        return Ok(());
    }
    if is_local(uri) {
        let dir = uri.strip_prefix("file://").unwrap_or(uri);
        std::fs::create_dir_all(dir).map_err(|e| Error::Io(format!("{dir}: {e}")))?;
    }
    let body = serde_json::to_vec(&Marker {
        engine: Engine::Delta.name().into(),
        version: FORMAT_VERSION,
        kind: Some(kind.name().into()),
        created_by: Some(format!("kv0 {}", env!("CARGO_PKG_VERSION"))),
    })?;
    let store = super::delta::table::handle(uri, options)?.object_store();
    let mut attempt = 0;
    loop {
        let put = store
            .put_opts(
                &Path::from(MARKER),
                body.clone().into(),
                PutOptions::from(PutMode::Create),
            )
            .await;
        match put {
            Ok(_) | Err(ObjectStoreError::AlreadyExists { .. }) => return Ok(()),
            Err(e) if attempt < RETRIES => {
                attempt += 1;
                tracing::debug!(uri, attempt, error = %e, "kv0: writing the format marker failed; again");
                tokio::time::sleep(crate::storage::backoff(attempt, 50, 1_000)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Attempts after the first for the marker's remote reads and writes (object stores fail
/// requests now and then).
#[cfg(feature = "delta")]
const RETRIES: u32 = 3;

/// Names directly under `uri` (empty when it does not exist).
pub(crate) async fn list(uri: &str, options: &StorageOptions) -> Result<Vec<String>> {
    if is_local(uri) {
        let path = uri.strip_prefix("file://").unwrap_or(uri);
        return match std::fs::read_dir(path) {
            Ok(rd) => Ok(rd
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(Error::Io(format!("{path}: {e}"))),
        };
    }
    #[cfg(feature = "delta")]
    {
        use deltalake_core::ObjectStoreError;
        let store = super::delta::table::handle(uri, options)?.object_store();
        let mut attempt = 0;
        let listed = loop {
            match store.list_with_delimiter(None).await {
                Ok(l) => break l,
                Err(ObjectStoreError::NotFound { .. }) => return Ok(Vec::new()),
                Err(e) if attempt < RETRIES => {
                    attempt += 1;
                    tracing::debug!(uri, attempt, error = %e, "kv0: listing a location failed; again");
                    tokio::time::sleep(crate::storage::backoff(attempt, 50, 1_000)).await;
                }
                Err(e) => return Err(e.into()),
            }
        };
        Ok(listed
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_string))
            .chain(
                listed
                    .objects
                    .iter()
                    .filter_map(|o| o.location.filename().map(str::to_string)),
            )
            .collect())
    }
    #[cfg(not(feature = "delta"))]
    super::list_dir(uri, options).await
}

/// The marker of `uri`, when it has one.
pub(crate) async fn read_marker(uri: &str, options: &StorageOptions) -> Result<Option<Marker>> {
    let body: Option<Vec<u8>> = if is_local(uri) {
        let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri)).join(MARKER);
        match std::fs::read(&path) {
            Ok(b) => Some(b),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(Error::Io(format!("{}: {e}", path.display()))),
        }
    } else {
        read_remote(uri, options).await?
    };
    match body {
        None => Ok(None),
        Some(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| Error::Corruption(format!("{uri}/{MARKER}: {e}"))),
    }
}

#[cfg(feature = "delta")]
async fn read_remote(uri: &str, options: &StorageOptions) -> Result<Option<Vec<u8>>> {
    use deltalake_core::logstore::object_store::ObjectStoreExt;
    use deltalake_core::{ObjectStoreError, Path};
    let store = super::delta::table::handle(uri, options)?.object_store();
    let mut attempt = 0;
    loop {
        let got = match store.get(&Path::from(MARKER)).await {
            Ok(r) => r.bytes().await.map(|b| Some(b.to_vec())),
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(e) => Err(e),
        };
        match got {
            Ok(b) => return Ok(b),
            Err(e) if attempt < RETRIES => {
                attempt += 1;
                tracing::debug!(uri, attempt, error = %e, "kv0: reading the format marker failed; again");
                tokio::time::sleep(crate::storage::backoff(attempt, 50, 1_000)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(not(feature = "delta"))]
async fn read_remote(uri: &str, options: &StorageOptions) -> Result<Option<Vec<u8>>> {
    super::lance::read_file(&super::join(uri, MARKER), options).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engines_parse_and_print() {
        assert_eq!("delta".parse::<Engine>().unwrap(), Engine::Delta);
        assert_eq!("Lance".parse::<Engine>().unwrap(), Engine::Lance);
        assert!(matches!(
            "parquet".parse::<Engine>(),
            Err(Error::InvalidArgument(_))
        ));
        assert_eq!(Engine::Delta.to_string(), "delta");
    }

    #[test]
    fn layouts_are_recognised() {
        let d = tempfile::tempdir().unwrap();
        let at = |name: &str, dirs: &[&str]| {
            let p = d.path().join(name);
            for x in dirs {
                std::fs::create_dir_all(p.join(x)).unwrap();
            }
            detect(&p.to_string_lossy(), &StorageOptions::new()).unwrap()
        };
        assert_eq!(at("none", &[]), Format::Empty);
        assert_eq!(at("empty", &[""]), Format::Empty);
        assert_eq!(
            at("kv-delta", &["_delta_log"]),
            Format::Database(Engine::Delta)
        );
        assert_eq!(
            at("kv-lance", &["_versions", "data"]),
            Format::Database(Engine::Lance)
        );
        assert_eq!(
            at("repo-lance", &["runs.lance"]),
            Format::Repository(Engine::Lance)
        );
        assert_eq!(
            at("legacy-only", &["records.lance"]),
            Format::Repository(Engine::Lance)
        );
        assert_eq!(
            at("repo-delta", &["runs/_delta_log"]),
            Format::Repository(Engine::Delta)
        );
        assert_eq!(at("other", &["photos"]), Format::Unknown);
        assert_eq!(
            detect("memory://x", &StorageOptions::new()).unwrap(),
            Format::Empty
        );
    }

    #[test]
    fn the_marker_decides_and_newer_versions_are_refused() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("loc");
        std::fs::create_dir_all(p.join("_kv0")).unwrap();
        let write = |body: &str| std::fs::write(p.join(MARKER), body).unwrap();
        let det = || detect(&p.to_string_lossy(), &StorageOptions::new());
        write(r#"{"engine":"delta","version":1,"kind":"repository","created_by":"kv0 x"}"#);
        assert_eq!(det().unwrap(), Format::Repository(Engine::Delta));
        write(r#"{"engine":"delta","version":1,"kind":"database"}"#);
        assert_eq!(det().unwrap(), Format::Database(Engine::Delta));
        // Only the marker: still nothing else, so the layout falls back to empty.
        write(r#"{"engine":"delta","version":1}"#);
        assert_eq!(det().unwrap(), Format::Empty);
        write(r#"{"engine":"delta","version":2,"kind":"database","created_by":"kv0 9.0.0"}"#);
        let e = det().unwrap_err().to_string();
        assert!(
            e.contains("format version 2") && e.contains("kv0 9.0.0"),
            "{e}"
        );
        write(r#"{"engine":"rocks","version":1}"#);
        assert!(matches!(det(), Err(Error::NotSupported(_))));
        write("not json");
        assert!(matches!(det(), Err(Error::Corruption(_))));
    }

    #[test]
    fn engines_are_chosen_and_mismatches_refused() {
        let d = tempfile::tempdir().unwrap();
        let o = StorageOptions::new();
        let lance = d.path().join("lance");
        std::fs::create_dir_all(lance.join("_versions")).unwrap();
        let lance = lance.to_string_lossy().into_owned();
        let repo = d.path().join("repo");
        std::fs::create_dir_all(repo.join("runs.lance")).unwrap();
        let repo = repo.to_string_lossy().into_owned();
        let new = d.path().join("new").to_string_lossy().into_owned();
        crate::rt::block_on(async move {
            let ch = |u: &str, k, w| {
                let (u, o) = (u.to_string(), o.clone());
                async move { choose(&u, &o, k, w).await }
            };
            assert_eq!(
                ch(&lance, Kind::Database, None).await.unwrap().0,
                Engine::Lance
            );
            assert_eq!(
                ch(&lance, Kind::Database, Some(Engine::Lance))
                    .await
                    .unwrap()
                    .0,
                Engine::Lance
            );
            let e = ch(&lance, Kind::Database, Some(Engine::Delta))
                .await
                .unwrap_err();
            assert!(
                matches!(&e, Error::NotSupported(m) if m.contains("kv0 migrate")),
                "{e}"
            );
            let e = ch(&repo, Kind::Database, None).await.unwrap_err();
            assert!(
                matches!(&e, Error::InvalidArgument(m) if m.contains("tracking repository")),
                "{e}"
            );
            let e = ch(&lance, Kind::Repository, None).await.unwrap_err();
            assert!(matches!(&e, Error::InvalidArgument(_)), "{e}");
            assert_eq!(
                ch(&new, Kind::Database, Some(Engine::Lance))
                    .await
                    .unwrap()
                    .1,
                Format::Empty
            );
            let delta = ch(&new, Kind::Database, Some(Engine::Delta)).await;
            if cfg!(feature = "delta") {
                assert_eq!(delta.unwrap().0, Engine::Delta);
            } else {
                assert!(matches!(delta, Err(Error::NotSupported(_))));
            }
        });
    }
}
