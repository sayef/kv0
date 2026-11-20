//! What a location holds, and whether this kv0 opens it.
//!
//! kv0 stores everything in moraine databases. A location carries a marker, `_kv0/format.json`,
//! written once when it is created:
//!
//! ```json
//! {"engine":"moraine","version":1,"kind":"database","created_by":"kv0 0.1.0-beta.3"}
//! ```
//!
//! Locations written by earlier kv0 releases, with the Lance or the Delta Lake engine, are
//! recognised by their marker or their layout, only to be refused: a Lance dataset
//! (`_versions/`) or a Delta log (`_delta_log/`) for a key-value database, `runs.lance/` or
//! `runs/_delta_log/` (and the other tables) for a tracking repository. A moraine location
//! without a marker is recognised by its tables' logs under `<table>/data/`. Detection costs
//! one listing of the location, plus one read when the marker is there.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::{is_local, StorageOptions};

/// The storage engine of a location. kv0 reads and writes moraine only; the other engines are
/// those of earlier releases, recognised so that their locations are refused rather than
/// overwritten.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Engine {
    /// Lance datasets: the format of kv0 0.1.0-beta.2 and earlier. Not readable.
    Lance,
    /// Delta Lake tables written by delta-rs, before moraine. Not readable.
    Delta,
    /// moraine databases: the only engine.
    Moraine,
}

impl Engine {
    /// The engine's name, as `KV0_ENGINE` and the marker spell it.
    pub fn name(self) -> &'static str {
        match self {
            Engine::Lance => "lance",
            Engine::Delta => "delta",
            Engine::Moraine => "moraine",
        }
    }

    /// The engine for new locations named by the environment: `KV0_ENGINE`, else
    /// `KV0_TEST_BACKEND` (what kv0's own test suites set). `None` when neither is set; an
    /// unknown name is an error, and only `moraine` can be used.
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
            "moraine" => Ok(Engine::Moraine),
            _ => Err(Error::InvalidArgument(format!(
                "unknown storage engine {s:?} (expected \"lance\", \"delta\" or \"moraine\")"
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
    pub(crate) fn name(self) -> &'static str {
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
/// The tables of a tracking repository, in moraine (`<table>/data/_delta_log/`) and in the
/// Delta Lake engine of earlier releases (`<table>/_delta_log/`).
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
    } else if has("ops")
        && list(&super::join(uri, "ops"), options)
            .await?
            .iter()
            .any(|n| n == "data")
    {
        // A moraine database: its one table `ops`, with the Delta log under `ops/data/`.
        Format::Database(Engine::Moraine)
    } else if LANCE_TABLES.iter().any(|t| has(t)) {
        Format::Repository(Engine::Lance)
    } else if let Some(t) = DELTA_TABLES.iter().find(|t| has(t)) {
        // moraine keeps a table's Delta log under `<table>/data/`, Delta under `<table>/`.
        if list(&super::join(uri, t), options)
            .await?
            .iter()
            .any(|n| n == "data")
        {
            Format::Repository(Engine::Moraine)
        } else {
            Format::Repository(Engine::Delta)
        }
    } else if names.is_empty() || names.iter().all(|n| n == "_kv0") {
        Format::Empty
    } else {
        Format::Unknown
    })
}

/// What a location of an earlier kv0 release holds, and what to do about it.
fn refused(uri: &str, kind: Kind, engine: Engine) -> Error {
    let name = match engine {
        Engine::Lance => "the Lance engine (kv0 0.1.0-beta.2 and earlier)",
        _ => "the Delta Lake engine of an earlier kv0",
    };
    Error::NotSupported(format!(
        "{uri} holds {} written with {name}, which this kv0 no longer reads. There is no \
         migration while kv0 is in beta: recreate it with this kv0 at a new location, reading \
         what you need to keep with the kv0 release that wrote it",
        kind.noun()
    ))
}

/// The engine that opens the location `uri` of `kind` (always moraine), and what the location
/// holds, or why it cannot be opened.
///
/// - A location written with the Lance or Delta Lake engine of an earlier release fails,
///   saying to recreate it.
/// - `wanted`, else the engine `KV0_ENGINE` names, must be moraine when given.
/// - A database location opened as a repository (or the reverse) fails.
pub(crate) async fn choose(
    uri: &str,
    options: &StorageOptions,
    kind: Kind,
    wanted: Option<Engine>,
) -> Result<(Engine, Format)> {
    let wanted = match wanted {
        Some(e) => Some(e),
        None => Engine::from_env()?,
    };
    if let Some(e @ (Engine::Lance | Engine::Delta)) = wanted {
        return Err(Error::NotSupported(format!(
            "the {e} engine was removed: kv0 stores every database and repository with moraine \
             (engine \"moraine\", or none)"
        )));
    }
    let format = detect_uri(uri, options).await?;
    match (format, kind) {
        (Format::Database(e), Kind::Database) | (Format::Repository(e), Kind::Repository) => {
            if e != Engine::Moraine {
                return Err(refused(uri, kind, e));
            }
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
        (Format::Empty | Format::Unknown, _) => {}
    }
    Ok((Engine::Moraine, format))
}

/// Attempts after the first for the marker's remote reads and listings (object stores fail
/// requests now and then).
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
    let (store, prefix) = super::moraine::store_for(uri, options)?;
    let mut attempt = 0;
    let listed = loop {
        match store.list_with_delimiter(Some(&prefix)).await {
            Ok(l) => break l,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
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

async fn read_remote(uri: &str, options: &StorageOptions) -> Result<Option<Vec<u8>>> {
    use object_store::ObjectStoreExt as _;
    let (store, prefix) = super::moraine::store_for(uri, options)?;
    let path = MARKER.split('/').fold(prefix, |p, part| p.join(part));
    let mut attempt = 0;
    loop {
        let got = match store.get(&path).await {
            Ok(r) => r.bytes().await.map(|b| Some(b.to_vec())),
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
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
        assert_eq!("MORAINE".parse::<Engine>().unwrap(), Engine::Moraine);
        assert_eq!(Engine::Moraine.to_string(), "moraine");
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
        assert_eq!(
            at(
                "repo-moraine",
                &["runs/data/_delta_log", "metrics/data/_delta_log"]
            ),
            Format::Repository(Engine::Moraine)
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
    fn earlier_engines_are_refused_and_mismatches_too() {
        let d = tempfile::tempdir().unwrap();
        let o = StorageOptions::new();
        let lance = d.path().join("lance");
        std::fs::create_dir_all(lance.join("_versions")).unwrap();
        let lance = lance.to_string_lossy().into_owned();
        let delta = d.path().join("delta");
        std::fs::create_dir_all(delta.join("runs/_delta_log")).unwrap();
        let delta = delta.to_string_lossy().into_owned();
        let repo = d.path().join("repo");
        std::fs::create_dir_all(repo.join("runs/data/_delta_log")).unwrap();
        let repo = repo.to_string_lossy().into_owned();
        let new = d.path().join("new").to_string_lossy().into_owned();
        crate::rt::block_on(async move {
            let ch = |u: &str, k, w| {
                let (u, o) = (u.to_string(), o.clone());
                async move { choose(&u, &o, k, w).await }
            };
            for (u, k) in [(&lance, Kind::Database), (&delta, Kind::Repository)] {
                let e = ch(u, k, Some(Engine::Moraine)).await.unwrap_err();
                assert!(
                    matches!(&e, Error::NotSupported(m) if m.contains("recreate")),
                    "{e}"
                );
            }
            for old in [Engine::Lance, Engine::Delta] {
                let e = ch(&new, Kind::Database, Some(old)).await.unwrap_err();
                assert!(
                    matches!(&e, Error::NotSupported(m) if m.contains("removed")),
                    "{e}"
                );
            }
            assert_eq!(
                ch(&repo, Kind::Repository, Some(Engine::Moraine))
                    .await
                    .unwrap(),
                (Engine::Moraine, Format::Repository(Engine::Moraine))
            );
            let e = ch(&repo, Kind::Database, Some(Engine::Moraine))
                .await
                .unwrap_err();
            assert!(
                matches!(&e, Error::InvalidArgument(m) if m.contains("tracking repository")),
                "{e}"
            );
            assert_eq!(
                ch(&new, Kind::Database, Some(Engine::Moraine))
                    .await
                    .unwrap(),
                (Engine::Moraine, Format::Empty)
            );
        });
    }
}
