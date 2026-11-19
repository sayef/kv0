//! Format detection, the format marker and the choice of engine: what `storage::detect`
//! reports for each kind of location, and the errors for locations written with the Lance or
//! Delta Lake engines of earlier releases, which this kv0 refuses without writing anything.
//! Runs against S3 too when `KV0_TEST_S3_ENDPOINT` is set.

use std::collections::HashMap;
use std::path::Path;

use kv0::kv::{Db, Options};
use kv0::rt::block_on;
use kv0::storage::{detect, Engine, Format};
use kv0::tracking::{Repo, RepoOptions};
use kv0::Error;

fn s3() -> Option<(String, HashMap<String, String>)> {
    let endpoint = std::env::var("KV0_TEST_S3_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())?;
    let bucket = std::env::var("KV0_TEST_S3_BUCKET").unwrap_or_else(|_| "kv0-test".into());
    let opts = HashMap::from([
        ("aws_endpoint".to_string(), endpoint),
        ("aws_access_key_id".to_string(), "minioadmin".to_string()),
        (
            "aws_secret_access_key".to_string(),
            "minioadmin".to_string(),
        ),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("allow_http".to_string(), "true".to_string()),
    ]);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some((format!("s3://{bucket}/format-{nanos:x}"), opts))
}

fn kv_opts(wal: &Path, storage: &HashMap<String, String>, engine: Option<Engine>) -> Options {
    Options {
        wal_dir: Some(wal.to_path_buf()),
        flush_interval: None,
        refresh_interval: None,
        storage_options: storage.clone(),
        engine,
        ..Options::default()
    }
}

fn repo_opts(wal: &Path, storage: &HashMap<String, String>, engine: Option<Engine>) -> RepoOptions {
    RepoOptions {
        wal_dir: Some(wal.to_path_buf()),
        flush_interval: None,
        storage_options: storage.clone(),
        engine,
        ..RepoOptions::default()
    }
}

fn open_repo(uri: &str, o: RepoOptions) -> kv0::Result<Repo> {
    let uri = uri.to_string();
    block_on(async move { Repo::open(&uri, o).await })
}

fn close_repo(r: Repo) {
    block_on(async move { r.close().await }).unwrap();
}

fn not_supported(e: Error, needle: &str) {
    match &e {
        Error::NotSupported(m) if m.contains(needle) => {}
        _ => panic!("expected NotSupported mentioning {needle:?}, got {e}"),
    }
}

/// Every check, at `base` (a local directory or an S3 prefix).
fn formats_are_detected_and_earlier_engines_refused(base: &str, storage: &HashMap<String, String>) {
    let wal = tempfile::tempdir().unwrap();
    let at = |name: &str| format!("{}/{name}", base.trim_end_matches('/'));
    let det = |uri: &str| detect(uri, storage).unwrap();

    assert_eq!(det(&at("nothing")), Format::Empty);

    let db_uri = at("db");
    let db = Db::open(&db_uri, kv_opts(&wal.path().join("1"), storage, None)).unwrap();
    db.put(b"k", b"v").unwrap();
    db.close().unwrap();
    assert_eq!(det(&db_uri), Format::Database(Engine::Moraine));
    let db = Db::open(
        &db_uri,
        kv_opts(&wal.path().join("2"), storage, Some(Engine::Moraine)),
    )
    .unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    db.close().unwrap();

    let repo_uri = at("repo");
    close_repo(open_repo(&repo_uri, repo_opts(&wal.path().join("3"), storage, None)).unwrap());
    assert_eq!(det(&repo_uri), Format::Repository(Engine::Moraine));

    // The earlier engines cannot be asked for, for an existing location or a new one.
    for old in [Engine::Lance, Engine::Delta] {
        let e = Db::open(&db_uri, kv_opts(&wal.path().join("4"), storage, Some(old))).unwrap_err();
        not_supported(e, "was removed");
        let e = open_repo(
            &at("new-repo"),
            repo_opts(&wal.path().join("5"), storage, Some(old)),
        )
        .err()
        .unwrap();
        not_supported(e, "was removed");
    }
    assert_eq!(det(&at("new-repo")), Format::Empty);

    // A database is not a repository, and the reverse.
    let e = Db::open(&repo_uri, kv_opts(&wal.path().join("6"), storage, None)).unwrap_err();
    assert!(
        matches!(&e, Error::InvalidArgument(m) if m.contains("tracking repository")),
        "{e}"
    );
    let e = open_repo(&db_uri, repo_opts(&wal.path().join("7"), storage, None))
        .err()
        .unwrap();
    assert!(
        matches!(&e, Error::InvalidArgument(m) if m.contains("key-value database")),
        "{e}"
    );

    // A read-only open of nothing creates nothing, marker included.
    let e = Db::open(
        &at("absent"),
        Options {
            create_if_missing: false,
            ..kv_opts(&wal.path().join("8"), storage, None)
        },
    )
    .unwrap_err();
    assert!(matches!(e, Error::NotFound(_)), "{e}");
    assert_eq!(det(&at("absent")), Format::Empty);
}

#[test]
fn local_formats_are_detected_and_earlier_engines_refused() {
    let d = tempfile::tempdir().unwrap();
    formats_are_detected_and_earlier_engines_refused(&d.path().to_string_lossy(), &HashMap::new());
}

#[test]
fn s3_formats_are_detected_and_earlier_engines_refused() {
    let Some((base, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    formats_are_detected_and_earlier_engines_refused(&base, &storage);
}

/// Every file under `dir`, relative, sorted.
fn files(dir: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(root, &p, out);
            } else {
                out.push(p.strip_prefix(root).unwrap().to_string_lossy().into_owned());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let dst = to.join(e.file_name());
        if e.path().is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).unwrap();
        }
    }
}

/// A key-value database and a tracking repository written by kv0 0.1.0-beta.2 (Lance) are
/// recognised, and opening them fails with the way out, leaving them as they were.
#[test]
fn lance_locations_of_beta_2_are_refused_untouched() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lance-unsigned");
    let d = tempfile::tempdir().unwrap();
    copy_dir(&fixtures, d.path());
    let o = HashMap::new();
    let kv = d.path().join("kv");
    let repo = d.path().join("repo");
    assert_eq!(
        detect(&kv.to_string_lossy(), &o).unwrap(),
        Format::Database(Engine::Lance)
    );
    assert_eq!(
        detect(&repo.to_string_lossy(), &o).unwrap(),
        Format::Repository(Engine::Lance)
    );
    let (kv_before, repo_before) = (files(&kv), files(&repo));
    let wal = d.path().join("wal");
    let e = Db::open(&kv.to_string_lossy(), kv_opts(&wal, &o, None)).unwrap_err();
    not_supported(e, "recreate it");
    let e = open_repo(&repo.to_string_lossy(), repo_opts(&wal, &o, None))
        .err()
        .unwrap();
    not_supported(e, "Lance engine");
    assert_eq!(files(&kv), kv_before, "nothing written");
    assert_eq!(files(&repo), repo_before, "nothing written");
}

fn fake_delta_table(dir: &Path) {
    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    std::fs::write(log.join("00000000000000000000.json"), b"{}\n").unwrap();
}

/// Locations of the Delta Lake engine of earlier releases are refused as well.
#[test]
fn delta_engine_locations_are_refused_untouched() {
    let d = tempfile::tempdir().unwrap();
    let o = HashMap::new();
    let db = d.path().join("db");
    fake_delta_table(&db);
    let wal = d.path().join("wal");
    let e = Db::open(&db.to_string_lossy(), kv_opts(&wal, &o, None)).unwrap_err();
    not_supported(e, "Delta Lake engine");
    assert_eq!(files(&db), vec!["_delta_log/00000000000000000000.json"]);

    let repo = d.path().join("repo");
    fake_delta_table(&repo.join("runs"));
    let e = open_repo(&repo.to_string_lossy(), repo_opts(&wal, &o, None))
        .err()
        .unwrap();
    not_supported(e, "recreate it");
    assert_eq!(
        files(&repo),
        vec!["runs/_delta_log/00000000000000000000.json"]
    );

    // A Delta-engine marker says the same without a layout to look at.
    let marked = d.path().join("marked");
    std::fs::create_dir_all(marked.join("_kv0")).unwrap();
    std::fs::write(
        marked.join("_kv0/format.json"),
        r#"{"engine":"delta","version":1,"kind":"database","created_by":"kv0 0.1.0-beta.3"}"#,
    )
    .unwrap();
    let e = Db::open(&marked.to_string_lossy(), kv_opts(&wal, &o, None)).unwrap_err();
    not_supported(e, "Delta Lake engine");
}

#[derive(serde::Deserialize)]
struct Marker {
    engine: String,
    version: u32,
    kind: String,
    created_by: String,
}

fn marker(dir: &Path) -> Marker {
    serde_json::from_slice(&std::fs::read(dir.join("_kv0/format.json")).unwrap()).unwrap()
}

#[test]
fn new_locations_get_the_marker() {
    let d = tempfile::tempdir().unwrap();
    let o = HashMap::new();
    let db = d.path().join("db");
    Db::open(
        &db.to_string_lossy(),
        kv_opts(&d.path().join("wal"), &o, None),
    )
    .unwrap()
    .close()
    .unwrap();
    let m = marker(&db);
    assert_eq!(
        (m.engine.as_str(), m.version, m.kind.as_str()),
        ("moraine", 1, "database")
    );
    assert_eq!(m.created_by, format!("kv0 {}", env!("CARGO_PKG_VERSION")));
    let repo = d.path().join("repo");
    close_repo(
        open_repo(
            &repo.to_string_lossy(),
            repo_opts(&d.path().join("wal"), &o, None),
        )
        .unwrap(),
    );
    let m = marker(&repo);
    assert_eq!(
        (m.engine.as_str(), m.kind.as_str()),
        ("moraine", "repository")
    );
}

#[test]
fn a_marker_from_a_newer_kv0_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    std::fs::create_dir_all(db.join("_kv0")).unwrap();
    std::fs::write(
        db.join("_kv0/format.json"),
        r#"{"engine":"moraine","version":7,"kind":"database","created_by":"kv0 3.0.0"}"#,
    )
    .unwrap();
    let e = Db::open(
        &db.to_string_lossy(),
        kv_opts(&d.path().join("wal"), &HashMap::new(), None),
    )
    .unwrap_err();
    not_supported(e, "format version 7");
    assert_eq!(files(&db), vec!["_kv0/format.json"], "nothing written");
}

/// `KV0_ENGINE` may name moraine, the default. (Other values would race with this binary's
/// other tests, which open with the default; `Engine::from_env` is tested on its own.)
#[test]
fn the_environment_may_name_moraine() {
    let d = tempfile::tempdir().unwrap();
    let o = HashMap::new();
    std::env::set_var("KV0_ENGINE", "moraine");
    let a = d.path().join("a");
    let opened = Db::open(
        &a.to_string_lossy(),
        kv_opts(&d.path().join("w1"), &o, None),
    );
    std::env::remove_var("KV0_ENGINE");
    opened.unwrap().close().unwrap();
    assert_eq!(
        detect(&a.to_string_lossy(), &o).unwrap(),
        Format::Database(Engine::Moraine)
    );
}
