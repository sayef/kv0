//! Format detection, the format marker and the choice of engine, on every build: what
//! `storage::detect` reports for each kind of location, which engine opens it, and the errors
//! for an engine that does not match. Runs against S3 too when `KV0_TEST_S3_ENDPOINT` is set.

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
fn formats_are_detected_and_engines_kept(base: &str, storage: &HashMap<String, String>) {
    let wal = tempfile::tempdir().unwrap();
    let at = |name: &str| format!("{}/{name}", base.trim_end_matches('/'));
    let det = |uri: &str| detect(uri, storage).unwrap();

    assert_eq!(det(&at("nothing")), Format::Empty);

    // A Lance database and repository: no marker; Delta asked for points to the migration.
    let lance_db = at("lance-db");
    let db = Db::open(
        &lance_db,
        kv_opts(&wal.path().join("1"), storage, Some(Engine::Lance)),
    )
    .unwrap();
    db.put(b"k", b"v").unwrap();
    db.close().unwrap();
    assert_eq!(det(&lance_db), Format::Database(Engine::Lance));
    let e = Db::open(
        &lance_db,
        kv_opts(&wal.path().join("2"), storage, Some(Engine::Delta)),
    )
    .unwrap_err();
    not_supported(e, "kv0 migrate");
    // Without a choice it opens with Lance, whatever the environment asks for new locations.
    let db = Db::open(&lance_db, kv_opts(&wal.path().join("3"), storage, None)).unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    db.close().unwrap();

    let lance_repo = at("lance-repo");
    close_repo(
        open_repo(
            &lance_repo,
            repo_opts(&wal.path().join("4"), storage, Some(Engine::Lance)),
        )
        .unwrap(),
    );
    assert_eq!(det(&lance_repo), Format::Repository(Engine::Lance));
    let e = open_repo(
        &lance_repo,
        repo_opts(&wal.path().join("5"), storage, Some(Engine::Delta)),
    )
    .err()
    .unwrap();
    not_supported(e, "kv0 migrate");

    // A database is not a repository, and the reverse.
    let e = Db::open(&lance_repo, kv_opts(&wal.path().join("6"), storage, None)).unwrap_err();
    assert!(
        matches!(&e, Error::InvalidArgument(m) if m.contains("tracking repository")),
        "{e}"
    );
    let e = open_repo(&lance_db, repo_opts(&wal.path().join("7"), storage, None))
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
            ..kv_opts(&wal.path().join("8"), storage, Some(Engine::Delta))
        },
    )
    .unwrap_err();
    assert!(
        matches!(e, Error::NotFound(_) | Error::NotSupported(_)),
        "{e}"
    );
    assert_eq!(det(&at("absent")), Format::Empty);

    #[cfg(not(feature = "delta"))]
    {
        let e = Db::open(
            &at("new-delta"),
            kv_opts(&wal.path().join("9"), storage, Some(Engine::Delta)),
        )
        .unwrap_err();
        not_supported(e, "`delta` feature");
    }

    #[cfg(feature = "delta")]
    {
        // A Delta database and repository carry the marker; Lance cannot open them.
        let delta_db = at("delta-db");
        let db = Db::open(
            &delta_db,
            kv_opts(&wal.path().join("10"), storage, Some(Engine::Delta)),
        )
        .unwrap();
        db.put(b"k", b"v").unwrap();
        db.close().unwrap();
        assert_eq!(det(&delta_db), Format::Database(Engine::Delta));
        let e = Db::open(
            &delta_db,
            kv_opts(&wal.path().join("11"), storage, Some(Engine::Lance)),
        )
        .unwrap_err();
        not_supported(e, "Delta Lake format");
        let db = Db::open(&delta_db, kv_opts(&wal.path().join("12"), storage, None)).unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        db.close().unwrap();

        let delta_repo = at("delta-repo");
        close_repo(
            open_repo(
                &delta_repo,
                repo_opts(&wal.path().join("13"), storage, Some(Engine::Delta)),
            )
            .unwrap(),
        );
        assert_eq!(det(&delta_repo), Format::Repository(Engine::Delta));
        let e = open_repo(
            &delta_repo,
            repo_opts(&wal.path().join("14"), storage, Some(Engine::Lance)),
        )
        .err()
        .unwrap();
        not_supported(e, "Delta Lake format");
        let e = Db::open(&delta_repo, kv_opts(&wal.path().join("15"), storage, None)).unwrap_err();
        assert!(matches!(e, Error::InvalidArgument(_)), "{e}");
        close_repo(
            open_repo(
                &delta_repo,
                repo_opts(&wal.path().join("16"), storage, None),
            )
            .unwrap(),
        );
    }
}

#[test]
fn local_formats_are_detected_and_engines_kept() {
    let d = tempfile::tempdir().unwrap();
    formats_are_detected_and_engines_kept(&d.path().to_string_lossy(), &HashMap::new());
}

#[test]
fn s3_formats_are_detected_and_engines_kept() {
    let Some((base, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    formats_are_detected_and_engines_kept(&base, &storage);
}

/// The beta.2 fixtures are recognised as Lance locations.
#[test]
fn the_lance_fixtures_are_lance_locations() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lance-unsigned");
    let o = HashMap::new();
    assert_eq!(
        detect(&root.join("kv").to_string_lossy(), &o).unwrap(),
        Format::Database(Engine::Lance)
    );
    assert_eq!(
        detect(&root.join("repo").to_string_lossy(), &o).unwrap(),
        Format::Repository(Engine::Lance)
    );
}

#[cfg(feature = "delta")]
mod delta {
    use super::*;

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
    fn new_delta_locations_get_the_marker() {
        let d = tempfile::tempdir().unwrap();
        let o = HashMap::new();
        let db = d.path().join("db");
        Db::open(
            &db.to_string_lossy(),
            kv_opts(&d.path().join("wal"), &o, Some(Engine::Delta)),
        )
        .unwrap()
        .close()
        .unwrap();
        let m = marker(&db);
        assert_eq!(
            (m.engine.as_str(), m.version, m.kind.as_str()),
            ("delta", 1, "database")
        );
        assert_eq!(m.created_by, format!("kv0 {}", env!("CARGO_PKG_VERSION")));
        let repo = d.path().join("repo");
        close_repo(
            open_repo(
                &repo.to_string_lossy(),
                repo_opts(&d.path().join("wal"), &o, Some(Engine::Delta)),
            )
            .unwrap(),
        );
        let m = marker(&repo);
        assert_eq!(
            (m.engine.as_str(), m.kind.as_str()),
            ("delta", "repository")
        );
        // Every table records the format in its first commit.
        for t in [db.clone(), repo.join("runs"), repo.join("metrics")] {
            let first =
                std::fs::read_to_string(t.join("_delta_log/00000000000000000000.json")).unwrap();
            assert!(
                first.contains(r#""kv0.format":"delta/1""#),
                "{}",
                t.display()
            );
        }
    }

    #[test]
    fn a_marker_from_a_newer_kv0_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("db");
        std::fs::create_dir_all(db.join("_kv0")).unwrap();
        std::fs::write(
            db.join("_kv0/format.json"),
            r#"{"engine":"delta","version":7,"kind":"database","created_by":"kv0 3.0.0"}"#,
        )
        .unwrap();
        let e = Db::open(
            &db.to_string_lossy(),
            kv_opts(&d.path().join("wal"), &HashMap::new(), None),
        )
        .unwrap_err();
        not_supported(e, "format version 7");
        assert!(!db.join("_delta_log").exists(), "nothing written");
    }

    #[test]
    fn a_delta_table_kv0_did_not_create_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("db");
        Db::open(
            &db.to_string_lossy(),
            kv_opts(&d.path().join("wal"), &HashMap::new(), Some(Engine::Delta)),
        )
        .unwrap()
        .close()
        .unwrap();
        // Strip the property from the creating commit, as another tool's table would lack it.
        let first = db.join("_delta_log/00000000000000000000.json");
        // Configuration is a map: the property's place in the JSON varies, so edit it as JSON.
        let body: Vec<String> = std::fs::read_to_string(&first)
            .unwrap()
            .lines()
            .map(|l| {
                let mut a: serde_json::Value = serde_json::from_str(l).unwrap();
                if let Some(c) = a
                    .pointer_mut("/metaData/configuration")
                    .and_then(|c| c.as_object_mut())
                {
                    assert!(c.remove("kv0.format").is_some());
                }
                a.to_string()
            })
            .collect();
        std::fs::write(&first, body.join("\n") + "\n").unwrap();
        for e in std::fs::read_dir(db.join("_delta_log")).unwrap().flatten() {
            if e.file_name().to_string_lossy().contains("checkpoint") {
                std::fs::remove_file(e.path()).unwrap();
            }
        }
        let e = Db::open(
            &db.to_string_lossy(),
            kv_opts(&d.path().join("wal2"), &HashMap::new(), None),
        )
        .unwrap_err();
        not_supported(e, "did not create");
    }

    /// The engine for new locations comes from the option, else `KV0_ENGINE`. (This binary's
    /// other tests always name the engine, so setting the variable here races with nothing.)
    #[test]
    fn the_environment_chooses_the_engine_of_new_locations() {
        let d = tempfile::tempdir().unwrap();
        let o = HashMap::new();
        std::env::set_var("KV0_ENGINE", "delta");
        let a = d.path().join("a");
        Db::open(
            &a.to_string_lossy(),
            kv_opts(&d.path().join("w1"), &o, None),
        )
        .unwrap()
        .close()
        .unwrap();
        let b = d.path().join("b");
        Db::open(
            &b.to_string_lossy(),
            kv_opts(&d.path().join("w2"), &o, Some(Engine::Lance)),
        )
        .unwrap()
        .close()
        .unwrap();
        std::env::set_var("KV0_ENGINE", "rocks");
        let e = Db::open(
            &d.path().join("c").to_string_lossy(),
            kv_opts(&d.path().join("w3"), &o, None),
        )
        .unwrap_err();
        std::env::remove_var("KV0_ENGINE");
        assert!(
            matches!(&e, Error::InvalidArgument(m) if m.contains("KV0_ENGINE")),
            "{e}"
        );
        assert_eq!(
            detect(&a.to_string_lossy(), &o).unwrap(),
            Format::Database(Engine::Delta)
        );
        assert_eq!(
            detect(&b.to_string_lossy(), &o).unwrap(),
            Format::Database(Engine::Lance)
        );
    }
}
