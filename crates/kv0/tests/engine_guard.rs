//! A build with only the Lance engine refuses locations that hold Delta Lake tables, instead of
//! creating a Lance dataset next to them.
#![cfg(not(feature = "delta"))]

use std::path::Path;

use kv0::kv::{Db, Options};
use kv0::rt::block_on;
use kv0::tracking::{Repo, RepoOptions};

fn fake_delta_table(dir: &Path) {
    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    std::fs::write(log.join("00000000000000000000.json"), b"{}\n").unwrap();
}

#[test]
fn a_delta_database_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    fake_delta_table(&db);
    let e = Db::open(
        &db.to_string_lossy(),
        Options {
            wal_dir: Some(d.path().join("wal")),
            ..Options::default()
        },
    )
    .unwrap_err();
    assert!(matches!(e, kv0::Error::NotSupported(_)), "{e}");
    assert!(e.to_string().contains("delta"), "{e}");
    assert!(!db.join("_versions").exists(), "nothing written");
}

#[test]
fn a_delta_repository_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let repo = d.path().join("repo");
    fake_delta_table(&repo.join("runs"));
    let uri = repo.to_string_lossy().into_owned();
    let o = RepoOptions {
        wal_dir: Some(d.path().join("wal")),
        ..RepoOptions::default()
    };
    let e = block_on(async move { Repo::open(&uri, o).await }).unwrap_err();
    assert!(matches!(e, kv0::Error::NotSupported(_)), "{e}");
    assert!(!repo.join("runs.lance").exists(), "nothing written");
}
