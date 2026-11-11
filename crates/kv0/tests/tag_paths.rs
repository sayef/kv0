//! Tags must land in the dataset they name, whatever characters its path holds: a tag written
//! elsewhere does not protect its version from cleanup.

use kv0::kv::{Db, Options};

#[test]
fn key_value_tags_under_a_path_with_special_characters_protect_their_version() {
    let d = tempfile::tempdir().unwrap();
    for name in ["db~1", "db:2", "db 3", "dü=4"] {
        let path = d.path().join(name).to_string_lossy().into_owned();
        let db = Db::open(
            &path,
            Options {
                flush_interval: None,
                wal_dir: Some(d.path().join("wal")),
                ..Options::default()
            },
        )
        .unwrap();
        db.put(b"k", b"v1").unwrap();
        db.flush().unwrap();
        db.create_tag("t", None).unwrap();
        let v = db.tag_version("t").unwrap();
        db.put(b"k", b"v2").unwrap();
        db.flush().unwrap();
        db.cleanup_old_versions(std::time::Duration::ZERO).unwrap();
        let old = Db::open(
            &path,
            Options {
                version: Some(v),
                wal_dir: Some(d.path().join("wal")),
                ..Options::default()
            },
        )
        .unwrap_or_else(|e| panic!("{name}: tagged version {v} gone: {e}"));
        assert_eq!(
            old.get(b"k").unwrap().as_deref(),
            Some(&b"v1"[..]),
            "{name}"
        );
        let refs = std::path::Path::new(&path).join("_refs");
        assert!(
            refs.exists(),
            "{name}: the tag is in the dataset's own directory"
        );
    }
    // Lance (14.0) also creates an empty, percent-encoded twin of such a path (`db%7E1`) when
    // tagging; it must hold nothing, since the tags above are in the dataset itself.
    fn files(p: &std::path::Path) -> usize {
        std::fs::read_dir(p).map_or(0, |rd| {
            rd.flatten()
                .map(|e| {
                    if e.path().is_dir() {
                        files(&e.path())
                    } else {
                        1
                    }
                })
                .sum()
        })
    }
    for e in std::fs::read_dir(d.path()).unwrap().flatten() {
        let n = e.file_name().into_string().unwrap();
        if n.contains('%') {
            assert_eq!(files(&e.path()), 0, "{n} holds files");
        }
    }
}

#[test]
fn records_tags_on_tables_with_special_characters_protect_their_version() {
    use kv0::rt::block_on;
    use kv0::tracking::{At, NewRecord, RecordQuery, Repo, RepoOptions};
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo~x").to_string_lossy().into_owned();
    let o = RepoOptions {
        flush_interval: None,
        wal_dir: Some(d.path().join("wal")),
        ..RepoOptions::default()
    };
    let repo = {
        let uri = uri.clone();
        block_on(async move { Repo::open(&uri, o).await }).unwrap()
    };
    for table in ["dataset:support-intents", "a b", "ü~=%_x", "plain"] {
        let (r, t) = (repo.clone(), table.to_string());
        let got = block_on(async move {
            let rec = |v: i64| NewRecord {
                item_id: "a".into(),
                run_id: None,
                data: serde_json::json!({ "v": v }),
            };
            r.append_records(&t, vec![rec(1)]).await?;
            r.tag_records(&t, "audit", None).await?;
            r.append_records(&t, vec![rec(2)]).await?;
            r.cleanup_old_versions(std::time::Duration::ZERO).await?;
            r.records(
                &t,
                RecordQuery {
                    at: At::Tag("audit".into()),
                    ..RecordQuery::default()
                },
            )
            .await
        });
        let got = got.unwrap_or_else(|e| panic!("{table}: tagged version gone: {e}"));
        assert_eq!(got[0].data["v"], 1, "{table}");
    }
    // Purge sees the tag on a table whose name needs escaping, and refuses.
    repo.create_run(kv0::tracking::NewRun {
        run_id: Some("r1".into()),
        ..Default::default()
    })
    .unwrap();
    let r = repo.clone();
    let e = block_on(async move {
        r.flush().await?;
        r.append_records(
            "scores:v1 ü",
            vec![NewRecord {
                item_id: "a".into(),
                run_id: Some("r1".into()),
                data: serde_json::json!({}),
            }],
        )
        .await?;
        r.tag_records("scores:v1 ü", "kept", None).await?;
        r.delete_runs(&["r1".into()]).await?;
        r.purge(false).await
    })
    .unwrap_err();
    assert!(e.to_string().contains("kept"), "{e}");
    let names: Vec<String> = std::fs::read_dir(d.path().join("repo~x/records"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        names.iter().all(|n| !n.contains('%')),
        "no stray tag directory: {names:?}"
    );
}

#[test]
fn records_tables_from_the_earlier_directory_names_are_moved_with_their_tags() {
    use kv0::rt::block_on;
    use kv0::tracking::{At, RecordQuery, Repo, RepoOptions};
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("repo");
    let records = root.join("records");
    std::fs::create_dir_all(&records).unwrap();
    // The layout an earlier build left: data under `~XX`, a tag under the `%7E` twin.
    let o = || RepoOptions {
        flush_interval: None,
        wal_dir: Some(d.path().join("wal")),
        ..RepoOptions::default()
    };
    let uri = root.to_string_lossy().into_owned();
    let repo = {
        let (uri, o) = (uri.clone(), o());
        block_on(async move { Repo::open(&uri, o).await }).unwrap()
    };
    let r = repo.clone();
    let v = block_on(async move {
        r.append_records(
            "dataset:qa",
            vec![kv0::tracking::NewRecord {
                item_id: "a".into(),
                run_id: None,
                data: serde_json::json!({ "v": 1 }),
            }],
        )
        .await
    })
    .unwrap();
    drop(repo);
    let new = records.join("dataset=3Aqa.lance");
    std::fs::rename(&new, records.join("dataset~3Aqa.lance")).unwrap();
    let stray = records.join("dataset%7E3Aqa.lance/_refs/tags");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(
        stray.join("audit.json"),
        serde_json::json!({ "branch": null, "version": v, "manifestSize": 0 }).to_string(),
    )
    .unwrap();

    let repo = {
        let (uri, o) = (uri.clone(), o());
        block_on(async move { Repo::open(&uri, o).await }).unwrap()
    };
    assert!(new.join("_refs/tags/audit.json").exists());
    let names: Vec<String> = std::fs::read_dir(&records)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, ["dataset=3Aqa.lance"]);
    let r = repo.clone();
    let (tables, tags) = block_on(async move {
        Ok::<_, kv0::Error>((r.record_tables().await?, r.record_tags("dataset:qa").await?))
    })
    .unwrap();
    assert_eq!(tables, ["dataset:qa"]);
    assert_eq!(tags.len(), 1);
    let _ = (At::Latest, RecordQuery::default());
}

/// Runs `f` on another thread and fails the test if it takes longer than `secs`.
fn within<T: Send + 'static>(secs: u64, what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(std::time::Duration::from_secs(secs))
        .unwrap_or_else(|_| panic!("{what} did not finish within {secs} s"))
}

#[test]
fn reads_by_tag_or_cleaned_up_version_finish_promptly() {
    // On the earlier layout a read by tag after cleanup hung; every such read must return.
    use kv0::rt::block_on;
    use kv0::tracking::{At, NewRecord, RecordQuery, Repo, RepoOptions};
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let o = RepoOptions {
        flush_interval: None,
        wal_dir: Some(d.path().join("wal")),
        ..RepoOptions::default()
    };
    let repo = block_on(async move { Repo::open(&uri, o).await }).unwrap();
    let table = "dataset:support-intents v2";
    let r = repo.clone();
    let v1 = block_on(async move {
        let rec = |v: i64| NewRecord {
            item_id: "a".into(),
            run_id: None,
            data: serde_json::json!({ "v": v }),
        };
        let v1 = r.append_records(table, vec![rec(1)]).await?;
        r.tag_records(table, "audit", None).await?;
        r.append_records(table, vec![rec(2)]).await?;
        let v3 = r.append_records(table, vec![rec(3)]).await?;
        r.delete_record_tag(table, "audit").await?;
        r.tag_records(table, "audit", Some(v1)).await?;
        // optimize folds the table into new files between tagging and cleanup: the order in
        // which the earlier layout hung.
        r.optimize().await?;
        r.cleanup_old_versions(std::time::Duration::ZERO).await?;
        Ok::<_, kv0::Error>((v1, v3))
    })
    .unwrap()
    .0;
    let read = |at: At| {
        let r = repo.clone();
        within(60, &format!("records read at {at:?}"), move || {
            block_on(async move {
                r.records(
                    table,
                    RecordQuery {
                        at,
                        ..RecordQuery::default()
                    },
                )
                .await
            })
        })
    };
    assert_eq!(read(At::Tag("audit".into())).unwrap()[0].data["v"], 1);
    assert_eq!(read(At::Version(v1)).unwrap()[0].data["v"], 1);
    assert!(
        read(At::Version(v1 + 1)).is_err(),
        "a cleaned-up version is an error"
    );
    assert!(read(At::Tag("missing".into())).is_err());

    // The key-value store: an old version opened after cleanup is an error, promptly.
    let path = d.path().join("db:x~").to_string_lossy().into_owned();
    let wal = d.path().join("wal2");
    let (p2, w2) = (path.clone(), wal.clone());
    let old = within(60, "key-value setup", move || {
        let db = kv0::kv::Db::open(
            &p2,
            kv0::kv::Options {
                flush_interval: None,
                wal_dir: Some(w2),
                ..kv0::kv::Options::default()
            },
        )
        .unwrap();
        db.put(b"k", b"1").unwrap();
        db.flush().unwrap();
        let old = db.version();
        db.put(b"k", b"2").unwrap();
        db.flush().unwrap();
        db.cleanup_old_versions(std::time::Duration::ZERO).unwrap();
        old
    });
    let opened = within(60, "opening a cleaned-up key-value version", move || {
        kv0::kv::Db::open(
            &path,
            kv0::kv::Options {
                version: Some(old),
                wal_dir: Some(wal),
                ..kv0::kv::Options::default()
            },
        )
        .is_err()
    });
    assert!(opened);
}

#[test]
fn processes_opening_an_earlier_layout_at_once_all_succeed() {
    use kv0::rt::block_on;
    use kv0::tracking::{Repo, RepoOptions};
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("repo");
    let records = root.join("records");
    let uri = root.to_string_lossy().into_owned();
    let open = |wal: &str| {
        let o = RepoOptions {
            flush_interval: None,
            wal_dir: Some(d.path().join(wal)),
            ..RepoOptions::default()
        };
        let uri = uri.clone();
        move || block_on(async move { Repo::open(&uri, o).await })
    };
    let repo = open("w")().unwrap();
    let r = repo.clone();
    let v = block_on(async move {
        let mut v = 0;
        for t in ["a:1", "b:2", "c:3"] {
            v = r
                .append_records(
                    t,
                    vec![kv0::tracking::NewRecord {
                        item_id: "x".into(),
                        run_id: None,
                        data: serde_json::json!({}),
                    }],
                )
                .await?;
        }
        Ok::<_, kv0::Error>(v)
    })
    .unwrap();
    drop(repo);
    for (new, old) in [("a=3A1", "a~3A1"), ("b=3A2", "b~3A2"), ("c=3A3", "c~3A3")] {
        std::fs::rename(
            records.join(format!("{new}.lance")),
            records.join(format!("{old}.lance")),
        )
        .unwrap();
        let stray = records.join(format!("{}.lance/_refs/tags", old.replace('~', "%7E")));
        std::fs::create_dir_all(&stray).unwrap();
        std::fs::write(
            stray.join("t.json"),
            serde_json::json!({ "branch": null, "version": v, "manifestSize": 0 }).to_string(),
        )
        .unwrap();
    }
    let opens: Vec<_> = (0..6)
        .map(|i| std::thread::spawn(open(&format!("w{i}"))))
        .collect();
    for h in opens {
        h.join().unwrap().expect("every concurrent open succeeds");
    }
    let mut names: Vec<String> = std::fs::read_dir(&records)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["a=3A1.lance", "b=3A2.lance", "c=3A3.lance"]);
    for n in &names {
        assert!(records.join(n).join("_refs/tags/t.json").exists(), "{n}");
    }
}

#[test]
fn a_tag_survives_append_optimize_and_cleanup_and_reads_promptly() {
    // The sequence that hung on the earlier `~` layout (4414814): append, tag, append,
    // optimize, cleanup_old_versions(0), then read by tag.
    use kv0::rt::block_on;
    use kv0::tracking::{At, NewRecord, RecordQuery, Repo, RepoOptions};
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let o = RepoOptions {
        flush_interval: None,
        wal_dir: Some(d.path().join("wal")),
        ..RepoOptions::default()
    };
    let repo = block_on(async move { Repo::open(&uri, o).await }).unwrap();
    let rec = |v: i64| {
        vec![NewRecord {
            item_id: "a".into(),
            run_id: None,
            data: serde_json::json!({ "v": v }),
        }]
    };
    let r = repo.clone();
    within(60, "append, tag, append, optimize, cleanup", move || {
        block_on(async move {
            r.append_records("dataset:qa", rec(1)).await?;
            r.tag_records("dataset:qa", "audit", None).await?;
            r.append_records("dataset:qa", rec(2)).await?;
            r.optimize().await?;
            r.cleanup_old_versions(std::time::Duration::ZERO).await
        })
    })
    .unwrap();
    for fresh in [false, true] {
        // The same handle, then a new one, as a reader process would open it.
        let repo = if fresh {
            let uri = d.path().join("repo").to_string_lossy().into_owned();
            let o = RepoOptions {
                flush_interval: None,
                wal_dir: Some(d.path().join("wal2")),
                ..RepoOptions::default()
            };
            block_on(async move { Repo::open(&uri, o).await }).unwrap()
        } else {
            repo.clone()
        };
        let r = repo.clone();
        let got = within(60, "the read by tag", move || {
            block_on(async move {
                r.records(
                    "dataset:qa",
                    RecordQuery {
                        at: At::Tag("audit".into()),
                        ..RecordQuery::default()
                    },
                )
                .await
            })
        })
        .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].data["v"], 1);
    }
}
