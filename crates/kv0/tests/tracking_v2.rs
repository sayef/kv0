//! The tracker's requests: per-field run updates, bulk reads, change detection, run kind and
//! target, creation and end times, deleting runs, and upgrading tables written before.

use std::path::Path;
use std::sync::Arc;

use kv0::rt::block_on;
use kv0::tracking::{NewRun, Repo, RepoOptions, RunStatus, StepRange, Track};
use serde_json::json;

fn opts(wal: &Path) -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        flush_rows: usize::MAX,
        wal_dir: Some(wal.to_path_buf()),
        ..RepoOptions::default()
    }
}

fn setup() -> (tempfile::TempDir, String, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let wal = d.path().join("wal");
    (d, uri, wal)
}

fn open(uri: &str, wal: &Path) -> Repo {
    let (uri, o) = (uri.to_string(), opts(wal));
    block_on(async move { Repo::open(&uri, o).await }).unwrap()
}

fn run_info(repo: &Repo, id: &str) -> kv0::tracking::RunInfo {
    let (r, id) = (repo.clone(), id.to_string());
    block_on(async move { r.run(&id).await }).unwrap().unwrap()
}

fn flush(repo: &Repo) {
    let r = repo.clone();
    block_on(async move { r.flush().await }).unwrap();
}

#[test]
fn a_note_saved_while_the_run_finishes_keeps_the_status() {
    // The race the tracker reported: the web app reads the record, the training process
    // finishes the run, then the web app saves a note. The run must stay finished.
    let (_d, uri, wal) = setup();
    let trainer = open(&uri, &wal);
    let run = trainer
        .create_run(NewRun {
            run_id: Some("r1".into()),
            name: Some("baseline".into()),
            ..NewRun::default()
        })
        .unwrap();
    flush(&trainer);
    let web = open(&uri, &wal.join("web"));
    assert_eq!(run_info(&web, "r1").status, RunStatus::Running);
    // The trainer finishes (sequence number assigned now) but its commit lands last.
    run.update(|i| {
        i.status = RunStatus::Finished;
        i.ended_at = Some(42);
    })
    .unwrap();
    let w = web.clone();
    let written = block_on(async move {
        w.update_run("r1", |i| i.description = Some("note".into()))
            .await
    })
    .unwrap();
    assert_eq!(
        written.status,
        RunStatus::Running,
        "the web app never saw the finish"
    );
    flush(&web);
    flush(&trainer);
    for repo in [&web, &trainer] {
        let now = run_info(repo, "r1");
        assert_eq!(now.status, RunStatus::Finished);
        assert_eq!(now.ended_at, Some(42));
        assert_eq!(now.description.as_deref(), Some("note"));
        assert_eq!(now.name.as_deref(), Some("baseline"));
    }
    // A later change of the same field still wins.
    let w = web.clone();
    block_on(async move { w.update_run("r1", |i| i.status = RunStatus::Failed).await }).unwrap();
    flush(&web);
    assert_eq!(run_info(&trainer, "r1").status, RunStatus::Failed);
}

#[test]
fn update_run_needs_no_points_and_fails_for_unknown_runs() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    for i in 0..1000 {
        run.track("loss", i as f64, Track::default()).unwrap();
    }
    let r = repo.clone();
    let info = block_on(async move {
        r.update_run("r1", |i| {
            i.tags = vec!["a".into()];
            i.archived = true;
        })
        .await
    })
    .unwrap();
    assert!(info.archived);
    let r = repo.clone();
    let e = block_on(async move { r.update_run("missing", |_| {}).await }).unwrap_err();
    assert!(matches!(e, kv0::Error::NotFound(_)), "{e:?}");
    flush(&repo);
    let now = run_info(&repo, "r1");
    assert_eq!(now.tags, vec!["a".to_string()]);
    assert!(now.archived);
}

#[test]
fn kind_target_and_times_round_trip() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("e1".into()),
            kind: Some("eval".into()),
            target: Some(json!({"model": "model-a", "commit": "abc"})),
            created_at: Some(1_700_000_000_000_000),
            ..NewRun::default()
        })
        .unwrap();
    let r = run.clone();
    block_on(async move {
        r.finish_at(RunStatus::Finished, Some(1_700_000_100_000_000))
            .await
    })
    .unwrap();
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
    let repo = open(&uri, &wal);
    let i = run_info(&repo, "e1");
    assert_eq!(i.kind.as_deref(), Some("eval"));
    assert_eq!(i.target, Some(json!({"model": "model-a", "commit": "abc"})));
    assert_eq!(i.created_at, 1_700_000_000_000_000);
    assert_eq!(i.ended_at, Some(1_700_000_100_000_000));
}

#[test]
fn bulk_reads_match_per_run_reads() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    for r in 0..5 {
        let run = repo
            .create_run(NewRun {
                run_id: Some(format!("r{r}")),
                ..NewRun::default()
            })
            .unwrap();
        run.set_params(&json!({"lr": r, "opt": {"name": "adam"}}))
            .unwrap();
        run.set_param("lr", json!(r * 10)).unwrap();
        for m in ["loss", "acc"] {
            run.track(m, 1.0, Track::default()).unwrap();
        }
        flush(&repo);
    }
    let r = repo.clone();
    let params = block_on(async move { r.all_params().await }).unwrap();
    assert_eq!(params.len(), 5);
    for (id, p) in &params {
        let (r, id2) = (repo.clone(), id.clone());
        assert_eq!(&block_on(async move { r.params(&id2).await }).unwrap(), p);
    }
    assert_eq!(params["r3"], json!({"lr": 30, "opt": {"name": "adam"}}));
    let r = repo.clone();
    let mut series = block_on(async move { r.all_series().await }).unwrap();
    assert_eq!(series.len(), 10);
    series.sort_by_key(|s| s.series_id);
    let mut per_run = Vec::new();
    for r in 0..5 {
        let (rp, id) = (repo.clone(), format!("r{r}"));
        per_run.extend(block_on(async move { rp.series(&id).await }).unwrap());
    }
    per_run.sort_by_key(|s| s.series_id);
    assert_eq!(series, per_run);
}

#[test]
fn latest_versions_see_other_writers_without_scanning() {
    let (_d, uri, wal) = setup();
    let a = open(&uri, &wal);
    let b = open(&uri, &wal.join("b"));
    let r = b.clone();
    let before = block_on(async move { r.latest_versions().await }).unwrap();
    a.create_run(NewRun::default()).unwrap();
    flush(&a);
    let r = b.clone();
    let after = block_on(async move { r.latest_versions().await }).unwrap();
    assert!(after.0 > before.0, "{before:?} -> {after:?}");
    assert_eq!(after.3, before.3, "no points were written");
}

#[test]
fn deleting_runs_removes_everything_recorded_for_them() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    for id in ["keep", "drop1", "drop2"] {
        let run = repo
            .create_run(NewRun {
                run_id: Some(id.into()),
                ..NewRun::default()
            })
            .unwrap();
        run.set_param("x", json!(1)).unwrap();
        for s in 0..10 {
            run.track("m", s as f64, Track::default()).unwrap();
        }
    }
    let r = repo.clone();
    let n = block_on(async move {
        r.delete_runs(&["drop1".into(), "drop2".into(), "never".into()])
            .await
    })
    .unwrap();
    assert_eq!(n, 2);
    let r = repo.clone();
    let runs = block_on(async move { r.runs().await }).unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id, "keep");
    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.all_params().await }).unwrap().len(),
        1
    );
    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.all_series().await }).unwrap().len(),
        1
    );
    let r = repo.clone();
    let pts = block_on(async move {
        r.read_metric("drop1", "m", &json!({}), StepRange::default())
            .await
    })
    .unwrap();
    assert!(pts.is_empty());
    let r = repo.clone();
    let kept = block_on(async move {
        r.read_metric("keep", "m", &json!({}), StepRange::default())
            .await
    })
    .unwrap();
    assert_eq!(kept.len(), 10);
}

#[test]
fn runs_tables_from_before_gain_the_new_columns() {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{
        BooleanArray, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    let (_d, uri, wal) = setup();
    // The `runs` table as the first release wrote it, with one run.
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("experiment", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("status", DataType::Utf8, false),
        Field::new("archived", DataType::Boolean, false),
        Field::new("created_at", ts.clone(), false),
        Field::new("updated_at", ts.clone(), false),
        Field::new("ended_at", ts, true),
    ]));
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.append(true);
    let t = |v: Option<i64>| TimestampMicrosecondArray::from(vec![v]).with_timezone("UTC");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(StringArray::from(vec!["old"])),
            Arc::new(StringArray::from(vec![Some("legacy")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(tags.finish()),
            Arc::new(StringArray::from(vec!["finished"])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(t(Some(5))),
            Arc::new(t(Some(6))),
            Arc::new(t(None)),
        ],
    )
    .unwrap();
    let runs_uri = format!("{uri}/runs.lance");
    block_on(async move {
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        lance::Dataset::write(reader, &runs_uri, None).await
    })
    .unwrap();

    let repo = open(&uri, &wal);
    let old = run_info(&repo, "old");
    assert_eq!(old.name.as_deref(), Some("legacy"));
    assert_eq!(old.status, RunStatus::Finished);
    assert_eq!(old.kind, None);
    let r = repo.clone();
    block_on(async move {
        r.update_run("old", |i| i.kind = Some("training".into()))
            .await
    })
    .unwrap();
    repo.create_run(NewRun {
        run_id: Some("new".into()),
        kind: Some("eval".into()),
        ..NewRun::default()
    })
    .unwrap();
    flush(&repo);
    let old = run_info(&repo, "old");
    assert_eq!(old.kind.as_deref(), Some("training"));
    assert_eq!(
        old.name.as_deref(),
        Some("legacy"),
        "unset fields come from the old row"
    );
    assert_eq!(run_info(&repo, "new").kind.as_deref(), Some("eval"));
}

#[test]
fn records_append_read_by_version_and_tag() {
    use kv0::tracking::{At, NewRecord, RecordQuery};
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let item = |id: &str, run: Option<&str>, v: i64| NewRecord {
        item_id: id.into(),
        run_id: run.map(str::to_string),
        data: json!({"v": v}),
    };
    let r = repo.clone();
    let v1 = block_on(async move {
        r.append_records("dataset:qa", vec![item("q1", None, 1), item("q2", None, 2)])
            .await
    })
    .unwrap();
    let r = repo.clone();
    block_on(async move { r.tag_records("dataset:qa", "qa-v1", None).await }).unwrap();
    let r = repo.clone();
    let v2 = block_on(async move {
        r.append_records("dataset:qa", vec![item("q3", None, 3)])
            .await?;
        r.append_records(
            "scores",
            vec![
                item("q1", Some("r1"), 10),
                item("q2", Some("r1"), 20),
                item("q1", Some("r2"), 30),
            ],
        )
        .await
    })
    .unwrap();
    assert!(v2 >= 1, "each table has its own versions");
    let read = |table: &str, q: RecordQuery| {
        let (r, t) = (repo.clone(), table.to_string());
        block_on(async move { r.records(&t, q).await }).unwrap()
    };
    let now = read("dataset:qa", RecordQuery::default());
    assert_eq!(
        now.iter().map(|r| r.item_id.as_str()).collect::<Vec<_>>(),
        ["q1", "q2", "q3"]
    );
    let then = read(
        "dataset:qa",
        RecordQuery {
            at: At::Version(v1),
            ..RecordQuery::default()
        },
    );
    assert_eq!(then.len(), 2);
    let tagged = read(
        "dataset:qa",
        RecordQuery {
            at: At::Tag("qa-v1".into()),
            ..RecordQuery::default()
        },
    );
    assert_eq!(tagged, then);
    let r1 = read(
        "scores",
        RecordQuery {
            run_id: Some("r1".into()),
            ..RecordQuery::default()
        },
    );
    assert_eq!(r1.len(), 2);
    let q1 = read(
        "scores",
        RecordQuery {
            item_id: Some("q1".into()),
            ..RecordQuery::default()
        },
    );
    assert_eq!(
        q1.iter()
            .map(|r| r.data["v"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [10, 30]
    );

    // Cleanup keeps the tagged version; deleting a run removes its records.
    let r = repo.clone();
    block_on(async move { r.cleanup_old_versions(std::time::Duration::ZERO).await }).unwrap();
    let tagged_after = read(
        "dataset:qa",
        RecordQuery {
            at: At::Tag("qa-v1".into()),
            ..RecordQuery::default()
        },
    );
    assert_eq!(tagged_after, then);
    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.record_tags("dataset:qa").await }).unwrap(),
        vec![("qa-v1".to_string(), v1)]
    );
    repo.create_run(NewRun {
        run_id: Some("r1".into()),
        ..NewRun::default()
    })
    .unwrap();
    let r = repo.clone();
    block_on(async move { r.delete_runs(&["r1".into()]).await }).unwrap();
    assert!(read(
        "scores",
        RecordQuery {
            run_id: Some("r1".into()),
            ..RecordQuery::default()
        }
    )
    .is_empty());
    assert_eq!(read("scores", RecordQuery::default()).len(), 1);
}

#[test]
fn time_range_reads() {
    use kv0::tracking::TimeRange;
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r".into()),
            ..NewRun::default()
        })
        .unwrap();
    for i in 0..10i64 {
        run.track(
            "score",
            i as f64,
            Track {
                timestamp: Some(1_000_000 * i),
                ..Track::default()
            },
        )
        .unwrap();
    }
    let id = kv0::tracking::series_id("r", "score", &json!({}));
    let r = repo.clone();
    let got = block_on(async move {
        r.read_many_in(
            &[id],
            StepRange::default(),
            TimeRange {
                start: Some(3_000_000),
                stop: Some(6_000_000),
            },
        )
        .await
    })
    .unwrap();
    assert_eq!(got[&id].values, vec![3.0, 4.0, 5.0]);
}

#[test]
fn series_stats_follow_flushes_and_crashes() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r".into()),
            ..NewRun::default()
        })
        .unwrap();
    let id = kv0::tracking::series_id("r", "loss", &json!({}));
    let track = |run: &kv0::tracking::Run, step: i64, v: f64| {
        run.track(
            "loss",
            v,
            Track {
                step: Some(step),
                timestamp: Some(step * 10),
                ..Track::default()
            },
        )
        .unwrap()
    };
    for (s, v) in [(0, 3.0), (1, 1.0), (2, f64::NAN)] {
        track(&run, s, v);
    }
    flush(&repo);
    for (s, v) in [(3, 5.0), (4, 2.0)] {
        track(&run, s, v);
    }
    let r = repo.clone();
    let st = block_on(async move { r.series_stats(&[id]).await }).unwrap()[&id];
    assert_eq!(st.count, 5);
    assert_eq!((st.first_step, st.last_step, st.last), (0, 4, 2.0));
    assert_eq!((st.min, st.min_step), (Some(1.0), Some(1)));
    assert_eq!((st.max, st.max_step), (Some(5.0), Some(3)));
    assert_eq!(st.non_finite, 1);
    assert_eq!(st.last_timestamp, 40);

    // A crash after more points: the next open recomputes the totals from the points.
    track(&run, 5, 0.5);
    flush(&repo);
    track(&run, 6, 9.0);
    drop(run);
    repo.abandon();
    let repo = open(&uri, &wal);
    let r = repo.clone();
    let all = block_on(async move { r.all_series_stats().await }).unwrap();
    let st = all[&id];
    assert_eq!(st.count, 7);
    assert_eq!((st.last_step, st.last), (6, 9.0));
    assert_eq!((st.min, st.max), (Some(0.5), Some(9.0)));

    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.recompute_series_stats(None).await }).unwrap(),
        1
    );
    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.series_stats(&[id]).await }).unwrap()[&id],
        st
    );
    let r = repo.clone();
    block_on(async move { r.delete_runs(&["r".into()]).await }).unwrap();
    let r = repo.clone();
    assert!(block_on(async move { r.all_series_stats().await })
        .unwrap()
        .is_empty());
}

#[test]
fn optimize_sorts_points_and_drops_overwritten_steps() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r".into()),
            ..NewRun::default()
        })
        .unwrap();
    let put = |name: &str, step: i64, v: f64| {
        run.track(
            name,
            v,
            Track {
                step: Some(step),
                ..Track::default()
            },
        )
        .unwrap()
    };
    for step in (0..20).rev() {
        put("a", step, step as f64);
        put("b", step, -(step as f64));
        if step % 5 == 0 {
            flush(&repo);
        }
    }
    put("a", 3, 300.0); // overwrite
    flush(&repo);
    let read = |name: &str| {
        let (r, n) = (repo.clone(), name.to_string());
        block_on(async move {
            r.read_metric("r", &n, &json!({}), StepRange::default())
                .await
        })
        .unwrap()
    };
    let (a, b) = (read("a"), read("b"));
    assert_eq!(a.values[3], 300.0);
    let r = repo.clone();
    let before = block_on(async move { r.metric_rows().await }).unwrap();
    let r = repo.clone();
    block_on(async move { r.optimize().await }).unwrap();
    assert_eq!(read("a"), a);
    assert_eq!(read("b"), b);
    let r = repo.clone();
    let after = block_on(async move { r.metric_rows().await }).unwrap();
    assert!(
        after < before,
        "the overwritten point is dropped: {before} -> {after}"
    );
}

fn step(s: i64) -> Track {
    Track {
        step: Some(s),
        ..Track::default()
    }
}

#[test]
fn deleting_a_run_while_its_writer_continues_does_not_bring_it_back() {
    // Review N10: the web app deletes a run while the training process keeps writing.
    let (_d, uri, wal) = setup();
    let sdk = open(&uri, &wal);
    let ui = open(&uri, &wal.join("ui"));
    let run = sdk.create_run(NewRun::default()).unwrap();
    let id = run.id();
    for i in 0..10 {
        run.track("loss", i as f64, step(i)).unwrap();
    }
    flush(&sdk);
    let (u, i2) = (ui.clone(), id.clone());
    assert_eq!(
        block_on(async move { u.delete_runs(&[i2]).await }).unwrap(),
        1
    );
    // A flush checks for deletions by other handles at most once a second.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    for i in 10..20 {
        run.track("loss", i as f64, step(i)).unwrap();
    }
    // The flush in finish sees the tombstone, drops the writes and reports the deletion.
    let r = run.clone();
    let e = block_on(async move { r.finish(RunStatus::Finished).await }).unwrap_err();
    assert!(matches!(e, kv0::Error::NotFound(_)), "{e}");
    assert!(run.track("loss", 0.0, step(30)).is_err());
    for repo in [&ui, &sdk] {
        let (r, i2) = (repo.clone(), id.clone());
        let (info, series, rows, stats, runs) = block_on(async move {
            (
                r.run(&i2).await.unwrap(),
                r.series(&i2).await.unwrap(),
                r.metric_rows().await.unwrap(),
                r.all_series_stats().await.unwrap(),
                r.runs().await.unwrap(),
            )
        });
        assert!(info.is_none() && series.is_empty() && stats.is_empty() && runs.is_empty());
        // Only WAL marks are left.
        assert!(rows <= 3, "{rows} metric rows");
    }
}

#[test]
fn a_wal_replayed_after_the_delete_does_not_bring_the_run_back() {
    let (_d, uri, wal) = setup();
    let sdk = open(&uri, &wal);
    let run = sdk
        .create_run(NewRun {
            run_id: Some("r".into()),
            ..NewRun::default()
        })
        .unwrap();
    run.track("loss", 1.0, step(0)).unwrap();
    flush(&sdk);
    // Unflushed writes, then a crash.
    run.track("loss", 2.0, step(1)).unwrap();
    run.track("acc", 0.5, step(1)).unwrap();
    run.update(|i| i.description = Some("late".into())).unwrap();
    drop(run);
    sdk.abandon();
    let ui = open(&uri, &wal.join("ui"));
    let u = ui.clone();
    assert_eq!(
        block_on(async move { u.delete_runs(&["r".into()]).await }).unwrap(),
        1
    );
    // The next open on the crashed writer's machine replays its WAL.
    let back = open(&uri, &wal);
    let r = back.clone();
    let (info, all, rows) = block_on(async move {
        (
            r.run("r").await.unwrap(),
            r.all_series().await.unwrap(),
            r.metric_rows().await.unwrap(),
        )
    });
    assert!(info.is_none() && all.is_empty(), "{info:?} {all:?}");
    assert!(rows <= 3, "{rows} metric rows");
}

#[test]
fn statistics_agree_with_reads_when_a_step_is_logged_again() {
    // Review N11: step 5 logged as 1000, flushed, then logged again as 1.
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo.create_run(NewRun::default()).unwrap();
    let sid = kv0::tracking::series_id(&run.id(), "loss", &json!({}));
    run.track("loss", 1000.0, step(5)).unwrap();
    run.track("loss", 3.0, step(6)).unwrap();
    flush(&repo);
    run.track("loss", 1.0, step(5)).unwrap();
    let stats = |repo: &Repo| {
        let r = repo.clone();
        block_on(async move { r.series_stats(&[sid]).await }).unwrap()[&sid]
    };
    let st = stats(&repo);
    assert_eq!(
        (st.count, st.max, st.min, st.last_step, st.last),
        (2, Some(3.0), Some(1.0), 6, 3.0)
    );
    // Exact after optimize too, from one stored delta.
    let r = repo.clone();
    block_on(async move { r.optimize().await }).unwrap();
    assert_eq!(stats(&repo), st);
    // Another handle that never wrote the series sees the same.
    assert_eq!(stats(&open(&uri, &wal.join("other"))), st);
}

#[test]
fn concurrent_writers_to_one_series_both_count() {
    let (_d, uri, wal) = setup();
    let a = open(&uri, &wal);
    let b = open(&uri, &wal.join("b"));
    let ra = a
        .create_run(NewRun {
            run_id: Some("r".into()),
            ..NewRun::default()
        })
        .unwrap();
    flush(&a);
    let rb = {
        let b2 = b.clone();
        block_on(async move { b2.resume_run("r").await }).unwrap()
    };
    for i in 0..5 {
        ra.track("m", i as f64, step(i)).unwrap();
        rb.track("m", (100 + i) as f64, step(100 + i)).unwrap();
    }
    flush(&a);
    flush(&b);
    let sid = kv0::tracking::series_id("r", "m", &json!({}));
    let r = a.clone();
    let st = block_on(async move { r.series_stats(&[sid]).await }).unwrap()[&sid];
    assert_eq!(
        (st.count, st.min, st.max, st.last_step),
        (10, Some(0.0), Some(104.0), 104)
    );
}

#[test]
fn a_time_range_read_never_returns_an_overwritten_point() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo.create_run(NewRun::default()).unwrap();
    let t = |s: i64, ts: i64| Track {
        step: Some(s),
        timestamp: Some(ts),
        ..Track::default()
    };
    run.track("m", 1.0, t(0, 100)).unwrap();
    run.track("m", 2.0, t(0, 900)).unwrap(); // the same step again, later
    let sid = kv0::tracking::series_id(&run.id(), "m", &json!({}));
    let r = repo.clone();
    let got = block_on(async move {
        r.read_many_in(
            &[sid],
            StepRange::default(),
            kv0::tracking::TimeRange {
                start: Some(0),
                stop: Some(500),
            },
        )
        .await
    })
    .unwrap();
    assert!(got.get(&sid).is_none_or(|d| d.steps.is_empty()), "{got:?}");
}

#[test]
fn optimize_rewrites_only_new_fragments_and_skips_when_nothing_is_new() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo.create_run(NewRun::default()).unwrap();
    let sid = kv0::tracking::series_id(&run.id(), "m", &json!({}));
    for i in 0..10 {
        run.track("m", i as f64, step(i)).unwrap();
    }
    flush(&repo);
    let optimize = |repo: &Repo| {
        let r = repo.clone();
        block_on(async move { r.optimize().await }).unwrap();
    };
    let metrics_version = |repo: &Repo| {
        let r = repo.clone();
        block_on(async move { r.latest_versions().await })
            .unwrap()
            .3
    };
    optimize(&repo);
    let v = metrics_version(&repo);
    optimize(&repo);
    assert_eq!(metrics_version(&repo), v, "nothing new: no commit");
    // New steps only: the sorted fragments stay, statistics add up.
    for i in 10..15 {
        run.track("m", i as f64, step(i)).unwrap();
    }
    flush(&repo);
    optimize(&repo);
    let stats = |repo: &Repo| {
        let r = repo.clone();
        block_on(async move { r.series_stats(&[sid]).await }).unwrap()[&sid]
    };
    let st = stats(&repo);
    assert_eq!((st.count, st.max, st.last_step), (15, Some(14.0), 14));
    // A step written again (by a handle that never wrote the series): a full rewrite keeps
    // the statistics exact.
    let other = open(&uri, &wal.join("o"));
    let r2 = {
        let o = other.clone();
        let id = run.id();
        block_on(async move { o.resume_run(&id).await }).unwrap()
    };
    let other_writer = open(&uri, &wal.join("w"));
    let raw = other_writer
        .create_run(NewRun {
            run_id: Some(run.id()),
            ..NewRun::default()
        })
        .unwrap();
    raw.track("m", 100.0, step(3)).unwrap();
    flush(&other_writer);
    drop(r2);
    optimize(&repo);
    let st = stats(&repo);
    assert_eq!((st.count, st.max), (15, Some(100.0)));
    let r = repo.clone();
    let id = run.id();
    let pts = block_on(async move {
        r.read_metric(&id, "m", &json!({}), StepRange::default())
            .await
    })
    .unwrap();
    assert_eq!((pts.len(), pts.values[3]), (15, 100.0));
}

#[test]
fn optimize_after_compaction_merged_sorted_fragments_keeps_statistics_exact() {
    // Background compaction may merge fragments optimize sorted (with their exact statistics)
    // into new ones; the next optimize must not count those points twice or drop them.
    let (_d, uri, wal) = setup();
    let o = RepoOptions {
        max_fragments: 2,
        compaction_target_rows: 1_000_000,
        ..opts(&wal)
    };
    let repo = {
        let uri = uri.clone();
        block_on(async move { Repo::open(&uri, o).await }).unwrap()
    };
    let run = repo.create_run(NewRun::default()).unwrap();
    let sid = kv0::tracking::series_id(&run.id(), "m", &json!({}));
    let settle = |repo: &Repo| {
        let r = repo.clone();
        block_on(async move {
            r.flush().await.unwrap();
            r.wait_for_maintenance().await;
        });
    };
    let mut n = 0;
    for round in 0..3 {
        for _ in 0..4 {
            for _ in 0..5 {
                run.track("m", n as f64, step(n)).unwrap();
                n += 1;
            }
            settle(&repo);
        }
        let r = repo.clone();
        block_on(async move { r.optimize().await }).unwrap();
        let r = repo.clone();
        let st = block_on(async move { r.series_stats(&[sid]).await }).unwrap()[&sid];
        assert_eq!(
            (st.count, st.max, st.last_step),
            (n as u64, Some((n - 1) as f64), n - 1),
            "round {round}"
        );
    }
    assert!(repo.background_compactions() > 0);
    let (r, id) = (repo.clone(), run.id());
    let pts = block_on(async move {
        r.read_metric(&id, "m", &json!({}), StepRange::default())
            .await
    })
    .unwrap();
    assert_eq!(pts.len(), n as usize);
}

#[test]
fn optimize_folds_superseded_run_rows() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("busy".into()),
            name: Some("n".into()),
            ..NewRun::default()
        })
        .unwrap();
    let gone = repo
        .create_run(NewRun {
            run_id: Some("gone".into()),
            ..NewRun::default()
        })
        .unwrap();
    drop(gone);
    // A heartbeat: one row per flush.
    for i in 0..30 {
        run.update(|r| r.description = Some(format!("beat {i}")))
            .unwrap();
        flush(&repo);
    }
    let r = repo.clone();
    block_on(async move { r.delete_runs(&["gone".into()]).await }).unwrap();
    let rows = || {
        let p = format!("{uri}/runs.lance");
        block_on(async move { lance::Dataset::open(&p).await?.count_rows(None).await }).unwrap()
    };
    let before = rows();
    let info = run_info(&repo, "busy");
    let r = repo.clone();
    block_on(async move { r.optimize().await }).unwrap();
    assert!(rows() < before / 4, "{before} -> {}", rows());
    assert_eq!(run_info(&repo, "busy"), info);
    let r = repo.clone();
    assert!(block_on(async move { r.run("gone").await })
        .unwrap()
        .is_none());
    // Later changes from another handle still win, field by field.
    let web = open(&uri, &wal.join("web"));
    let w = web.clone();
    block_on(async move { w.update_run("busy", |r| r.tags = vec!["kept".into()]).await }).unwrap();
    run.update(|r| r.description = Some("last".into())).unwrap();
    flush(&repo);
    let got = run_info(&web, "busy");
    assert_eq!(got.tags, ["kept"]);
    assert_eq!(got.description.as_deref(), Some("last"));
    assert_eq!(got.name.as_deref(), Some("n"));
}

#[test]
fn a_change_buffered_before_a_fold_keeps_its_fields() {
    // Review F1: the training process buffers a description; the web app renames the run and
    // flushes; the server optimizes (folding run rows); then the training process flushes. Its
    // description has an older sequence number than the rename but sets another field.
    let (_d, uri, wal) = setup();
    let sdk = open(&uri, &wal);
    let run = sdk
        .create_run(NewRun {
            run_id: Some("r".into()),
            name: Some("sdk".into()),
            ..NewRun::default()
        })
        .unwrap();
    flush(&sdk);
    run.update(|r| r.description = Some("from-sdk".into()))
        .unwrap();
    run.update(|r| r.status = RunStatus::Finished).unwrap();
    let web = open(&uri, &wal.join("web"));
    let w = web.clone();
    block_on(async move { w.update_run("r", |r| r.name = Some("web".into())).await }).unwrap();
    let w = web.clone();
    block_on(async move { w.optimize().await }).unwrap();
    flush(&sdk);
    let got = run_info(&web, "r");
    assert_eq!(
        (got.name.as_deref(), got.description.as_deref(), got.status),
        (Some("web"), Some("from-sdk"), RunStatus::Finished)
    );
}
