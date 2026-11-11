//! Records tables: the newest row per key, folding and indices in `optimize`, and tags that keep
//! one table only.

use std::path::Path;

use kv0::rt::block_on;
use kv0::tracking::{At, NewRecord, RecordQuery, Repo, RepoOptions};
use serde_json::json;

fn repo(uri: &str, wal: &Path) -> Repo {
    let o = RepoOptions {
        flush_interval: None,
        wal_dir: Some(wal.to_path_buf()),
        ..RepoOptions::default()
    };
    let uri = uri.to_string();
    block_on(async move { Repo::open(&uri, o).await }).unwrap()
}

#[test]
fn records_keep_the_newest_row_per_key_and_tags_pin_one_table() {
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let repo = repo(&uri, &d.path().join("wal"));
    let rec = |item: &str, v: i64| NewRecord {
        item_id: item.into(),
        run_id: None,
        data: json!({ "v": v }),
    };
    let r = repo.clone();
    let (dataset_v1, state_v1) = block_on(async move {
        let d = r.append_records("dataset", vec![rec("a", 1)]).await?;
        r.tag_records("dataset", "v1", None).await?;
        let s = r
            .append_records("state", vec![rec("k", 1), rec("j", 1)])
            .await?;
        r.append_records("state", vec![rec("k", 2)]).await?;
        r.append_records("state", vec![rec("k", 3)]).await?;
        Ok::<_, kv0::Error>((d, s))
    })
    .unwrap();
    let read = |table: &str, q: RecordQuery| {
        let (r, t) = (repo.clone(), table.to_string());
        block_on(async move { r.records(&t, q).await }).unwrap()
    };
    let k = || RecordQuery {
        item_id: Some("k".into()),
        ..RecordQuery::default()
    };
    let got = read("state", k());
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].data["v"], 3);
    assert_eq!(read("state", RecordQuery::default()).len(), 2);
    let r = repo.clone();
    let tables = block_on(async move {
        r.optimize().await?;
        r.cleanup_old_versions(std::time::Duration::ZERO).await?;
        r.record_tables().await
    })
    .unwrap();
    assert_eq!(tables, ["dataset", "state"]);
    // Folded and indexed: the same answers.
    assert_eq!(read("state", k())[0].data["v"], 3);
    assert_eq!(read("state", RecordQuery::default()).len(), 2);
    // The dataset tag kept the dataset's version, not the state table's old versions.
    let tagged = read(
        "dataset",
        RecordQuery {
            at: At::Tag("v1".into()),
            ..RecordQuery::default()
        },
    );
    assert_eq!(tagged.len(), 1);
    let (r2, v) = (repo.clone(), state_v1);
    assert!(block_on(async move {
        r2.records(
            "state",
            RecordQuery {
                at: At::Version(v),
                ..RecordQuery::default()
            },
        )
        .await
    })
    .is_err());
    let _ = dataset_v1;
}

#[test]
fn records_of_several_runs_in_one_query() {
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let repo = repo(&uri, &d.path().join("wal"));
    // 1200 runs, so the ids are read in several batches; run r0 has a newer row for q.
    let rows: Vec<NewRecord> = (0..1200)
        .map(|r| NewRecord {
            item_id: "q".into(),
            run_id: Some(format!("r{r}")),
            data: json!({ "v": r }),
        })
        .collect();
    let r = repo.clone();
    block_on(async move {
        r.append_records("scores", rows).await?;
        r.append_records(
            "scores",
            vec![NewRecord {
                item_id: "q".into(),
                run_id: Some("r0".into()),
                data: json!({ "v": -1 }),
            }],
        )
        .await
    })
    .unwrap();
    let read = |ids: Vec<String>| {
        let r = repo.clone();
        block_on(async move {
            r.records(
                "scores",
                RecordQuery {
                    run_ids: Some(ids),
                    ..RecordQuery::default()
                },
            )
            .await
        })
        .unwrap()
    };
    let all: Vec<String> = (0..1200).map(|r| format!("r{r}")).collect();
    let got = read(all);
    assert_eq!(got.len(), 1200);
    assert_eq!(
        got.iter()
            .find(|x| x.run_id.as_deref() == Some("r0"))
            .unwrap()
            .data["v"],
        -1,
        "newest row per key"
    );
    let some = read(vec!["r5".into(), "r7".into(), "missing".into()]);
    assert_eq!(some.len(), 2);
    assert!(read(Vec::new()).is_empty());
}

#[test]
fn a_repository_with_the_old_shared_records_table() {
    use arrow_array::{
        RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use kv0::tracking::LegacyRecords;
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("repo");
    let uri = root.to_string_lossy().into_owned();
    // What earlier builds wrote: every table in records.lance.
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("item_id", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(UInt64Array::from(vec![1, 2])),
            std::sync::Arc::new(StringArray::from(vec!["dataset", "scores"])),
            std::sync::Arc::new(StringArray::from(vec![None, Some("r1")])),
            std::sync::Arc::new(StringArray::from(vec!["a", "a"])),
            std::sync::Arc::new(StringArray::from(vec!["{\"x\":1}", "{\"s\":2}"])),
            std::sync::Arc::new(TimestampMicrosecondArray::from(vec![10, 20]).with_timezone("UTC")),
        ],
    )
    .unwrap();
    let legacy = root.join("records.lance").to_string_lossy().into_owned();
    block_on(async move {
        lance::Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &legacy,
            None,
        )
        .await
    })
    .unwrap();
    let open = |legacy: LegacyRecords| {
        let o = RepoOptions {
            flush_interval: None,
            wal_dir: Some(d.path().join("wal")),
            legacy_records: legacy,
            ..RepoOptions::default()
        };
        let uri = uri.clone();
        block_on(async move { Repo::open(&uri, o).await })
    };
    let e = open(LegacyRecords::Error).unwrap_err();
    assert!(e.to_string().contains("records.lance"), "{e}");
    // The migration path: open, read, copy into tables, drop, reopen with the default.
    let repo = open(LegacyRecords::Allow).unwrap();
    let r = repo.clone();
    assert!(block_on(async move { r.has_legacy_records().await }).unwrap());
    let r = repo.clone();
    let (all, scores) = block_on(async move {
        let all = r.legacy_records(None).await?;
        let scores = r.legacy_records(Some("scores")).await?;
        Ok::<_, kv0::Error>((all, scores))
    })
    .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(scores.len(), 1);
    assert_eq!(scores[0].run_id.as_deref(), Some("r1"));
    let r = repo.clone();
    let rows = scores
        .into_iter()
        .map(|x| NewRecord {
            item_id: x.item_id,
            run_id: x.run_id,
            data: x.data,
        })
        .collect();
    block_on(async move {
        r.append_records("scores", rows).await?;
        assert!(r.drop_legacy_records().await?);
        assert!(!r.drop_legacy_records().await?);
        assert!(!r.has_legacy_records().await?);
        Ok::<_, kv0::Error>(())
    })
    .unwrap();
    drop(repo);
    let repo = open(LegacyRecords::Error).unwrap();
    let r = repo.clone();
    let n = block_on(async move { r.records("scores", RecordQuery::default()).await }).unwrap();
    assert_eq!(n.len(), 1);
}
