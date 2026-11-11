use kv0::tracking::{NewRun, Repo, RepoOptions, RunStatus, StepRange, Track};
use serde_json::json;

fn opts() -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        ..RepoOptions::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn runs_params_and_metrics_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let repo = Repo::open(&uri, opts()).await.unwrap();
    let run = repo
        .create_run(NewRun {
            name: Some("baseline".into()),
            experiment: Some("exp-a".into()),
            tags: vec!["synthetic".into()],
            ..NewRun::default()
        })
        .unwrap();
    run.set_params(&json!({"lr": 0.001, "optimizer": {"name": "adam", "betas": [0.9, 0.999]}}))
        .unwrap();
    run.set_param("optimizer.name", json!("sgd")).unwrap();
    for step in 0..100 {
        run.track("loss", 1.0 / (step as f64 + 1.0), Track::default())
            .unwrap();
        run.track(
            "acc",
            step as f64 / 100.0,
            Track {
                step: Some(step),
                epoch: Some(step / 10),
                context: Some(json!({"subset": "val"})),
                ..Track::default()
            },
        )
        .unwrap();
    }
    // A repeated step: the later point wins.
    run.track(
        "loss",
        42.0,
        Track {
            step: Some(5),
            ..Track::default()
        },
    )
    .unwrap();
    run.finish(RunStatus::Finished).await.unwrap();

    let runs = repo.runs().await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, RunStatus::Finished);
    assert!(runs[0].ended_at.is_some());
    assert_eq!(runs[0].tags, vec!["synthetic".to_string()]);

    let params = repo.params(&run.id()).await.unwrap();
    assert_eq!(params["optimizer"]["name"], json!("sgd"));
    assert_eq!(params["optimizer"]["betas"], json!([0.9, 0.999]));
    assert_eq!(params["lr"], json!(0.001));

    let series = repo.series(&run.id()).await.unwrap();
    assert_eq!(series.len(), 2);

    let loss = repo
        .read_metric(&run.id(), "loss", &json!({}), StepRange::default())
        .await
        .unwrap();
    assert_eq!(loss.len(), 100);
    assert_eq!(loss.steps[..3], [0, 1, 2]);
    assert_eq!(loss.values[5], 42.0);

    let acc = repo
        .read_metric(
            &run.id(),
            "acc",
            &json!({"subset": "val"}),
            StepRange {
                start: Some(10),
                stop: Some(20),
            },
        )
        .await
        .unwrap();
    assert_eq!(acc.steps, (10..20).collect::<Vec<_>>());
    assert_eq!(acc.epochs[0], Some(1));

    // The index does not change results.
    repo.optimize().await.unwrap();
    let again = repo
        .read_metric(&run.id(), "loss", &json!({}), StepRange::default())
        .await
        .unwrap();
    assert_eq!(again, loss);
    repo.close().await.unwrap();

    // A second handle sees everything and resumes step numbering.
    let repo2 = Repo::open(&uri, opts()).await.unwrap();
    let resumed = repo2.resume_run(&run.id()).await.unwrap();
    resumed.track("loss", 0.0, Track::default()).unwrap();
    repo2.flush().await.unwrap();
    let loss = repo2
        .read_metric(
            &run.id(),
            "loss",
            &json!({}),
            StepRange {
                start: Some(99),
                stop: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(loss.steps, vec![99, 100]);
}

#[test]
fn blocking_use_and_many_writers() {
    // Several handles append to the same tables at once; appends never conflict.
    // `memory://` stores are private to one handle, so several handles need a directory.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let uri = uri.as_str();
    let repo = {
        let uri = uri.to_string();
        kv0::rt::block_on(async move { Repo::open(&uri, opts()).await }).unwrap()
    };
    std::thread::scope(|s| {
        for w in 0..4 {
            let repo = repo.clone();
            s.spawn(move || {
                // Separate handles share nothing but storage.
                drop(repo);
                let uri = uri.to_string();
                let other = kv0::rt::block_on(async move {
                    Repo::open(
                        &uri,
                        RepoOptions {
                            flush_rows: 50,
                            ..opts()
                        },
                    )
                    .await
                })
                .unwrap();
                let run = other
                    .create_run(NewRun {
                        name: Some(format!("w{w}")),
                        ..NewRun::default()
                    })
                    .unwrap();
                for i in 0..500 {
                    run.track("x", i as f64, Track::default()).unwrap();
                }
                kv0::rt::block_on(async move { other.close().await }).unwrap();
            });
        }
    });
    let runs = kv0::rt::block_on({
        let r = repo.clone();
        async move { r.runs().await }
    })
    .unwrap();
    assert_eq!(runs.len(), 4);
    for info in runs {
        let r = repo.clone();
        let d = kv0::rt::block_on(async move {
            r.read_metric(&info.run_id, "x", &json!({}), StepRange::default())
                .await
        })
        .unwrap();
        assert_eq!(d.len(), 500);
    }
}
