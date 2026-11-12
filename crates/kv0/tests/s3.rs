//! Runs against an S3-compatible server when `KV0_TEST_S3_ENDPOINT` is set (CI starts one
//! in Docker). Skipped otherwise.

use std::collections::HashMap;
use std::time::Duration;

use kv0::kv::{Db, Options};
use kv0::tracking::{NewRun, Repo, RepoOptions, StepRange, Track};
use serde_json::json;

fn storage() -> Option<(String, HashMap<String, String>)> {
    let endpoint = std::env::var("KV0_TEST_S3_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())?;
    let bucket = std::env::var("KV0_TEST_S3_BUCKET").unwrap_or_else(|_| "kv0-test".into());
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.into());
    let opts = HashMap::from([
        ("aws_endpoint".to_string(), endpoint),
        (
            "aws_access_key_id".to_string(),
            env("KV0_TEST_S3_KEY", "minioadmin"),
        ),
        (
            "aws_secret_access_key".to_string(),
            env("KV0_TEST_S3_SECRET", "minioadmin"),
        ),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("allow_http".to_string(), "true".to_string()),
    ]);
    let prefix = format!("s3://{bucket}/rs-{}", uuid_like());
    Some((prefix, opts))
}

fn uuid_like() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Runs `f` and fails the test when it takes longer than `limit`, so a stuck request or task
/// fails fast instead of holding the whole job.
fn within<F: FnOnce() + Send + 'static>(limit: Duration, f: F) {
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(limit) {
        Ok(()) => worker.join().unwrap(),
        // The closure panicked: join to surface its message.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(e) = worker.join() {
                std::panic::resume_unwind(e);
            }
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("the test did not finish within {limit:?}")
        }
    }
}

const LIMIT: Duration = Duration::from_secs(180);

#[test]
fn kv_concurrent_writers_on_s3() {
    let Some((prefix, so)) = storage() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    within(LIMIT, move || kv_concurrent_writers(prefix, so));
}

fn kv_concurrent_writers(prefix: String, so: HashMap<String, String>) {
    let uri = format!("{prefix}/kv");
    let opts = Options {
        storage_options: so,
        flush_interval: None,
        max_fragments: 8,
        ..Options::default()
    };
    let a = Db::open(&uri, opts.clone()).unwrap();
    let b = Db::open(&uri, opts.clone()).unwrap();
    std::thread::scope(|s| {
        for (db, tag) in [(&a, "a"), (&b, "b")] {
            s.spawn(move || {
                for i in 0..30 {
                    db.put(format!("{tag}{i:03}").as_bytes(), b"v").unwrap();
                    db.flush().unwrap();
                }
            });
        }
    });
    let c = Db::open(&uri, opts).unwrap();
    let mut it = c.iter();
    it.seek_to_first();
    assert_eq!(it.count(), 60);
}

#[test]
fn tracking_on_s3() {
    let Some((prefix, so)) = storage() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    within(LIMIT, move || tracking(prefix, so));
}

/// Calls the blocking tracking APIs from a task on the kv0 runtime, as code inside
/// `kv0::rt::block_on` may.
fn tracking(prefix: String, so: HashMap<String, String>) {
    let uri = format!("{prefix}/repo");
    kv0::rt::block_on(async move {
        let repo = Repo::open(
            &uri,
            RepoOptions {
                storage_options: so,
                flush_interval: None,
                ..RepoOptions::default()
            },
        )
        .await
        .unwrap();
        let run = repo.create_run(NewRun::default()).unwrap();
        for i in 0..1000 {
            run.track("loss", i as f64, Track::default()).unwrap();
        }
        repo.optimize().await.unwrap();
        let d = repo
            .read_metric(
                &run.id(),
                "loss",
                &json!({}),
                StepRange {
                    start: Some(990),
                    stop: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(d.steps, (990..1000).collect::<Vec<_>>());
    });
}
