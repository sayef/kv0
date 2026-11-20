//! The concurrency and crash suite of the tracking layer: several processes
//! writing, deleting and reading at once; object-store faults (errors, timeouts, lost
//! responses, duplicated requests); and crashes at every write of a flush, so between the
//! commits of its four tables.
//!
//! Every scenario checks the result against what the writers acknowledged, counting raw rows
//! (not newest-wins results): no acknowledged write is lost or stored twice, and deleted runs
//! do not come back. A replayed write-ahead log lands exactly once because each table's commit
//! carries the writer's transaction id.
//!
//! Child processes are this test binary again (`moraine_child`), told what to do through
//! environment variables. Knobs:
//! - `KV0_CHAOS_SEED`: the random seed (printed by every test; set it to reproduce a run);
//! - `KV0_CHAOS_SCALE`: multiplies writers' work (default 1);
//! - `KV0_TEST_S3_ENDPOINT`: also runs the S3 variants against an S3-compatible server.
//!
//! The scenarios with maintenance (`optimize`, version cleanup, purge), tags and key-value
//! databases are in the shared chaos suite (`tests/chaos.rs`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use kv0::rt::block_on;
use kv0::testing::{set_faults, Crash, FaultKind, Faults, StoreOp};
use kv0::tracking::{NewRecord, NewRun, RecordQuery, Repo, RepoOptions, StepRange, TrackPoint};
use serde_json::json;

/// Points each run logs.
const STEPS: i64 = 30;

/// SplitMix64.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn hash(s: &str) -> u64 {
    s.bytes().fold(0, |h, b| mix(h ^ u64::from(b)))
}

/// The seed of this run: `KV0_CHAOS_SEED`, else the clock. Printed for reproduction.
fn seed(test: &str) -> u64 {
    let s = std::env::var("KV0_CHAOS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("{test}: KV0_CHAOS_SEED={s}");
    s
}

fn scale() -> u32 {
    std::env::var("KV0_CHAOS_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1)
}

fn s3() -> Option<(String, HashMap<String, String>)> {
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
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some((format!("s3://{bucket}/moraine-{nanos:x}"), opts))
}

/// Retries `f` while it fails (faults, races), up to two minutes.
fn retry<T>(what: &str, mut f: impl FnMut() -> kv0::Result<T>) -> T {
    let start = Instant::now();
    loop {
        match f() {
            Ok(v) => return v,
            Err(e) if start.elapsed() < Duration::from_secs(120) => {
                eprintln!("{what}: {e}; again");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("{what}: {e}"),
        }
    }
}

/// Faults on every operation kind: writes at `rate`, reads at a tenth of it; from `seed`.
fn random_faults(seed: u64, rate: f64) -> Faults {
    Faults {
        seed,
        rate,
        read_rate: rate / 10.0,
        ops: vec![
            StoreOp::Put,
            StoreOp::ConditionalPut,
            StoreOp::Get,
            StoreOp::List,
            StoreOp::Delete,
        ],
        kinds: vec![
            FaultKind::Error,
            FaultKind::Timeout,
            FaultKind::LostResponse,
            FaultKind::DuplicateSend,
        ],
        paths: None,
        crash: None,
        stall: None,
    }
}

// ---------------------------------------------------------------------------------------------
// Child processes

struct Spawner {
    exe: PathBuf,
    uri: String,
    dir: PathBuf,
    storage: HashMap<String, String>,
    seed: u64,
}

impl Spawner {
    fn new(uri: &str, dir: &Path, storage: &HashMap<String, String>, seed: u64) -> Spawner {
        Spawner {
            exe: std::env::current_exe().unwrap(),
            uri: uri.to_string(),
            dir: dir.to_path_buf(),
            storage: storage.clone(),
            seed,
        }
    }

    fn spawn(&self, role: &str, id: &str, arg: &str, extra: &[(&str, String)]) -> Child {
        let mut c = Command::new(&self.exe);
        c.args([
            "--exact",
            "moraine_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("KV0_CHAOS_ROLE", role)
        .env("KV0_CHAOS_URI", &self.uri)
        .env("KV0_CHAOS_DIR", &self.dir)
        .env("KV0_CHAOS_ID", id)
        .env("KV0_CHAOS_ARG", arg)
        .env("KV0_CHAOS_SEED", self.seed.to_string())
        .env(
            "KV0_CHAOS_OPTS",
            serde_json::to_string(&self.storage).unwrap(),
        );
        for (k, v) in extra {
            c.env(k, v);
        }
        c.spawn().unwrap()
    }

    fn stop(&self) {
        std::fs::write(self.dir.join("stop"), b"").unwrap();
    }
}

fn wait_all(children: Vec<(String, Child)>) {
    for (name, mut c) in children {
        let st = c.wait().unwrap();
        assert!(st.success(), "child {name} failed: {st}");
    }
}

/// What a child was told.
struct Ctx {
    uri: String,
    dir: PathBuf,
    id: String,
    arg: String,
    storage: HashMap<String, String>,
}

impl Ctx {
    fn stopped(&self) -> bool {
        self.dir.join("stop").exists()
    }
    fn wal(&self) -> PathBuf {
        self.dir.join("wal").join(&self.id)
    }
}

#[test]
fn moraine_child() {
    let Ok(role) = std::env::var("KV0_CHAOS_ROLE") else {
        return;
    };
    let var = |k: &str| std::env::var(k).unwrap();
    let cx = Ctx {
        uri: var("KV0_CHAOS_URI"),
        dir: PathBuf::from(var("KV0_CHAOS_DIR")),
        id: var("KV0_CHAOS_ID"),
        arg: var("KV0_CHAOS_ARG"),
        storage: serde_json::from_str(&var("KV0_CHAOS_OPTS")).unwrap(),
    };
    let seed: u64 = var("KV0_CHAOS_SEED").parse().unwrap();
    // Faults first: only repositories opened afterwards are wrapped.
    if let Ok(rate) = std::env::var("KV0_CHAOS_FAULTS") {
        let rate: f64 = rate.parse().unwrap();
        set_faults(Some(random_faults(mix(seed) ^ hash(&cx.id), rate)));
    }
    if std::env::var("KV0_CHAOS_CRASH").is_ok() {
        // Armed later, once the setup writes are done.
        set_faults(Some(Faults::default()));
    }
    match role.as_str() {
        "write" => trk_write(&cx),
        "delete" => trk_delete(&cx),
        "read" => trk_read(&cx),
        "crash" => trk_crash(&cx),
        other => panic!("unknown role {other}"),
    }
}

/// Arms the crash point `KV0_CHAOS_CRASH` (`<nth>:<before|after|lost>`): the process ends
/// before or after the `nth` write, or (`lost`) the `nth` write lands but its response is lost
/// and the writer carries on.
fn arm_crash() {
    let spec = std::env::var("KV0_CHAOS_CRASH").unwrap();
    let (nth, when) = spec.split_once(':').unwrap();
    let nth: u64 = nth.parse().unwrap();
    if when == "lost" {
        set_faults(Some(Faults {
            seed: nth,
            rate: 1.0,
            ops: vec![StoreOp::ConditionalPut],
            kinds: vec![FaultKind::LostResponse],
            paths: Some("_delta_log".into()),
            ..Faults::default()
        }));
        return;
    }
    set_faults(Some(Faults {
        crash: Some(Crash {
            ops: vec![StoreOp::Put, StoreOp::ConditionalPut],
            nth,
            after: when == "after",
        }),
        ..Faults::default()
    }));
}

// ---------------------------------------------------------------------------------------------
// Tracking

fn trk_opts(wal: &Path, storage: &HashMap<String, String>) -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        wal_dir: Some(wal.to_path_buf()),
        storage_options: storage.clone(),
        ..RepoOptions::default()
    }
}

fn trk_open_at(uri: &str, o: RepoOptions) -> Repo {
    retry("open", || {
        let (uri, o) = (uri.to_string(), o.clone());
        block_on(async move { Repo::open(&uri, o).await })
    })
}

fn trk_open(cx: &Ctx) -> Repo {
    trk_open_at(&cx.uri, trk_opts(&cx.wal(), &cx.storage))
}

fn points(n: i64) -> Vec<TrackPoint> {
    (0..n)
        .map(|i| TrackPoint {
            value: i as f64,
            step: Some(i),
            epoch: None,
            timestamp: Some(1_000 + i),
        })
        .collect()
}

/// The marker a run's parameter and record carry.
fn marker(run_id: &str) -> String {
    format!("marker-{}", hash(run_id))
}

/// One run with a parameter, `STEPS` points and a record; flushed.
fn write_run(repo: &Repo, run_id: &str) {
    let run = repo
        .create_run(NewRun {
            run_id: Some(run_id.to_string()),
            ..NewRun::default()
        })
        .unwrap();
    run.set_param("note", json!(marker(run_id))).unwrap();
    run.track_many("loss", None, &points(STEPS)).unwrap();
    let r = repo.clone();
    retry("flush", || {
        let r = r.clone();
        block_on(async move { r.flush().await })
    });
    let rec = NewRecord {
        item_id: run_id.to_string(),
        run_id: Some(run_id.to_string()),
        data: json!({ "out": marker(run_id) }),
    };
    retry("append records", || {
        let (r, rec) = (repo.clone(), rec.clone());
        block_on(async move { r.append_records("evals", vec![rec]).await })
    });
}

fn close(repo: &Repo) {
    set_faults(None);
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
}

fn trk_write(cx: &Ctx) {
    let repo = trk_open(cx);
    for k in 0..cx.arg.parse::<u32>().unwrap() {
        write_run(&repo, &format!("{}-{k}", cx.id));
    }
    close(&repo);
}

fn trk_delete(cx: &Ctx) {
    let repo = trk_open(cx);
    for victim in cx.arg.split(',') {
        let v = vec![victim.to_string()];
        let r = repo.clone();
        retry("delete", || {
            let (r, v) = (r.clone(), v.clone());
            block_on(async move { r.delete_runs(&v).await })
        });
    }
    close(&repo);
}

/// Reads what the setup wrote while others write and delete: the run `stable` stays complete.
fn trk_read(cx: &Ctx) {
    let repo = trk_open(cx);
    let mut reads = 0;
    while !cx.stopped() {
        let r = repo.clone();
        let (d, recs) = block_on(async move {
            let d = r
                .read_metric("stable", "loss", &json!({}), StepRange::default())
                .await?;
            let recs = r
                .records(
                    "evals",
                    RecordQuery {
                        run_ids: Some(vec!["stable".into()]),
                        ..RecordQuery::default()
                    },
                )
                .await?;
            Ok::<_, kv0::Error>((d, recs))
        })
        .unwrap();
        assert_eq!(d.steps, (0..STEPS).collect::<Vec<_>>());
        assert_eq!(recs.len(), 1);
        reads += 1;
    }
    eprintln!("{reads} reads");
    close(&repo);
}

fn trk_crash(cx: &Ctx) {
    let o = RepoOptions {
        wal_sync: kv0::kv::WalSync::Always,
        ..trk_opts(&cx.wal(), &cx.storage)
    };
    let repo = trk_open_at(&cx.uri, o);
    write_run(&repo, "a");
    arm_crash();
    let run = repo
        .create_run(NewRun {
            run_id: Some("b".into()),
            ..NewRun::default()
        })
        .unwrap();
    run.set_param("note", json!(marker("b"))).unwrap();
    run.track_many("loss", None, &points(STEPS)).unwrap();
    let r = repo.clone();
    let flushed = block_on(async move { r.flush().await });
    if std::env::var("KV0_CHAOS_CRASH").is_ok_and(|s| s.ends_with(":lost")) {
        // Every commit's response lost: the flush fails or finds its commit by its
        // transaction id. Either way, end without flushing again, as a crash would.
        eprintln!("flush with lost responses: {flushed:?}");
        repo.abandon();
        std::fs::write(cx.dir.join("lost"), b"").unwrap();
        return;
    }
    flushed.unwrap();
    close(&repo);
    std::fs::write(cx.dir.join("survived"), b"").unwrap();
}

/// Every run of `live` is complete and stored once; `gone` left nothing.
fn trk_check(repo: &Repo, live: &[String], gone: &[String], records: bool) {
    let r = repo.clone();
    let mut have: Vec<String> = block_on(async move { r.runs().await })
        .unwrap()
        .into_iter()
        .map(|i| i.run_id)
        .collect();
    have.sort();
    let mut want = live.to_vec();
    want.sort();
    assert_eq!(have, want, "runs");
    let r = repo.clone();
    let params = block_on(async move { r.all_params().await }).unwrap();
    for id in live {
        assert_eq!(params[id]["note"], json!(marker(id)), "{id}");
        let (r, i) = (repo.clone(), id.clone());
        let d = block_on(async move {
            r.read_metric(&i, "loss", &json!({}), StepRange::default())
                .await
        })
        .unwrap();
        assert_eq!(d.steps, (0..STEPS).collect::<Vec<_>>(), "{id}");
        assert_eq!(
            d.values,
            (0..STEPS).map(|i| i as f64).collect::<Vec<_>>(),
            "{id}"
        );
    }
    for id in gone {
        assert!(!params.contains_key(id), "{id}");
    }
    if records {
        let r = repo.clone();
        let rows =
            block_on(async move { r.records("evals", RecordQuery::default()).await }).unwrap();
        let mut items: Vec<String> = rows.into_iter().map(|x| x.item_id).collect();
        items.sort();
        assert_eq!(items, want, "records");
    }
    // Raw rows, not newest-wins results: a commit applied twice shows here.
    let r = repo.clone();
    let rows = block_on(async move {
        Ok::<_, kv0::Error>((
            r.metric_rows().await?,
            r.table_rows("params").await?,
            r.table_rows("series").await?,
        ))
    })
    .unwrap();
    let n = live.len();
    assert_eq!(
        rows,
        (n * STEPS as usize, n, n),
        "points, params, series stored once"
    );
    // Statistics stored with the points agree with them.
    let r = repo.clone();
    let stats = block_on(async move { r.all_series_stats().await }).unwrap();
    for s in stats.values() {
        assert_eq!(s.count, STEPS as u64, "{s:?}");
    }
}

/// Writers from several processes append while one deletes runs written before and one reads:
/// no append is lost or stored twice, and the deleted runs are gone from every table.
fn trk_race(uri: &str, storage: &HashMap<String, String>, writers: u32, runs: u32) {
    let seed = seed("trk_race");
    let dir = tempfile::tempdir().unwrap();
    let sp = Spawner::new(uri, dir.path(), storage, seed);
    let repo = trk_open_at(uri, trk_opts(&dir.path().join("wal/setup"), storage));
    let victims: Vec<String> = (0..writers).map(|k| format!("victim-{k}")).collect();
    for v in victims.iter().chain(std::iter::once(&"stable".to_string())) {
        write_run(&repo, v);
    }
    close(&repo);

    let mut writing: Vec<(String, Child)> = (0..writers)
        .map(|w| {
            let id = format!("w{w}");
            (id.clone(), sp.spawn("write", &id, &runs.to_string(), &[]))
        })
        .collect();
    writing.push((
        "deleter".into(),
        sp.spawn("delete", "deleter", &victims.join(","), &[]),
    ));
    let reader = vec![("reader".to_string(), sp.spawn("read", "reader", "", &[]))];
    wait_all(writing);
    sp.stop();
    wait_all(reader);

    let mut live: Vec<String> = (0..writers)
        .flat_map(|w| (0..runs).map(move |k| format!("w{w}-{k}")))
        .collect();
    live.push("stable".into());
    let repo = trk_open_at(uri, trk_opts(&dir.path().join("wal/check"), storage));
    trk_check(&repo, &live, &victims, true);
    close(&repo);
}

#[test]
fn tracking_writers_deletes_and_readers_race() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    trk_race(&uri, &HashMap::new(), 6, 4 * scale());
}

#[test]
fn tracking_writers_deletes_and_readers_race_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    trk_race(&uri, &storage, 3, 2 * scale());
}

fn trk_faults(uri: &str, storage: &HashMap<String, String>, writers: u32, runs: u32) {
    let seed = seed("trk_faults");
    let dir = tempfile::tempdir().unwrap();
    let sp = Spawner::new(uri, dir.path(), storage, seed);
    close(&trk_open_at(
        uri,
        trk_opts(&dir.path().join("wal/setup"), storage),
    ));
    let children = (0..writers)
        .map(|w| {
            let id = format!("w{w}");
            let c = sp.spawn(
                "write",
                &id,
                &runs.to_string(),
                &[("KV0_CHAOS_FAULTS", "0.05".into())],
            );
            (id, c)
        })
        .collect();
    wait_all(children);
    let live: Vec<String> = (0..writers)
        .flat_map(|w| (0..runs).map(move |k| format!("w{w}-{k}")))
        .collect();
    let repo = trk_open_at(uri, trk_opts(&dir.path().join("wal/check"), storage));
    trk_check(&repo, &live, &[], true);
    close(&repo);
}

#[test]
fn tracking_writers_survive_object_store_faults() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    trk_faults(&uri, &HashMap::new(), 4, 4 * scale());
}

#[test]
fn tracking_writers_survive_object_store_faults_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    trk_faults(&uri, &storage, 3, 2 * scale());
}

/// A process ending before or after each write of a tracking flush, which commits the runs,
/// params, series and metrics tables one after another (a crash between table commits among
/// them): the replay commits exactly what the crash left uncommitted, once.
fn crash_every_write(uri_of: impl Fn(&Path) -> (String, HashMap<String, String>)) {
    let seed = seed("trk_crash");
    for after in [false, true] {
        for nth in 1.. {
            let dir = tempfile::tempdir().unwrap();
            let (uri, storage) = uri_of(dir.path());
            let sp = Spawner::new(&uri, dir.path(), &storage, seed);
            let when = if after { "after" } else { "before" };
            let st = sp
                .spawn(
                    "crash",
                    "c",
                    "",
                    &[("KV0_CHAOS_CRASH", format!("{nth}:{when}"))],
                )
                .wait()
                .unwrap();
            if dir.path().join("survived").exists() {
                assert!(st.success());
                assert!(
                    nth > 8,
                    "a flush writes a data file and a log entry per table"
                );
                break;
            }
            assert!(!st.success(), "{nth} {when}: {st}");
            let repo = trk_open_at(&uri, trk_opts(&dir.path().join("wal/c"), &storage));
            trk_check(&repo, &["a".into(), "b".into()], &[], false);
            close(&repo);
        }
    }
}

#[test]
fn tracking_crash_at_every_write_of_a_flush_then_replay() {
    crash_every_write(|d| {
        (
            d.join("repo").to_string_lossy().into_owned(),
            HashMap::new(),
        )
    });
}

#[test]
fn tracking_crash_at_every_write_of_a_flush_then_replay_on_s3() {
    if s3().is_none() {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    }
    crash_every_write(|_| s3().unwrap());
}

/// Every log entry of a flush lands but its response is lost; the writer then ends. Opening
/// again replays the write-ahead log: each table holds the flush once.
#[test]
fn lost_commit_responses_then_replay_store_each_write_once() {
    let seed = seed("trk_lost");
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let sp = Spawner::new(&uri, dir.path(), &HashMap::new(), seed);
    let st = sp
        .spawn("crash", "c", "", &[("KV0_CHAOS_CRASH", "1:lost".into())])
        .wait()
        .unwrap();
    assert!(st.success() && dir.path().join("lost").exists(), "{st}");
    let repo = trk_open_at(&uri, trk_opts(&dir.path().join("wal/c"), &HashMap::new()));
    trk_check(&repo, &["a".into(), "b".into()], &[], false);
    close(&repo);
}

/// Concurrent creators of one repository: each table is created once, and every writer's
/// runs are there.
#[test]
fn concurrent_creators_make_each_table_once() {
    let seed = seed("create_race");
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let sp = Spawner::new(&uri, dir.path(), &HashMap::new(), seed);
    let children = (0..12)
        .map(|w| {
            let id = format!("w{w}");
            (id.clone(), sp.spawn("write", &id, "1", &[]))
        })
        .collect();
    wait_all(children);
    let tables: Vec<PathBuf> = std::fs::read_dir(&uri)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.join("data/_delta_log").is_dir())
        .collect();
    assert_eq!(
        tables.len(),
        5,
        "runs, params, series, metrics, one records table"
    );
    for t in &tables {
        let first = t.join("data/_delta_log").join(format!("{:020}.json", 0));
        let creates = std::fs::read_dir(t.join("data/_delta_log"))
            .unwrap()
            .flatten()
            .filter(|e| std::fs::read_to_string(e.path()).is_ok_and(|s| s.contains("\"metaData\"")))
            .count();
        assert!(first.exists(), "{}", t.display());
        assert_eq!(creates, 1, "{}", t.display());
    }
    let live: Vec<String> = (0..12).map(|w| format!("w{w}-0")).collect();
    let repo = trk_open_at(
        &uri,
        trk_opts(&dir.path().join("wal/check"), &HashMap::new()),
    );
    trk_check(&repo, &live, &[], true);
    close(&repo);
}

/// A `memory://` repository: every table of a handle shares one store, so records tables can
/// be listed; another handle sees nothing.
#[test]
fn a_memory_repository_shares_one_store_across_its_tables() {
    let uri = format!("memory://trk-{}", std::process::id());
    let o = RepoOptions {
        flush_interval: None,
        wal: false,
        ..RepoOptions::default()
    };
    let (u, oo) = (uri.clone(), o.clone());
    let repo = block_on(async move { Repo::open(&u, oo).await }).unwrap();
    let r = repo.clone();
    let tables = block_on(async move {
        for t in ["a", "b:c", &"long name ".repeat(10)] {
            r.append_records(
                t,
                vec![NewRecord {
                    item_id: "x".into(),
                    run_id: None,
                    data: json!(1),
                }],
            )
            .await?;
        }
        r.record_tables().await
    })
    .unwrap();
    assert_eq!(tables, ["a", "b:c", &"long name ".repeat(10)]);
    let other = block_on(async move { Repo::open(&uri, o).await }).unwrap();
    let tables = block_on(async move { other.record_tables().await }).unwrap();
    assert!(tables.is_empty());
}

/// moraine keeps the latest version and pinned (tagged) ones readable, nothing else: reading an
/// older version that no tag names fails with an error naming the way out, in both layers.
#[test]
fn only_the_latest_and_tagged_versions_are_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db").to_string_lossy().into_owned();
    let o = kv0::kv::Options {
        flush_interval: None,
        wal_dir: Some(dir.path().join("wal-kv")),
        ..kv0::kv::Options::default()
    };
    let db = kv0::kv::Db::open(&path, o.clone()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    let v1 = db.version();
    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();
    let at = |v: u64| {
        kv0::kv::Db::open(
            &path,
            kv0::kv::Options {
                version: Some(v),
                ..o.clone()
            },
        )
    };
    let Err(e) = at(v1) else {
        panic!("an untagged old version opened");
    };
    assert!(
        matches!(&e, kv0::Error::NotFound(m) if m.contains("tag")),
        "{e}"
    );
    db.create_tag("first", Some(v1)).unwrap();
    assert_eq!(
        at(v1).unwrap().get(b"k").unwrap().as_deref(),
        Some(&b"v1"[..])
    );
    // The tag is a commit of its own (a table property), so the latest version moved on.
    let versions: Vec<u64> = db.versions().unwrap().iter().map(|v| v.version).collect();
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert_eq!(versions[0], v1);
    db.close().unwrap();

    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let repo = trk_open_at(&uri, trk_opts(&dir.path().join("wal"), &HashMap::new()));
    let r = repo.clone();
    let rec = |v: i64| NewRecord {
        item_id: "q".into(),
        run_id: None,
        data: json!({ "v": v }),
    };
    let e = block_on(async move {
        let v = r.append_records("evals", vec![rec(1)]).await?;
        r.append_records("evals", vec![rec(2)]).await?;
        r.records(
            "evals",
            RecordQuery {
                at: kv0::tracking::At::Version(v),
                ..RecordQuery::default()
            },
        )
        .await
    })
    .unwrap_err();
    assert!(
        matches!(&e, kv0::Error::NotFound(m) if m.contains("tag")),
        "{e}"
    );
    close(&repo);
}
