//! The concurrency and crash suite of the Delta Lake engine: several processes writing,
//! compacting, deleting, optimizing, cleaning up and purging at once; object-store faults
//! (errors, timeouts, lost responses, duplicated requests); crashes at every write of a flush;
//! readers pinned to old and tagged versions; and the protocol guards.
//!
//! Every scenario checks the result against a reference model built from what the writers
//! acknowledged: no acknowledged write is lost or stored twice, deleted data never comes back,
//! pinned readers keep working, and purge removes the bytes of deleted data.
//!
//! Child processes are this test binary again (`chaos_child`), told what to do through
//! environment variables. Knobs:
//! - `KV0_CHAOS_SEED`: the random seed (printed by every test; set it to reproduce a run);
//! - `KV0_CHAOS_SCALE`: multiplies writers' work (default 1; the nightly job runs more);
//! - `KV0_TEST_S3_ENDPOINT`: also runs the S3 variants (CI starts an S3-compatible server).
#![cfg(feature = "delta")]

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use kv0::kv::{Db, Options, WalSync};
use kv0::rt::block_on;
use kv0::testing::{set_faults, stored_anywhere, Crash, FaultKind, Faults, StoreOp};
use kv0::tracking::{At, NewRecord, NewRun, RecordQuery, Repo, RepoOptions, StepRange, TrackPoint};
use serde_json::json;

// ---------------------------------------------------------------------------------------------
// Setup shared by parent and children

fn delta() {
    // Every test of this binary sets the same value, so the races between them are harmless.
    std::env::set_var("KV0_TEST_BACKEND", "delta");
}

/// SplitMix64.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// A small deterministic random source.
struct Rng(u64);

impl Rng {
    fn new(seed: u64, stream: &str) -> Rng {
        let h = stream.bytes().fold(seed, |h, b| mix(h ^ u64::from(b)));
        Rng(h)
    }
    fn next(&mut self) -> u64 {
        self.0 = mix(self.0);
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
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
    Some((format!("s3://{bucket}/chaos-{nanos:x}"), opts))
}

/// Retries `f` while it fails (faults, races), up to about a minute.
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
    }
}

/// An append-only log of acknowledged operations, one line each, written before the next
/// operation starts (so it survives the process ending at any point).
struct Acks(std::fs::File);

impl Acks {
    fn create(path: &Path) -> Acks {
        Acks(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
        )
    }
    fn put(&mut self, key: &str, value: &str) {
        writeln!(self.0, "P {key} {value}").unwrap();
    }
    fn delete(&mut self, key: &str) {
        writeln!(self.0, "D {key}").unwrap();
    }
}

/// What the acknowledged operations under `dir/*.acks` leave: the live value per key, the
/// values deleted or overwritten since, and the number of operations.
#[derive(Default)]
struct Model {
    live: BTreeMap<String, String>,
    dead: Vec<String>,
    ops: usize,
}

fn model(dir: &Path) -> Model {
    let mut m = Model::default();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        if e.path().extension().is_none_or(|x| x != "acks") {
            continue;
        }
        for line in std::fs::read_to_string(e.path()).unwrap().lines() {
            let mut parts = line.splitn(3, ' ');
            let (op, key) = (parts.next().unwrap(), parts.next().unwrap().to_string());
            m.ops += 1;
            let old = match op {
                "P" => m.live.insert(key, parts.next().unwrap().to_string()),
                _ => m.live.remove(&key),
            };
            m.dead.extend(old);
        }
    }
    m
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
        c.args(["--exact", "chaos_child", "--nocapture", "--test-threads=1"])
            .env("KV0_TEST_BACKEND", "delta")
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
    seed: u64,
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
fn chaos_child() {
    let Ok(role) = std::env::var("KV0_CHAOS_ROLE") else {
        return;
    };
    let var = |k: &str| std::env::var(k).unwrap();
    let cx = Ctx {
        uri: var("KV0_CHAOS_URI"),
        dir: PathBuf::from(var("KV0_CHAOS_DIR")),
        id: var("KV0_CHAOS_ID"),
        arg: var("KV0_CHAOS_ARG"),
        seed: var("KV0_CHAOS_SEED").parse().unwrap(),
        storage: serde_json::from_str(&var("KV0_CHAOS_OPTS")).unwrap(),
    };
    // Faults first: only tables opened afterwards are wrapped.
    if let Ok(rate) = std::env::var("KV0_CHAOS_FAULTS") {
        let rate: f64 = rate.parse().unwrap();
        set_faults(Some(random_faults(
            mix(cx.seed ^ mix(cx.id.len() as u64)) ^ hash(&cx.id),
            rate,
        )));
    }
    if std::env::var("KV0_CHAOS_CRASH").is_ok() {
        // Armed later, once the setup writes are done.
        set_faults(Some(Faults::default()));
    }
    match role.as_str() {
        "kv-write" => kv_write(&cx),
        "kv-compact" => kv_compact(&cx),
        "kv-clean" => kv_clean(&cx),
        "kv-purge" => kv_purge(&cx),
        "kv-read" => kv_read(&cx),
        "kv-crash" => kv_crash(&cx),
        "kv-checkpoint" => kv_checkpoint(&cx),
        "trk-write" => trk_write(&cx),
        "trk-delete" => trk_delete(&cx),
        "trk-optimize" => trk_optimize(&cx),
        "trk-clean" => trk_clean(&cx),
        "trk-purge" => trk_purge(&cx),
        "trk-read" => trk_read(&cx),
        "trk-crash" => trk_crash(&cx),
        other => panic!("unknown role {other}"),
    }
}

fn hash(s: &str) -> u64 {
    s.bytes().fold(0, |h, b| mix(h ^ u64::from(b)))
}

/// Arms the crash point `KV0_CHAOS_CRASH` (`<nth>:<before|after>`).
fn arm_crash() {
    let spec = std::env::var("KV0_CHAOS_CRASH").unwrap();
    let (nth, when) = spec.split_once(':').unwrap();
    set_faults(Some(Faults {
        crash: Some(Crash {
            ops: vec![StoreOp::Put, StoreOp::ConditionalPut],
            nth: nth.parse().unwrap(),
            after: when == "after",
        }),
        ..Faults::default()
    }));
}

// ---------------------------------------------------------------------------------------------
// Key-value store

/// Values above this are lazy (read from storage when needed).
const LAZY: usize = 64;

fn kv_opts(wal: &Path, storage: &HashMap<String, String>) -> Options {
    Options {
        flush_interval: None,
        refresh_interval: Some(Duration::from_millis(100)),
        write_buffer_size: usize::MAX,
        max_fragments: 0,
        lazy_value_threshold: LAZY,
        snapshot_min_entries: usize::MAX,
        // Racing cleanups must never take a file a writer is about to commit; keep the grace
        // well above the time a faulty commit can take.
        orphan_grace: Duration::from_secs(120),
        wal_dir: Some(wal.to_path_buf()),
        storage_options: storage.clone(),
        ..Options::default()
    }
}

fn kv_open(cx: &Ctx, o: Options) -> Db {
    retry("open", || Db::open(&cx.uri, o.clone()))
}

/// A value for `key`: unique (its token appears nowhere else) and sometimes long (lazy).
fn kv_value(rng: &mut Rng, key: &str) -> String {
    let token = format!("tok{:016x}", rng.next());
    let pad = if rng.below(3) == 0 { 120 } else { 8 };
    format!("{key}.{token}.{}", "x".repeat(pad))
}

/// `KV0_CHAOS_ARG` batches of puts, overwrites and deletes of its own keys, a flush each.
fn kv_write(cx: &Ctx) {
    let db = kv_open(cx, kv_opts(&cx.wal(), &cx.storage));
    let mut acks = Acks::create(&cx.dir.join(format!("{}.acks", cx.id)));
    let mut rng = Rng::new(cx.seed, &cx.id);
    let mut keys: Vec<String> = Vec::new();
    let mut next = 0;
    for _ in 0..cx.arg.parse::<u32>().unwrap() {
        for _ in 0..8 {
            match rng.below(8) {
                0 | 1 if !keys.is_empty() => {
                    let k = keys.swap_remove(rng.below(keys.len() as u64) as usize);
                    db.delete(k.as_bytes()).unwrap();
                    acks.delete(&k);
                }
                2 if !keys.is_empty() => {
                    let k = keys[rng.below(keys.len() as u64) as usize].clone();
                    let v = kv_value(&mut rng, &k);
                    db.put(k.as_bytes(), v.as_bytes()).unwrap();
                    acks.put(&k, &v);
                }
                _ => {
                    let k = format!("{}-{next:05}", cx.id);
                    next += 1;
                    let v = kv_value(&mut rng, &k);
                    db.put(k.as_bytes(), v.as_bytes()).unwrap();
                    acks.put(&k, &v);
                    keys.push(k);
                }
            }
        }
        retry("flush", || db.flush());
    }
    set_faults(None);
    db.close().unwrap();
}

fn tolerated(e: &kv0::Error) -> bool {
    // Another rewrite took the same files first, or cleanup removed what an older version
    // read: both leave the data as it was.
    matches!(e, kv0::Error::Conflict(_) | kv0::Error::NotFound(_))
}

fn kv_compact(cx: &Ctx) {
    let db = kv_open(cx, kv_opts(&cx.wal(), &cx.storage));
    let mut done = 0;
    while !cx.stopped() {
        match db.compact() {
            Ok(()) => done += 1,
            Err(e) if tolerated(&e) => eprintln!("compact: {e}"),
            Err(e) => panic!("compact: {e}"),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("{done} compactions");
    db.close().unwrap();
}

fn kv_clean(cx: &Ctx) {
    let db = kv_open(cx, kv_opts(&cx.wal(), &cx.storage));
    while !cx.stopped() {
        db.cleanup_old_versions(Duration::from_secs(10)).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    }
    db.close().unwrap();
}

fn kv_purge(cx: &Ctx) {
    let db = kv_open(cx, kv_opts(&cx.wal(), &cx.storage));
    while !cx.stopped() {
        match db.purge(true) {
            Ok(r) => assert!(r.tags_deleted.is_empty(), "{r:?}"),
            Err(e) if tolerated(&e) => eprintln!("purge: {e}"),
            Err(e) => panic!("purge: {e}"),
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    db.close().unwrap();
}

/// Keys the setup writes once and tags `stable`; readers check them throughout.
fn stable(i: usize) -> (String, String) {
    (
        format!("stable-{i:03}"),
        format!("stable-value-{i}-{}", "s".repeat(200)),
    )
}

const STABLE: usize = 20;

/// Reads the stable keys through a refreshing handle and through one opened at the tag, while
/// others compact, clean up and purge: every read succeeds with the value written.
fn kv_read(cx: &Ctx) {
    let o = Options {
        read_only: true,
        ..kv_opts(&cx.wal(), &cx.storage)
    };
    let live = kv_open(cx, o.clone());
    let v = live.tag_version("stable").unwrap();
    let pinned = kv_open(
        cx,
        Options {
            version: Some(v),
            ..o
        },
    );
    let mut reads = 0;
    while !cx.stopped() {
        for db in [&live, &pinned] {
            for i in 0..STABLE {
                let (k, want) = stable(i);
                let got = db.get(k.as_bytes()).unwrap();
                assert_eq!(got.as_deref(), Some(want.as_bytes()), "{k}");
                reads += 1;
            }
        }
    }
    eprintln!("{reads} reads");
}

/// Writes and flushes setup keys, arms the crash point, then writes more and flushes: the
/// process ends at the crash point, or writes `survived` when the flush had fewer writes.
fn kv_crash(cx: &Ctx) {
    let o = Options {
        wal_sync: WalSync::Always,
        ..kv_opts(&cx.wal(), &cx.storage)
    };
    let db = kv_open(cx, o);
    let mut acks = Acks::create(&cx.dir.join(format!("{}.acks", cx.id)));
    for i in 0..5 {
        let (k, v) = (
            format!("a-{i}"),
            format!("a-value-{i}-{}", "a".repeat(i * 40)),
        );
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        acks.put(&k, &v);
    }
    db.flush().unwrap();
    arm_crash();
    for i in 0..5 {
        let (k, v) = (
            format!("b-{i}"),
            format!("b-value-{i}-{}", "b".repeat(i * 40)),
        );
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        acks.put(&k, &v);
    }
    db.flush().unwrap();
    set_faults(None);
    db.close().unwrap();
    std::fs::write(cx.dir.join("survived"), b"").unwrap();
}

/// Every key of the model has its value, and no other key (beyond `stable-*`) exists.
fn kv_check(db: &Db, m: &Model) {
    let mut it = db.iter();
    it.seek_to_first();
    let mut have: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in it {
        let k = String::from_utf8(k.to_vec()).unwrap();
        if !k.starts_with("stable-") {
            have.insert(k, String::from_utf8(v.to_vec()).unwrap());
        }
    }
    assert_eq!(have.len(), m.live.len(), "live keys");
    for (k, v) in &m.live {
        assert_eq!(have.get(k), Some(v), "{k}");
    }
}

/// Writers, a compactor, a cleaner, a purger and a pinned reader at once.
fn kv_race(uri: &str, storage: &HashMap<String, String>, writers: u32, batches: u32) {
    delta();
    let seed = seed("kv_race");
    let dir = tempfile::tempdir().unwrap();
    let sp = Spawner::new(uri, dir.path(), storage, seed);
    let db = Db::open(uri, kv_opts(&dir.path().join("wal/setup"), storage)).unwrap();
    for i in 0..STABLE {
        let (k, v) = stable(i);
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.create_tag("stable", None).unwrap();
    db.close().unwrap();

    let mut writing = Vec::new();
    for w in 0..writers {
        let id = format!("w{w}");
        writing.push((
            id.clone(),
            sp.spawn("kv-write", &id, &batches.to_string(), &[]),
        ));
    }
    let others: Vec<(String, Child)> = ["kv-compact", "kv-clean", "kv-purge", "kv-read"]
        .into_iter()
        .map(|r| (r.to_string(), sp.spawn(r, r, "", &[])))
        .collect();
    wait_all(writing);
    sp.stop();
    wait_all(others);

    let m = model(dir.path());
    let db = Db::open(uri, kv_opts(&dir.path().join("wal/check"), storage)).unwrap();
    kv_check(&db, &m);
    for i in 0..STABLE {
        let (k, v) = stable(i);
        assert_eq!(db.get(k.as_bytes()).unwrap().as_deref(), Some(v.as_bytes()));
    }
    // Erasure: with the writers gone, a purge leaves no byte of a deleted or overwritten
    // value, and the tag (whose keys are all live) stays.
    let r = db.purge(false).unwrap();
    assert!(r.tags_deleted.is_empty() && r.tags_left.is_empty(), "{r:?}");
    kv_check(&db, &m);
    if kv0::storage::is_local(uri) {
        let root = Path::new(uri);
        for v in &m.dead {
            let token = v.split('.').nth(1).unwrap();
            assert!(
                !stored_anywhere(root, token.as_bytes()),
                "{token} of {v} survived purge"
            );
        }
        let (_, live) = m.live.iter().next().unwrap();
        let token = live.split('.').nth(1).unwrap();
        assert!(stored_anywhere(root, token.as_bytes()), "the search works");
    }
    assert_eq!(db.tags().unwrap().len(), 1);
    db.close().unwrap();
}

#[test]
fn kv_writers_compaction_cleanup_purge_and_readers_race() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("db").to_string_lossy().into_owned();
    kv_race(&uri, &HashMap::new(), 4, 15 * scale());
}

#[test]
fn kv_writers_compaction_cleanup_purge_and_readers_race_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    kv_race(&uri, &storage, 3, 5 * scale());
}

/// Writers under random object-store faults: every acknowledged operation is stored exactly
/// once (a lost commit response is recognised by the writer's mark, not committed again).
fn kv_faults(uri: &str, storage: &HashMap<String, String>, writers: u32, batches: u32) {
    delta();
    let seed = seed("kv_faults");
    let dir = tempfile::tempdir().unwrap();
    let sp = Spawner::new(uri, dir.path(), storage, seed);
    let db = Db::open(uri, kv_opts(&dir.path().join("wal/setup"), storage)).unwrap();
    db.close().unwrap();
    let children = (0..writers)
        .map(|w| {
            let id = format!("w{w}");
            let c = sp.spawn(
                "kv-write",
                &id,
                &batches.to_string(),
                &[("KV0_CHAOS_FAULTS", "0.05".into())],
            );
            (id, c)
        })
        .collect();
    wait_all(children);
    let m = model(dir.path());
    let db = Db::open(uri, kv_opts(&dir.path().join("wal/check"), storage)).unwrap();
    kv_check(&db, &m);
    assert_eq!(db.stats().stored_rows, m.ops, "every operation stored once");
    db.close().unwrap();
}

#[test]
fn kv_writers_survive_object_store_faults() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("db").to_string_lossy().into_owned();
    kv_faults(&uri, &HashMap::new(), 4, 10 * scale());
}

#[test]
fn kv_writers_survive_object_store_faults_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    kv_faults(&uri, &storage, 3, 4 * scale());
}

/// A process ending before or after each write of a flush (data file, commit, ...): the next
/// open replays its write-ahead log, and every acknowledged write is stored exactly once.
#[test]
fn kv_crash_at_every_write_of_a_flush_then_replay() {
    delta();
    let seed = seed("kv_crash");
    for after in [false, true] {
        for nth in 1.. {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().join("db").to_string_lossy().into_owned();
            let sp = Spawner::new(&uri, dir.path(), &HashMap::new(), seed);
            let when = if after { "after" } else { "before" };
            let st = sp
                .spawn(
                    "kv-crash",
                    "c",
                    "",
                    &[("KV0_CHAOS_CRASH", format!("{nth}:{when}"))],
                )
                .wait()
                .unwrap();
            if dir.path().join("survived").exists() {
                assert!(st.success());
                assert!(nth > 2, "a flush writes a data file and a commit");
                break;
            }
            assert!(!st.success(), "{nth} {when}: {st}");
            let m = model(dir.path());
            // The same WAL directory: this open adopts and replays the crashed writer's log.
            let db = Db::open(&uri, kv_opts(&dir.path().join("wal/c"), &HashMap::new())).unwrap();
            kv_check(&db, &m);
            assert_eq!(db.stats().stored_rows, 10, "{nth} {when}: stored once");
            // A data file written but never committed is collected after the grace period.
            db.close().unwrap();
            let db = Db::open(
                &uri,
                Options {
                    orphan_grace: Duration::ZERO,
                    ..kv_opts(&dir.path().join("wal/d"), &HashMap::new())
                },
            )
            .unwrap();
            db.cleanup_old_versions(Duration::ZERO).unwrap();
            let files = std::fs::read_dir(&uri)
                .unwrap()
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                .count();
            assert_eq!(files, db.stats().fragments, "{nth} {when}: no orphan left");
            kv_check(&db, &m);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Tracking

const STEPS: i64 = 30;

fn trk_opts(wal: &Path, storage: &HashMap<String, String>) -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        orphan_grace: Duration::from_secs(120),
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

/// The marker a run's parameter and record carry: found in storage only while the run is.
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

fn trk_optimize(cx: &Ctx) {
    let repo = trk_open(cx);
    let mut rounds = 0;
    while !cx.stopped() {
        let r = repo.clone();
        // Optimize plans each rewrite again when another took the same files first.
        if let Err(e) = block_on(async move { r.optimize().await }) {
            panic!("optimize: {e}");
        }
        rounds += 1;
    }
    eprintln!("{rounds} optimize rounds");
    close(&repo);
}

fn trk_clean(cx: &Ctx) {
    let repo = trk_open(cx);
    while !cx.stopped() {
        let r = repo.clone();
        block_on(async move { r.cleanup_old_versions(Duration::from_secs(10)).await }).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    }
    close(&repo);
}

fn trk_purge(cx: &Ctx) {
    let repo = trk_open(cx);
    while !cx.stopped() {
        let r = repo.clone();
        match block_on(async move { r.purge(true).await }) {
            Ok(r) => assert!(r.tags_deleted.is_empty(), "{r:?}"),
            Err(e) if tolerated(&e) => eprintln!("purge: {e}"),
            Err(e) => panic!("purge: {e}"),
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    close(&repo);
}

/// Rows of the `dataset` records table, tagged `v1` by the setup.
const DATASET: usize = 10;

fn dataset_rows() -> Vec<NewRecord> {
    (0..DATASET)
        .map(|i| NewRecord {
            item_id: format!("q{i}"),
            run_id: None,
            data: json!({ "input": i }),
        })
        .collect()
}

/// Reads the tagged dataset while others write, optimize, clean up and purge.
fn trk_read(cx: &Ctx) {
    let repo = trk_open(cx);
    let mut reads = 0;
    while !cx.stopped() {
        let r = repo.clone();
        let rows = block_on(async move {
            r.records(
                "dataset",
                RecordQuery {
                    at: At::Tag("v1".into()),
                    ..RecordQuery::default()
                },
            )
            .await
        })
        .unwrap();
        assert_eq!(rows.len(), DATASET);
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
    block_on(async move { r.flush().await }).unwrap();
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
}

fn trk_race(uri: &str, storage: &HashMap<String, String>, writers: u32, runs: u32) {
    delta();
    let seed = seed("trk_race");
    let dir = tempfile::tempdir().unwrap();
    let sp = Spawner::new(uri, dir.path(), storage, seed);
    let repo = trk_open_at(uri, trk_opts(&dir.path().join("wal/setup"), storage));
    let victims: Vec<String> = (0..writers).map(|k| format!("victim-{k}")).collect();
    for v in &victims {
        write_run(&repo, v);
    }
    let r = repo.clone();
    block_on(async move {
        r.append_records("dataset", dataset_rows()).await?;
        r.tag_records("dataset", "v1", None).await
    })
    .unwrap();
    close(&repo);

    let mut writing: Vec<(String, Child)> = (0..writers)
        .map(|w| {
            let id = format!("w{w}");
            (
                id.clone(),
                sp.spawn("trk-write", &id, &runs.to_string(), &[]),
            )
        })
        .collect();
    writing.push((
        "trk-delete".into(),
        sp.spawn("trk-delete", "deleter", &victims.join(","), &[]),
    ));
    let others: Vec<(String, Child)> = [
        ("trk-optimize", "opt-a"),
        ("trk-optimize", "opt-b"),
        ("trk-clean", "cleaner"),
        ("trk-purge", "purger"),
        ("trk-read", "reader"),
    ]
    .into_iter()
    .map(|(role, id)| (id.to_string(), sp.spawn(role, id, "", &[])))
    .collect();
    wait_all(writing);
    sp.stop();
    wait_all(others);

    let live: Vec<String> = (0..writers)
        .flat_map(|w| (0..runs).map(move |k| format!("w{w}-{k}")))
        .collect();
    let repo = trk_open_at(uri, trk_opts(&dir.path().join("wal/check"), storage));
    // The last optimize drops points of deleted runs that a racing one kept.
    let r = repo.clone();
    block_on(async move { r.optimize().await }).unwrap();
    trk_check(&repo, &live, &victims, true);
    let r = repo.clone();
    let report = block_on(async move { r.purge(false).await }).unwrap();
    assert!(report.tags_deleted.is_empty(), "{report:?}");
    trk_check(&repo, &live, &victims, true);
    if kv0::storage::is_local(uri) {
        for v in &victims {
            assert!(
                !stored_anywhere(Path::new(uri), marker(v).as_bytes()),
                "{v} survived purge"
            );
        }
        assert!(stored_anywhere(Path::new(uri), marker(&live[0]).as_bytes()));
    }
    let r = repo.clone();
    let tagged = block_on(async move {
        r.records(
            "dataset",
            RecordQuery {
                at: At::Tag("v1".into()),
                ..RecordQuery::default()
            },
        )
        .await
    })
    .unwrap();
    assert_eq!(tagged.len(), DATASET, "the tag outlives cleanup and purge");
    close(&repo);
}

#[test]
fn tracking_writers_deletes_optimize_cleanup_purge_and_readers_race() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    trk_race(&uri, &HashMap::new(), 4, 4 * scale());
}

#[test]
fn tracking_writers_deletes_optimize_cleanup_purge_and_readers_race_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    trk_race(&uri, &storage, 3, 2 * scale());
}

fn trk_faults(uri: &str, storage: &HashMap<String, String>, writers: u32, runs: u32) {
    delta();
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
                "trk-write",
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
/// params, series and metrics tables one after another: the replay commits exactly the tables
/// the crash left uncommitted.
#[test]
fn tracking_crash_at_every_write_of_a_flush_then_replay() {
    delta();
    let seed = seed("trk_crash");
    for after in [false, true] {
        for nth in 1.. {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().join("repo").to_string_lossy().into_owned();
            let sp = Spawner::new(&uri, dir.path(), &HashMap::new(), seed);
            let when = if after { "after" } else { "before" };
            let st = sp
                .spawn(
                    "trk-crash",
                    "c",
                    "",
                    &[("KV0_CHAOS_CRASH", format!("{nth}:{when}"))],
                )
                .wait()
                .unwrap();
            if dir.path().join("survived").exists() {
                assert!(st.success());
                assert!(nth > 8, "a flush writes a data file and a commit per table");
                break;
            }
            assert!(!st.success(), "{nth} {when}: {st}");
            let repo = trk_open_at(&uri, trk_opts(&dir.path().join("wal/c"), &HashMap::new()));
            trk_check(&repo, &["a".into(), "b".into()], &[], false);
            close(&repo);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Protocol behaviour and guards

/// Concurrent creators of one repository: each table is created once (delta-rs #4982), and
/// every writer's runs are there.
#[test]
fn concurrent_creators_make_each_table_once() {
    delta();
    let seed = seed("create_race");
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let sp = Spawner::new(&uri, dir.path(), &HashMap::new(), seed);
    let children = (0..12)
        .map(|w| {
            let id = format!("w{w}");
            (id.clone(), sp.spawn("trk-write", &id, "1", &[]))
        })
        .collect();
    wait_all(children);
    for table in ["runs", "params", "series", "metrics", "records/evals"] {
        let log = Path::new(&uri).join(table).join("_delta_log");
        let creates = std::fs::read_dir(&log)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .filter(|e| std::fs::read_to_string(e.path()).is_ok_and(|s| s.contains("\"metaData\"")))
            .count();
        assert_eq!(creates, 1, "{table}");
    }
    let live: Vec<String> = (0..12).map(|w| format!("w{w}-0")).collect();
    let repo = trk_open_at(
        &uri,
        trk_opts(&dir.path().join("wal/check"), &HashMap::new()),
    );
    trk_check(&repo, &live, &[], true);
    close(&repo);
}

fn local_db() -> (tempfile::TempDir, String, PathBuf) {
    delta();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db").to_string_lossy().into_owned();
    let wal = dir.path().join("wal");
    (dir, db, wal)
}

fn commit_file(db: &str, v: u64) -> PathBuf {
    Path::new(db)
        .join("_delta_log")
        .join(format!("{v:020}.json"))
}

/// A version 1 that creates the table again (the #4982 race) fails the open.
#[test]
fn a_second_create_at_version_one_trips_the_open() {
    let (_d, path, wal) = local_db();
    let db = Db::open(&path, kv_opts(&wal, &HashMap::new())).unwrap();
    db.close().unwrap();
    let create = std::fs::read_to_string(commit_file(&path, 0)).unwrap();
    let again: String = create
        .lines()
        .filter(|l| l.contains("\"protocol\"") || l.contains("\"metaData\""))
        .map(|l| format!("{l}\n"))
        .collect();
    std::fs::write(commit_file(&path, 1), again).unwrap();
    let e = Db::open(&path, kv_opts(&wal, &HashMap::new())).unwrap_err();
    assert!(
        matches!(e, kv0::Error::Corruption(_)) && e.to_string().contains("#4982"),
        "{e}"
    );
}

/// Rewrites the version-0 commit with `edit` applied to its JSON lines.
fn edit_create(db: &str, edit: impl Fn(&mut serde_json::Value)) {
    let path = commit_file(db, 0);
    let body: String = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| {
            let mut v: serde_json::Value = serde_json::from_str(l).unwrap();
            edit(&mut v);
            format!("{v}\n")
        })
        .collect();
    std::fs::write(path, body).unwrap();
}

#[test]
fn tables_with_features_kv0_does_not_implement_are_refused() {
    type Edit = Box<dyn Fn(&mut serde_json::Value)>;
    let cases: Vec<(&str, Edit)> = vec![
        (
            "deletion vectors",
            Box::new(|v| {
                if let Some(p) = v.get_mut("protocol") {
                    *p = json!({
                        "minReaderVersion": 3, "minWriterVersion": 7,
                        "readerFeatures": ["deletionVectors"],
                        "writerFeatures": ["deletionVectors"]
                    });
                }
            }),
        ),
        (
            "column mapping",
            Box::new(|v| {
                if let Some(p) = v.get_mut("protocol") {
                    *p = json!({ "minReaderVersion": 2, "minWriterVersion": 5 });
                }
            }),
        ),
        (
            "row tracking",
            Box::new(|v| {
                if let Some(p) = v.get_mut("protocol") {
                    *p = json!({
                        "minReaderVersion": 1, "minWriterVersion": 7,
                        "writerFeatures": ["domainMetadata", "rowTracking"]
                    });
                }
            }),
        ),
        (
            "change data feed",
            Box::new(|v| {
                if let Some(m) = v.get_mut("metaData") {
                    m["configuration"]["delta.enableChangeDataFeed"] = json!("true");
                }
            }),
        ),
        (
            "append only",
            Box::new(|v| {
                if let Some(m) = v.get_mut("metaData") {
                    m["configuration"]["delta.appendOnly"] = json!("true");
                }
            }),
        ),
    ];
    for (what, edit) in cases {
        let (_d, path, wal) = local_db();
        let db = Db::open(&path, kv_opts(&wal, &HashMap::new())).unwrap();
        db.close().unwrap();
        edit_create(&path, edit);
        let e = Db::open(&path, kv_opts(&wal, &HashMap::new())).unwrap_err();
        assert!(matches!(e, kv0::Error::NotSupported(_)), "{what}: {e}");
    }
}

/// Every version kept by a cleanup, and every tag below it, loads with exactly the keys it had
/// (the log has gaps below the oldest kept version and checkpoints only at the tags), across a
/// checkpoint delta-rs writes itself (every 100 commits).
#[test]
fn log_gaps_below_tag_checkpoints_load_every_kept_version() {
    let (_d, path, wal) = local_db();
    let db = Db::open(&path, kv_opts(&wal, &HashMap::new())).unwrap();
    let mut tagged = Vec::new();
    let mut resumed = None;
    for i in 0..120u64 {
        db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
        if [10, 50, 99].contains(&i) {
            db.create_tag(&format!("t{i}"), None).unwrap();
            tagged.push(db.version());
        }
        if i == 104 {
            std::thread::sleep(Duration::from_millis(1100));
            resumed = Some(Instant::now());
        }
    }
    let latest = db.version();
    // Keeps the commits since the pause: version 106 (key 105) on.
    let age = resumed.unwrap().elapsed() + Duration::from_millis(100);
    db.cleanup_old_versions(age).unwrap();
    let kept: Vec<u64> = db.versions().unwrap().iter().map(|v| v.version).collect();
    let oldest = *kept.iter().find(|v| !tagged.contains(v)).unwrap();
    assert_eq!(oldest, 106, "{kept:?}");
    assert!(!commit_file(&path, oldest - 1).exists());
    let mut want: Vec<u64> = tagged.clone();
    want.extend(oldest..=latest);
    assert_eq!(kept, want);
    for v in kept {
        let old = Db::open(
            &path,
            Options {
                version: Some(v),
                ..kv_opts(&wal, &HashMap::new())
            },
        )
        .unwrap();
        // Version v holds the keys of its v commits.
        assert_eq!(old.stats().live_keys as u64, v, "version {v}");
    }
}

/// Two handles tag the same name at once: exactly one wins.
#[test]
fn a_tag_create_race_has_one_winner() {
    let (_d, path, wal) = local_db();
    let a = Db::open(&path, kv_opts(&wal.join("a"), &HashMap::new())).unwrap();
    let b = Db::open(&path, kv_opts(&wal.join("b"), &HashMap::new())).unwrap();
    for round in 0..20 {
        a.put(b"k", b"v").unwrap();
        a.flush().unwrap();
        b.refresh().unwrap();
        let name = format!("t{round}");
        let won = std::thread::scope(|s| {
            let x = s.spawn(|| a.create_tag(&name, None).is_ok());
            let y = s.spawn(|| b.create_tag(&name, None).is_ok());
            [x.join().unwrap(), y.join().unwrap()]
        });
        assert_eq!(won.iter().filter(|w| **w).count(), 1, "{name}");
    }
}

/// A handle opened at a fixed version reads it until a cleanup removes its files; then it
/// fails with an error rather than wrong data, while a refreshing handle reads on.
#[test]
fn a_fixed_version_reader_fails_cleanly_after_cleanup() {
    let (_d, path, wal) = local_db();
    let w = Db::open(&path, kv_opts(&wal.join("w"), &HashMap::new())).unwrap();
    let big = "z".repeat(500);
    w.put(b"big", big.as_bytes()).unwrap();
    w.flush().unwrap();
    let v = w.version();
    let fixed = Db::open(
        &path,
        Options {
            version: Some(v),
            value_cache_size: 0,
            ..kv_opts(&wal.join("f"), &HashMap::new())
        },
    )
    .unwrap();
    let live = Db::open(
        &path,
        Options {
            read_only: true,
            value_cache_size: 0,
            ..kv_opts(&wal.join("r"), &HashMap::new())
        },
    )
    .unwrap();
    assert_eq!(fixed.get(b"big").unwrap().as_deref(), Some(big.as_bytes()));
    assert_eq!(live.get(b"big").unwrap().as_deref(), Some(big.as_bytes()));
    w.put(b"other", b"1").unwrap();
    w.flush().unwrap();
    w.compact().unwrap();
    w.cleanup_old_versions(Duration::ZERO).unwrap();
    assert!(fixed.get(b"big").is_err());
    assert_eq!(live.get(b"big").unwrap().as_deref(), Some(big.as_bytes()));
}

/// A commit whose checkpoint (written after it, every 100 commits) cannot be stored is still a
/// commit: the writer must not report it failed and write it again. (In a child process: fault
/// configuration is process-wide.)
#[test]
fn a_failed_checkpoint_does_not_fail_its_commit() {
    let (d, path, _) = local_db();
    let sp = Spawner::new(&path, d.path(), &HashMap::new(), seed("checkpoint"));
    wait_all(vec![(
        "checkpoint".into(),
        sp.spawn("kv-checkpoint", "c", "", &[]),
    )]);
}

fn kv_checkpoint(cx: &Ctx) {
    set_faults(Some(Faults::default()));
    let db = kv_open(
        cx,
        Options {
            wal: false,
            ..kv_opts(&cx.wal(), &cx.storage)
        },
    );
    set_faults(Some(Faults {
        seed: cx.seed,
        rate: 1.0,
        read_rate: 0.0,
        ops: vec![StoreOp::Put],
        kinds: vec![FaultKind::Error],
        paths: Some("checkpoint".into()),
        crash: None,
    }));
    for i in 0..105u64 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap_or_else(|e| panic!("flush {i}: {e}"));
    }
    assert!(kv0::testing::faults_injected() > 0, "the checkpoint failed");
    set_faults(None);
    assert_eq!(db.stats().stored_rows, 105, "no commit stored twice");
    db.close().unwrap();
}

/// A writer whose version is older than a purge (which cuts the log below the latest version)
/// still commits, and its commit is part of the table: it must not recreate a commit file the
/// purge deleted, which no reader would see.
#[test]
fn a_writer_behind_a_purge_commits_on_the_main_line() {
    let (_d, path, wal) = local_db();
    let a = Db::open(&path, kv_opts(&wal.join("a"), &HashMap::new())).unwrap();
    a.put(b"a0", b"1").unwrap();
    a.flush().unwrap();
    let b = Db::open(&path, kv_opts(&wal.join("b"), &HashMap::new())).unwrap();
    for i in 0..4 {
        b.put(format!("b{i}").as_bytes(), b"1").unwrap();
        b.flush().unwrap();
    }
    b.purge(false).unwrap();
    // `a` has not seen any of it.
    a.put(b"a1", b"1").unwrap();
    a.flush().unwrap();
    b.close().unwrap();
    a.close().unwrap();
    let c = Db::open(&path, kv_opts(&wal.join("c"), &HashMap::new())).unwrap();
    for k in ["a0", "a1", "b0", "b3"] {
        assert_eq!(
            c.get(k.as_bytes()).unwrap().as_deref(),
            Some(&b"1"[..]),
            "{k}"
        );
    }
}

/// The same for a tracking writer behind another handle's purge.
#[test]
fn a_tracking_writer_behind_a_purge_commits_on_the_main_line() {
    delta();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    let a = trk_open_at(&uri, trk_opts(&dir.path().join("wal/a"), &HashMap::new()));
    write_run(&a, "a0");
    let b = trk_open_at(&uri, trk_opts(&dir.path().join("wal/b"), &HashMap::new()));
    for k in 0..3 {
        write_run(&b, &format!("b{k}"));
    }
    let r = b.clone();
    block_on(async move { r.purge(false).await }).unwrap();
    write_run(&a, "a1");
    close(&a);
    close(&b);
    let c = trk_open_at(&uri, trk_opts(&dir.path().join("wal/c"), &HashMap::new()));
    let live: Vec<String> = ["a0", "a1", "b0", "b1", "b2"].map(String::from).to_vec();
    trk_check(&c, &live, &[], true);
    close(&c);
}

/// `_last_checkpoint` naming an older checkpoint whose following commits a cleanup deleted
/// (a tag's checkpoint written after the cleanup's) leaves the table readable.
#[test]
fn an_old_last_checkpoint_hint_below_a_log_gap_still_loads() {
    let (_d, path, wal) = local_db();
    let db = Db::open(&path, kv_opts(&wal, &HashMap::new())).unwrap();
    for i in 0..2 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    db.create_tag("old", Some(1)).unwrap();
    let hint = Path::new(&path).join("_delta_log/_last_checkpoint");
    let stale = std::fs::read(&hint).unwrap();
    for i in 2..5 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    db.cleanup_old_versions(Duration::ZERO).unwrap();
    assert!(!commit_file(&path, 2).exists());
    db.close().unwrap();
    std::fs::write(&hint, stale).unwrap();
    let db = Db::open(&path, kv_opts(&wal.join("again"), &HashMap::new())).unwrap();
    assert_eq!(db.stats().live_keys, 5);
    let old = Db::open(
        &path,
        Options {
            version: Some(db.tag_version("old").unwrap()),
            ..kv_opts(&wal, &HashMap::new())
        },
    )
    .unwrap();
    assert_eq!(old.stats().live_keys, 1);
}
