//! Fault injection for the concurrency and crash tests: an object store wrapper that fails,
//! delays or loses the response of operations at seeded random points, or ends the process
//! at a chosen operation, as a crash would.
//!
//! The configuration is process-wide ([`crate::testing::set_faults`]) and read at every
//! operation, so a test can switch faults off before it verifies. Tables opened while no
//! faults are configured are not wrapped at all.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use deltalake_core::logstore::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use deltalake_core::{ObjectStore, Path};
use futures::stream::BoxStream;
use futures::StreamExt;

use crate::testing::{Crash, FaultKind, Faults, StoreOp};

/// Whether faults were ever configured in this process: until then, stores are not wrapped.
static ARMED: AtomicBool = AtomicBool::new(false);
static CONFIG: RwLock<Option<Faults>> = RwLock::new(None);
/// Operations seen (they number the random draws) and faults injected.
static SEEN: AtomicU64 = AtomicU64::new(0);
static INJECTED: AtomicU64 = AtomicU64::new(0);
/// Operations matching the crash point so far.
static CRASH_COUNT: AtomicU64 = AtomicU64::new(0);

pub(crate) fn set(f: Option<Faults>) {
    if f.is_some() {
        ARMED.store(true, Ordering::Release);
    }
    CRASH_COUNT.store(0, Ordering::Relaxed);
    *CONFIG.write().expect("not poisoned") = f;
}

pub(crate) fn injected() -> u64 {
    INJECTED.load(Ordering::Relaxed)
}

/// `store`, wrapped when faults have been configured in this process.
pub(crate) fn wrap(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    if ARMED.load(Ordering::Acquire) {
        Arc::new(FaultyStore { inner: store })
    } else {
        store
    }
}

pub(crate) fn armed() -> bool {
    ARMED.load(Ordering::Acquire)
}

/// SplitMix64: a well-mixed 64-bit value from `x`.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// What to do with one operation.
enum Plan {
    Pass,
    Fail(FaultKind),
    Crash { after: bool },
}

fn plan(op: StoreOp, path: &str) -> Plan {
    let guard = CONFIG.read().expect("not poisoned");
    let Some(f) = guard.as_ref() else {
        return Plan::Pass;
    };
    if let Some(Crash { ops, nth, after }) = &f.crash {
        if ops.contains(&op) && CRASH_COUNT.fetch_add(1, Ordering::Relaxed) + 1 == *nth {
            return Plan::Crash { after: *after };
        }
    }
    let rate = match op {
        StoreOp::Get | StoreOp::List => f.read_rate,
        _ => f.rate,
    };
    if rate <= 0.0 || !f.ops.contains(&op) || !f.paths.as_deref().is_none_or(|p| path.contains(p)) {
        return Plan::Pass;
    }
    let n = SEEN.fetch_add(1, Ordering::Relaxed);
    let r = mix(f.seed ^ mix(n));
    if (r >> 11) as f64 / (1u64 << 53) as f64 >= rate {
        return Plan::Pass;
    }
    let kinds: Vec<FaultKind> = f
        .kinds
        .iter()
        .copied()
        .filter(|k| match k {
            // Only a write can complete without its caller hearing about it.
            FaultKind::LostResponse | FaultKind::DuplicateSend => {
                matches!(op, StoreOp::Put | StoreOp::ConditionalPut)
            }
            _ => true,
        })
        .collect();
    if kinds.is_empty() {
        return Plan::Pass;
    }
    INJECTED.fetch_add(1, Ordering::Relaxed);
    Plan::Fail(kinds[(mix(r) % kinds.len() as u64) as usize])
}

fn injected_error(what: &str, path: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "faults",
        source: format!("injected fault: {what} {path}").into(),
    }
}

fn crash(op: StoreOp, path: &str) -> ! {
    eprintln!("kv0 faults: crashing at {op:?} {path}");
    // SIGKILL, as `kill -9`: no destructors, no flushing, and (unlike `abort`) no core file.
    let _ = std::process::Command::new("kill")
        .args(["-KILL", &std::process::id().to_string()])
        .status();
    std::process::exit(137)
}

#[derive(Debug)]
struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Faulty({})", self.inner)
    }
}

/// Runs `call` under the plan for `op` on `path`: `again` repeats the call (a client retry
/// after a lost response).
macro_rules! guarded {
    ($op:expr, $path:expr, $call:expr) => {{
        let (op, path) = ($op, $path.to_string());
        match plan(op, &path) {
            Plan::Pass => $call,
            Plan::Crash { after: false } => crash(op, &path),
            Plan::Crash { after: true } => {
                let r = $call;
                if r.is_ok() {
                    crash(op, &path)
                }
                r
            }
            Plan::Fail(FaultKind::Error) => Err(injected_error("error", &path)),
            Plan::Fail(FaultKind::Timeout) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Err(injected_error("timeout", &path))
            }
            Plan::Fail(FaultKind::LostResponse) => {
                let _ = $call;
                Err(injected_error("lost response", &path))
            }
            Plan::Fail(FaultKind::DuplicateSend) => {
                let _ = $call;
                $call
            }
        }
    }};
}

#[async_trait::async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let op = match opts.mode {
            object_store::PutMode::Overwrite => StoreOp::Put,
            _ => StoreOp::ConditionalPut,
        };
        guarded!(
            op,
            location,
            self.inner
                .put_opts(location, payload.clone(), opts.clone())
                .await
        )
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        match plan(StoreOp::Put, location.as_ref()) {
            Plan::Pass | Plan::Fail(FaultKind::DuplicateSend) => {}
            Plan::Crash { .. } => crash(StoreOp::Put, location.as_ref()),
            Plan::Fail(_) => return Err(injected_error("error", location.as_ref())),
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        guarded!(
            StoreOp::Get,
            location,
            self.inner.get_opts(location, options.clone()).await
        )
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        guarded!(
            StoreOp::Get,
            location,
            self.inner.get_ranges(location, ranges).await
        )
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        locations
            .then(move |p| {
                let inner = inner.clone();
                async move {
                    let p = p?;
                    guarded!(StoreOp::Delete, &p, {
                        let one = futures::stream::iter([Ok(p.clone())]).boxed();
                        match inner.delete_stream(one).next().await {
                            Some(r) => r,
                            None => Ok(p.clone()),
                        }
                    })
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        match plan(StoreOp::List, &path) {
            Plan::Pass | Plan::Fail(FaultKind::DuplicateSend | FaultKind::LostResponse) => {
                self.inner.list(prefix)
            }
            Plan::Crash { .. } => crash(StoreOp::List, &path),
            Plan::Fail(_) => futures::stream::iter([Err(injected_error("error", &path))]).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        match plan(StoreOp::List, &path) {
            Plan::Pass | Plan::Fail(FaultKind::DuplicateSend | FaultKind::LostResponse) => {
                self.inner.list_with_offset(prefix, offset)
            }
            Plan::Crash { .. } => crash(StoreOp::List, &path),
            Plan::Fail(_) => futures::stream::iter([Err(injected_error("error", &path))]).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        guarded!(
            StoreOp::List,
            path,
            self.inner.list_with_delimiter(prefix).await
        )
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
