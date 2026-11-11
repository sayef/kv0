//! A process-wide Tokio runtime for the blocking APIs.
//!
//! Lance is async. The key-value layer and the Python bindings are blocking, so they run their
//! futures on this runtime and wait for the result. Spawning onto a dedicated runtime (instead of
//! `Runtime::block_on`) means a blocking call never panics when the caller is itself inside a Tokio
//! runtime; it only blocks the calling thread.

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Builder::new_multi_thread()
            .thread_name("kv0")
            .enable_all()
            .build()
            .expect("failed to start the kv0 Tokio runtime")
    })
}

/// Runs `fut` to completion on the kv0 runtime, blocking the current thread.
pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = runtime().spawn(fut);
    match futures::executor::block_on(handle) {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("kv0 task was cancelled: {e}"),
    }
}

/// Runs `fut` on the kv0 runtime and awaits it from any executor.
///
/// The async APIs use this so that locks held across Lance I/O are only ever held by tasks on
/// the kv0 runtime, never by a caller's (possibly single-threaded) runtime.
pub async fn spawn<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match runtime().spawn(fut).await {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("kv0 task was cancelled: {e}"),
    }
}

/// Runs blocking `f` on the kv0 runtime's blocking pool.
pub(crate) fn spawn_blocking<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    runtime().spawn_blocking(f);
}

/// Runs `fut` on the kv0 runtime in the background.
pub(crate) fn detach<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    runtime().spawn(fut);
}
