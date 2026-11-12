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
///
/// Safe to call from a task on the kv0 runtime itself: the worker is handed off first.
pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let wait = move || match futures::executor::block_on(runtime().spawn(fut)) {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("kv0 task was cancelled: {e}"),
    };
    if on_kv0_runtime() {
        // A blocking API called from a task on this runtime, such as a blocking call inside
        // `block_on`. Parking the worker would strand the task it spawns (and everything queued
        // on that worker) once all workers wait this way, and with one worker at once.
        // `block_in_place` first hands the worker's queue to another thread, and spawning
        // after it puts the task on the shared queue.
        tokio::task::block_in_place(wait)
    } else {
        wait()
    }
}

/// Whether the current thread runs tasks of the kv0 runtime.
fn on_kv0_runtime() -> bool {
    tokio::runtime::Handle::try_current().is_ok_and(|h| h.id() == runtime().handle().id())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn nested_block_on_never_starves_the_runtime() {
        // More nested calls than workers: parking each worker would leave the inner tasks
        // without a thread.
        let n = 4 * std::thread::available_parallelism().map_or(4, |n| n.get());
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..n {
            let tx = tx.clone();
            std::thread::spawn(move || {
                let v = super::block_on(async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    super::block_on(async move {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        i
                    })
                });
                tx.send(v).unwrap();
            });
        }
        let mut got: Vec<usize> = (0..n)
            .map(|_| {
                rx.recv_timeout(Duration::from_secs(30))
                    .expect("nested block_on calls are stuck")
            })
            .collect();
        got.sort_unstable();
        assert_eq!(got, (0..n).collect::<Vec<_>>());
    }
}
