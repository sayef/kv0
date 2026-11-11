//! One background thread that drives every handle's timers.
//!
//! Each tick it lets every handle fsync its write-ahead log if due, then starts the flushes that
//! are due. A flush runs on the runtime's blocking pool, at most one per handle at a time, so a
//! slow commit on one handle never delays another handle's fsync or flush.
//!
//! Handles register a weak reference, so the thread never keeps a database alive; a handle that
//! has been dropped is forgotten on the next tick.

use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

pub(crate) trait Background: Send + Sync {
    /// Called about every [`TICK`]: cheap work only (an fsync if due). Returns whether a flush
    /// is due.
    fn tick(&self, now: Instant) -> bool;

    /// Runs one flush; called on the blocking pool, never twice at once for one handle.
    fn flush_now(self: Arc<Self>);

    /// The handle's "flush in flight" flag.
    fn flushing(&self) -> &std::sync::atomic::AtomicBool;
}

const TICK: Duration = Duration::from_millis(100);

fn registry() -> &'static Mutex<Vec<Weak<dyn Background>>> {
    static REG: OnceLock<Mutex<Vec<Weak<dyn Background>>>> = OnceLock::new();
    REG.get_or_init(|| {
        std::thread::Builder::new()
            .name("kv0-flush".into())
            .spawn(|| loop {
                std::thread::sleep(TICK);
                let live: Vec<_> = {
                    let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
                    reg.retain(|w| w.strong_count() > 0);
                    reg.iter().filter_map(Weak::upgrade).collect()
                };
                let now = Instant::now();
                let due: Vec<_> = live.into_iter().filter(|h| h.tick(now)).collect();
                for h in due {
                    use std::sync::atomic::Ordering;
                    if h.flushing().swap(true, Ordering::AcqRel) {
                        continue; // the previous flush is still running
                    }
                    crate::rt::spawn_blocking(move || {
                        // Clears the flag however the flush ends, so a panic cannot stop this
                        // handle's background flushes for good.
                        struct Clear(Arc<dyn Background>);
                        impl Drop for Clear {
                            fn drop(&mut self) {
                                self.0.flushing().store(false, Ordering::Release);
                            }
                        }
                        let guard = Clear(h.clone());
                        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            guard.0.clone().flush_now()
                        }));
                        if r.is_err() {
                            tracing::error!("kv0: a background flush panicked; it will be retried");
                        }
                    });
                }
            })
            .expect("failed to start the kv0 flush thread");
        Mutex::new(Vec::new())
    })
}

pub(crate) fn register(handle: Weak<dyn Background>) {
    registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(handle);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct Panicky {
        calls: AtomicUsize,
        flushing: AtomicBool,
    }

    impl Background for Panicky {
        fn tick(&self, _now: Instant) -> bool {
            true
        }

        fn flush_now(self: Arc<Self>) {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("injected flush panic");
            }
        }

        fn flushing(&self) -> &AtomicBool {
            &self.flushing
        }
    }

    #[test]
    fn a_panicking_flush_does_not_stop_later_flushes() {
        let h = Arc::new(Panicky {
            calls: AtomicUsize::new(0),
            flushing: AtomicBool::new(false),
        });
        let weak: Weak<dyn Background> = Arc::downgrade(&h) as Weak<dyn Background>;
        register(weak);
        let start = Instant::now();
        while h.calls.load(Ordering::SeqCst) < 3 && start.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            h.calls.load(Ordering::SeqCst) >= 3,
            "flushes resumed after the panic"
        );
    }
}
