//! The lock-free core of [`super::worker::SharedProgress`]: the applied LSN and
//! the flag that decides which raise wakes the worker.
//!
//! The consumer raises the applied LSN; the worker clears the flag and then
//! reads the LSN before deciding whether anything is pending. Both sides use
//! SeqCst: either the worker's read sees a raise, or that raise sees the flag
//! cleared and asks for a wake. A raise is never both unseen and unsignalled.
//! The `loom` model in this module checks that for every interleaving.

#[cfg(all(test, pgwire_loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(all(test, pgwire_loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(crate) struct ProgressCell {
    applied: AtomicU64,
    /// Set by the first raise the worker has not looked at yet.
    signalled: AtomicBool,
}

impl ProgressCell {
    pub(crate) fn new(start: u64) -> Self {
        Self {
            applied: AtomicU64::new(start),
            signalled: AtomicBool::new(false),
        }
    }

    pub(crate) fn load(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// Monotonic raise to `new` (a lower value is a no-op). Returns true when
    /// the caller must wake the worker: this is the first raise since the
    /// worker last looked.
    pub(crate) fn raise(&self, new: u64) -> bool {
        let mut cur = self.applied.load(Ordering::Relaxed);
        while new > cur {
            match self
                .applied
                .compare_exchange_weak(cur, new, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => {
                    // The fast-path load must be SeqCst too: a Relaxed load
                    // may still see a flag the worker already cleared, after
                    // the worker read the LSN before this raise.
                    return !self.signalled.load(Ordering::SeqCst)
                        && !self.signalled.swap(true, Ordering::SeqCst);
                }
                Err(observed) => cur = observed,
            }
        }
        false
    }

    /// The worker is about to decide whether a position is pending: clear the
    /// signal, then read the applied LSN. A raise after the clear wakes it.
    pub(crate) fn take_signal(&self) -> u64 {
        self.signalled.store(false, Ordering::SeqCst);
        self.applied.load(Ordering::SeqCst)
    }
}

#[cfg(all(test, pgwire_loom))]
mod loom_model {
    use super::ProgressCell;
    use loom::sync::atomic::{AtomicU64, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// The worker's wait-phase cycle against two concurrent raises. The watch
    /// generation stands in for the wake channel (bumped with SeqCst by a
    /// raise that asks for a wake; read before `take_signal`, as the worker
    /// marks the watch seen). In every interleaving, once both sides are done,
    /// the newest raised position has been sent, is still pending, or has a
    /// wake waiting: it is never lost.
    #[test]
    fn a_raise_concurrent_with_send_or_clear_is_never_lost() {
        loom::model(|| {
            let cell = Arc::new(ProgressCell::new(0));
            let generation = Arc::new(AtomicU64::new(0));

            let consumer = {
                let (cell, generation) = (Arc::clone(&cell), Arc::clone(&generation));
                thread::spawn(move || {
                    for lsn in [1, 2] {
                        if cell.raise(lsn) {
                            generation.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            };

            // Two worker iterations: each marks the watch seen, clears the
            // signal, reads the position and sends it if it is pending.
            let mut last_sent = 0;
            let mut seen = 0;
            let mut pending = false;
            for _ in 0..2 {
                seen = generation.load(Ordering::SeqCst);
                let applied = cell.take_signal();
                pending = applied > last_sent;
                if pending {
                    last_sent = applied; // the send
                    pending = false;
                }
            }
            consumer.join().unwrap();

            let newest = cell.load();
            let woken = generation.load(Ordering::SeqCst) != seen;
            assert!(
                last_sent == newest || pending || woken,
                "lost raise: newest {newest}, sent {last_sent}, no wake pending"
            );
        });
    }
}
