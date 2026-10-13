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
    /// When the oldest raise not yet reported to the server happened (a
    /// caller-defined clock reading, never 0), or 0. Kept only when `timed`.
    pending_since: AtomicU64,
    timed: bool,
}

impl ProgressCell {
    /// `timed`: keep when the oldest unreported raise happened, for a
    /// feedback observer. Without it no clock is read.
    pub(crate) fn new(start: u64, timed: bool) -> Self {
        Self {
            applied: AtomicU64::new(start),
            signalled: AtomicBool::new(false),
            pending_since: AtomicU64::new(0),
            timed,
        }
    }

    pub(crate) fn load(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// Monotonic raise to `new` (a lower value is a no-op). Returns true when
    /// the caller must wake the worker: this is the first raise since the
    /// worker last looked. When timed, the first raise since the last report
    /// records `now()` as the pending start; later raises keep it.
    pub(crate) fn raise(&self, new: u64, now: impl FnOnce() -> u64) -> bool {
        let mut cur = self.applied.load(Ordering::Relaxed);
        while new > cur {
            match self
                .applied
                .compare_exchange_weak(cur, new, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => {
                    // SeqCst pairs with `take_pending`, like the signal below.
                    if self.timed && self.pending_since.load(Ordering::SeqCst) == 0 {
                        let _ = self.pending_since.compare_exchange(
                            0,
                            now(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        );
                    }
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

    /// After `sent` was written to the server (read at clock `loaded_at`):
    /// the pending start it reported, or 0. A raise the write did not cover
    /// keeps a pending start: its own, or `loaded_at` when it raced this
    /// clear (it happened after the position was read, so the wait is not
    /// understated).
    pub(crate) fn take_pending(&self, sent: u64, loaded_at: u64) -> u64 {
        let since = self.pending_since.swap(0, Ordering::SeqCst);
        if self.applied.load(Ordering::SeqCst) > sent {
            let _ = self.pending_since.compare_exchange(
                0,
                loaded_at,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
        since
    }

    /// The pending start of the oldest unreported raise, or 0.
    pub(crate) fn pending_since(&self) -> u64 {
        self.pending_since.load(Ordering::SeqCst)
    }

    /// Carry a pending start over from an earlier session: the older wins.
    pub(crate) fn carry_pending(&self, since: u64) {
        if !self.timed || since == 0 {
            return;
        }
        // A compare-exchange loop: `fetch_update` is deprecated on newer
        // toolchains and its replacement is not available at the MSRV.
        let mut cur = self.pending_since.load(Ordering::SeqCst);
        while cur == 0 || since < cur {
            match self.pending_since.compare_exchange_weak(
                cur,
                since,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[cfg(all(test, not(pgwire_loom)))]
mod tests {
    use super::ProgressCell;

    #[test]
    fn a_carried_pending_start_keeps_the_oldest() {
        let cell = ProgressCell::new(0, true);
        cell.carry_pending(0);
        assert_eq!(cell.pending_since(), 0, "0 means nothing pending");
        cell.carry_pending(50);
        assert_eq!(cell.pending_since(), 50);
        cell.carry_pending(70);
        assert_eq!(cell.pending_since(), 50, "a newer start is ignored");
        cell.carry_pending(20);
        assert_eq!(cell.pending_since(), 20, "an older start wins");

        let untimed = ProgressCell::new(0, false);
        untimed.carry_pending(20);
        assert_eq!(untimed.pending_since(), 0, "untimed keeps nothing");
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
            let cell = Arc::new(ProgressCell::new(0, false));
            let generation = Arc::new(AtomicU64::new(0));

            let consumer = {
                let (cell, generation) = (Arc::clone(&cell), Arc::clone(&generation));
                thread::spawn(move || {
                    for lsn in [1, 2] {
                        if cell.raise(lsn, || 1) {
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

    /// A raise concurrent with the worker reporting a write: once both are
    /// done, a position newer than the one written still has a pending start
    /// (its own, or the write's load time), so its wait is reported later.
    #[test]
    fn a_raise_concurrent_with_a_report_keeps_a_pending_start() {
        loom::model(|| {
            let cell = Arc::new(ProgressCell::new(0, true));
            assert!(cell.raise(1, || 10)); // pending since 10
            let consumer = {
                let cell = Arc::clone(&cell);
                thread::spawn(move || {
                    cell.raise(2, || 30);
                })
            };
            // The worker wrote position 1 (read at clock 20) and reports it.
            let since = cell.take_pending(1, 20);
            consumer.join().unwrap();
            assert_eq!(since, 10, "the write reports the oldest start");
            assert_eq!(cell.load(), 2);
            assert_ne!(cell.pending_since(), 0, "the newer raise lost its start");
        });
    }
}
