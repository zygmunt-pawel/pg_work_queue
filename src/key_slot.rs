//! `KeySlotGuard` — RAII guard for the in-memory per-key concurrency counter.
//!
//! The Worker keeps an `Arc<Mutex<HashMap<String, u32>>>` of live handler
//! tasks per concurrency key. One `KeySlotGuard` owns exactly one slot:
//! `acquire` increments, `Drop` decrements. Because `Drop` runs on every
//! task exit — normal return, panic, and `JoinSet` abort-cancellation — the
//! decrement is exhaustive by construction.
//!
//! `std::sync::Mutex` (not `tokio::sync::Mutex`): the decrement runs inside a
//! synchronous `Drop`. The critical sections are O(1), panic-free `HashMap`
//! work and never hold the lock across an `.await`.

// `pub(crate)` inside a `pub(crate)` module is intentional — items need crate
// visibility so Task 9 can import them from worker.rs. Clippy treats the module
// as "private" for the redundant_pub_crate lint; suppress it here.
#![allow(clippy::redundant_pub_crate)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Shared per-key live-task counter.
// wired into the Worker in Task 9
#[allow(dead_code)]
pub(crate) type ConcurrencyCounter = Arc<Mutex<HashMap<String, u32>>>;

/// Owns one per-key slot. Increments on `acquire`, decrements on `Drop`.
/// A no-op for jobs without a configured-limit key (`none()`).
// wired into the Worker in Task 9
#[allow(dead_code)]
pub(crate) struct KeySlotGuard {
    slot: Option<(ConcurrencyCounter, String)>,
}

impl KeySlotGuard {
    /// Increment the counter for `key` and return a guard owning that slot.
    /// On a poisoned mutex (unreachable — the critical section is panic-free)
    /// returns a slot-less guard: increment succeeded iff the guard owns a slot.
    // wired into the Worker in Task 9
    #[allow(dead_code)]
    pub(crate) fn acquire(counter: ConcurrencyCounter, key: String) -> Self {
        let ok = match counter.lock() {
            Ok(mut map) => {
                let n = map.entry(key.clone()).or_insert(0);
                *n = n.saturating_add(1);
                true
            }
            Err(_poisoned) => false,
        };
        if ok {
            Self {
                slot: Some((counter, key)),
            }
        } else {
            Self { slot: None }
        }
    }

    /// A guard owning no slot — for unkeyed / unconfigured-key jobs.
    // wired into the Worker in Task 9
    #[allow(dead_code)]
    pub(crate) const fn none() -> Self {
        Self { slot: None }
    }
}

impl Drop for KeySlotGuard {
    fn drop(&mut self) {
        if let Some((counter, key)) = &self.slot
            && let Ok(mut map) = counter.lock()
            && let Some(n) = map.get_mut(key)
        {
            *n = n.saturating_sub(1);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn counter_with(keys: &[&str]) -> ConcurrencyCounter {
        let map: HashMap<String, u32> = keys.iter().map(|k| ((*k).to_string(), 0)).collect();
        Arc::new(Mutex::new(map))
    }

    fn count(counter: &ConcurrencyCounter, key: &str) -> u32 {
        *counter.lock().unwrap().get(key).unwrap()
    }

    #[test]
    fn acquire_increments_drop_decrements() {
        let counter = counter_with(&["k"]);
        {
            let _g = KeySlotGuard::acquire(counter.clone(), "k".to_string());
            assert_eq!(count(&counter, "k"), 1);
        }
        assert_eq!(count(&counter, "k"), 0);
    }

    #[test]
    fn two_guards_stack() {
        let counter = counter_with(&["k"]);
        let g1 = KeySlotGuard::acquire(counter.clone(), "k".to_string());
        let g2 = KeySlotGuard::acquire(counter.clone(), "k".to_string());
        assert_eq!(count(&counter, "k"), 2);
        drop(g1);
        assert_eq!(count(&counter, "k"), 1);
        drop(g2);
        assert_eq!(count(&counter, "k"), 0);
    }

    #[test]
    fn none_guard_is_noop() {
        let counter = counter_with(&["k"]);
        {
            let _g = KeySlotGuard::none();
        }
        assert_eq!(count(&counter, "k"), 0);
    }
}
