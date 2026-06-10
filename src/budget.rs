//! Token-cost governance: a global budget with per-task reservations.
//!
//! The governor enforces two invariants:
//! 1. a task cannot start unless its full budget can be reserved, and
//! 2. total settled + reserved spend never exceeds the global budget.
//!
//! Reservation-then-settlement (rather than spend-then-check) means a
//! runaway fleet stops *before* the bill, not after it.

use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum BudgetError {
    #[error("global budget exhausted: requested {requested}, available {available}")]
    Exhausted { requested: u64, available: u64 },
    #[error("no reservation found for task {0}")]
    NoReservation(String),
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct BudgetSnapshot {
    pub global_budget: u64,
    pub settled: u64,
    pub reserved: u64,
    pub available: u64,
}

#[derive(Debug)]
struct Inner {
    global_budget: u64,
    settled: u64,
    reservations: Vec<(String, u64)>,
}

/// Thread-safe cost governor.
#[derive(Debug)]
pub struct CostGovernor {
    inner: Mutex<Inner>,
}

impl CostGovernor {
    pub fn new(global_budget: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                global_budget,
                settled: 0,
                reservations: Vec::new(),
            }),
        }
    }

    /// Reserve `tokens` for `task`. Fails if the global budget cannot cover
    /// all outstanding reservations plus this one.
    pub fn reserve(&self, task: &str, tokens: u64) -> Result<(), BudgetError> {
        let mut inner = self.inner.lock().expect("governor lock");
        let reserved: u64 = inner.reservations.iter().map(|(_, t)| t).sum();
        let available = inner
            .global_budget
            .saturating_sub(inner.settled)
            .saturating_sub(reserved);
        if tokens > available {
            return Err(BudgetError::Exhausted {
                requested: tokens,
                available,
            });
        }
        inner.reservations.push((task.to_string(), tokens));
        Ok(())
    }

    /// Settle a reservation with the actual spend. Actual spend above the
    /// reservation is clamped to the reservation (the worker was cut off
    /// at its budget; the governor never books more than it authorized).
    pub fn settle(&self, task: &str, actual_tokens: u64) -> Result<(), BudgetError> {
        let mut inner = self.inner.lock().expect("governor lock");
        let idx = inner
            .reservations
            .iter()
            .position(|(t, _)| t == task)
            .ok_or_else(|| BudgetError::NoReservation(task.to_string()))?;
        let (_, reserved) = inner.reservations.swap_remove(idx);
        inner.settled += actual_tokens.min(reserved);
        Ok(())
    }

    /// Release a reservation without spend (task never ran / was retried).
    pub fn release(&self, task: &str) {
        let mut inner = self.inner.lock().expect("governor lock");
        inner.reservations.retain(|(t, _)| t != task);
    }

    pub fn snapshot(&self) -> BudgetSnapshot {
        let inner = self.inner.lock().expect("governor lock");
        let reserved: u64 = inner.reservations.iter().map(|(_, t)| t).sum();
        BudgetSnapshot {
            global_budget: inner.global_budget,
            settled: inner.settled,
            reserved,
            available: inner
                .global_budget
                .saturating_sub(inner.settled)
                .saturating_sub(reserved),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_settle_lifecycle() {
        let gov = CostGovernor::new(1000);
        gov.reserve("a", 400).unwrap();
        gov.reserve("b", 400).unwrap();

        // Third reservation exceeds what's left.
        let err = gov.reserve("c", 400).unwrap_err();
        assert_eq!(
            err,
            BudgetError::Exhausted {
                requested: 400,
                available: 200
            }
        );

        // a actually used less than reserved: the difference returns.
        gov.settle("a", 100).unwrap();
        gov.reserve("c", 400).unwrap();

        let snap = gov.snapshot();
        assert_eq!(snap.settled, 100);
        assert_eq!(snap.reserved, 800);
        assert_eq!(snap.available, 100);
    }

    #[test]
    fn overspend_is_clamped_to_reservation() {
        let gov = CostGovernor::new(500);
        gov.reserve("a", 200).unwrap();
        gov.settle("a", 9999).unwrap();
        assert_eq!(gov.snapshot().settled, 200);
    }

    #[test]
    fn release_returns_capacity() {
        let gov = CostGovernor::new(300);
        gov.reserve("a", 300).unwrap();
        assert!(gov.reserve("b", 1).is_err());
        gov.release("a");
        gov.reserve("b", 300).unwrap();
    }

    #[test]
    fn settle_without_reservation_errors() {
        let gov = CostGovernor::new(100);
        assert!(matches!(
            gov.settle("ghost", 10),
            Err(BudgetError::NoReservation(_))
        ));
    }
}
