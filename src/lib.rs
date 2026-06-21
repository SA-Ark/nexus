//! # Nexus
//!
//! A multi-agent orchestration runtime — the supervision pattern behind a
//! production fleet of 40+ services, extracted as a standalone,
//! embeddable library.
//!
//! ## The pattern
//!
//! - **[`task::TaskDag`]** — work is a validated dependency DAG (duplicate,
//!   unknown-dep, and cycle checks at build time). Ready tasks dispatch in
//!   parallel; a permanent failure cancels its transitive blast radius.
//! - **[`worker::Worker`]** — the execution backend is a trait. An LLM
//!   agent session, a subprocess, a container, a remote job — the runtime
//!   does not care.
//! - **[`supervisor::Supervisor`]** — evidence-based liveness. Workers beat
//!   a [`worker::Heartbeat`] while making progress; the supervisor never
//!   kills on a wall-clock guess. Detect (heartbeat stale) → confirm
//!   (second observation window) → recover (abort + retry with backoff) →
//!   escalate (fail loudly, cancel dependents).
//! - **Blockers** — a worker that needs a human/orchestrator decision
//!   returns [`worker::WorkerOutcome::Blocked`]; the run stays alive and
//!   [`supervisor::Supervisor::resolve_blocker`] resumes it with the answer.
//! - **[`budget::CostGovernor`]** — token-cost governance by
//!   reserve-then-settle: a task cannot start unless its budget fits, so a
//!   runaway fleet stops *before* the bill.
//! - **[`supervisor::RuntimeEvent`]** — every decision the runtime makes is
//!   broadcast; dashboards, logs, and tests consume the same stream.
//!
//! ## Example
//!
//! ```
//! use nexus::prelude::*;
//! use std::sync::Arc;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let dag = TaskDag::build(vec![
//!     TaskSpec::new("fetch", "fetch the data").budget(10_000),
//!     TaskSpec::new("analyze", "analyze it").after("fetch").budget(50_000),
//! ]).unwrap();
//!
//! let worker = Arc::new(|spec: TaskSpec, hb: Heartbeat, _answer: Option<String>| {
//!     Box::pin(async move {
//!         hb.beat();
//!         WorkerOutcome::Success { output: format!("done: {}", spec.id), tokens_used: 500 }
//!     }) as std::pin::Pin<Box<dyn std::future::Future<Output = WorkerOutcome> + Send>>
//! });
//!
//! let supervisor = Supervisor::new(
//!     dag,
//!     worker,
//!     Arc::new(CostGovernor::new(100_000)),
//!     SupervisorConfig::default(),
//! );
//! let report = supervisor.run().await;
//! assert_eq!(report.completed, 2);
//! # }
//! ```

pub mod budget;
pub mod supervisor;
pub mod task;
pub mod worker;

/// The built-in example workflow that powers the live demo server.
/// Compiled only with the `server` feature.
#[cfg(feature = "server")]
pub mod demo;

pub mod prelude {
    pub use crate::budget::{BudgetError, BudgetSnapshot, CostGovernor};
    pub use crate::supervisor::{RunReport, RuntimeEvent, Supervisor, SupervisorConfig};
    pub use crate::task::{DagError, TaskDag, TaskId, TaskSpec, TaskState};
    pub use crate::worker::{Heartbeat, Worker, WorkerOutcome};
}
