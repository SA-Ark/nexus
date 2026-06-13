# Nexus

[![CI](https://github.com/SA-Ark/nexus/actions/workflows/ci.yml/badge.svg)](https://github.com/SA-Ark/nexus/actions/workflows/ci.yml)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-stable-orange.svg)

**A multi-agent orchestration runtime — dependency-DAG scheduling, supervised execution with
evidence-based liveness, automatic recovery, human-in-the-loop blockers, and token-cost governance.**

Nexus is the supervision pattern behind a production fleet of **40+ services** with evidence-based
3-second stall recovery, extracted as a standalone, embeddable Rust library. It answers the question
every agent system eventually faces: *what happens when a worker hangs, fails, overspends, or needs a
human?* — with explicit machinery instead of hope.

## Architecture

```
                          ┌────────────────────────────────────────┐
   TaskSpec[]  ──build──► │   TaskDag (validated at construction)  │
                          │  · duplicate-id / unknown-dep checks   │
                          │  · cycle detection (Kahn's algorithm)  │
                          └─────────────────┬──────────────────────┘
                                            │ ready set (deps completed)
                                            ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                            SUPERVISOR                               │
   │                                                                     │
   │   dispatch ──► CostGovernor.reserve()  ──ok──► spawn Worker         │
   │                   │ insufficient                  │                 │
   │                   ▼                               │ Heartbeat       │
   │            BudgetRejected                         ▼                 │
   │            (fail loudly if                 liveness monitor         │
   │             nothing can free it)     suspect → confirm → recover    │
   │                                                                     │
   │   WorkerOutcome:                                                    │
   │     Success   → settle spend, mark Completed, unblock dependents    │
   │     Retryable → settle, backoff, retry (≤ max_attempts)             │
   │     Blocked   → park task, surface question, await resolve_blocker  │
   │     Fatal     → fail + cancel transitive dependents (blast radius)  │
   │                                                                     │
   │   every decision ──► broadcast RuntimeEvent stream                  │
   └─────────────────────────────────────────────────────────────────────┘
                                            ▲
                resolve_blocker(task, answer) — human / orchestrator
```

## The five mechanisms

| Mechanism | What it does | Why it matters |
|---|---|---|
| **DAG scheduling** | Tasks declare dependencies; the ready set dispatches in parallel up to `max_concurrent`. Cycles and unknown deps are construction-time errors. | Parallelism without coordination bugs; a failed task cancels its *transitive* dependents rather than leaving them pending forever. |
| **Evidence-based liveness** | Workers beat a `Heartbeat` while progressing. Stale heartbeat → *suspected* → second observation window → *confirmed* → abort + retry. | No arbitrary wall-clock timeouts. A slow task that's alive is left alone; a dead one is detected in seconds, on evidence, in two phases. |
| **Auto-recovery** | `Retryable` outcomes and confirmed stalls retry with linear backoff up to `max_attempts`, then fail loudly. | Transient failure is normal in agent fleets; recovery is the runtime's job, not the caller's. Default config detects + confirms a stall in 3 s. |
| **Blockers** | A worker that needs a decision returns `Blocked { question }`. The run stays alive; `resolve_blocker(task, answer)` resumes it with the answer injected. | Human-in-the-loop without polling hacks — escalation is a first-class state, not an error. |
| **Cost governance** | `CostGovernor` reserves a task's full token budget *before* dispatch and settles actual spend after. Overspend is clamped to the reservation. | A runaway fleet stops **before** the bill. Budget rejection is an explicit event, and deadlock (waiting for budget nothing will free) fails loudly. |

Everything the runtime does is announced on a `broadcast` event stream (`TaskStarted`, `TaskRetrying`,
`WorkerStallSuspected`, `BudgetRejected`, ...) — dashboards, logs, and the test suite all consume the
same source of truth.

## Quickstart

```rust
use nexus::prelude::*;
use std::sync::Arc;

let dag = TaskDag::build(vec![
    TaskSpec::new("research", "gather sources").budget(50_000),
    TaskSpec::new("draft",  "write the report").after("research").budget(80_000),
    TaskSpec::new("review", "critique the draft").after("draft").budget(30_000).attempts(2),
])?;

let supervisor = Supervisor::new(
    dag,
    Arc::new(MyAgentWorker::new()),          // impl Worker for your backend
    Arc::new(CostGovernor::new(500_000)),    // global token budget
    SupervisorConfig::default(),
);

// Surface blockers to a human from the event stream:
let mut events = supervisor.events();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        if let RuntimeEvent::TaskBlocked { task, question } = event {
            // ask the human, then: supervisor.resolve_blocker(&task, answer);
        }
    }
});

let report = supervisor.run().await;
println!("{} completed, {} failed, {} cancelled",
         report.completed, report.failed, report.cancelled);
```

A `Worker` is one method — plug in an LLM session, a subprocess, a container, an HTTP job runner:

```rust
impl Worker for MyAgentWorker {
    fn execute(&self, spec: TaskSpec, hb: Heartbeat, answer: Option<String>)
        -> Pin<Box<dyn Future<Output = WorkerOutcome> + Send>>
    {
        Box::pin(async move {
            // ... drive your agent; call hb.beat() on every progress step
            WorkerOutcome::Success { output, tokens_used }
        })
    }
}
```

## Verified behavior (the test suite is the spec)

`tests/runtime.rs` exercises the runtime end-to-end with mock workers:

- diamond DAG completes in dependency order, independent branches in parallel (wall-clock asserted)
- flaky worker recovers on retry; exhausted attempts fail and **cancel the downstream blast radius**
- a worker that stops heartbeating is *suspected*, *confirmed*, aborted, and successfully retried —
  with both phases visible on the event stream
- a blocked task parks, surfaces its question, and resumes correctly when answered
- the budget governor rejects an unaffordable task and fails it loudly when nothing can free budget

```bash
cargo test            # 18 tests
cargo doc --open      # full API docs
```

## Provenance & scope

Extracted from the orchestration layer of a production multi-service system (40+ services,
evidence-based 3 s auto-recovery, token-cost governance). This repository is the *pattern* — the scheduling,
supervision, recovery, escalation, and governance machinery — as a clean-room library with zero
deployment-specific code. Persistence, transports, and agent integrations are deliberately out of
scope: implement `Worker` and bring your own.

## License

MIT — see [LICENSE](LICENSE).
