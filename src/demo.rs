//! The built-in example workflow used by the live demo server.
//!
//! This module is feature-gated behind `server`. It is **not** part of the
//! core runtime contract — it exists so a visitor can trigger a realistic
//! multi-agent DAG and watch the supervisor drive it to completion in the
//! browser. The [`DemoWorker`](crate::demo::DemoWorker) is a *simulated*
//! agent: instead of calling a real LLM, it sleeps for a scripted duration
//! while beating its heartbeat, then returns a scripted outcome. Every
//! supervisor mechanism — parallel dispatch, retry-with-backoff,
//! human-in-the-loop blockers, and successful completion — is exercised by
//! the example DAG below.

use crate::prelude::*;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// How a single demo task should behave when the simulated agent runs it.
#[derive(Clone)]
struct Script {
    /// Roughly how long the agent "works" (ms). Split into heartbeat steps.
    duration_ms: u64,
    /// Simulated tokens spent on a successful run.
    tokens: u64,
    /// Human-readable summary the agent "produces".
    output: &'static str,
    /// If set, the task fails transiently on its first attempt with this
    /// error, then succeeds — demonstrates evidence-based auto-recovery.
    flaky_once: Option<&'static str>,
    /// If set, the task blocks for a human decision on its first attempt,
    /// surfacing this question — demonstrates the blocker mechanism.
    blocks_with: Option<&'static str>,
}

impl Script {
    const fn quick(output: &'static str) -> Self {
        Self {
            duration_ms: 1_400,
            tokens: 1_000,
            output,
            flaky_once: None,
            blocks_with: None,
        }
    }
    const fn work(duration_ms: u64, tokens: u64, output: &'static str) -> Self {
        Self {
            duration_ms,
            tokens,
            output,
            flaky_once: None,
            blocks_with: None,
        }
    }
    const fn flaky(mut self, error: &'static str) -> Self {
        self.flaky_once = Some(error);
        self
    }
    const fn blocks(mut self, question: &'static str) -> Self {
        self.blocks_with = Some(question);
        self
    }
}

/// A simulated agent fleet. Each task id maps to a `Script`; per-task
/// attempt counters let "flaky" and "blocking" tasks behave differently on
/// their first run versus their retry/resume.
pub struct DemoWorker {
    scripts: HashMap<TaskId, Script>,
    attempts: std::sync::Mutex<HashMap<TaskId, Arc<AtomicU32>>>,
    /// Global speed multiplier; 1.0 = scripted timing. Higher = faster demo.
    speed: f64,
}

impl DemoWorker {
    fn new(scripts: HashMap<TaskId, Script>, speed: f64) -> Self {
        Self {
            scripts,
            attempts: std::sync::Mutex::new(HashMap::new()),
            speed: if speed <= 0.0 { 1.0 } else { speed },
        }
    }

    fn attempt_counter(&self, id: &TaskId) -> Arc<AtomicU32> {
        let mut map = self.attempts.lock().expect("attempts lock");
        Arc::clone(
            map.entry(id.clone())
                .or_insert_with(|| Arc::new(AtomicU32::new(0))),
        )
    }
}

impl Worker for DemoWorker {
    fn execute(
        &self,
        spec: TaskSpec,
        hb: Heartbeat,
        answer: Option<String>,
    ) -> Pin<Box<dyn Future<Output = WorkerOutcome> + Send>> {
        let script = self
            .scripts
            .get(&spec.id)
            .cloned()
            .unwrap_or_else(|| Script::quick("done"));
        let counter = self.attempt_counter(&spec.id);
        let speed = self.speed;

        Box::pin(async move {
            let attempt = counter.fetch_add(1, Ordering::SeqCst); // 0-based

            // Human-in-the-loop: block on the first run, resume once answered.
            if let Some(question) = script.blocks_with {
                if answer.is_none() {
                    // A little "thinking" before raising the question.
                    work_with_heartbeat(&hb, (500.0 / speed) as u64).await;
                    return WorkerOutcome::Blocked {
                        question: question.into(),
                        tokens_used: script.tokens / 4,
                    };
                }
            }

            // Transient failure on the first attempt, then recover.
            if let Some(error) = script.flaky_once {
                if attempt == 0 {
                    work_with_heartbeat(&hb, (script.duration_ms as f64 / 2.0 / speed) as u64)
                        .await;
                    return WorkerOutcome::Retryable {
                        error: error.into(),
                        tokens_used: script.tokens / 2,
                    };
                }
            }

            let dur = (script.duration_ms as f64 / speed) as u64;
            work_with_heartbeat(&hb, dur).await;

            let output = match answer {
                Some(a) => format!("{} (decision: {a})", script.output),
                None => script.output.to_string(),
            };
            WorkerOutcome::Success {
                output,
                tokens_used: script.tokens,
            }
        })
    }
}

/// Sleep for `total_ms`, beating the heartbeat every ~250ms so the
/// supervisor sees the worker as alive throughout.
async fn work_with_heartbeat(hb: &Heartbeat, total_ms: u64) {
    let step = 250u64;
    let mut remaining = total_ms;
    hb.beat();
    while remaining > 0 {
        let chunk = remaining.min(step);
        tokio::time::sleep(Duration::from_millis(chunk)).await;
        hb.beat();
        remaining -= chunk;
    }
}

/// Node metadata for the UI: a label and a short role, keyed by task id.
pub struct DemoNodeMeta {
    pub label: &'static str,
    pub role: &'static str,
}

/// Human-friendly labels for the example DAG, surfaced to the UI.
pub fn node_meta() -> HashMap<TaskId, DemoNodeMeta> {
    let m: &[(&str, &str, &str)] = &[
        ("intake", "Intake & Spec", "planner"),
        ("research", "Research Sources", "researcher"),
        ("schema", "Design Schema", "architect"),
        ("backend", "Build Backend", "engineer"),
        ("frontend", "Build Frontend", "engineer"),
        ("tests", "Write & Run Tests", "qa"),
        ("deploy_gate", "Deploy Approval", "release-mgr"),
        ("deploy", "Deploy to Prod", "release-mgr"),
        ("verify", "E2E Verify", "qa"),
    ];
    m.iter()
        .map(|(id, label, role)| (id.to_string(), DemoNodeMeta { label, role }))
        .collect()
}

/// Build the example workflow: a realistic "ship a feature" pipeline.
///
/// ```text
///                 ┌─ research ─┐
///   intake ──────►│            ├─► backend ─┐
///          └──────►│  schema   │            ├─► tests ─► deploy_gate ─► deploy ─► verify
///                 └────────────┴─► frontend ┘            (blocker)
/// ```
///
/// - `research` and `schema` run in parallel after `intake`.
/// - `backend` and `frontend` run in parallel after `schema`.
/// - `frontend` is **flaky** (transient failure on attempt 1, recovers).
/// - `deploy_gate` is a **blocker** (asks prod/staging; awaits a human).
/// - a permanent failure anywhere would cancel its transitive dependents.
pub fn example_dag() -> TaskDag {
    TaskDag::build(vec![
        TaskSpec::new(
            "intake",
            "Parse the feature request into an actionable spec",
        )
        .budget(8_000),
        TaskSpec::new("research", "Gather prior art and reference implementations")
            .after("intake")
            .budget(40_000),
        TaskSpec::new("schema", "Design the data schema and API contract")
            .after("intake")
            .budget(30_000),
        TaskSpec::new("backend", "Implement the backend service")
            .after("schema")
            .after("research")
            .budget(120_000),
        TaskSpec::new("frontend", "Implement the frontend UI")
            .after("schema")
            .budget(90_000)
            .attempts(3),
        TaskSpec::new("tests", "Write and run the integration test suite")
            .after("backend")
            .after("frontend")
            .budget(60_000),
        TaskSpec::new("deploy_gate", "Get human approval for the deploy target")
            .after("tests")
            .budget(4_000),
        TaskSpec::new("deploy", "Deploy the build to the chosen environment")
            .after("deploy_gate")
            .budget(20_000),
        TaskSpec::new(
            "verify",
            "Run end-to-end checks against the live deployment",
        )
        .after("deploy")
        .budget(30_000),
    ])
    .expect("example DAG is acyclic and well-formed")
}

/// The per-task scripts that drive [`DemoWorker`] for the example DAG.
fn example_scripts() -> HashMap<TaskId, Script> {
    let entries: Vec<(&str, Script)> = vec![
        (
            "intake",
            Script::work(1_300, 3_500, "Spec: add CSV export to reports"),
        ),
        (
            "research",
            Script::work(
                2_600,
                22_000,
                "Found 3 reference exporters; chose streaming approach",
            ),
        ),
        (
            "schema",
            Script::work(2_100, 14_000, "Schema + OpenAPI contract drafted"),
        ),
        (
            "backend",
            Script::work(
                3_200,
                84_000,
                "Export endpoint + streaming writer implemented",
            ),
        ),
        (
            "frontend",
            Script::work(2_800, 61_000, "Export button + progress UI wired")
                .flaky("vite dev-server connection reset"),
        ),
        (
            "tests",
            Script::work(2_400, 38_000, "14 tests written, all green"),
        ),
        (
            "deploy_gate",
            Script::quick("Approved").blocks("Deploy to prod or staging?"),
        ),
        (
            "deploy",
            Script::work(1_900, 12_000, "Build shipped, systemd restarted"),
        ),
        (
            "verify",
            Script::work(2_000, 18_000, "Health 200; E2E export smoke-test passed"),
        ),
    ];
    entries
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

/// Construct a supervisor wired with the example DAG, a simulated agent
/// fleet, and a generous cost budget. `speed` > 1.0 speeds up the demo.
pub fn build_demo_supervisor(speed: f64) -> Supervisor {
    let dag = example_dag();
    let worker = Arc::new(DemoWorker::new(example_scripts(), speed));
    // Sum of task budgets is ~270k; give headroom so cost is governed but
    // the happy path always fits.
    let governor = Arc::new(CostGovernor::new(500_000));
    let config = SupervisorConfig {
        max_concurrent: 4,
        // Generous liveness windows: the demo never falsely flags a stall.
        stall_suspect_ms: 6_000,
        stall_confirm_ms: 3_000,
        retry_backoff_ms: 600,
        tick_ms: 200,
    };
    Supervisor::new(dag, worker, governor, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn example_workflow_runs_to_completion() {
        // Fast speed so the test is quick; auto-resolve the blocker.
        let supervisor = Arc::new(build_demo_supervisor(20.0));
        let mut events = supervisor.events();

        let resolver = {
            let sup = Arc::clone(&supervisor);
            tokio::spawn(async move {
                while let Ok(ev) = events.recv().await {
                    if let RuntimeEvent::TaskBlocked { task, .. } = ev {
                        sup.resolve_blocker(&task, "staging");
                    }
                }
            })
        };

        let report = supervisor.run().await;
        resolver.abort();

        assert_eq!(report.failed, 0, "states: {:?}", report.states);
        assert_eq!(report.cancelled, 0);
        assert_eq!(report.completed, 9);
    }

    #[test]
    fn example_dag_is_valid_and_has_expected_shape() {
        let dag = example_dag();
        assert_eq!(dag.tasks.len(), 9);
        // intake has two direct dependents: research and schema.
        let deps = dag.dependents.get("intake").cloned().unwrap_or_default();
        assert!(deps.contains(&"research".to_string()));
        assert!(deps.contains(&"schema".to_string()));
        // A failure at intake would cancel everything downstream.
        assert_eq!(dag.transitive_dependents(&"intake".to_string()).len(), 8);
    }

    #[test]
    fn node_meta_covers_every_task() {
        let dag = example_dag();
        let meta = node_meta();
        for id in dag.tasks.keys() {
            assert!(meta.contains_key(id), "missing meta for {id}");
        }
    }
}
