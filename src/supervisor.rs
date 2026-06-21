//! The supervisor: dependency-aware dispatch, heartbeat liveness,
//! automatic recovery, blocker escalation, and budget enforcement —
//! one event loop, fully observable.
//!
//! ## Failure philosophy
//!
//! The supervisor never uses a wall-clock guess about how long a task
//! "should" take. Liveness is evidence-based: a worker that stops beating
//! its [`Heartbeat`] is first *suspected*, then *confirmed* stalled after a
//! second observation window, and only then recovered (aborted and
//! retried). Permanent failures cancel their transitive dependents loudly
//! instead of leaving them pending forever.

use crate::budget::CostGovernor;
use crate::task::{TaskDag, TaskId, TaskState};
use crate::worker::{Heartbeat, Worker, WorkerOutcome};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Maximum concurrently running tasks.
    pub max_concurrent: usize,
    /// Heartbeat age (ms) after which a worker becomes *suspected* stalled.
    pub stall_suspect_ms: u64,
    /// Additional confirmation window (ms) before a suspected worker is
    /// declared stalled and recovered. Detect → confirm → recover.
    pub stall_confirm_ms: u64,
    /// Base backoff between retry attempts; attempt `n` waits `n * base`.
    pub retry_backoff_ms: u64,
    /// Supervisor tick for liveness checks.
    pub tick_ms: u64,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 8,
            stall_suspect_ms: 2_000,
            stall_confirm_ms: 1_000,
            retry_backoff_ms: 500,
            tick_ms: 100,
        }
    }
}

/// Everything the runtime does is announced here — drive dashboards,
/// logs, or tests from the same stream.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "event")]
pub enum RuntimeEvent {
    TaskStarted {
        task: TaskId,
        attempt: u32,
    },
    TaskCompleted {
        task: TaskId,
    },
    TaskRetrying {
        task: TaskId,
        attempt: u32,
        error: String,
    },
    TaskBlocked {
        task: TaskId,
        question: String,
    },
    BlockerResolved {
        task: TaskId,
    },
    TaskFailed {
        task: TaskId,
        error: String,
    },
    TaskCancelled {
        task: TaskId,
        reason: String,
    },
    WorkerStallSuspected {
        task: TaskId,
        heartbeat_age_ms: u64,
    },
    WorkerStallConfirmed {
        task: TaskId,
    },
    BudgetRejected {
        task: TaskId,
        requested: u64,
        available: u64,
    },
    RunFinished {
        completed: usize,
        failed: usize,
        cancelled: usize,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub states: HashMap<TaskId, TaskState>,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
}

struct Running {
    handle: JoinHandle<()>,
    heartbeat: Heartbeat,
    attempt: u32,
    suspected_at: Option<Instant>,
}

pub struct Supervisor {
    dag: TaskDag,
    worker: Arc<dyn Worker>,
    governor: Arc<CostGovernor>,
    config: SupervisorConfig,
    states: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    answers: Arc<Mutex<HashMap<TaskId, String>>>,
    events_tx: broadcast::Sender<RuntimeEvent>,
    wake: Arc<Notify>,
}

impl Supervisor {
    pub fn new(
        dag: TaskDag,
        worker: Arc<dyn Worker>,
        governor: Arc<CostGovernor>,
        config: SupervisorConfig,
    ) -> Self {
        let states = dag
            .tasks
            .keys()
            .map(|id| (id.clone(), TaskState::Pending))
            .collect();
        let (events_tx, _) = broadcast::channel(1024);
        Self {
            dag,
            worker,
            governor,
            config,
            states: Arc::new(Mutex::new(states)),
            answers: Arc::new(Mutex::new(HashMap::new())),
            events_tx,
            wake: Arc::new(Notify::new()),
        }
    }

    /// Subscribe to the runtime event stream.
    pub fn events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.events_tx.subscribe()
    }

    /// The validated DAG this supervisor is driving (for introspection and
    /// visualization).
    pub fn dag(&self) -> &TaskDag {
        &self.dag
    }

    /// Current task states (cloned snapshot).
    pub fn states(&self) -> HashMap<TaskId, TaskState> {
        self.states.lock().expect("states lock").clone()
    }

    /// Answer a blocked task's question; it re-enters the schedule and the
    /// answer is handed to the worker on its next attempt.
    pub fn resolve_blocker(&self, task: &str, answer: impl Into<String>) -> bool {
        let mut states = self.states.lock().expect("states lock");
        match states.get(task) {
            Some(TaskState::Blocked { .. }) => {
                states.insert(task.to_string(), TaskState::Pending);
                self.answers
                    .lock()
                    .expect("answers lock")
                    .insert(task.to_string(), answer.into());
                self.emit(RuntimeEvent::BlockerResolved {
                    task: task.to_string(),
                });
                self.wake.notify_one();
                true
            }
            _ => false,
        }
    }

    fn emit(&self, event: RuntimeEvent) {
        let _ = self.events_tx.send(event);
    }

    /// Drive the DAG to completion. Returns when every task is terminal.
    /// Blocked tasks keep the run alive until [`Self::resolve_blocker`] is
    /// called (typically from another task subscribed to the event stream).
    pub async fn run(&self) -> RunReport {
        let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel::<(TaskId, WorkerOutcome)>();
        let mut running: HashMap<TaskId, Running> = HashMap::new();
        let mut attempts_done: HashMap<TaskId, u32> = HashMap::new();
        let mut next_retry_at: HashMap<TaskId, Instant> = HashMap::new();

        loop {
            self.dispatch_ready(
                &outcome_tx,
                &mut running,
                &mut attempts_done,
                &next_retry_at,
            );

            if self.all_terminal() {
                break;
            }

            tokio::select! {
                outcome = outcome_rx.recv() => {
                    if let Some((task_id, outcome)) = outcome {
                        running.remove(&task_id);
                        self.handle_outcome(
                            &task_id,
                            outcome,
                            &mut attempts_done,
                            &mut next_retry_at,
                        );
                    }
                }
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(self.config.tick_ms)) => {
                    self.check_stalls(&mut running, &mut attempts_done, &mut next_retry_at);
                }
            }
        }

        let states = self.states();
        let count = |f: fn(&TaskState) -> bool| states.values().filter(|s| f(s)).count();
        let report = RunReport {
            completed: count(|s| matches!(s, TaskState::Completed { .. })),
            failed: count(|s| matches!(s, TaskState::Failed { .. })),
            cancelled: count(|s| matches!(s, TaskState::Cancelled { .. })),
            states,
        };
        self.emit(RuntimeEvent::RunFinished {
            completed: report.completed,
            failed: report.failed,
            cancelled: report.cancelled,
        });
        report
    }

    fn dispatch_ready(
        &self,
        outcome_tx: &mpsc::UnboundedSender<(TaskId, WorkerOutcome)>,
        running: &mut HashMap<TaskId, Running>,
        attempts_done: &mut HashMap<TaskId, u32>,
        next_retry_at: &HashMap<TaskId, Instant>,
    ) {
        let ready = {
            let states = self.states.lock().expect("states lock");
            self.dag.ready(&states)
        };
        let now = Instant::now();

        for task_id in ready {
            if running.len() >= self.config.max_concurrent {
                break;
            }
            if next_retry_at.get(&task_id).is_some_and(|&t| now < t) {
                continue; // backoff window still open
            }
            let spec = self.dag.tasks[&task_id].clone();

            if let Err(e) = self.governor.reserve(&task_id, spec.token_budget) {
                let snapshot = self.governor.snapshot();
                self.emit(RuntimeEvent::BudgetRejected {
                    task: task_id.clone(),
                    requested: spec.token_budget,
                    available: snapshot.available,
                });
                // If nothing is running, no settlement will ever free
                // budget: fail loudly instead of waiting forever.
                if running.is_empty() {
                    self.fail_task(&task_id, &format!("budget exhausted: {e}"));
                }
                continue;
            }

            let attempt = attempts_done.get(&task_id).copied().unwrap_or(0) + 1;
            let heartbeat = Heartbeat::new();
            let answer = self.answers.lock().expect("answers lock").remove(&task_id);

            {
                let mut states = self.states.lock().expect("states lock");
                states.insert(task_id.clone(), TaskState::Running { attempt });
            }
            self.emit(RuntimeEvent::TaskStarted {
                task: task_id.clone(),
                attempt,
            });

            let worker = Arc::clone(&self.worker);
            let tx = outcome_tx.clone();
            let hb = heartbeat.clone();
            let id_for_task = task_id.clone();
            let handle = tokio::spawn(async move {
                let outcome = worker.execute(spec, hb, answer).await;
                let _ = tx.send((id_for_task, outcome));
            });

            running.insert(
                task_id,
                Running {
                    handle,
                    heartbeat,
                    attempt,
                    suspected_at: None,
                },
            );
        }
    }

    fn handle_outcome(
        &self,
        task_id: &TaskId,
        outcome: WorkerOutcome,
        attempts_done: &mut HashMap<TaskId, u32>,
        next_retry_at: &mut HashMap<TaskId, Instant>,
    ) {
        match outcome {
            WorkerOutcome::Success {
                output,
                tokens_used,
            } => {
                let _ = self.governor.settle(task_id, tokens_used);
                self.states
                    .lock()
                    .expect("states lock")
                    .insert(task_id.clone(), TaskState::Completed { output });
                self.emit(RuntimeEvent::TaskCompleted {
                    task: task_id.clone(),
                });
            }
            WorkerOutcome::Blocked {
                question,
                tokens_used,
            } => {
                let _ = self.governor.settle(task_id, tokens_used);
                self.states.lock().expect("states lock").insert(
                    task_id.clone(),
                    TaskState::Blocked {
                        question: question.clone(),
                    },
                );
                self.emit(RuntimeEvent::TaskBlocked {
                    task: task_id.clone(),
                    question,
                });
            }
            WorkerOutcome::Fatal { error, tokens_used } => {
                let _ = self.governor.settle(task_id, tokens_used);
                self.fail_task(task_id, &error);
            }
            WorkerOutcome::Retryable { error, tokens_used } => {
                let _ = self.governor.settle(task_id, tokens_used);
                let attempts = attempts_done.entry(task_id.clone()).or_insert(0);
                *attempts += 1;
                let max = self.dag.tasks[task_id].max_attempts;
                if *attempts >= max {
                    self.fail_task(task_id, &format!("{error} (after {max} attempts)"));
                } else {
                    let backoff =
                        Duration::from_millis(self.config.retry_backoff_ms * (*attempts as u64));
                    next_retry_at.insert(task_id.clone(), Instant::now() + backoff);
                    self.states
                        .lock()
                        .expect("states lock")
                        .insert(task_id.clone(), TaskState::Pending);
                    self.emit(RuntimeEvent::TaskRetrying {
                        task: task_id.clone(),
                        attempt: *attempts + 1,
                        error,
                    });
                }
            }
        }
        self.wake.notify_one();
    }

    /// Evidence-based liveness: suspect on heartbeat age, confirm after a
    /// second window, only then abort and recover.
    fn check_stalls(
        &self,
        running: &mut HashMap<TaskId, Running>,
        attempts_done: &mut HashMap<TaskId, u32>,
        next_retry_at: &mut HashMap<TaskId, Instant>,
    ) {
        let mut stalled: Vec<TaskId> = Vec::new();
        let now = Instant::now();

        for (task_id, run) in running.iter_mut() {
            let age = run.heartbeat.age_ms();
            if age < self.config.stall_suspect_ms {
                run.suspected_at = None;
                continue;
            }
            match run.suspected_at {
                None => {
                    run.suspected_at = Some(now);
                    self.emit(RuntimeEvent::WorkerStallSuspected {
                        task: task_id.clone(),
                        heartbeat_age_ms: age,
                    });
                }
                Some(since)
                    if now.duration_since(since).as_millis() as u64
                        >= self.config.stall_confirm_ms =>
                {
                    stalled.push(task_id.clone());
                }
                Some(_) => {} // still inside the confirmation window
            }
        }

        for task_id in stalled {
            let run = running.remove(&task_id).expect("stalled task is running");
            run.handle.abort();
            self.governor.release(&task_id);
            self.emit(RuntimeEvent::WorkerStallConfirmed {
                task: task_id.clone(),
            });

            let attempts = attempts_done.entry(task_id.clone()).or_insert(0);
            *attempts += 1;
            let max = self.dag.tasks[&task_id].max_attempts;
            if *attempts >= max {
                self.fail_task(
                    &task_id,
                    &format!(
                        "worker stalled (no heartbeat) on attempt {}/{max}",
                        run.attempt
                    ),
                );
            } else {
                next_retry_at.insert(task_id.clone(), Instant::now());
                self.states
                    .lock()
                    .expect("states lock")
                    .insert(task_id.clone(), TaskState::Pending);
                self.emit(RuntimeEvent::TaskRetrying {
                    task: task_id.clone(),
                    attempt: *attempts + 1,
                    error: "worker stalled (no heartbeat)".into(),
                });
            }
        }
    }

    /// Permanent failure: mark the task failed and cancel its entire
    /// transitive blast radius — loudly, never silently.
    fn fail_task(&self, task_id: &TaskId, error: &str) {
        {
            let mut states = self.states.lock().expect("states lock");
            states.insert(
                task_id.clone(),
                TaskState::Failed {
                    error: error.to_string(),
                },
            );
        }
        self.emit(RuntimeEvent::TaskFailed {
            task: task_id.clone(),
            error: error.to_string(),
        });

        for dependent in self.dag.transitive_dependents(task_id) {
            let mut states = self.states.lock().expect("states lock");
            if let Some(state) = states.get(&dependent) {
                if !state.is_terminal() {
                    let reason = format!("upstream task {task_id} failed");
                    states.insert(
                        dependent.clone(),
                        TaskState::Cancelled {
                            reason: reason.clone(),
                        },
                    );
                    drop(states);
                    self.emit(RuntimeEvent::TaskCancelled {
                        task: dependent,
                        reason,
                    });
                }
            }
        }
        self.wake.notify_one();
    }

    fn all_terminal(&self) -> bool {
        self.states
            .lock()
            .expect("states lock")
            .values()
            .all(|s| s.is_terminal())
    }
}
