//! End-to-end runtime scenarios with mock workers: parallelism, retry,
//! stall recovery, blockers, budget enforcement, and failure cascades.

use nexus::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

type BoxedOutcome = Pin<Box<dyn Future<Output = WorkerOutcome> + Send>>;

fn fast_config() -> SupervisorConfig {
    SupervisorConfig {
        max_concurrent: 8,
        stall_suspect_ms: 100,
        stall_confirm_ms: 50,
        retry_backoff_ms: 10,
        tick_ms: 10,
    }
}

fn governor() -> Arc<CostGovernor> {
    Arc::new(CostGovernor::new(10_000_000))
}

#[tokio::test]
async fn diamond_dag_completes_in_dependency_order() {
    let dag = TaskDag::build(vec![
        TaskSpec::new("a", ""),
        TaskSpec::new("b", "").after("a"),
        TaskSpec::new("c", "").after("a"),
        TaskSpec::new("d", "").after("b").after("c"),
    ])
    .unwrap();

    let order = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let order_clone = Arc::clone(&order);
    let worker = Arc::new(move |spec: TaskSpec, _hb: Heartbeat, _a: Option<String>| {
        let order = Arc::clone(&order_clone);
        Box::pin(async move {
            order.lock().unwrap().push(spec.id.clone());
            WorkerOutcome::Success {
                output: spec.id,
                tokens_used: 10,
            }
        }) as BoxedOutcome
    });

    let supervisor = Supervisor::new(dag, worker, governor(), fast_config());
    let report = supervisor.run().await;

    assert_eq!(report.completed, 4);
    let order = order.lock().unwrap().clone();
    assert_eq!(order[0], "a");
    assert_eq!(order[3], "d");
}

#[tokio::test]
async fn flaky_worker_recovers_via_retry() {
    let dag = TaskDag::build(vec![TaskSpec::new("flaky", "").attempts(3)]).unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let calls_clone = Arc::clone(&calls);

    let worker = Arc::new(move |_spec: TaskSpec, _hb: Heartbeat, _a: Option<String>| {
        let calls = Arc::clone(&calls_clone);
        Box::pin(async move {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                WorkerOutcome::Retryable {
                    error: "transient network failure".into(),
                    tokens_used: 5,
                }
            } else {
                WorkerOutcome::Success {
                    output: "third time lucky".into(),
                    tokens_used: 5,
                }
            }
        }) as BoxedOutcome
    });

    let supervisor = Supervisor::new(dag, worker, governor(), fast_config());
    let report = supervisor.run().await;

    assert_eq!(report.completed, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn attempts_exhausted_fails_and_cancels_dependents() {
    let dag = TaskDag::build(vec![
        TaskSpec::new("doomed", "").attempts(2),
        TaskSpec::new("downstream", "").after("doomed"),
    ])
    .unwrap();

    let worker = Arc::new(move |_s: TaskSpec, _hb: Heartbeat, _a: Option<String>| {
        Box::pin(async move {
            WorkerOutcome::Retryable {
                error: "always fails".into(),
                tokens_used: 1,
            }
        }) as BoxedOutcome
    });

    let supervisor = Supervisor::new(dag, worker, governor(), fast_config());
    let report = supervisor.run().await;

    assert_eq!(report.failed, 1);
    assert_eq!(report.cancelled, 1);
    assert!(matches!(
        report.states["downstream"],
        TaskState::Cancelled { .. }
    ));
}

#[tokio::test]
async fn stalled_worker_is_detected_confirmed_and_recovered() {
    let dag = TaskDag::build(vec![TaskSpec::new("sleepy", "").attempts(2)]).unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let calls_clone = Arc::clone(&calls);

    let worker = Arc::new(move |_s: TaskSpec, hb: Heartbeat, _a: Option<String>| {
        let calls = Arc::clone(&calls_clone);
        Box::pin(async move {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                // First attempt: stop beating forever -> stall.
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            }
            // Second attempt behaves: beats and succeeds.
            hb.beat();
            WorkerOutcome::Success {
                output: "recovered".into(),
                tokens_used: 7,
            }
        }) as BoxedOutcome
    });

    let supervisor = Supervisor::new(dag, worker, governor(), fast_config());
    let mut events = supervisor.events();
    let report = supervisor.run().await;

    assert_eq!(report.completed, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // The event stream must show suspect -> confirm, not a silent kill.
    let mut saw_suspect = false;
    let mut saw_confirm = false;
    while let Ok(event) = events.try_recv() {
        match event {
            RuntimeEvent::WorkerStallSuspected { .. } => saw_suspect = true,
            RuntimeEvent::WorkerStallConfirmed { .. } => saw_confirm = true,
            _ => {}
        }
    }
    assert!(saw_suspect && saw_confirm);
}

#[tokio::test]
async fn blocked_task_resumes_with_answer() {
    let dag = TaskDag::build(vec![TaskSpec::new("asker", "")]).unwrap();

    let worker = Arc::new(
        move |_s: TaskSpec, _hb: Heartbeat, answer: Option<String>| {
            Box::pin(async move {
                match answer {
                    None => WorkerOutcome::Blocked {
                        question: "prod or staging?".into(),
                        tokens_used: 3,
                    },
                    Some(answer) => WorkerOutcome::Success {
                        output: format!("deployed to {answer}"),
                        tokens_used: 3,
                    },
                }
            }) as BoxedOutcome
        },
    );

    let supervisor = Arc::new(Supervisor::new(dag, worker, governor(), fast_config()));
    let mut events = supervisor.events();

    // Resolver: answers the blocker when it appears on the event stream.
    let resolver = {
        let supervisor = Arc::clone(&supervisor);
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(RuntimeEvent::TaskBlocked { task, question }) => {
                        assert_eq!(question, "prod or staging?");
                        assert!(supervisor.resolve_blocker(&task, "staging"));
                        break;
                    }
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        })
    };

    let report = supervisor.run().await;
    resolver.await.unwrap();

    assert_eq!(report.completed, 1);
    assert!(matches!(
        &report.states["asker"],
        TaskState::Completed { output } if output == "deployed to staging"
    ));
}

#[tokio::test]
async fn budget_governor_blocks_overspend_and_fails_loudly() {
    // Global budget fits the first task but not the second.
    let dag = TaskDag::build(vec![
        TaskSpec::new("cheap", "").budget(100),
        TaskSpec::new("expensive", "").after("cheap").budget(10_000),
    ])
    .unwrap();

    let worker = Arc::new(move |spec: TaskSpec, _hb: Heartbeat, _a: Option<String>| {
        Box::pin(async move {
            WorkerOutcome::Success {
                output: spec.id,
                tokens_used: 100,
            }
        }) as BoxedOutcome
    });

    let governor = Arc::new(CostGovernor::new(500));
    let supervisor = Supervisor::new(dag, worker, governor, fast_config());
    let mut events = supervisor.events();
    let report = supervisor.run().await;

    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 1);
    assert!(matches!(
        &report.states["expensive"],
        TaskState::Failed { error } if error.contains("budget")
    ));

    let mut saw_rejection = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, RuntimeEvent::BudgetRejected { .. }) {
            saw_rejection = true;
        }
    }
    assert!(saw_rejection);
}

#[tokio::test]
async fn parallel_dispatch_actually_overlaps() {
    // Two independent 100ms tasks under max_concurrent=2 must finish in
    // well under 200ms of serial time.
    let dag = TaskDag::build(vec![TaskSpec::new("p1", ""), TaskSpec::new("p2", "")]).unwrap();

    let worker = Arc::new(move |spec: TaskSpec, hb: Heartbeat, _a: Option<String>| {
        Box::pin(async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                hb.beat();
            }
            WorkerOutcome::Success {
                output: spec.id,
                tokens_used: 1,
            }
        }) as BoxedOutcome
    });

    let supervisor = Supervisor::new(dag, worker, governor(), fast_config());
    let started = std::time::Instant::now();
    let report = supervisor.run().await;
    let elapsed = started.elapsed();

    assert_eq!(report.completed, 2);
    assert!(
        elapsed < Duration::from_millis(180),
        "tasks ran serially: {elapsed:?}"
    );
}
