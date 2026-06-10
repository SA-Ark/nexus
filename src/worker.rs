//! Worker abstraction: anything that can execute a task plugs in here —
//! an LLM agent session, a subprocess, a container, a remote job.

use crate::task::TaskSpec;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// What a worker reports back for one execution attempt.
#[derive(Debug, Clone)]
pub enum WorkerOutcome {
    /// Task finished; `output` is the result, `tokens_used` the actual spend.
    Success { output: String, tokens_used: u64 },
    /// Task failed in a way a retry might fix (transient error, crash).
    Retryable { error: String, tokens_used: u64 },
    /// Worker needs an external answer before it can proceed.
    Blocked { question: String, tokens_used: u64 },
    /// Task failed in a way retries will not fix (bad input, impossible).
    Fatal { error: String, tokens_used: u64 },
}

/// Liveness handle a worker beats while making progress. The supervisor
/// declares a worker stalled only on *evidence* (heartbeat age), never on
/// a wall-clock guess about how long work "should" take.
#[derive(Debug, Clone, Default)]
pub struct Heartbeat {
    last_beat_ms: Arc<AtomicU64>,
}

impl Heartbeat {
    pub fn new() -> Self {
        let hb = Self {
            last_beat_ms: Arc::new(AtomicU64::new(0)),
        };
        hb.beat();
        hb
    }

    /// Signal liveness. Call this whenever the worker makes progress.
    pub fn beat(&self) {
        self.last_beat_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Milliseconds since the last beat.
    pub fn age_ms(&self) -> u64 {
        now_ms().saturating_sub(self.last_beat_ms.load(Ordering::Relaxed))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The execution backend. Implementations receive the task spec, a
/// heartbeat to keep beating, and (on resume after a blocker) the answer
/// to the question they raised.
pub trait Worker: Send + Sync + 'static {
    fn execute(
        &self,
        spec: TaskSpec,
        heartbeat: Heartbeat,
        blocker_answer: Option<String>,
    ) -> Pin<Box<dyn Future<Output = WorkerOutcome> + Send>>;
}

/// Blanket impl so closures can be used as workers in tests and simple
/// embeddings.
impl<F> Worker for F
where
    F: Fn(
            TaskSpec,
            Heartbeat,
            Option<String>,
        ) -> Pin<Box<dyn Future<Output = WorkerOutcome> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute(
        &self,
        spec: TaskSpec,
        heartbeat: Heartbeat,
        blocker_answer: Option<String>,
    ) -> Pin<Box<dyn Future<Output = WorkerOutcome> + Send>> {
        self(spec, heartbeat, blocker_answer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_age_grows_then_resets() {
        let hb = Heartbeat::new();
        assert!(hb.age_ms() < 1000);
        std::thread::sleep(std::time::Duration::from_millis(15));
        assert!(hb.age_ms() >= 10);
        hb.beat();
        assert!(hb.age_ms() < 10);
    }
}
