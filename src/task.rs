//! Task model: specs, states, and the dependency DAG.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

pub type TaskId = String;

/// A unit of work with explicit dependencies and a token budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    /// Free-form payload handed to the worker (a prompt, a command, a job
    /// description — the runtime does not interpret it).
    pub payload: String,
    /// Ids of tasks that must complete before this one may start.
    pub depends_on: Vec<TaskId>,
    /// Maximum tokens (or cost units) this task may spend.
    pub token_budget: u64,
    /// Maximum execution attempts before the task is failed and escalated.
    pub max_attempts: u32,
}

impl TaskSpec {
    pub fn new(id: impl Into<TaskId>, payload: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            payload: payload.into(),
            depends_on: Vec::new(),
            token_budget: 100_000,
            max_attempts: 3,
        }
    }

    pub fn after(mut self, dep: impl Into<TaskId>) -> Self {
        self.depends_on.push(dep.into());
        self
    }

    pub fn budget(mut self, tokens: u64) -> Self {
        self.token_budget = tokens;
        self
    }

    pub fn attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum TaskState {
    Pending,
    Running {
        attempt: u32,
    },
    /// Waiting on a human/orchestrator answer to a question the worker raised.
    Blocked {
        question: String,
    },
    Completed {
        output: String,
    },
    Failed {
        error: String,
    },
    /// Dependencies can never be satisfied (an upstream task failed).
    Cancelled {
        reason: String,
    },
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed { .. } | TaskState::Failed { .. } | TaskState::Cancelled { .. }
        )
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum DagError {
    #[error("duplicate task id: {0}")]
    DuplicateId(TaskId),
    #[error("task {task} depends on unknown task {dep}")]
    UnknownDependency { task: TaskId, dep: TaskId },
    #[error("dependency cycle involving task {0}")]
    Cycle(TaskId),
}

/// Validated dependency DAG over a set of task specs.
#[derive(Debug, Clone)]
pub struct TaskDag {
    pub tasks: HashMap<TaskId, TaskSpec>,
    /// task -> tasks that depend on it
    pub dependents: HashMap<TaskId, Vec<TaskId>>,
}

impl TaskDag {
    /// Validate uniqueness, dependency existence, and acyclicity.
    pub fn build(specs: Vec<TaskSpec>) -> Result<Self, DagError> {
        let mut tasks = HashMap::new();
        for spec in specs {
            if tasks.insert(spec.id.clone(), spec.clone()).is_some() {
                return Err(DagError::DuplicateId(spec.id));
            }
        }

        let mut dependents: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        for spec in tasks.values() {
            for dep in &spec.depends_on {
                if !tasks.contains_key(dep) {
                    return Err(DagError::UnknownDependency {
                        task: spec.id.clone(),
                        dep: dep.clone(),
                    });
                }
                dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(spec.id.clone());
            }
        }

        // Kahn's algorithm for cycle detection.
        let mut in_degree: HashMap<&TaskId, usize> = tasks
            .iter()
            .map(|(id, spec)| (id, spec.depends_on.len()))
            .collect();
        let mut queue: Vec<&TaskId> = in_degree
            .iter()
            .filter(|(_, &d)| d == 0)
            .map(|(id, _)| *id)
            .collect();
        let mut visited = 0usize;
        while let Some(id) = queue.pop() {
            visited += 1;
            if let Some(deps) = dependents.get(id) {
                for dependent in deps {
                    let d = in_degree.get_mut(dependent).expect("known task");
                    *d -= 1;
                    if *d == 0 {
                        queue.push(dependent);
                    }
                }
            }
        }
        if visited != tasks.len() {
            let cyclic = tasks
                .keys()
                .find(|id| in_degree[id] > 0)
                .expect("cycle implies a remaining node")
                .clone();
            return Err(DagError::Cycle(cyclic));
        }

        Ok(Self { tasks, dependents })
    }

    /// Tasks whose dependencies are all completed, given current states.
    /// Deterministic: sorted by id.
    pub fn ready(&self, states: &HashMap<TaskId, TaskState>) -> Vec<TaskId> {
        let mut ready: Vec<TaskId> = self
            .tasks
            .values()
            .filter(|spec| matches!(states.get(&spec.id), Some(TaskState::Pending)))
            .filter(|spec| {
                spec.depends_on
                    .iter()
                    .all(|dep| matches!(states.get(dep), Some(TaskState::Completed { .. })))
            })
            .map(|spec| spec.id.clone())
            .collect();
        ready.sort();
        ready
    }

    /// All transitive dependents of `id` (the blast radius of its failure).
    pub fn transitive_dependents(&self, id: &TaskId) -> HashSet<TaskId> {
        let mut result = HashSet::new();
        let mut stack = vec![id.clone()];
        while let Some(current) = stack.pop() {
            if let Some(deps) = self.dependents.get(&current) {
                for d in deps {
                    if result.insert(d.clone()) {
                        stack.push(d.clone());
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn states_of(dag: &TaskDag) -> HashMap<TaskId, TaskState> {
        dag.tasks
            .keys()
            .map(|id| (id.clone(), TaskState::Pending))
            .collect()
    }

    #[test]
    fn rejects_duplicate_ids() {
        let err =
            TaskDag::build(vec![TaskSpec::new("a", "x"), TaskSpec::new("a", "y")]).unwrap_err();
        assert_eq!(err, DagError::DuplicateId("a".into()));
    }

    #[test]
    fn rejects_unknown_dependency() {
        let err = TaskDag::build(vec![TaskSpec::new("a", "x").after("ghost")]).unwrap_err();
        assert!(matches!(err, DagError::UnknownDependency { .. }));
    }

    #[test]
    fn rejects_cycles() {
        let err = TaskDag::build(vec![
            TaskSpec::new("a", "x").after("b"),
            TaskSpec::new("b", "y").after("a"),
        ])
        .unwrap_err();
        assert!(matches!(err, DagError::Cycle(_)));
    }

    #[test]
    fn ready_set_respects_dependencies() {
        let dag = TaskDag::build(vec![
            TaskSpec::new("a", "x"),
            TaskSpec::new("b", "y").after("a"),
            TaskSpec::new("c", "z"),
        ])
        .unwrap();
        let mut states = states_of(&dag);

        assert_eq!(dag.ready(&states), vec!["a".to_string(), "c".to_string()]);

        states.insert(
            "a".into(),
            TaskState::Completed {
                output: "done".into(),
            },
        );
        states.insert("c".into(), TaskState::Running { attempt: 1 });
        assert_eq!(dag.ready(&states), vec!["b".to_string()]);
    }

    #[test]
    fn transitive_dependents_compute_blast_radius() {
        let dag = TaskDag::build(vec![
            TaskSpec::new("a", ""),
            TaskSpec::new("b", "").after("a"),
            TaskSpec::new("c", "").after("b"),
            TaskSpec::new("d", ""),
        ])
        .unwrap();
        let blast = dag.transitive_dependents(&"a".to_string());
        assert_eq!(blast.len(), 2);
        assert!(blast.contains("b") && blast.contains("c"));
    }
}
