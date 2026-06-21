//! Nexus live demo server.
//!
//! A tiny, self-hosted HTTP + Server-Sent-Events server that lets a visitor
//! trigger the built-in example workflow and watch the Nexus supervisor
//! drive a multi-agent DAG to completion in the browser. No web framework,
//! no database, no third-party services — just `hyper` and the runtime.
//!
//! Routes:
//!   GET  /             → the single-page demo UI
//!   GET  /api/dag      → the example DAG structure (nodes + edges)
//!   POST /api/run      → start a fresh run (resets state)
//!   GET  /api/events   → SSE stream: runtime events + state snapshots
//!   POST /api/resolve  → answer the pending human-in-the-loop blocker
//!   GET  /healthz      → liveness probe (200 "ok")

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Mutex};

use nexus::demo::{self, node_meta};
use nexus::prelude::*;

/// One line on the SSE stream: either a runtime event or a full snapshot.
#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StreamMsg {
    /// A decision the supervisor made (mirrors `RuntimeEvent`).
    Event { event: serde_json::Value },
    /// The full state of every task — sent on connect and after each event.
    Snapshot { tasks: Vec<NodeState> },
    /// A run has begun (clears the UI).
    RunStarted,
}

#[derive(Clone, Serialize)]
struct NodeState {
    id: String,
    /// "pending" | "running" | "blocked" | "completed" | "failed" | "cancelled"
    status: String,
    detail: String,
}

/// Shared server state: the current run, if any, and a broadcast of stream
/// messages that every connected SSE client subscribes to.
struct AppState {
    supervisor: Mutex<Option<Arc<Supervisor>>>,
    tx: broadcast::Sender<StreamMsg>,
}

fn task_state_to_node(id: &str, st: &TaskState) -> NodeState {
    let (status, detail) = match st {
        TaskState::Pending => ("pending", String::new()),
        TaskState::Running { attempt } => ("running", format!("attempt {attempt}")),
        TaskState::Blocked { question } => ("blocked", question.clone()),
        TaskState::Completed { output } => ("completed", output.clone()),
        TaskState::Failed { error } => ("failed", error.clone()),
        TaskState::Cancelled { reason } => ("cancelled", reason.clone()),
    };
    NodeState {
        id: id.to_string(),
        status: status.to_string(),
        detail,
    }
}

fn snapshot_from(states: &HashMap<TaskId, TaskState>) -> StreamMsg {
    let mut tasks: Vec<NodeState> = states
        .iter()
        .map(|(id, st)| task_state_to_node(id, st))
        .collect();
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    StreamMsg::Snapshot { tasks }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8099);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let (tx, _rx) = broadcast::channel::<StreamMsg>(2048);
    let state = Arc::new(AppState {
        supervisor: Mutex::new(None),
        tx,
    });

    let listener = TcpListener::bind(addr).await?;
    eprintln!("nexus-server listening on http://{addr}  (open it in a browser)");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(req, Arc::clone(&state)));
            // Client disconnects (notably on SSE) are normal; ignore errors.
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

type BoxedBody = BoxBody<Bytes, Infallible>;

fn full(body: impl Into<Bytes>) -> BoxedBody {
    Full::new(body.into()).boxed()
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response<BoxedBody> {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(full(body))
        .unwrap()
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<BoxedBody>, Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Response::builder()
            .header("content-type", "text/html; charset=utf-8")
            .body(full(INDEX_HTML))
            .unwrap(),
        (&Method::GET, "/healthz") => Response::builder()
            .header("content-type", "text/plain")
            .body(full("ok"))
            .unwrap(),
        (&Method::GET, "/api/dag") => dag_response(),
        (&Method::POST, "/api/run") => start_run(state).await,
        (&Method::GET, "/api/events") => events_stream(state),
        (&Method::POST, "/api/resolve") => resolve_blocker(req, state).await,
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(full("not found"))
            .unwrap(),
    };
    Ok(resp)
}

#[derive(Serialize)]
struct DagNode {
    id: String,
    label: String,
    role: String,
    depends_on: Vec<String>,
    budget: u64,
}

#[derive(Serialize)]
struct DagPayload {
    nodes: Vec<DagNode>,
}

fn dag_response() -> Response<BoxedBody> {
    let dag = demo::example_dag();
    let meta = node_meta();
    let mut nodes: Vec<DagNode> = dag
        .tasks
        .values()
        .map(|spec| {
            let m = meta.get(&spec.id);
            DagNode {
                id: spec.id.clone(),
                label: m.map(|m| m.label).unwrap_or(&spec.id).to_string(),
                role: m.map(|m| m.role).unwrap_or("agent").to_string(),
                depends_on: spec.depends_on.clone(),
                budget: spec.token_budget,
            }
        })
        .collect();
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    json_response(StatusCode::OK, &DagPayload { nodes })
}

async fn start_run(state: Arc<AppState>) -> Response<BoxedBody> {
    // Build a fresh supervisor for each run so repeated demos start clean.
    let speed: f64 = std::env::var("NEXUS_DEMO_SPEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1.0);
    let supervisor = Arc::new(demo::build_demo_supervisor(speed));

    {
        let mut slot = state.supervisor.lock().await;
        *slot = Some(Arc::clone(&supervisor));
    }

    // Forward the supervisor's event stream onto the SSE broadcast, emitting
    // a fresh snapshot after every event so the UI is always consistent.
    let tx = state.tx.clone();
    let mut events = supervisor.events();
    let sup_for_snap = Arc::clone(&supervisor);
    let _ = tx.send(StreamMsg::RunStarted);
    let _ = tx.send(snapshot_from(&supervisor.states()));

    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(ev) => {
                    let finished = matches!(ev, RuntimeEvent::RunFinished { .. });
                    if let Ok(val) = serde_json::to_value(&ev) {
                        let _ = tx.send(StreamMsg::Event { event: val });
                    }
                    let _ = tx.send(snapshot_from(&sup_for_snap.states()));
                    if finished {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Drive the run to completion in the background.
    let run_handle = Arc::clone(&supervisor);
    tokio::spawn(async move {
        let _ = run_handle.run().await;
    });

    json_response(StatusCode::OK, &serde_json::json!({"started": true}))
}

fn events_stream(state: Arc<AppState>) -> Response<BoxedBody> {
    // Per-client pump: read from the broadcast, push SSE frames into an mpsc
    // whose Receiver we adapt into the response body stream. Robust against
    // slow clients (lagged broadcast just drops to latest; snapshots heal).
    let mut rx = state.tx.subscribe();
    let (frame_tx, frame_rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);

    // Send a comment frame immediately so the browser opens the stream.
    let _ = frame_tx.try_send(Ok(Frame::data(Bytes::from_static(b": connected\n\n"))));

    tokio::spawn(async move {
        loop {
            let msg = match rx.recv().await {
                Ok(m) => m,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let Ok(json) = serde_json::to_string(&msg) else {
                continue;
            };
            let framed = format!("data: {json}\n\n");
            if frame_tx
                .send(Ok(Frame::data(Bytes::from(framed))))
                .await
                .is_err()
            {
                break; // client disconnected
            }
        }
    });

    let stream = futures_util::stream::unfold(frame_rx, |mut rx| async move {
        rx.recv().await.map(|f| (f, rx))
    });
    let body = BodyExt::boxed(StreamBody::new(stream));

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-store")
        .header("connection", "keep-alive")
        .header("x-accel-buffering", "no")
        .body(body)
        .unwrap()
}

async fn resolve_blocker(req: Request<Incoming>, state: Arc<AppState>) -> Response<BoxedBody> {
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": "bad body"}),
            )
        }
    };
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let task = parsed.get("task").and_then(|v| v.as_str()).unwrap_or("");
    let answer = parsed
        .get("answer")
        .and_then(|v| v.as_str())
        .unwrap_or("staging");

    let slot = state.supervisor.lock().await;
    let ok = match slot.as_ref() {
        Some(sup) => sup.resolve_blocker(task, answer),
        None => false,
    };
    json_response(StatusCode::OK, &serde_json::json!({"resolved": ok}))
}

// The single-page UI is compiled into the binary so the server is a single
// self-contained artifact (no asset directory to deploy).
const INDEX_HTML: &str = include_str!("../../ui/index.html");
