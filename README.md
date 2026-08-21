# Nexus

**Multi-agent orchestration that survives production. Watch it work — live.**

Nexus is the orchestration pattern that supervises a production fleet of 120+ services and the
AI agent teams that operate them. This repo is an architecture showcase: the runtime itself is
proprietary (it runs a commercial estate), but the demo below streams the **real supervisor's
decisions** — not a mock-up.

## ▶ Live demo

**[nexus.chakrakali.com](https://nexus.chakrakali.com)** — press **Run workflow** and watch a
nine-agent "ship a feature" workflow execute:

- **Parallel dispatch** — independent agents fan out the moment their dependencies clear
- **Auto-recovery** — one agent hits a transient failure; the supervisor retries it, visibly
- **Human-in-the-loop** — a deploy gate blocks and asks *"prod or staging?"*; your click resumes the run
- Every decision streams to the browser in real time

## What it does

| Capability | In plain terms |
|---|---|
| Dependency-DAG scheduling | Work runs the moment it *can*, never before, never serialized by accident |
| Evidence-based liveness | A worker is "alive" because it proves progress — not because a timeout hasn't fired |
| Automatic recovery | Stalls and transient failures are detected and retried automatically, no human in the loop |
| Human-in-the-loop blockers | Agents escalate real decisions to a human and wait; everything else continues |
| Cost governance | Token/compute budgets are enforced per run — agents cannot overspend silently |

## Production record

- Supervises **120+ services** in a live commercial estate
- **Evidence-based liveness** — health asserted by real checks, with automatic stall recovery (no timeout guesswork)
- Runs unattended for days; escalates only what genuinely needs a human

## What this means if you're building with AI

You've heard "AI agents will run your business." Here is what that actually requires before it's
true: something watching every agent, catching the one that hangs at 2 a.m., retrying the
transient failure, capping the runaway spend, and knowing which decisions must wait for a human.
**That machinery is the difference between a demo and an employee** — and the demo above shows
it working on real infrastructure, not slides. When it exists, one person can operate what used
to take a team. That's not a claim; the estate this supervises is the proof.

## Why the source is private

The runtime operates a commercial estate end-to-end — publishing it would publish the estate's
control plane. The demo exists so you can judge behavior instead of taking claims on faith.
I'll walk through the architecture in depth in an interview or scoping call.

---

**Portfolio & contact:** [ark.chakrakali.com](https://ark.chakrakali.com) ·
**Open-source companions:** [scour](https://github.com/SA-Ark/scour) ·
[aegis](https://github.com/SA-Ark/aegis)
