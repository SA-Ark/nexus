# Nexus

**Multi-agent orchestration that survives production. Watch it work — live.**

Nexus is the orchestration pattern that supervises a production fleet of 120+ services and the AI agent teams that operate them. This repo is an architecture showcase: the runtime itself is proprietary — it runs a commercial estate — but the demo below streams the **real supervisor's** decisions, not a mock-up.

## ▶ Live demo

**[nexus.chakrakali.com](https://nexus.chakrakali.com)** — press **Run workflow** and watch a nine-agent "ship a feature" run execute:

- **Parallel dispatch** — independent agents fan out the moment their dependencies clear
- **Auto-recovery** — one agent hits a transient failure and the supervisor retries it, visibly
- **Human-in-the-loop** — a deploy gate blocks and asks *"prod or staging?"*; your click resumes the run
- Every decision streams to the browser in real time

## What it does

Work is scheduled off a dependency DAG, so a task runs the moment it can and not a step before — nothing gets serialized by accident. A worker counts as "alive" because it's proving progress, not because a timeout hasn't fired yet; when something stalls or hits a transient failure, the supervisor detects it and retries without a human in the loop. The decisions that genuinely need a person — a deploy target, a risky irreversible step — escalate and wait, while everything independent keeps moving. And every run carries a token/compute budget it can't quietly blow past.

## Production record

This supervises **120+ services** in a live commercial estate. Liveness is evidence-based — asserted by real checks, with automatic stall recovery, no timeout guesswork. It runs unattended for days and escalates only what actually needs a human.

## What this buys you

"AI agents will run your business" is the pitch everyone's heard. Here's the part that has to be true first: something has to watch every agent, catch the one that hangs at 2 a.m., retry the transient failure, cap the runaway spend, and know which calls have to wait for a person. That machinery is what separates a demo from an operator you can leave alone — and the demo above is it running on real infrastructure, not slides. Once it exists, one person can run what used to take a team. The estate this supervises is the evidence.

## Why the source is private

The runtime operates a commercial estate end to end — publishing it would publish the estate's control plane. The demo exists so you can judge behavior instead of taking my word for it. Happy to walk through the architecture in depth in an interview or scoping call.

---

**Portfolio & contact:** [ark.chakrakali.com](https://ark.chakrakali.com) ·
**Open-source companions:** [scour](https://github.com/SA-Ark/scour) ·
[aegis](https://github.com/SA-Ark/aegis)
