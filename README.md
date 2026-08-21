# nexus

nexus is a runtime for orchestrating multi-agent systems — scheduling, supervising, and recovering fleets of LLM agents that run in parallel.

## Overview

Agents fan out across a dependency DAG, each one starting the moment its inputs are ready and not a step before. nexus watches them prove progress, retries the ones that stall or hit transient failures, escalates the decisions that genuinely need a person, and caps what any run can spend. It supervises a live fleet of 120+ services in a commercial estate, running unattended for days at a time.

## Demo

**[Try it live → nexus.chakrakali.com](https://nexus.chakrakali.com)** — press **Run workflow** and watch a nine-agent "ship a feature" run execute, streaming every decision to the browser in real time:

- **Parallel dispatch** — independent agents fan out the moment their dependencies clear
- **Auto-recovery** — one agent hits a transient failure and the supervisor retries it, visibly
- **Human-in-the-loop** — a deploy gate blocks and asks *"prod or staging?"*; your click resumes the run

## Features

- **DAG scheduling** — work is scheduled off a dependency graph, so a task runs the moment it can and nothing gets serialized by accident.
- **Evidence-based liveness** — a worker counts as alive because it's proving progress, not because a timeout hasn't fired yet.
- **Auto-recovery** — stalls and transient failures are detected and retried without a human in the loop.
- **Human-in-the-loop gates** — a deploy target or a risky irreversible step escalates and waits, while everything independent keeps moving.
- **Cost governance** — every run carries a token/compute budget it can't quietly blow past.

## How it works

A run is a DAG of agent tasks. The scheduler dispatches any task whose dependencies have cleared, so independent work executes in parallel by default. Each running agent reports progress rather than a heartbeat, and the supervisor asserts liveness from that evidence — when progress stops, it recovers the task instead of waiting out a timer. Gates that need human judgment pause their branch and resume on a decision, leaving the rest of the graph moving. A budget rides along with every run and stops it before spend runs away.
