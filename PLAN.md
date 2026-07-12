# AML Sentinel — Real-Time Transaction Monitoring Service

A portfolio project: a streaming AML transaction-monitoring stack that ingests synthetic
bank transactions (produced by [gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph)),
detects money-laundering typologies in near-real-time, and manages alerts through a case API —
scored against ground-truth labels.

**What it demonstrates:** streaming/stateful backend engineering, graph algorithms in
production code, and working knowledge of how AML transaction monitoring actually operates
(typology rules, alert triage, case evidence, detection metrics).

---

## Architecture

```
gen-fraud-graph CSVs ──> Replay Engine ──> Detection Service ──> Case API + UI
   (data/, generated       (simulates a       (rolling graph +      (alert queue,
    once, offline)          live stream)       typology rules)       evidence subgraph,
                                                                     scoring vs ground truth)
```

Single `docker compose up` demo. All components in one repo, Python 3.12.

### Stack decisions

| Concern | Choice | Rationale |
|:---|:---|:---|
| Language | Python 3.12 + asyncio | Matches data-source ecosystem; fast to demo |
| Transport | Redpanda (Kafka API) via docker compose | Real streaming infra signal; `--direct` mode bypasses it for local dev |
| Rolling graph | In-memory (NetworkX or hand-rolled adjacency) with time-window eviction | Cycle detection on a windowed graph; swap-to-Neo4j documented as extension |
| Case store | SQLite via SQLAlchemy (Postgres in compose) | Zero-friction dev, real DB in demo |
| API | FastAPI | Alert/case endpoints + websocket for live UI |
| UI | Single-page htmx or small React app | Alert queue + evidence subgraph rendering (vis.js/cytoscape) |

---

## Phase 0 — Scaffold (half a day)

- `uv`-managed project: `src/aml_sentinel/{replay,detect,api,scoring}`, `tests/`, `docker/`.
- Ruff + pytest + mypy, GitHub Actions CI (mirror the discipline of the upstream repo).
- `make data`: clones/installs gen-fraud-graph, runs
  `gen-fraud-graph --scale 0.001 --provider fake --output ./data` (10K accounts / 90K tx / 10 rings).
  Data dir is gitignored; a tiny committed fixture (~200 rows) drives tests.

## Phase 1 — Replay Engine

Reads the generated CSVs and emits transactions as a time-ordered live stream.

- **Known upstream quirk to handle:** gen-fraud-graph writes *constant* timestamps
  (`2024-01-01T10:00:00` for normal tx, `12:00:00` for fraud). The replay engine must
  synthesize event times: assign each transaction a timestamp drawn over a configurable
  simulated horizon (e.g. 30 days), keeping each fraud ring's hops clustered within a
  short window (minutes–hours) so cycles are detectable inside a rolling window. Seeded
  RNG for reproducible runs.
- Merge normal + fraud streams, sort by synthesized time, emit at a configurable
  speed-up factor (e.g. 1 simulated day/second) to Redpanda topic `transactions`
  (or directly into the detector in `--direct` mode).
- Ground-truth pass-through: fraud tx ids and `fraud_cases.csv` are **never** given to the
  detector — they go only to the scoring module.

**Exit criteria:** `aml-sentinel replay --speed 3600 --direct` streams the full dataset;
unit tests for time synthesis and ordering.

## Phase 2 — Detection Service (the core)

Consumes the stream, maintains a rolling transaction graph, evaluates typology rules,
emits alerts.

- **Windowed graph state:** edges expire after a configurable window (default 72h
  simulated). Eviction via monotonic event-time watermark.
- **Rules (each a pluggable `Typology` class with `evaluate(graph, tx) -> list[Alert]`):**
  1. **Cycle detection** — on each new edge, bounded DFS from `dst` back to `src`
     (max depth 7, matching upstream ring depths). Fires when a cycle closes; evidence =
     the cycle's accounts + edges.
  2. **Structuring / velocity** — N transactions in [9000, 10000) involving one account
     within 24h (catches the fixed 9999 amounts, stays honest for realistic data later).
  3. **High-value + suspicious counterparty degree** — fan-in/fan-out z-score on rolling
     account degree (simple statistical rule; shows you're not only pattern-matching the
     injected fraud).
- Alert model: `alert_id, typology, fired_at, accounts, evidence_edges, score`.
- Performance target: process the scale-0.001 stream (90K tx) in seconds; document
  per-tx latency (p50/p99) — this is the software-engineering headline.

**Exit criteria:** all 10 injected rings produce cycle alerts on the fixture dataset;
property-based test that no cycle alert fires on a fraud-free stream.

## Phase 3 — Case API + Scoring

- FastAPI service: `POST` internal alert ingestion; `GET /alerts`, `GET /cases/{id}`
  (alert + evidence subgraph JSON), `POST /cases/{id}/disposition` (true-positive /
  false-positive — the analyst workflow).
- **Scoring module:** after a replay run, join alerts against `fraud_cases.csv`:
  per-typology precision / recall, and **detection latency** (simulated time between a
  ring's last hop and its alert). Output a markdown/JSON report — these numbers go in
  the README.

**Exit criteria:** `aml-sentinel score` prints a metrics table; API covered by tests.

## Phase 4 — UI + Compose Demo

- Minimal SPA: live alert feed (websocket), case detail with rendered evidence subgraph,
  disposition buttons, metrics panel.
- `docker compose up` = Redpanda + detector + API + UI + one-shot replay job.
- Record a short demo GIF for the README.

## Phase 5 — README + Benchmarks (the portfolio layer)

- README: architecture diagram, the metrics table, latency numbers at scale 0.001 and
  0.01, honest "limitations" section (synthetic data is easy; link the upstream
  typology-realism issue), and a "how a real bank's stack differs" paragraph — that's
  the AML-domain credibility signal.
- Stretch (documented, not required): Neo4j-backed graph store; harder typologies via
  upstream contribution (project 2); horizontal scaling of detectors by account-id
  partition.

---

## Milestone order & sizing

| Milestone | Scope | Rough effort |
|:---|:---|:---|
| M1 | Phase 0 + 1 (scaffold, data, replay) | 1–2 days |
| M2 | Phase 2 (detection engine) | 2–4 days |
| M3 | Phase 3 (API + scoring) | 1–2 days |
| M4 | Phase 4 + 5 (UI, compose, README) | 2–3 days |

## Relationship to gen-fraud-graph

Consumer only. Never vendored or forked; installed from source in `make data`
(pin a commit SHA). Apache-2.0 attribution note in README. Quirks found while building
(constant timestamps, fixed fraud amount) become upstream issues/PRs — feeds project 2.
