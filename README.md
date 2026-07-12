# AML Sentinel

A real-time anti-money-laundering (AML) transaction-monitoring stack: it replays
synthetic bank transactions as a live stream, detects laundering typologies over a
rolling transaction graph, manages the resulting alerts through an analyst case
API + UI, and scores itself against ground-truth labels.

Synthetic data comes from
[gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph) (Apache-2.0,
consumed as an external tool, pinned by commit — never vendored).

**What it demonstrates:** streaming/stateful backend engineering (event-time
windows, watermarks, backpressure-free O(1) eviction), graph algorithms in
production code (bounded cycle search on a rolling multigraph), and working
knowledge of how AML transaction monitoring operates (typology rules, alert
triage, case evidence, detection metrics — and where the toy parts diverge from
a real bank's stack).

## Architecture

```
gen-fraud-graph CSVs ──▶ Replay Engine ──▶ Detection Service ──▶ Case API + UI
   (data/, generated       (event-time         (rolling 72h          (FastAPI + Postgres/
    once, offline)          synthesis, sorted    graph + typology      SQLite; websocket
                            stream at a          rules; alerts         feed, evidence
                            speed-up factor)     with evidence)        subgraphs, dispositions)
                                  │                    ▲
                                  └── Redpanda topic ──┘        ground truth ──▶ Scoring
                                      `transactions`            (never seen      (precision/
                                      (or --direct,              by the          recall/latency
                                      in-process)                detector)       report)
```

Python 3.12 + asyncio throughout; one repo, one `docker compose up` demo.

| Concern | Choice |
|:---|:---|
| Transport | Redpanda (Kafka API) via aiokafka; `--direct` in-process mode for dev |
| Rolling graph | Hand-rolled in-memory adjacency with event-time window eviction |
| Case store | SQLAlchemy 2.0 — SQLite for dev, Postgres in compose |
| API / UI | FastAPI + websocket; no-build-step vanilla JS + SVG single page |
| Quality | ruff, strict mypy, pytest + hypothesis, GitHub Actions CI |

## Quick start

```sh
uv sync --dev
make data                                        # generate ./data (pinned gen-fraud-graph)
uv run aml-sentinel score                        # replay -> detect -> metrics table
make check                                       # ruff + mypy + pytest
```

Full demo with streaming infra (requires Docker):

```sh
make data          # once, offline
docker compose up  # Redpanda + Postgres + detector + API/UI + one-shot replay
```

Open http://localhost:8000: the replay job produces the stream to the Redpanda
`transactions` topic at one simulated day every two seconds, the detector
consumes it and POSTs alerts to the case API, and the UI shows them arriving
live over a websocket. Click a case to see its evidence subgraph, then dispose
it as a true/false positive; the stat tiles track your triage.

Local demo without Docker:

```sh
uv run aml-sentinel serve &                        # API + UI on :8000, SQLite
uv run aml-sentinel detect --api-url http://127.0.0.1:8000 --speed 86400
```

## How it works

**Replay** ([src/aml_sentinel/replay](src/aml_sentinel/replay)). The upstream
generator writes *constant* placeholder timestamps, so the replay engine
synthesizes event times over a seeded 30-day horizon: normal traffic lands
uniformly; each fraud ring gets a random start with 1–30-minute gaps between
hops, so a full cycle closes well inside the detection window. The merged,
time-sorted stream is emitted at a configurable speed-up factor. Fraud labels
never enter the stream — ground truth (ring ids, hop tx ids, synthesized hop
times) is written to a separate `ground_truth.json` consumed only by scoring.

**Detection** ([src/aml_sentinel/detect](src/aml_sentinel/detect)). A rolling
directed multigraph holds the last 72 simulated hours. Because the stream is
time-ordered, the global eviction deque and every per-node adjacency deque
share insertion order — evicting an expired edge is O(1) pops, no scans. The
graph also maintains degree sum/sum-of-squares incrementally, so population
mean/std for the statistical rule is O(1) per transaction. Three pluggable
`Typology` rules run per transaction:

1. **Cycle detection** — on each new edge `src -> dst`, a bounded DFS
   (depth ≤ 7, expansion budget 50K) searches for a path `dst ->* src`. Fires
   the instant a cycle closes; evidence is the cycle itself; score comes from
   amount uniformity (round-tripping identical amounts is the classic layering
   signature).
2. **Structuring** — ≥ 3 transactions in `[9 000, 10 000)` touching one
   account within 24h, with a per-account cooldown.
3. **High-value degree outlier** — a high-value transaction hitting an account
   whose rolling degree z-score is ≥ 3 vs the active population. Statistical,
   not pattern-matched to the injected fraud.

**Cases + scoring** ([src/aml_sentinel/api](src/aml_sentinel/api),
[src/aml_sentinel/scoring](src/aml_sentinel/scoring)). Alerts become cases:
`GET /alerts` is the queue, `GET /cases/{id}` returns the evidence subgraph,
`POST /cases/{id}/disposition` records the analyst's true/false-positive call,
and `WS /ws/alerts` feeds the live UI. Scoring joins alerts against ground
truth: an alert is a true positive if its evidence contains an injected fraud
transaction; a ring is detected if any alert includes one of its hops;
detection latency is simulated time from a ring's last hop to the earliest
matching alert.

## Results

Seed 42, single process, M-series laptop. Scale 0.001 = 10K accounts / 90K tx,
scale 0.01 = 100K accounts / 900K tx; 10 injected rings each.

| Scale | Transactions | Throughput | p50 / p99 per-tx latency |
|:---|---:|---:|---:|
| 0.001 | 90 055 | ~195K tx/s | 4 µs / 17 µs |
| 0.01 | 900 059 | ~138K tx/s | 5 µs / 23 µs |

`aml-sentinel score`, scale 0.001:

| Typology | Alerts | TP | FP | Precision | Rings detected | Recall | Latency median |
|:---|---:|---:|---:|---:|---:|---:|---:|
| cycle | 34 | 10 | 24 | 0.29 | 10/10 | 1.00 | 0s |
| high_value_degree_outlier | 8 | 8 | 0 | 1.00 | 6/10 | 0.60 | −1879s |
| overall | 42 | 18 | 24 | 0.43 | 10/10 | 1.00 | −279s |

Scale 0.01:

| Typology | Alerts | TP | FP | Precision | Rings detected | Recall | Latency median |
|:---|---:|---:|---:|---:|---:|---:|---:|
| cycle | 42 | 10 | 32 | 0.24 | 10/10 | 1.00 | 0s |
| high_value_degree_outlier | 4 | 4 | 0 | 1.00 | 4/10 | 0.40 | −2349s |
| overall | 46 | 14 | 32 | 0.30 | 10/10 | 1.00 | 0s |

Three things worth reading out of these numbers rather than past them:

- **Cycle detection catches every ring at zero latency** — it fires on the
  closing hop. Its false positives are *accidental* cycles that random
  background traffic forms inside a 72h window, and they grow with traffic
  density (24 → 32 FP). That precision/recall trade is the real shape of
  rules-based AML monitoring.
- **The degree-outlier rule is the mirror image**: perfect precision, partial
  recall, *negative* latency — it flags mule hubs before the ring even
  completes. Recall falls at scale because ring accounts stand out less
  against a larger population.
- **Structuring never fires on this data** — each ring account touches exactly
  two sub-threshold transactions, below the rule's threshold of three. The
  rule is kept honest for realistic data instead of being tuned to the
  generator's artifacts.

## Limitations (read this before being impressed)

- **Synthetic data is easy.** Injected fraud uses a fixed $9 999 amount and
  distinctive descriptions, so the classes are nearly separable without any
  graph reasoning — see the upstream issue
  [gen-fraud-graph#27](https://github.com/SantanderAI/gen-fraud-graph/issues/27)
  (decorrelate amounts & descriptions). The detector deliberately ignores
  descriptions and never reads labels, but perfect cycle recall here says
  little about recall on real traffic.
- **Event times are synthesized by this project's own replay engine** — the
  generator emits constant placeholder timestamps (a quirk found while
  building this; worth an upstream issue alongside
  [#26](https://github.com/SantanderAI/gen-fraud-graph/issues/26)). Detection
  latency numbers are therefore relative to a clock this repo invented; the
  honest reading is "fires on the closing hop," not the absolute seconds.
- **Single-node, in-memory state.** The rolling graph lives in one process;
  restarts lose the window. Real deployments checkpoint state or rebuild from
  the log.
- **Latency is measured in-process** around `DetectionService.process()`; it
  excludes Kafka hop time, serialization, and the case API round-trip.
- Alert scores are heuristic and uncalibrated; dispositions don't feed back
  into anything.

## How a real bank's stack differs

Real transaction monitoring runs mostly in **batch** (nightly scenario runs
over core-banking extracts) with real-time screening reserved for
sanctions/payments; a stream-first design like this one is the aspiration, not
the norm. Rules engines are vendor platforms (Actimize, Oracle FCCM, SAS)
with hundreds of tuned scenarios, thresholds calibrated per segment, and
model-risk governance over every change. Alerts feed a multi-tier human
review (L1 triage → L2 investigation → SAR filing decision) with regulatory
deadlines, full audit trails, and case management that aggregates by customer
across accounts and channels — not one case per alert. Identity is harder than
detection: entity resolution across products, KYC risk ratings, and beneficial
ownership matter more than any single typology rule. And there is no ground
truth: banks measure SAR conversion and alert-to-case ratios, not
precision/recall against labels — which is exactly why synthetic benchmarks
like this one exist, and why their numbers must be read with the limitations
above.

## Extensions (documented, not built)

- **Neo4j-backed graph store** — swap `WindowedGraph` behind its current
  interface (`add`, `out_edges`, `degree_stats`) for a property graph with TTL
  indexes; buys durability and multi-hop query tooling at the cost of the
  microsecond latencies above.
- **Harder typologies upstream** — realistic amounts/timing in gen-fraud-graph
  (issues [#26](https://github.com/SantanderAI/gen-fraud-graph/issues/26) and
  [#27](https://github.com/SantanderAI/gen-fraud-graph/issues/27)) would make
  the precision numbers here meaningful; smurfing networks and trade-based
  patterns would exercise rules beyond cycles.
- **Horizontal scaling** — the Kafka producer already keys by source account;
  partition the topic and run one detector per partition, with cross-partition
  cycles handled by a second-stage join on boundary accounts.

## Repository layout

```
src/aml_sentinel/
  replay/     CSV loading, event-time synthesis, paced emission, Kafka producer
  detect/     rolling graph, typology rules, detection service, Kafka consumer
  api/        FastAPI case service, SQLAlchemy store, static UI
  scoring/    precision/recall/latency vs ground truth
tests/        64 tests incl. property-based (no cycle alert on acyclic streams)
docker/       Dockerfile; docker-compose.yml at repo root
```

## Attribution

Synthetic data generated by
[gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph)
(© Santander Group, Apache-2.0), installed at a pinned commit by `make data`.
This project is MIT-licensed and consumes it as an external tool.
