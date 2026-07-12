# AML Sentinel

Real-time AML transaction-monitoring stack: replays synthetic bank transactions
(from [gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph), Apache-2.0),
detects money-laundering typologies over a rolling graph, and manages alerts through
a case API — scored against ground-truth labels. See [PLAN.md](PLAN.md) for the roadmap.

## Status

- [x] Phase 0 — scaffold, CI, `make data`
- [x] Phase 1 — replay engine (event-time synthesis, seeded, `--direct` mode)
- [x] Phase 2 — detection service (rolling graph + typology rules)
- [ ] Phase 3 — case API + scoring
- [ ] Phase 4 — UI + docker compose demo
- [ ] Phase 5 — README + benchmarks

## Quick start

```sh
uv sync --dev
make data          # generate ./data with gen-fraud-graph (pinned commit)
uv run aml-sentinel replay --direct              # full speed
uv run aml-sentinel replay --direct --speed 86400  # 1 simulated day per second
uv run aml-sentinel detect                       # replay straight into the detector
make check         # ruff + mypy + pytest
```

## Detection

The detector consumes the stream into a rolling graph (72h simulated window,
O(1) event-time eviction) and evaluates three pluggable typologies per
transaction: **cycle detection** (bounded DFS on each new edge, depth <= 7),
**structuring** (>= 3 tx in [9000, 10000) touching one account within 24h),
and a **degree-outlier** rule (high-value tx into an account whose rolling
degree z-score is anomalous — statistical, not pattern-matched to the
injected fraud).

On the scale-0.001 dataset (90K tx / 10K accounts / 10 rings): all 10 injected
rings alert, ~195K tx/s end-to-end, per-tx latency p50 4us / p99 17us
(M-series laptop). Extra cycle alerts from accidental cycles in background
traffic are real false positives — Phase 3 scores them.

The upstream generator writes constant placeholder timestamps, so the replay
engine synthesizes event times over a configurable horizon (default 30 days,
seeded RNG): normal traffic lands uniformly, each fraud ring's hops stay
clustered within minutes–hours so cycles close inside a detection window.
Ground truth (`fraud_cases.csv` + synthesized hop times) never reaches the
detector; it is written to `ground_truth.json` for the scoring module.
