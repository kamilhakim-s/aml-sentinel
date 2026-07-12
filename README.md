# AML Sentinel

Real-time AML transaction-monitoring stack: replays synthetic bank transactions
(from [gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph), Apache-2.0),
detects money-laundering typologies over a rolling graph, and manages alerts through
a case API — scored against ground-truth labels. See [PLAN.md](PLAN.md) for the roadmap.

## Status

- [x] Phase 0 — scaffold, CI, `make data`
- [x] Phase 1 — replay engine (event-time synthesis, seeded, `--direct` mode)
- [ ] Phase 2 — detection service (rolling graph + typology rules)
- [ ] Phase 3 — case API + scoring
- [ ] Phase 4 — UI + docker compose demo
- [ ] Phase 5 — README + benchmarks

## Quick start

```sh
uv sync --dev
make data          # generate ./data with gen-fraud-graph (pinned commit)
uv run aml-sentinel replay --direct              # full speed
uv run aml-sentinel replay --direct --speed 86400  # 1 simulated day per second
make check         # ruff + mypy + pytest
```

The upstream generator writes constant placeholder timestamps, so the replay
engine synthesizes event times over a configurable horizon (default 30 days,
seeded RNG): normal traffic lands uniformly, each fraud ring's hops stay
clustered within minutes–hours so cycles close inside a detection window.
Ground truth (`fraud_cases.csv` + synthesized hop times) never reaches the
detector; it is written to `ground_truth.json` for the scoring module.
