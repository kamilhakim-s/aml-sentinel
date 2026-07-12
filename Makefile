# Pinned gen-fraud-graph commit; bump deliberately.
GEN_FRAUD_GRAPH_REF := 598722f8c7ee954b374b020b5bf2c5616ee7bc31
GEN_FRAUD_GRAPH_URL := https://github.com/SantanderAI/gen-fraud-graph

.PHONY: data lint typecheck test check

## Generate the working dataset (10K accounts / ~90K tx / 10 rings) into ./data
data:
	uvx --from git+$(GEN_FRAUD_GRAPH_URL)@$(GEN_FRAUD_GRAPH_REF) \
		gen-fraud-graph --scale 0.001 --provider fake --output ./data

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

typecheck:
	uv run mypy

test:
	uv run pytest

check: lint typecheck test
