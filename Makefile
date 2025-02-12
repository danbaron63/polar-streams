lint:
	uv run ruff format .

check-lint:
	uv run ruff format --check

type:
	uv run mypy .

test:
	uv run pytest tests

checks: test type check-lint