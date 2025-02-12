lint:
	uv run ruff format .

isort:
	uv run isort .

check-lint:
	uv run ruff format --check

type:
	uv run mypy .

test:
	uv run pytest tests

check: test type check-lint

format: isort lint