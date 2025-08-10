install:
	uv sync

lint:
	uv run ruff check .

lint-fix:
	uv run ruff format .

test-app:
	PYTHONPATH=src uv run pytest -v --tb=short tests/app

test-cdk:
	PYTHONPATH=./cdk uv run pytest -v --tb=short tests/cdk

tests: test-app test-cdk
