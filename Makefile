.PHONY: install init collect bulk-dry-run bulk-export target-dry-run target-export-sample examples test lint format

install:
	python -m pip install -e ".[dev]"

init:
	ghiw init-db

collect:
	ghiw collect --config config/targets.example.yml

bulk-dry-run:
	ghiw collect-org-monthly --start-month 2015-01 --end-month 2025-12 --dry-run

bulk-export:
	ghiw export-org-json --start-month 2015-01 --end-month 2025-12

target-dry-run:
	ghiw collect-target-org-monthly --start-month 2015-01 --end-month 2025-12 --dry-run

target-export-sample:
	ghiw export-target-org-json --shards 0-3 --workers 4 --no-gzip

examples:
	ghiw query --file sql/example_queries.sql

test:
	pytest --cov=github_innovation --cov-report=term-missing

lint:
	ruff check .

format:
	ruff format .
