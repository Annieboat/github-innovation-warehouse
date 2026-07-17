.PHONY: install init collect examples test lint format

install:
	python -m pip install -e ".[dev]"

init:
	ghiw init-db

collect:
	ghiw collect --config config/targets.example.yml

examples:
	ghiw query --file sql/example_queries.sql

test:
	pytest --cov=github_innovation --cov-report=term-missing

lint:
	ruff check .

format:
	ruff format .

