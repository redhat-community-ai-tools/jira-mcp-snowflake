# note: on MacOS you need to install make via brew ( min version 4.0+)
# brew install make
.ONESHELL:

SHELL			= /bin/bash
CONTAINER_NAME	= "localhost/jira-mcp-snowflake:latest"

uv_sync_dev:
	uv sync --dev

test: lint pytest

build:
	podman build -t $(CONTAINER_NAME) .

lint: uv_sync_dev
	uv run flake8 src/ --max-line-length=120 --ignore=E501,W503

pytest: uv_sync_dev
	uv run pytest tests/ --cov=src --cov-report=xml --cov-report=term -v --tb=short

