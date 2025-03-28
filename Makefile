SHELL := /bin/bash
.ONESHELL:

.PHONY: venv dependencies test lint build upload help run

help:
	@echo "Available commands:"
	@echo "  make venv         - Create a virtual environment with uv"
	@echo "  make dependencies - Install project dependencies with uv"
	@echo "  make test         - Run tests with pytest"
	@echo "  make lint         - Run ruff linting and formatting"
	@echo "  make build        - Build the project package"
	@echo "  make upload       - Upload the release"
	@echo "  make run          - Run the application"


venv:
	@test -d .venv || uv venv

dependencies:
	uv pip install -r <(uv pip compile pyproject.toml --all-extras)

test:
	uv run pytest ferry/tests

lint:
	uv run ruff check --fix .
	uv run ruff format .

build:
	uv build

upload:
	uv publish --index testpypi

run:
	PYTHONPATH=$(shell pwd) python3 ferry/main.py serve