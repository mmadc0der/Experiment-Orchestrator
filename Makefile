# Experiment Orchestrator Makefile

.PHONY: help install test test-unit test-integration test-coverage lint format type-check security clean docker-build docker-test docker-run docs

# Default target
help:
	@echo "Available commands:"
	@echo "  install          Install dependencies"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-coverage    Run tests with coverage report"
	@echo "  lint             Run linting checks"
	@echo "  format           Format code with black and isort"
	@echo "  type-check       Run type checking with mypy"
	@echo "  security         Run security checks"
	@echo "  clean            Clean up temporary files"
	@echo "  docker-build     Build Docker image"
	@echo "  docker-test      Run tests in Docker"
	@echo "  docker-run       Run application in Docker"
	@echo "  docs             Generate documentation"

# Installation
install:
	python -m venv venv
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements.txt
	. venv/bin/activate && pip install -r requirements-test.txt

# Testing
test:
	. venv/bin/activate && python -m pytest tests/ -v

test-unit:
	. venv/bin/activate && python -m pytest tests/test_*.py -v

test-integration:
	. venv/bin/activate && python -m pytest tests/test_integration.py -v

test-coverage:
	. venv/bin/activate && python -m pytest tests/ --cov=. --cov-report=html --cov-report=term

# Code quality
lint:
	. venv/bin/activate && flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	. venv/bin/activate && flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

format:
	. venv/bin/activate && black .
	. venv/bin/activate && isort .

type-check:
	. venv/bin/activate && mypy . --ignore-missing-imports --no-strict-optional

security:
	. venv/bin/activate && safety check
	. venv/bin/activate && bandit -r .

# Cleanup
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

# Docker commands
docker-build:
	docker build -t orchestrator:latest .

docker-test:
	docker-compose up --build --abort-on-container-exit

docker-run:
	docker-compose -f docker-compose.yml --profile production up --build

# Documentation
docs:
	. venv/bin/activate && pip install sphinx sphinx-rtd-theme
	. venv/bin/activate && sphinx-build -b html docs/ docs/_build/html

# Development setup
dev-setup: install
	@echo "Setting up development environment..."
	@echo "Creating necessary directories..."
	mkdir -p logs artifacts runtime modules
	@echo "Development environment ready!"
	@echo "Run 'make test' to verify everything works"

# CI/CD helpers
ci-test: test lint type-check security
	@echo "All CI checks passed!"

# Pre-commit hook
pre-commit: format lint type-check test
	@echo "Pre-commit checks passed!"

# Full check (run before pushing)
check: clean format lint type-check security test
	@echo "All checks passed! Ready to push."