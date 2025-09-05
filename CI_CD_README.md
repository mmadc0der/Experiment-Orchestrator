# CI/CD Pipeline Documentation

This document describes the continuous integration and deployment pipeline for the Experiment Orchestrator project.

## Overview

The CI/CD pipeline is designed to ensure code quality, run comprehensive tests, and enable reliable deployments. It consists of several stages:

1. **Test Suite** - Runs unit, integration, and performance tests
2. **Security Scan** - Performs security vulnerability checks
3. **Build Package** - Creates distributable packages
4. **Docker Build** - Builds containerized application
5. **Deploy** - Deploys to staging/production environments

## Pipeline Triggers

The pipeline runs on:
- Push to `main` or `develop` branches
- Pull requests targeting `main` or `develop` branches

## Test Matrix

Tests run on multiple Python versions:
- Python 3.11
- Python 3.12  
- Python 3.13

## Services

### Redis Service
- Version: Redis 7.0
- Used for integration tests
- Health checks ensure service is ready before tests run

## Jobs

### 1. Test Suite (`test`)

**Purpose**: Run comprehensive test suite across multiple Python versions

**Steps**:
- Checkout code
- Set up Python environment
- Cache pip dependencies
- Install system dependencies
- Install Python dependencies
- Wait for Redis to be ready
- Run linting (black, isort, flake8)
- Run type checking (mypy)
- Run unit tests with coverage
- Run integration tests
- Run performance tests
- Upload coverage to Codecov

**Artifacts**: Coverage reports, test results

### 2. Security Scan (`security`)

**Purpose**: Identify security vulnerabilities and issues

**Steps**:
- Checkout code
- Set up Python environment
- Install dependencies
- Run safety check for known vulnerabilities
- Run bandit security scan
- Upload security reports

**Artifacts**: Security reports (safety-report.json, bandit-report.json)

### 3. Build Package (`build`)

**Purpose**: Create distributable Python packages

**Steps**:
- Checkout code
- Set up Python environment
- Install build dependencies
- Build package using `python -m build`
- Check package with `twine check`
- Upload build artifacts

**Artifacts**: Python packages (wheel and source distribution)

### 4. Docker Build (`docker`)

**Purpose**: Build and test Docker container

**Steps**:
- Checkout code
- Set up Docker Buildx
- Build Docker image
- Test Docker image functionality

### 5. Deploy to Staging (`deploy-staging`)

**Purpose**: Deploy to staging environment

**Triggers**: Push to `develop` branch
**Dependencies**: test, security, build jobs must pass

### 6. Deploy to Production (`deploy-production`)

**Purpose**: Deploy to production environment

**Triggers**: Push to `main` branch
**Dependencies**: test, security, build jobs must pass

## Local Development

### Prerequisites

- Python 3.11+ (3.13 recommended)
- Redis server
- Docker (optional)

### Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd experiment-orchestrator
   ```

2. **Set up development environment**
   ```bash
   make dev-setup
   ```

3. **Run tests**
   ```bash
   make test
   ```

4. **Run all checks**
   ```bash
   make check
   ```

### Available Make Commands

- `make install` - Install dependencies
- `make test` - Run all tests
- `make test-unit` - Run unit tests only
- `make test-integration` - Run integration tests only
- `make test-coverage` - Run tests with coverage
- `make lint` - Run linting checks
- `make format` - Format code
- `make type-check` - Run type checking
- `make security` - Run security checks
- `make clean` - Clean temporary files
- `make docker-build` - Build Docker image
- `make docker-test` - Run tests in Docker
- `make docker-run` - Run application in Docker

### Docker Development

1. **Start services with Docker Compose**
   ```bash
   docker-compose up --build
   ```

2. **Run tests in Docker**
   ```bash
   make docker-test
   ```

3. **Run production build**
   ```bash
   docker-compose --profile production up --build
   ```

## Pre-commit Hooks

Pre-commit hooks are configured to run automatically before each commit:

- **Code formatting**: black, isort
- **Linting**: flake8
- **Type checking**: mypy
- **Security**: bandit, safety
- **Tests**: pytest (unit tests)

### Setup Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

## Code Quality Standards

### Linting
- **Black**: Code formatting (line length: 127)
- **isort**: Import sorting
- **flake8**: Style and complexity checks

### Type Checking
- **mypy**: Static type checking
- Configuration: `--ignore-missing-imports --no-strict-optional`

### Security
- **bandit**: Security linting
- **safety**: Dependency vulnerability scanning

### Testing
- **pytest**: Test framework
- **coverage**: Code coverage reporting
- **pytest-benchmark**: Performance testing

## Coverage Requirements

- Minimum coverage: 80%
- Coverage reports generated for each test run
- HTML coverage reports available in `htmlcov/`

## Performance Testing

- Benchmark tests run on every PR
- Performance regression detection
- Configurable thresholds for performance metrics

## Security Scanning

- **Safety**: Scans for known vulnerabilities in dependencies
- **Bandit**: Scans Python code for security issues
- Reports generated and uploaded as artifacts

## Deployment

### Staging Environment
- Automatic deployment on `develop` branch
- Used for integration testing
- Mirrors production configuration

### Production Environment
- Automatic deployment on `main` branch
- Requires all checks to pass
- Zero-downtime deployment strategy

## Monitoring and Alerts

- Build status notifications
- Test failure alerts
- Security vulnerability alerts
- Performance regression alerts

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   - Ensure Redis service is running
   - Check Redis configuration
   - Verify network connectivity

2. **Test Failures**
   - Check Python version compatibility
   - Verify all dependencies are installed
   - Review test logs for specific errors

3. **Docker Build Failures**
   - Check Dockerfile syntax
   - Verify base image availability
   - Review build logs

4. **Coverage Issues**
   - Ensure all code paths are tested
   - Check coverage configuration
   - Review excluded files

### Getting Help

- Check GitHub Actions logs for detailed error information
- Review test output and coverage reports
- Consult project documentation
- Create an issue for persistent problems

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make check` to ensure all checks pass
5. Commit your changes
6. Push to your fork
7. Create a pull request

The CI/CD pipeline will automatically run all checks on your pull request.