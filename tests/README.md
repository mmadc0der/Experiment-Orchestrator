# Experiment Orchestrator Test Suite

This directory contains comprehensive tests for the Experiment Orchestrator project.

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                 # Pytest configuration and shared fixtures
├── test_runner.py              # Test runner and utilities
├── test_utils.py               # Test utilities and helper functions
├── test_config_validator.py    # Configuration validator tests
├── test_manifest_parser.py     # Manifest parser tests
├── test_models.py              # Data models tests
├── test_redis_broker.py        # Redis broker tests
├── test_orchestrator_core.py   # Orchestrator core tests
├── test_integration.py         # Integration tests
└── README.md                   # This file
```

## Test Categories

### Unit Tests
- **test_config_validator.py**: Tests for configuration validation and schema validation
- **test_manifest_parser.py**: Tests for YAML manifest parsing and validation
- **test_models.py**: Tests for Pydantic data models (definitions and instances)
- **test_redis_broker.py**: Tests for Redis broker functionality
- **test_orchestrator_core.py**: Tests for orchestrator core functionality

### Integration Tests
- **test_integration.py**: End-to-end integration tests

## Running Tests

### Prerequisites

Install test dependencies:
```bash
pip install -r requirements-test.txt
```

### Basic Test Execution

Run all tests:
```bash
pytest
```

Run with verbose output:
```bash
pytest -v
```

Run specific test file:
```bash
pytest tests/test_config_validator.py
```

Run specific test function:
```bash
pytest tests/test_config_validator.py::TestLoggingConfig::test_valid_logging_config
```

### Test Categories

Run unit tests only:
```bash
pytest -m unit
```

Run integration tests only:
```bash
pytest -m integration
```

Run fast tests (exclude slow tests):
```bash
pytest -m "not slow"
```

### Coverage Reporting

Run tests with coverage:
```bash
pytest --cov=. --cov-report=html --cov-report=term
```

View HTML coverage report:
```bash
open htmlcov/index.html
```

### Parallel Execution

Run tests in parallel (requires pytest-xdist):
```bash
pytest -n 4
```

### Using the Test Runner

Use the custom test runner for more control:
```bash
python tests/test_runner.py --type all --verbose
python tests/test_runner.py --type unit
python tests/test_runner.py --type integration
python tests/test_runner.py --coverage
python tests/test_runner.py --parallel 4
```

## Test Fixtures

### Workspace Fixtures
- `temp_workspace`: Creates a temporary workspace directory
- `test_config`: Creates a test configuration
- `config_file`: Creates a test configuration file

### Mock Fixtures
- `mock_redis_broker`: Mock Redis broker for testing
- `mock_scheduler`: Mock scheduler for testing

### Sample Data Fixtures
- `sample_task_manifest`: Sample task manifest YAML
- `sample_experiment_manifest`: Sample experiment manifest YAML
- `sample_environment_manifest`: Sample environment manifest YAML
- `sample_data_manifest`: Sample data manifest YAML
- `sample_model_manifest`: Sample model manifest YAML
- `invalid_manifest`: Invalid manifest for error testing

### Instance Fixtures
- `sample_experiment_instance`: Sample experiment instance
- `sample_task_instance`: Sample task instance
- `sample_job_instance`: Sample job instance

## Test Utilities

### TestDataGenerator
Generates test manifests and data:
```python
from tests.test_utils import TestDataGenerator

generator = TestDataGenerator()
task_manifest = generator.create_task_manifest("my-task")
experiment_manifest = generator.create_experiment_manifest("my-experiment")
```

### MockRedisBroker
Mock Redis broker for testing:
```python
from tests.test_utils import MockRedisBroker

broker = MockRedisBroker()
broker.enqueue_job("test_queue", {"job_id": "123"})
```

### TestWorkspaceManager
Manages test workspaces:
```python
from tests.test_utils import TestWorkspaceManager

with TestWorkspaceManager() as manager:
    workspace = manager.create_workspace()
    # Use workspace for testing
    # Automatic cleanup on exit
```

## Test Configuration

### Pytest Configuration
The `pytest.ini` file contains pytest configuration:
- Test discovery patterns
- Markers for test categorization
- Warning filters
- Output formatting

### Test Markers
- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.slow`: Slow running tests
- `@pytest.mark.redis`: Tests requiring Redis
- `@pytest.mark.network`: Tests requiring network access

## Writing Tests

### Unit Test Example
```python
def test_valid_logging_config():
    """Test valid logging configuration."""
    config = LoggingConfig(
        file_level="DEBUG",
        console_level="INFO"
    )
    assert config.file_level == "DEBUG"
    assert config.console_level == "INFO"
```

### Integration Test Example
```python
@pytest.mark.integration
def test_full_orchestrator_workflow(temp_workspace, test_config):
    """Test complete orchestrator workflow."""
    # Setup
    config_path = temp_workspace / "config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(test_config.dict(), f)
    
    # Test
    with patch('orchestrator_core.RedisBroker') as mock_redis:
        # ... test implementation
        pass
```

### Using Fixtures
```python
def test_manifest_parsing(manifest_parser, sample_task_manifest):
    """Test manifest parsing with fixture."""
    result = manifest_parser.parse_manifest_from_string(sample_task_manifest)
    assert len(result) == 1
    assert result[0]["kind"] == "Task"
```

## Test Data

### Sample Manifests
The test suite includes sample manifests for all resource types:
- Task manifests with various configurations
- Experiment manifests with pipelines
- Environment manifests for different types
- Data manifests for different sources
- Model manifests for different architectures

### Invalid Test Data
Test data for error conditions:
- Invalid YAML syntax
- Missing required fields
- Invalid resource kinds
- Schema validation errors

## Continuous Integration

### GitHub Actions
The test suite is designed to work with GitHub Actions:
```yaml
- name: Run tests
  run: |
    pip install -r requirements.txt
    pip install -r requirements-test.txt
    pytest --cov=. --cov-report=xml
```

### Test Reports
Generate test reports:
```bash
pytest --html=report.html --self-contained-html
pytest --json-report --json-report-file=report.json
```

## Debugging Tests

### Verbose Output
```bash
pytest -v -s
```

### Debug Specific Test
```bash
pytest tests/test_config_validator.py::TestLoggingConfig::test_valid_logging_config -v -s --pdb
```

### Test Discovery
```bash
pytest --collect-only
```

## Performance Testing

### Benchmark Tests
```bash
pytest --benchmark-only
```

### Memory Profiling
```bash
pytest --profile
```

## Code Quality

### Linting
```bash
pylint orchestrator_core.py
black orchestrator_core.py
isort orchestrator_core.py
flake8 orchestrator_core.py
mypy orchestrator_core.py
```

### Pre-commit Hooks
```bash
pre-commit install
pre-commit run --all-files
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure the project root is in the Python path
2. **Redis Connection Errors**: Use mock Redis broker for unit tests
3. **File Permission Errors**: Ensure test directories are writable
4. **Memory Issues**: Use `pytest --maxfail=1` to stop on first failure

### Test Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt

# Run tests
pytest
```

### Debugging Configuration
```python
# In conftest.py or test files
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

When adding new tests:
1. Follow the existing naming conventions
2. Add appropriate test markers
3. Include docstrings explaining what is being tested
4. Use fixtures for common setup/teardown
5. Add both positive and negative test cases
6. Update this README if adding new test categories

## Test Coverage Goals

- **Unit Tests**: >90% coverage
- **Integration Tests**: Cover all major workflows
- **Error Handling**: Test all error conditions
- **Edge Cases**: Test boundary conditions
- **Performance**: Include benchmark tests for critical paths