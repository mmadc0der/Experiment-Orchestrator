"""
Pytest configuration and shared fixtures for the Experiment Orchestrator tests.
"""

import pytest
import tempfile
import shutil
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Generator
from unittest.mock import Mock, MagicMock

# Add the project root to the Python path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config_validator import OrchestratorConfig
from manifest_processing.manifest_parser import ManifestParser
from brokers.redis_broker import RedisBroker
from models.definitions import (
    TaskDefinition, ExperimentDefinition, EnvironmentDefinition,
    DataDefinition, ModelDefinition
)
from models.instances import ExperimentInstance, TaskInstance, JobInstance, InstanceStatus


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace directory for testing."""
    temp_dir = tempfile.mkdtemp(prefix="orchestrator_test_")
    workspace = Path(temp_dir)
    
    # Create necessary subdirectories
    (workspace / "modules").mkdir()
    (workspace / "artifacts").mkdir()
    (workspace / "runtime").mkdir()
    (workspace / "log").mkdir()
    
    yield workspace
    
    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_config() -> OrchestratorConfig:
    """Create a test configuration."""
    return OrchestratorConfig(
        logging={
            "file_level": "DEBUG",
            "console_level": "INFO",
            "format": "%(asctime)s %(levelname)s [%(process)d:%(module)s] %(funcName)s: %(message)s"
        },
        paths={
            "modules_root": "modules",
            "artifacts_root": "artifacts",
            "runtime_root": "runtime",
            "log_dir": "log"
        },
        redis={
            "host": "localhost",
            "port": 6379,
            "db": 1,  # Use different DB for testing
            "key_prefix_user": "test@"
        },
        scheduler={
            "polling_interval_seconds": 1,
            "default_worker_queue": "test_worker_queue",
            "pending_jobs_set_key": "test_pending_jobs"
        }
    )


@pytest.fixture
def config_file(temp_workspace: Path, test_config: OrchestratorConfig) -> Path:
    """Create a test configuration file."""
    config_path = temp_workspace / "config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(test_config.dict(), f, default_flow_style=False)
    return config_path


@pytest.fixture
def mock_redis_broker():
    """Create a mock Redis broker for testing."""
    mock_broker = Mock(spec=RedisBroker)
    mock_broker.key_prefix_user = "test@"
    mock_broker._get_prefixed_key = Mock(side_effect=lambda key: f"test@{key}")
    return mock_broker


@pytest.fixture
def manifest_parser():
    """Create a manifest parser instance for testing."""
    return ManifestParser(validate_schemas=True)


@pytest.fixture
def sample_task_manifest() -> str:
    """Sample task manifest for testing."""
    return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: test-task
  labels:
    test: "true"
spec:
  description: "Test task for unit testing"
  parametersSchema:
    epochs:
      type: "integer"
      default: 10
      description: "Number of training epochs"
    learning_rate:
      type: "number"
      default: 0.001
      description: "Learning rate"
  inputs:
    data:
      name: "input_data"
      description: "Input dataset"
  outputs:
    model:
      name: "trained_model"
      description: "Trained model artifact"
  steps:
    - name: "train"
      executor: "modules.jobs.test_job:TestJob"
      parameters:
        epochs: "{{parameters.epochs}}"
        learning_rate: "{{parameters.learning_rate}}"
      inputs:
        data: "{{inputs.data}}"
      outputs_templates:
        model: "model-{{parameters.epochs}}.pkl"
"""


@pytest.fixture
def sample_experiment_manifest() -> str:
    """Sample experiment manifest for testing."""
    return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Experiment
metadata:
  name: test-experiment
  labels:
    test: "true"
spec:
  description: "Test experiment for unit testing"
  globalParameters:
    - name: "dataset_name"
      type: "string"
      value: "cifar10"
      description: "Dataset name"
  pipeline:
    - name: "train-model"
      taskReference: "test-task"
      parameters:
        epochs: 20
        learning_rate: 0.01
      inputs:
        data: "{{globalParameters.dataset_name}}"
"""


@pytest.fixture
def sample_environment_manifest() -> str:
    """Sample environment manifest for testing."""
    return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Environment
metadata:
  name: test-environment
  labels:
    test: "true"
spec:
  description: "Test environment for unit testing"
  type: "docker"
  image: "python:3.9-slim"
  requirementsFile: "requirements.txt"
  setupCommands:
    - "pip install --upgrade pip"
    - "pip install torch"
  envVariables:
    - name: "PYTHONPATH"
      value: "/workspace"
"""


@pytest.fixture
def sample_data_manifest() -> str:
    """Sample data manifest for testing."""
    return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Data
metadata:
  name: test-data
  labels:
    test: "true"
spec:
  description: "Test data for unit testing"
  type: "local_files"
  uri: "./data/test_dataset"
  format: "csv"
  config:
    delimiter: ","
    header: true
"""


@pytest.fixture
def sample_model_manifest() -> str:
    """Sample model manifest for testing."""
    return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Model
metadata:
  name: test-model
  labels:
    test: "true"
spec:
  description: "Test model for unit testing"
  sourceType: "module"
  modulePath: "torchvision.models.resnet18"
  config:
    pretrained: false
    num_classes: 10
"""


@pytest.fixture
def invalid_manifest() -> str:
    """Invalid manifest for testing error handling."""
    return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: InvalidKind
metadata:
  name: invalid-resource
spec:
  invalidField: "invalid"
"""


@pytest.fixture
def sample_experiment_instance() -> ExperimentInstance:
    """Sample experiment instance for testing."""
    return ExperimentInstance(
        id="exp-123",
        name="test-experiment",
        experiment_definition_ref="test-experiment",
        status=InstanceStatus.PENDING,
        created_at="2024-01-01T00:00:00Z",
        parameters={"epochs": 10, "learning_rate": 0.001}
    )


@pytest.fixture
def sample_task_instance() -> TaskInstance:
    """Sample task instance for testing."""
    return TaskInstance(
        id="task-123",
        name="test-task",
        experiment_id="exp-123",
        task_definition_ref="test-task",
        status=InstanceStatus.PENDING,
        created_at="2024-01-01T00:00:00Z",
        parameters={"epochs": 10, "learning_rate": 0.001}
    )


@pytest.fixture
def sample_job_instance() -> JobInstance:
    """Sample job instance for testing."""
    return JobInstance(
        id="job-123",
        name="test-job",
        experiment_id="exp-123",
        task_instance_id="task-123",
        status=InstanceStatus.PENDING,
        created_at="2024-01-01T00:00:00Z",
        context=Mock()
    )


@pytest.fixture
def mock_scheduler():
    """Create a mock scheduler for testing."""
    mock_scheduler = Mock()
    mock_scheduler.start = Mock()
    mock_scheduler.stop = Mock()
    mock_scheduler.is_running = Mock(return_value=True)
    return mock_scheduler


# Test data generators
@pytest.fixture
def task_definition_data() -> Dict[str, Any]:
    """Sample task definition data."""
    return {
        "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
        "kind": "Task",
        "metadata": {
            "name": "test-task",
            "labels": {"test": "true"}
        },
        "spec": {
            "description": "Test task",
            "parametersSchema": {
                "epochs": {
                    "type": "integer",
                    "default": 10
                }
            },
            "steps": [
                {
                    "name": "train",
                    "executor": "test.job:TestJob",
                    "parameters": {
                        "epochs": "{{parameters.epochs}}"
                    }
                }
            ]
        }
    }


@pytest.fixture
def experiment_definition_data() -> Dict[str, Any]:
    """Sample experiment definition data."""
    return {
        "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
        "kind": "Experiment",
        "metadata": {
            "name": "test-experiment",
            "labels": {"test": "true"}
        },
        "spec": {
            "description": "Test experiment",
            "pipeline": [
                {
                    "name": "train-model",
                    "taskReference": "test-task",
                    "parameters": {
                        "epochs": 20
                    }
                }
            ]
        }
    }


# Utility functions for tests
def create_test_manifest_file(workspace: Path, content: str, filename: str = "test.manifest.yaml") -> Path:
    """Create a test manifest file in the workspace."""
    manifest_path = workspace / filename
    with open(manifest_path, 'w') as f:
        f.write(content)
    return manifest_path


def assert_valid_task_definition(task_def: TaskDefinition) -> None:
    """Assert that a task definition is valid."""
    assert task_def.kind == "Task"
    assert task_def.metadata["name"]
    assert task_def.spec.steps
    for step in task_def.spec.steps:
        assert step.name
        assert step.executor


def assert_valid_experiment_definition(exp_def: ExperimentDefinition) -> None:
    """Assert that an experiment definition is valid."""
    assert exp_def.kind == "Experiment"
    assert exp_def.metadata["name"]
    assert exp_def.spec.pipeline
    for task in exp_def.spec.pipeline:
        assert task.name
        assert task.task_reference


# Pytest configuration
def pytest_configure(config):
    """Configure pytest."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )