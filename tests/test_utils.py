"""
Test utilities and helper functions for the Experiment Orchestrator tests.
"""

import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml
import json
from unittest.mock import Mock, MagicMock

from config_validator import OrchestratorConfig
from models.definitions import (
    TaskDefinition, ExperimentDefinition, EnvironmentDefinition,
    DataDefinition, ModelDefinition
)
from models.instances import ExperimentInstance, TaskInstance, JobInstance, InstanceStatus


class TestDataGenerator:
    """Generator for test data and manifests."""
    
    @staticmethod
    def create_task_manifest(
        name: str = "test-task",
        description: str = "Test task",
        steps: Optional[List[Dict[str, Any]]] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a task manifest."""
        if steps is None:
            steps = [
                {
                    "name": "train",
                    "executor": "test.job:TrainJob",
                    "parameters": {"epochs": "{{parameters.epochs}}"}
                }
            ]
        
        if parameters is None:
            parameters = {
                "epochs": {
                    "type": "integer",
                    "default": 10,
                    "description": "Number of training epochs"
                }
            }
        
        manifest = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Task",
            "metadata": {
                "name": name,
                "labels": {"test": "true"}
            },
            "spec": {
                "description": description,
                "parametersSchema": parameters,
                "steps": steps
            }
        }
        
        return yaml.dump(manifest, default_flow_style=False)
    
    @staticmethod
    def create_experiment_manifest(
        name: str = "test-experiment",
        description: str = "Test experiment",
        pipeline: Optional[List[Dict[str, Any]]] = None,
        global_parameters: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """Create an experiment manifest."""
        if pipeline is None:
            pipeline = [
                {
                    "name": "train-model",
                    "taskReference": "test-task",
                    "parameters": {"epochs": 20}
                }
            ]
        
        if global_parameters is None:
            global_parameters = [
                {
                    "name": "dataset_name",
                    "type": "string",
                    "value": "cifar10",
                    "description": "Dataset name"
                }
            ]
        
        manifest = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Experiment",
            "metadata": {
                "name": name,
                "labels": {"test": "true"}
            },
            "spec": {
                "description": description,
                "globalParameters": global_parameters,
                "pipeline": pipeline
            }
        }
        
        return yaml.dump(manifest, default_flow_style=False)
    
    @staticmethod
    def create_environment_manifest(
        name: str = "test-environment",
        env_type: str = "docker",
        image: str = "python:3.9-slim",
        setup_commands: Optional[List[str]] = None
    ) -> str:
        """Create an environment manifest."""
        if setup_commands is None:
            setup_commands = ["pip install torch"]
        
        manifest = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Environment",
            "metadata": {
                "name": name,
                "labels": {"test": "true"}
            },
            "spec": {
                "type": env_type,
                "image": image,
                "setupCommands": setup_commands
            }
        }
        
        return yaml.dump(manifest, default_flow_style=False)
    
    @staticmethod
    def create_data_manifest(
        name: str = "test-data",
        data_type: str = "local_files",
        uri: str = "./data",
        format: str = "csv"
    ) -> str:
        """Create a data manifest."""
        manifest = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Data",
            "metadata": {
                "name": name,
                "labels": {"test": "true"}
            },
            "spec": {
                "type": data_type,
                "uri": uri,
                "format": format
            }
        }
        
        return yaml.dump(manifest, default_flow_style=False)
    
    @staticmethod
    def create_model_manifest(
        name: str = "test-model",
        source_type: str = "module",
        module_path: str = "torchvision.models.resnet18",
        config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a model manifest."""
        if config is None:
            config = {"pretrained": False, "num_classes": 10}
        
        manifest = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Model",
            "metadata": {
                "name": name,
                "labels": {"test": "true"}
            },
            "spec": {
                "sourceType": source_type,
                "modulePath": module_path,
                "config": config
            }
        }
        
        return yaml.dump(manifest, default_flow_style=False)
    
    @staticmethod
    def create_invalid_manifest(error_type: str = "missing_kind") -> str:
        """Create an invalid manifest for testing error handling."""
        if error_type == "missing_kind":
            return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
metadata:
  name: invalid-resource
spec:
  description: "Invalid resource"
"""
        elif error_type == "invalid_kind":
            return """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: InvalidKind
metadata:
  name: invalid-resource
spec:
  description: "Invalid resource"
"""
        elif error_type == "invalid_yaml":
            return "invalid: yaml: content: ["
        elif error_type == "empty":
            return ""
        else:
            return "invalid: yaml: content: ["


class MockRedisBroker:
    """Mock Redis broker for testing."""
    
    def __init__(self, key_prefix_user: str = "test@"):
        self.key_prefix_user = key_prefix_user
        self.data = {}
        self.queues = {}
        self.sets = {}
        self.channels = {}
    
    def _get_prefixed_key(self, key: str) -> str:
        return f"{self.key_prefix_user}{key}"
    
    def enqueue_job(self, queue_name: str, job_data: Dict[str, Any]) -> bool:
        """Mock job enqueueing."""
        try:
            prefixed_queue = self._get_prefixed_key(queue_name)
            if prefixed_queue not in self.queues:
                self.queues[prefixed_queue] = []
            self.queues[prefixed_queue].append(json.dumps(job_data))
            return True
        except Exception:
            return False
    
    def dequeue_job(self, queue_name: str, timeout: int = 0) -> Optional[Dict[str, Any]]:
        """Mock job dequeueing."""
        try:
            prefixed_queue = self._get_prefixed_key(queue_name)
            if prefixed_queue in self.queues and self.queues[prefixed_queue]:
                job_data = self.queues[prefixed_queue].pop(0)
                return json.loads(job_data)
            return None
        except Exception:
            return None
    
    def set_job_status(self, job_id: str, status: str, details: Optional[Dict[str, Any]] = None) -> bool:
        """Mock job status setting."""
        try:
            prefixed_key = self._get_prefixed_key(f"job_status:{job_id}")
            status_data = {"status": status}
            if details:
                status_data.update(details)
            self.data[prefixed_key] = status_data
            return True
        except Exception:
            return False
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Mock job status retrieval."""
        try:
            prefixed_key = self._get_prefixed_key(f"job_status:{job_id}")
            return self.data.get(prefixed_key)
        except Exception:
            return None
    
    def publish_message(self, channel: str, message: Dict[str, Any]) -> bool:
        """Mock message publishing."""
        try:
            prefixed_channel = self._get_prefixed_key(channel)
            if prefixed_channel not in self.channels:
                self.channels[prefixed_channel] = []
            self.channels[prefixed_channel].append(message)
            return True
        except Exception:
            return False
    
    def subscribe_to_channel(self, channel: str) -> Mock:
        """Mock channel subscription."""
        prefixed_channel = self._get_prefixed_key(channel)
        mock_pubsub = Mock()
        mock_pubsub.subscribe = Mock()
        return mock_pubsub
    
    def add_to_set(self, set_name: str, item: str) -> bool:
        """Mock set addition."""
        try:
            prefixed_set = self._get_prefixed_key(set_name)
            if prefixed_set not in self.sets:
                self.sets[prefixed_set] = set()
            self.sets[prefixed_set].add(item)
            return True
        except Exception:
            return False
    
    def remove_from_set(self, set_name: str, item: str) -> bool:
        """Mock set removal."""
        try:
            prefixed_set = self._get_prefixed_key(set_name)
            if prefixed_set in self.sets:
                self.sets[prefixed_set].discard(item)
                return True
            return False
        except Exception:
            return False
    
    def get_set_members(self, set_name: str) -> set:
        """Mock set members retrieval."""
        try:
            prefixed_set = self._get_prefixed_key(set_name)
            return self.sets.get(prefixed_set, set())
        except Exception:
            return set()
    
    def clear_set(self, set_name: str) -> bool:
        """Mock set clearing."""
        try:
            prefixed_set = self._get_prefixed_key(set_name)
            if prefixed_set in self.sets:
                del self.sets[prefixed_set]
            return True
        except Exception:
            return False
    
    def health_check(self) -> bool:
        """Mock health check."""
        return True
    
    def close(self):
        """Mock connection closing."""
        pass


class MockScheduler:
    """Mock scheduler for testing."""
    
    def __init__(self, redis_broker: Mock, config: Optional[Dict[str, Any]] = None):
        self.redis_broker = redis_broker
        self.config = config or {}
        self.is_running = False
    
    def start(self):
        """Mock scheduler start."""
        self.is_running = True
    
    def stop(self):
        """Mock scheduler stop."""
        self.is_running = False


class TestWorkspaceManager:
    """Manager for test workspaces."""
    
    def __init__(self):
        self.workspaces = []
    
    def create_workspace(self, config: Optional[Dict[str, Any]] = None) -> Path:
        """Create a test workspace."""
        workspace = Path(tempfile.mkdtemp(prefix="orchestrator_test_"))
        self.workspaces.append(workspace)
        
        # Create necessary directories
        (workspace / "modules").mkdir()
        (workspace / "artifacts").mkdir()
        (workspace / "runtime").mkdir()
        (workspace / "log").mkdir()
        
        # Create config file if provided
        if config:
            config_path = workspace / "config.yaml"
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
        
        return workspace
    
    def cleanup_all(self):
        """Clean up all test workspaces."""
        for workspace in self.workspaces:
            if workspace.exists():
                shutil.rmtree(workspace)
        self.workspaces.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_all()


class TestManifestValidator:
    """Validator for test manifests."""
    
    @staticmethod
    def validate_task_manifest(manifest: str) -> bool:
        """Validate a task manifest."""
        try:
            data = yaml.safe_load(manifest)
            return (
                data.get("kind") == "Task" and
                "metadata" in data and
                "spec" in data and
                "steps" in data["spec"]
            )
        except Exception:
            return False
    
    @staticmethod
    def validate_experiment_manifest(manifest: str) -> bool:
        """Validate an experiment manifest."""
        try:
            data = yaml.safe_load(manifest)
            return (
                data.get("kind") == "Experiment" and
                "metadata" in data and
                "spec" in data and
                "pipeline" in data["spec"]
            )
        except Exception:
            return False
    
    @staticmethod
    def validate_environment_manifest(manifest: str) -> bool:
        """Validate an environment manifest."""
        try:
            data = yaml.safe_load(manifest)
            return (
                data.get("kind") == "Environment" and
                "metadata" in data and
                "spec" in data and
                "type" in data["spec"]
            )
        except Exception:
            return False
    
    @staticmethod
    def validate_data_manifest(manifest: str) -> bool:
        """Validate a data manifest."""
        try:
            data = yaml.safe_load(manifest)
            return (
                data.get("kind") == "Data" and
                "metadata" in data and
                "spec" in data and
                "type" in data["spec"]
            )
        except Exception:
            return False
    
    @staticmethod
    def validate_model_manifest(manifest: str) -> bool:
        """Validate a model manifest."""
        try:
            data = yaml.safe_load(manifest)
            return (
                data.get("kind") == "Model" and
                "metadata" in data and
                "spec" in data and
                "sourceType" in data["spec"]
            )
        except Exception:
            return False


class TestDataFactory:
    """Factory for creating test data objects."""
    
    @staticmethod
    def create_experiment_instance(
        id: str = "exp-123",
        name: str = "test-experiment",
        status: InstanceStatus = InstanceStatus.PENDING,
        parameters: Optional[Dict[str, Any]] = None
    ) -> ExperimentInstance:
        """Create a test experiment instance."""
        return ExperimentInstance(
            id=id,
            name=name,
            experiment_definition_ref="test-experiment",
            status=status,
            created_at="2024-01-01T00:00:00Z",
            parameters=parameters or {}
        )
    
    @staticmethod
    def create_task_instance(
        id: str = "task-123",
        name: str = "test-task",
        experiment_id: str = "exp-123",
        status: InstanceStatus = InstanceStatus.PENDING,
        parameters: Optional[Dict[str, Any]] = None
    ) -> TaskInstance:
        """Create a test task instance."""
        return TaskInstance(
            id=id,
            name=name,
            experiment_id=experiment_id,
            task_definition_ref="test-task",
            status=status,
            created_at="2024-01-01T00:00:00Z",
            parameters=parameters or {}
        )
    
    @staticmethod
    def create_job_instance(
        id: str = "job-123",
        name: str = "test-job",
        experiment_id: str = "exp-123",
        task_instance_id: str = "task-123",
        status: InstanceStatus = InstanceStatus.PENDING
    ) -> JobInstance:
        """Create a test job instance."""
        return JobInstance(
            id=id,
            name=name,
            experiment_id=experiment_id,
            task_instance_id=task_instance_id,
            status=status,
            created_at="2024-01-01T00:00:00Z",
            context=Mock()
        )


def create_test_config(
    logging_level: str = "DEBUG",
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 1
) -> Dict[str, Any]:
    """Create a test configuration dictionary."""
    return {
        "logging": {
            "file_level": logging_level,
            "console_level": "INFO",
            "format": "%(asctime)s %(levelname)s [%(process)d:%(module)s] %(funcName)s: %(message)s"
        },
        "paths": {
            "modules_root": "modules",
            "artifacts_root": "artifacts",
            "runtime_root": "runtime",
            "log_dir": "log"
        },
        "redis": {
            "host": redis_host,
            "port": redis_port,
            "db": redis_db,
            "key_prefix_user": "test@"
        },
        "scheduler": {
            "polling_interval_seconds": 1,
            "default_worker_queue": "test_worker_queue",
            "pending_jobs_set_key": "test_pending_jobs"
        }
    }


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


def assert_valid_environment_definition(env_def: EnvironmentDefinition) -> None:
    """Assert that an environment definition is valid."""
    assert env_def.kind == "Environment"
    assert env_def.metadata["name"]
    assert env_def.spec.type


def assert_valid_data_definition(data_def: DataDefinition) -> None:
    """Assert that a data definition is valid."""
    assert data_def.kind == "Data"
    assert data_def.metadata["name"]
    assert data_def.spec.type


def assert_valid_model_definition(model_def: ModelDefinition) -> None:
    """Assert that a model definition is valid."""
    assert model_def.kind == "Model"
    assert model_def.metadata["name"]
    assert model_def.spec.source_type