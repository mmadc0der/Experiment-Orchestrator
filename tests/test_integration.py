"""
Integration tests for the Experiment Orchestrator.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
import yaml

from orchestrator_core import Orchestrator
from config_validator import OrchestratorConfig
from manifest_processing.manifest_parser import ManifestParser
from brokers.redis_broker import RedisBroker


@pytest.mark.integration
class TestOrchestratorIntegration:
    """Integration tests for the orchestrator system."""
    
    def test_full_orchestrator_workflow(self, temp_workspace, test_config):
        """Test complete orchestrator workflow."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(test_config.dict(), f)
        
        # Create sample manifests
        task_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: integration-test-task
  labels:
    test: "integration"
spec:
  description: "Integration test task"
  parametersSchema:
    epochs:
      type: "integer"
      default: 10
      description: "Number of training epochs"
  steps:
    - name: "train"
      executor: "modules.jobs.test_job:TestJob"
      parameters:
        epochs: "{{parameters.epochs}}"
"""
        
        experiment_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Experiment
metadata:
  name: integration-test-experiment
  labels:
    test: "integration"
spec:
  description: "Integration test experiment"
  globalParameters:
    - name: "dataset_name"
      type: "string"
      value: "cifar10"
      description: "Dataset name"
  pipeline:
    - name: "train-model"
      taskReference: "integration-test-task"
      parameters:
        epochs: 20
"""
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler, \
             patch('orchestrator_core.ManifestExpander') as mock_expander:
            
            # Setup mocks
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            mock_expander_instance = Mock()
            mock_expander.return_value = mock_expander_instance
            mock_expander_instance.expand_manifest.return_value = {
                "experiments": [{"name": "integration-test-experiment"}],
                "tasks": [{"name": "integration-test-task"}],
                "environments": [],
                "data": [],
                "models": []
            }
            
            # Initialize orchestrator
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Test task manifest processing
            task_result = orchestrator.process_manifest_from_string_content(task_manifest)
            assert task_result["status"] == "success"
            assert "processed_documents" in task_result["details"]
            
            # Test experiment manifest processing
            exp_result = orchestrator.process_manifest_from_string_content(experiment_manifest)
            assert exp_result["status"] == "success"
            assert "processed_documents" in exp_result["details"]
            
            # Test combined manifest processing
            combined_manifest = f"{task_manifest}\n---\n{experiment_manifest}"
            combined_result = orchestrator.process_manifest_from_string_content(combined_manifest)
            assert combined_result["status"] == "success"
            assert combined_result["details"]["processed_documents"] == 2
    
    def test_config_validation_integration(self, temp_workspace):
        """Test configuration validation integration."""
        # Test with valid config
        valid_config = {
            "logging": {
                "file_level": "DEBUG",
                "console_level": "INFO"
            },
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 1
            },
            "scheduler": {
                "polling_interval_seconds": 5
            }
        }
        
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(valid_config, f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            assert orchestrator.config.logging.file_level == "DEBUG"
            assert orchestrator.config.redis.host == "localhost"
    
    def test_manifest_parser_integration(self, temp_workspace):
        """Test manifest parser integration with different resource types."""
        parser = ManifestParser(validate_schemas=True)
        
        # Test all resource types
        manifests = {
            "task": """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: test-task
spec:
  description: "Test task"
  steps:
    - name: "test-step"
      executor: "test.job:TestJob"
""",
            "experiment": """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Experiment
metadata:
  name: test-experiment
spec:
  description: "Test experiment"
  pipeline:
    - name: "test-pipeline"
      taskReference: "test-task"
""",
            "environment": """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Environment
metadata:
  name: test-environment
spec:
  type: "docker"
  image: "python:3.9-slim"
""",
            "data": """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Data
metadata:
  name: test-data
spec:
  type: "local_files"
  uri: "./data"
""",
            "model": """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Model
metadata:
  name: test-model
spec:
  sourceType: "module"
  modulePath: "torchvision.models.resnet18"
"""
        }
        
        for resource_type, manifest in manifests.items():
            result = parser.parse_manifest_from_string(manifest)
            assert len(result) == 1
            assert result[0]["kind"] == resource_type.title()
    
    def test_error_handling_integration(self, temp_workspace, test_config):
        """Test error handling integration."""
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Test various error conditions
            error_cases = [
                ("Invalid YAML", "invalid: yaml: content: ["),
                ("Missing required fields", "kind: Task\nmetadata:\n  name: test"),
                ("Invalid resource kind", "apiVersion: v1\nkind: InvalidKind\nmetadata:\n  name: test\nspec: {}"),
                ("Empty manifest", ""),
                ("Whitespace only", "   \n  \t  \n  "),
            ]
            
            for error_type, manifest in error_cases:
                result = orchestrator.process_manifest_from_string_content(manifest)
                assert result["status"] == "error"
                assert "message" in result


@pytest.mark.integration
class TestRedisBrokerIntegration:
    """Integration tests for Redis broker."""
    
    def test_redis_broker_operations(self):
        """Test Redis broker operations integration."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_client.lpush.return_value = 1
            mock_client.brpop.return_value = ("test@test_queue", '{"job_id": "123"}')
            mock_client.hset.return_value = 1
            mock_client.hgetall.return_value = {b"status": b"Running"}
            mock_client.publish.return_value = 1
            mock_client.sadd.return_value = 1
            mock_client.smembers.return_value = {b"item1", b"item2"}
            mock_redis.return_value = mock_client
            
            broker = RedisBroker(key_prefix_user="test@")
            
            # Test job operations
            assert broker.enqueue_job("test_queue", {"job_id": "123"}) is True
            assert broker.dequeue_job("test_queue") == {"job_id": "123"}
            
            # Test status operations
            assert broker.set_job_status("job_123", "Running") is True
            assert broker.get_job_status("job_123")["status"] == "Running"
            
            # Test messaging
            assert broker.publish_message("test_channel", {"message": "test"}) is True
            
            # Test set operations
            assert broker.add_to_set("test_set", "item1") is True
            assert broker.get_set_members("test_set") == {"item1", "item2"}
    
    def test_redis_broker_error_handling(self):
        """Test Redis broker error handling integration."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_client.lpush.side_effect = Exception("Redis error")
            mock_redis.return_value = mock_client
            
            broker = RedisBroker(key_prefix_user="test@")
            
            # Test error handling
            assert broker.enqueue_job("test_queue", {"job_id": "123"}) is False
            assert broker.dequeue_job("test_queue") is None
            assert broker.set_job_status("job_123", "Running") is False
            assert broker.get_job_status("job_123") is None


@pytest.mark.integration
class TestManifestProcessingIntegration:
    """Integration tests for manifest processing."""
    
    def test_manifest_processing_pipeline(self, temp_workspace):
        """Test complete manifest processing pipeline."""
        # Create test manifests
        task_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: pipeline-test-task
spec:
  description: "Pipeline test task"
  parametersSchema:
    epochs:
      type: "integer"
      default: 10
  steps:
    - name: "train"
      executor: "test.job:TrainJob"
      parameters:
        epochs: "{{parameters.epochs}}"
"""
        
        experiment_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Experiment
metadata:
  name: pipeline-test-experiment
spec:
  description: "Pipeline test experiment"
  globalParameters:
    - name: "dataset"
      type: "string"
      value: "cifar10"
  pipeline:
    - name: "train-model"
      taskReference: "pipeline-test-task"
      parameters:
        epochs: 20
"""
        
        # Test individual parsing
        parser = ManifestParser(validate_schemas=True)
        
        task_result = parser.parse_manifest_from_string(task_manifest)
        assert len(task_result) == 1
        assert task_result[0]["kind"] == "Task"
        
        exp_result = parser.parse_manifest_from_string(experiment_manifest)
        assert len(exp_result) == 1
        assert exp_result[0]["kind"] == "Experiment"
        
        # Test combined parsing
        combined_manifest = f"{task_manifest}\n---\n{experiment_manifest}"
        combined_result = parser.parse_manifest_from_string(combined_manifest)
        assert len(combined_result) == 2
        assert combined_result[0]["kind"] == "Task"
        assert combined_result[1]["kind"] == "Experiment"
    
    def test_manifest_validation_integration(self):
        """Test manifest validation integration."""
        parser = ManifestParser(validate_schemas=True)
        
        # Test valid manifest
        valid_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: valid-task
spec:
  description: "Valid task"
  steps:
    - name: "test-step"
      executor: "test.job:TestJob"
"""
        result = parser.parse_manifest_from_string(valid_manifest)
        assert len(result) == 1
        
        # Test invalid manifest
        invalid_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: invalid-task
spec:
  description: "Invalid task"
  # Missing required 'steps' field
"""
        with pytest.raises(Exception):  # Should raise validation error
            parser.parse_manifest_from_string(invalid_manifest)


@pytest.mark.integration
class TestSystemIntegration:
    """System-wide integration tests."""
    
    def test_system_startup_shutdown(self, temp_workspace, test_config):
        """Test system startup and shutdown."""
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            # Test startup
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            assert orchestrator is not None
            assert orchestrator.workspace_path == temp_workspace
            
            # Test shutdown
            orchestrator.cleanup()
            if orchestrator.scheduler:
                orchestrator.scheduler.stop.assert_called_once()
            if orchestrator.redis_broker:
                orchestrator.redis_broker.close.assert_called_once()
    
    def test_directory_creation_integration(self, tempfile.TemporaryDirectory):
        """Test directory creation integration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_path = Path(temp_dir) / "test_workspace"
            
            with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
                 patch('orchestrator_core.Scheduler') as mock_scheduler:
                
                mock_broker_instance = Mock()
                mock_redis_broker.return_value = mock_broker_instance
                
                mock_scheduler_instance = Mock()
                mock_scheduler.return_value = mock_scheduler_instance
                
                orchestrator = Orchestrator(workspace_path=str(workspace_path))
                
                # Check that directories were created
                assert (workspace_path / "modules").exists()
                assert (workspace_path / "artifacts").exists()
                assert (workspace_path / "runtime").exists()
                assert (workspace_path / "log").exists()
    
    def test_configuration_loading_integration(self, temp_workspace):
        """Test configuration loading integration."""
        # Test with custom config
        custom_config = {
            "logging": {
                "file_level": "DEBUG",
                "console_level": "WARNING"
            },
            "paths": {
                "modules_root": "custom_modules",
                "artifacts_root": "custom_artifacts"
            },
            "redis": {
                "host": "custom-redis",
                "port": 6380,
                "db": 2
            }
        }
        
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(custom_config, f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Verify custom configuration was loaded
            assert orchestrator.config.logging.file_level == "DEBUG"
            assert orchestrator.config.paths.modules_root == "custom_modules"
            assert orchestrator.config.redis.host == "custom-redis"
            assert orchestrator.config.redis.port == 6380