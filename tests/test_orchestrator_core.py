"""
Tests for the orchestrator core module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import shutil

from orchestrator_core import Orchestrator, ProcessManifestRequest, StatusResponse
from config_validator import OrchestratorConfig
from manifest_processing.manifest_parser import ManifestParseError, ManifestValidationError


class TestProcessManifestRequest:
    """Test ProcessManifestRequest model."""
    
    def test_valid_request(self):
        """Test valid manifest request."""
        request = ProcessManifestRequest(manifest_content="apiVersion: v1\nkind: Task")
        assert request.manifest_content == "apiVersion: v1\nkind: Task"


class TestStatusResponse:
    """Test StatusResponse model."""
    
    def test_valid_response(self):
        """Test valid status response."""
        response = StatusResponse(
            status="success",
            message="Operation completed",
            details={"processed": 1}
        )
        assert response.status == "success"
        assert response.message == "Operation completed"
        assert response.details == {"processed": 1}
    
    def test_response_without_details(self):
        """Test response without details."""
        response = StatusResponse(status="error", message="Operation failed")
        assert response.status == "error"
        assert response.message == "Operation failed"
        assert response.details is None


class TestOrchestrator:
    """Test Orchestrator functionality."""
    
    def test_orchestrator_initialization_success(self, temp_workspace, test_config):
        """Test successful orchestrator initialization."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            assert orchestrator.workspace_path == temp_workspace
            assert orchestrator.config is not None
            assert orchestrator.redis_broker == mock_broker_instance
            assert orchestrator.scheduler == mock_scheduler_instance
    
    def test_orchestrator_initialization_no_config(self, temp_workspace):
        """Test orchestrator initialization without config file."""
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            assert orchestrator.workspace_path == temp_workspace
            assert orchestrator.config is not None  # Should use default config
            assert orchestrator.redis_broker == mock_broker_instance
    
    def test_orchestrator_initialization_redis_error(self, temp_workspace, test_config):
        """Test orchestrator initialization with Redis error."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_redis_broker.side_effect = Exception("Redis connection failed")
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            assert orchestrator.redis_broker is None
            assert orchestrator.scheduler is None
    
    def test_orchestrator_initialization_scheduler_error(self, temp_workspace, test_config):
        """Test orchestrator initialization with scheduler error."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler.side_effect = Exception("Scheduler failed")
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            assert orchestrator.redis_broker == mock_broker_instance
            assert orchestrator.scheduler is None
    
    def test_process_manifest_from_string_content_success(self, temp_workspace, test_config, sample_task_manifest):
        """Test successful manifest processing from string."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler, \
             patch('orchestrator_core.ManifestExpander') as mock_expander:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            mock_expander_instance = Mock()
            mock_expander.return_value = mock_expander_instance
            mock_expander_instance.expand_manifest.return_value = {
                "experiments": [],
                "tasks": [],
                "environments": [],
                "data": [],
                "models": []
            }
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            result = orchestrator.process_manifest_from_string_content(sample_task_manifest)
            
            assert result["status"] == "success"
            assert "message" in result
            mock_expander_instance.expand_manifest.assert_called_once()
    
    def test_process_manifest_from_string_content_parse_error(self, temp_workspace, test_config):
        """Test manifest processing with parse error."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Test with invalid YAML
            invalid_manifest = "invalid: yaml: content: ["
            result = orchestrator.process_manifest_from_string_content(invalid_manifest)
            
            assert result["status"] == "error"
            assert "YAML parsing error" in result["message"]
    
    def test_process_manifest_from_string_content_validation_error(self, temp_workspace, test_config):
        """Test manifest processing with validation error."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Test with invalid resource kind
            invalid_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: InvalidKind
metadata:
  name: test
spec:
  description: test
"""
            result = orchestrator.process_manifest_from_string_content(invalid_manifest)
            
            assert result["status"] == "error"
            assert "Unknown resource kind" in result["message"]
    
    def test_process_manifest_from_string_content_expansion_error(self, temp_workspace, test_config, sample_task_manifest):
        """Test manifest processing with expansion error."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler, \
             patch('orchestrator_core.ManifestExpander') as mock_expander:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            mock_expander_instance = Mock()
            mock_expander.return_value = mock_expander_instance
            mock_expander_instance.expand_manifest.side_effect = Exception("Expansion failed")
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            result = orchestrator.process_manifest_from_string_content(sample_task_manifest)
            
            assert result["status"] == "error"
            assert "Expansion failed" in result["message"]
    
    def test_process_manifest_from_string_content_empty_manifest(self, temp_workspace, test_config):
        """Test manifest processing with empty manifest."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            result = orchestrator.process_manifest_from_string_content("")
            
            assert result["status"] == "success"
            assert "No valid documents" in result["message"]
    
    def test_process_manifest_from_string_content_multiple_documents(self, temp_workspace, test_config, 
                                                                   sample_task_manifest, sample_experiment_manifest):
        """Test manifest processing with multiple documents."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler, \
             patch('orchestrator_core.ManifestExpander') as mock_expander:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            mock_expander_instance = Mock()
            mock_expander.return_value = mock_expander_instance
            mock_expander_instance.expand_manifest.return_value = {
                "experiments": [{"name": "test-experiment"}],
                "tasks": [{"name": "test-task"}],
                "environments": [],
                "data": [],
                "models": []
            }
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            multi_doc_manifest = f"{sample_task_manifest}\n---\n{sample_experiment_manifest}"
            result = orchestrator.process_manifest_from_string_content(multi_doc_manifest)
            
            assert result["status"] == "success"
            assert result["details"]["processed_documents"] == 2
            mock_expander_instance.expand_manifest.assert_called_once()
    
    def test_orchestrator_cleanup(self, temp_workspace, test_config):
        """Test orchestrator cleanup."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Test cleanup
            orchestrator.cleanup()
            
            if orchestrator.scheduler:
                orchestrator.scheduler.stop.assert_called_once()
            if orchestrator.redis_broker:
                orchestrator.redis_broker.close.assert_called_once()


class TestOrchestratorEdgeCases:
    """Test orchestrator edge cases and error conditions."""
    
    def test_orchestrator_with_invalid_config(self, temp_workspace):
        """Test orchestrator with invalid configuration."""
        # Create invalid config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            # Should not raise exception, but use default config
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            assert orchestrator.config is not None
    
    def test_orchestrator_workspace_creation(self, tempfile.TemporaryDirectory):
        """Test orchestrator with non-existent workspace."""
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_path = Path(temp_dir) / "nonexistent" / "workspace"
            
            with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
                 patch('orchestrator_core.Scheduler') as mock_scheduler:
                
                mock_broker_instance = Mock()
                mock_redis_broker.return_value = mock_broker_instance
                
                mock_scheduler_instance = Mock()
                mock_scheduler.return_value = mock_scheduler_instance
                
                orchestrator = Orchestrator(workspace_path=str(workspace_path))
                assert orchestrator.workspace_path.exists()
    
    def test_orchestrator_manifest_processing_with_unicode(self, temp_workspace, test_config):
        """Test manifest processing with Unicode content."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler, \
             patch('orchestrator_core.ManifestExpander') as mock_expander:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            mock_expander_instance = Mock()
            mock_expander.return_value = mock_expander_instance
            mock_expander_instance.expand_manifest.return_value = {
                "experiments": [],
                "tasks": [],
                "environments": [],
                "data": [],
                "models": []
            }
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            unicode_manifest = """
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: "тест-задача"  # Unicode name
  labels:
    description: "Тестовая задача для проверки Unicode"
spec:
  description: "Тестовая задача"
  steps:
    - name: "тест-шаг"
      executor: "test.job:TestJob"
"""
            result = orchestrator.process_manifest_from_string_content(unicode_manifest)
            
            assert result["status"] == "success"
    
    def test_orchestrator_manifest_processing_large_manifest(self, temp_workspace, test_config):
        """Test manifest processing with large manifest."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            import yaml
            yaml.dump(test_config.dict(), f)
        
        with patch('orchestrator_core.RedisBroker') as mock_redis_broker, \
             patch('orchestrator_core.Scheduler') as mock_scheduler, \
             patch('orchestrator_core.ManifestExpander') as mock_expander:
            
            mock_broker_instance = Mock()
            mock_redis_broker.return_value = mock_broker_instance
            
            mock_scheduler_instance = Mock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            mock_expander_instance = Mock()
            mock_expander.return_value = mock_expander_instance
            mock_expander_instance.expand_manifest.return_value = {
                "experiments": [],
                "tasks": [],
                "environments": [],
                "data": [],
                "models": []
            }
            
            orchestrator = Orchestrator(workspace_path=str(temp_workspace))
            
            # Create large manifest with many resources
            large_manifest = ""
            for i in range(100):
                task_manifest = f"""
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Task
metadata:
  name: "task-{i}"
spec:
  description: "Task {i}"
  steps:
    - name: "step-{i}"
      executor: "test.job:TestJob{i}"
"""
                large_manifest += task_manifest + "\n---\n"
            
            result = orchestrator.process_manifest_from_string_content(large_manifest)
            
            assert result["status"] == "success"
            assert result["details"]["processed_documents"] == 100