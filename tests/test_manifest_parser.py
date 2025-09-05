"""
Tests for the manifest parser module.
"""

import pytest
import tempfile
from pathlib import Path

from manifest_processing.manifest_parser import (
    ManifestParser, ManifestParseError, ManifestValidationError
)
from models.definitions import (
    TaskDefinition, ExperimentDefinition, EnvironmentDefinition,
    DataDefinition, ModelDefinition
)


class TestManifestParseError:
    """Test ManifestParseError exception."""
    
    def test_error_with_file_path(self):
        """Test error with file path."""
        error = ManifestParseError("Test error", file_path="test.yaml", line_number=5)
        assert error.message == "Test error"
        assert error.file_path == "test.yaml"
        assert error.line_number == 5


class TestManifestValidationError:
    """Test ManifestValidationError exception."""
    
    def test_error_with_resource_info(self):
        """Test error with resource information."""
        error = ManifestValidationError(
            "Validation failed", 
            resource_kind="Task", 
            resource_name="test-task"
        )
        assert error.message == "Validation failed"
        assert error.resource_kind == "Task"
        assert error.resource_name == "test-task"


class TestManifestParser:
    """Test ManifestParser functionality."""
    
    def test_parser_initialization(self):
        """Test parser initialization."""
        parser = ManifestParser(validate_schemas=True)
        assert parser.validate_schemas is True
        
        parser_no_validation = ManifestParser(validate_schemas=False)
        assert parser_no_validation.validate_schemas is False
    
    def test_get_supported_kinds(self):
        """Test getting supported resource kinds."""
        parser = ManifestParser()
        kinds = parser.get_supported_kinds()
        
        expected_kinds = ["Task", "Experiment", "Environment", "Data", "Model"]
        assert set(kinds) == set(expected_kinds)
    
    def test_parse_manifest_file_not_found(self, manifest_parser):
        """Test parsing non-existent file."""
        with pytest.raises(ManifestParseError, match="Manifest file not found"):
            manifest_parser.parse_manifest("nonexistent.yaml")
    
    def test_parse_manifest_not_a_file(self, temp_workspace, manifest_parser):
        """Test parsing a directory instead of file."""
        with pytest.raises(ManifestParseError, match="not a file"):
            manifest_parser.parse_manifest(str(temp_workspace))
    
    def test_parse_manifest_invalid_yaml(self, temp_workspace, manifest_parser):
        """Test parsing invalid YAML."""
        manifest_path = temp_workspace / "invalid.yaml"
        with open(manifest_path, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        with pytest.raises(ManifestParseError, match="YAML parsing error"):
            manifest_parser.parse_manifest(str(manifest_path))
    
    def test_parse_manifest_empty_file(self, temp_workspace, manifest_parser):
        """Test parsing empty file."""
        manifest_path = temp_workspace / "empty.yaml"
        with open(manifest_path, 'w') as f:
            f.write("")
        
        result = manifest_parser.parse_manifest(str(manifest_path))
        assert result == []
    
    def test_parse_manifest_with_comments(self, temp_workspace, manifest_parser):
        """Test parsing file with only comments."""
        manifest_path = temp_workspace / "comments.yaml"
        with open(manifest_path, 'w') as f:
            f.write("# This is a comment\n# Another comment\n---\n# More comments")
        
        result = manifest_parser.parse_manifest(str(manifest_path))
        assert result == []
    
    def test_parse_manifest_single_document(self, temp_workspace, manifest_parser, sample_task_manifest):
        """Test parsing single document manifest."""
        manifest_path = create_test_manifest_file(temp_workspace, sample_task_manifest)
        result = manifest_parser.parse_manifest(str(manifest_path))
        
        assert len(result) == 1
        assert result[0]["kind"] == "Task"
        assert result[0]["metadata"]["name"] == "test-task"
    
    def test_parse_manifest_multiple_documents(self, temp_workspace, manifest_parser, 
                                             sample_task_manifest, sample_experiment_manifest):
        """Test parsing multiple document manifest."""
        multi_doc_content = f"{sample_task_manifest}\n---\n{sample_experiment_manifest}"
        manifest_path = create_test_manifest_file(temp_workspace, multi_doc_content)
        result = manifest_parser.parse_manifest(str(manifest_path))
        
        assert len(result) == 2
        assert result[0]["kind"] == "Task"
        assert result[1]["kind"] == "Experiment"
    
    def test_parse_manifest_validation_disabled(self, temp_workspace, invalid_manifest):
        """Test parsing with validation disabled."""
        parser = ManifestParser(validate_schemas=False)
        manifest_path = create_test_manifest_file(temp_workspace, invalid_manifest)
        result = parser.parse_manifest(str(manifest_path))
        
        assert len(result) == 1
        assert result[0]["kind"] == "InvalidKind"
    
    def test_parse_manifest_validation_enabled_invalid_kind(self, temp_workspace, manifest_parser, invalid_manifest):
        """Test parsing invalid resource kind with validation enabled."""
        manifest_path = create_test_manifest_file(temp_workspace, invalid_manifest)
        
        with pytest.raises(ManifestValidationError, match="Unknown resource kind"):
            manifest_parser.parse_manifest(str(manifest_path))
    
    def test_parse_manifest_missing_required_fields(self, temp_workspace, manifest_parser):
        """Test parsing manifest with missing required fields."""
        invalid_manifest = """
kind: Task
metadata:
  name: test-task
spec:
  description: "Test task"
"""
        manifest_path = create_test_manifest_file(temp_workspace, invalid_manifest)
        
        with pytest.raises(ManifestValidationError, match="missing required field"):
            manifest_parser.parse_manifest(str(manifest_path))
    
    def test_parse_manifest_from_string_empty(self, manifest_parser):
        """Test parsing empty string."""
        result = manifest_parser.parse_manifest_from_string("")
        assert result == []
    
    def test_parse_manifest_from_string_whitespace(self, manifest_parser):
        """Test parsing whitespace-only string."""
        result = manifest_parser.parse_manifest_from_string("   \n  \t  \n  ")
        assert result == []
    
    def test_parse_manifest_from_string_valid(self, manifest_parser, sample_task_manifest):
        """Test parsing valid string."""
        result = manifest_parser.parse_manifest_from_string(sample_task_manifest)
        
        assert len(result) == 1
        assert result[0]["kind"] == "Task"
        assert result[0]["metadata"]["name"] == "test-task"
    
    def test_parse_manifest_from_string_invalid_yaml(self, manifest_parser):
        """Test parsing invalid YAML string."""
        with pytest.raises(ManifestParseError, match="YAML parsing error"):
            manifest_parser.parse_manifest_from_string("invalid: yaml: [")
    
    def test_validate_single_resource_valid(self, manifest_parser, task_definition_data):
        """Test validating single valid resource."""
        result = manifest_parser.validate_single_resource(task_definition_data)
        
        assert result["kind"] == "Task"
        assert result["metadata"]["name"] == "test-task"
    
    def test_validate_single_resource_invalid_kind(self, manifest_parser):
        """Test validating single resource with invalid kind."""
        invalid_data = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "InvalidKind",
            "metadata": {"name": "test"},
            "spec": {}
        }
        
        with pytest.raises(ManifestValidationError, match="Unknown resource kind"):
            manifest_parser.validate_single_resource(invalid_data)
    
    def test_validate_single_resource_missing_fields(self, manifest_parser):
        """Test validating single resource with missing fields."""
        invalid_data = {
            "kind": "Task",
            "metadata": {"name": "test"}
            # Missing apiVersion and spec
        }
        
        with pytest.raises(ManifestValidationError, match="missing required field"):
            manifest_parser.validate_single_resource(invalid_data)
    
    def test_validate_single_resource_not_dict(self, manifest_parser):
        """Test validating non-dictionary resource."""
        with pytest.raises(ManifestValidationError, match="must be a dictionary"):
            manifest_parser.validate_single_resource("not a dict")


class TestResourceValidation:
    """Test specific resource type validation."""
    
    def test_task_definition_validation(self, manifest_parser, sample_task_manifest):
        """Test Task definition validation."""
        result = manifest_parser.parse_manifest_from_string(sample_task_manifest)
        task_def = TaskDefinition(**result[0])
        
        assert task_def.kind == "Task"
        assert task_def.metadata["name"] == "test-task"
        assert len(task_def.spec.steps) == 1
        assert task_def.spec.steps[0].name == "train"
    
    def test_experiment_definition_validation(self, manifest_parser, sample_experiment_manifest):
        """Test Experiment definition validation."""
        result = manifest_parser.parse_manifest_from_string(sample_experiment_manifest)
        exp_def = ExperimentDefinition(**result[0])
        
        assert exp_def.kind == "Experiment"
        assert exp_def.metadata["name"] == "test-experiment"
        assert len(exp_def.spec.pipeline) == 1
        assert exp_def.spec.pipeline[0].name == "train-model"
    
    def test_environment_definition_validation(self, manifest_parser, sample_environment_manifest):
        """Test Environment definition validation."""
        result = manifest_parser.parse_manifest_from_string(sample_environment_manifest)
        env_def = EnvironmentDefinition(**result[0])
        
        assert env_def.kind == "Environment"
        assert env_def.metadata["name"] == "test-environment"
        assert env_def.spec.type == "docker"
        assert env_def.spec.image == "python:3.9-slim"
    
    def test_data_definition_validation(self, manifest_parser, sample_data_manifest):
        """Test Data definition validation."""
        result = manifest_parser.parse_manifest_from_string(sample_data_manifest)
        data_def = DataDefinition(**result[0])
        
        assert data_def.kind == "Data"
        assert data_def.metadata["name"] == "test-data"
        assert data_def.spec.type == "local_files"
        assert data_def.spec.uri == "./data/test_dataset"
    
    def test_model_definition_validation(self, manifest_parser, sample_model_manifest):
        """Test Model definition validation."""
        result = manifest_parser.parse_manifest_from_string(sample_model_manifest)
        model_def = ModelDefinition(**result[0])
        
        assert model_def.kind == "Model"
        assert model_def.metadata["name"] == "test-model"
        assert model_def.spec.source_type == "module"
        assert model_def.spec.module_path == "torchvision.models.resnet18"


class TestManifestParserEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_parse_manifest_with_none_documents(self, temp_workspace, manifest_parser):
        """Test parsing manifest with None documents."""
        manifest_path = temp_workspace / "none_docs.yaml"
        with open(manifest_path, 'w') as f:
            f.write("---\n---\n---")
        
        result = manifest_parser.parse_manifest(str(manifest_path))
        assert result == []
    
    def test_parse_manifest_mixed_valid_invalid(self, temp_workspace, manifest_parser, 
                                              sample_task_manifest, invalid_manifest):
        """Test parsing manifest with mix of valid and invalid documents."""
        multi_doc_content = f"{sample_task_manifest}\n---\n{invalid_manifest}"
        manifest_path = create_test_manifest_file(temp_workspace, multi_doc_content)
        
        with pytest.raises(ManifestValidationError):
            manifest_parser.parse_manifest(str(manifest_path))
    
    def test_parse_manifest_large_file(self, temp_workspace, manifest_parser, sample_task_manifest):
        """Test parsing large manifest file."""
        # Create a large manifest with many documents
        large_content = ""
        for i in range(100):
            task_manifest = sample_task_manifest.replace("test-task", f"test-task-{i}")
            large_content += task_manifest + "\n---\n"
        
        manifest_path = create_test_manifest_file(temp_workspace, large_content)
        result = manifest_parser.parse_manifest(str(manifest_path))
        
        assert len(result) == 100
        for i, doc in enumerate(result):
            assert doc["metadata"]["name"] == f"test-task-{i}"


# Helper function
def create_test_manifest_file(workspace: Path, content: str, filename: str = "test.manifest.yaml") -> Path:
    """Create a test manifest file in the workspace."""
    manifest_path = workspace / filename
    with open(manifest_path, 'w') as f:
        f.write(content)
    return manifest_path