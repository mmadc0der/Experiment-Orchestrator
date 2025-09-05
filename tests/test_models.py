"""
Tests for the data models (definitions and instances).
"""

import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from models.definitions import (
    ResourceRequirements, ArtifactDefinition, ParameterDefinition,
    JobStepDefinition, TaskSpecification, TaskDefinition,
    ExperimentPipelineTask, GlobalParameterDefinition, ExperimentSpecification,
    ExperimentDefinition, EnvironmentVariable, EnvironmentSpecification,
    EnvironmentDefinition, DataSpecification, DataDefinition,
    ModelSpecification, ModelDefinition
)
from models.instances import (
    InstanceStatus, ArtifactMetadata, JobInstanceContext,
    ExperimentInstance, TaskInstance, JobInstance
)


class TestResourceRequirements:
    """Test ResourceRequirements model."""
    
    def test_valid_resource_requirements(self):
        """Test valid resource requirements."""
        req = ResourceRequirements(
            cpus=4.0,
            memory_gb=8.0,
            gpus=1,
            custom={"storage": "100Gi"}
        )
        assert req.cpus == 4.0
        assert req.memory_gb == 8.0
        assert req.gpus == 1
        assert req.custom["storage"] == "100Gi"
    
    def test_minimal_resource_requirements(self):
        """Test minimal resource requirements."""
        req = ResourceRequirements()
        assert req.cpus is None
        assert req.memory_gb is None
        assert req.gpus is None
        assert req.custom == {}


class TestArtifactDefinition:
    """Test ArtifactDefinition model."""
    
    def test_valid_artifact_definition(self):
        """Test valid artifact definition."""
        artifact = ArtifactDefinition(
            name="model_weights",
            description="Trained model weights",
            optional=False
        )
        assert artifact.name == "model_weights"
        assert artifact.description == "Trained model weights"
        assert artifact.optional is False
    
    def test_minimal_artifact_definition(self):
        """Test minimal artifact definition."""
        artifact = ArtifactDefinition(name="data")
        assert artifact.name == "data"
        assert artifact.description is None
        assert artifact.optional is False


class TestParameterDefinition:
    """Test ParameterDefinition model."""
    
    def test_valid_parameter_definition(self):
        """Test valid parameter definition."""
        param = ParameterDefinition(
            name="learning_rate",
            type="number",
            default=0.001,
            required=True,
            description="Learning rate for training"
        )
        assert param.name == "learning_rate"
        assert param.type == "number"
        assert param.default == 0.001
        assert param.required is True
    
    def test_parameter_definition_with_enum(self):
        """Test parameter definition with enum values."""
        param = ParameterDefinition(
            name="optimizer",
            type="string",
            enum=["adam", "sgd", "rmsprop"],
            required=True
        )
        assert param.enum == ["adam", "sgd", "rmsprop"]


class TestJobStepDefinition:
    """Test JobStepDefinition model."""
    
    def test_valid_job_step_definition(self):
        """Test valid job step definition."""
        step = JobStepDefinition(
            name="train",
            executor="modules.jobs.train:TrainJob",
            parameters={"epochs": 10},
            inputs={"data": "input_data"},
            outputs_templates={"model": "model.pkl"},
            depends_on=["preprocess"],
            resources=ResourceRequirements(cpus=2.0),
            retry_policy={"max_attempts": 3},
            priority=1
        )
        assert step.name == "train"
        assert step.executor == "modules.jobs.train:TrainJob"
        assert step.parameters["epochs"] == 10
        assert step.depends_on == ["preprocess"]
        assert step.priority == 1


class TestTaskSpecification:
    """Test TaskSpecification model."""
    
    def test_valid_task_specification(self):
        """Test valid task specification."""
        spec = TaskSpecification(
            description="Training task",
            parameters_schema={
                "epochs": ParameterDefinition(name="epochs", type="integer", default=10)
            },
            inputs={
                "data": ArtifactDefinition(name="data")
            },
            outputs={
                "model": ArtifactDefinition(name="model")
            },
            steps=[
                JobStepDefinition(
                    name="train",
                    executor="test.job:TrainJob",
                    parameters={"epochs": "{{parameters.epochs}}"}
                )
            ]
        )
        assert spec.description == "Training task"
        assert "epochs" in spec.parameters_schema
        assert len(spec.steps) == 1
        assert spec.steps[0].name == "train"
    
    def test_task_specification_parameter_name_population(self):
        """Test that parameter name population works correctly."""
        # Create the data as it would come from YAML parsing
        data = {
            "steps": [
                {
                    "name": "train",
                    "executor": "test.job:TrainJob"
                }
            ],
            "parametersSchema": {
                "epochs": {
                    "type": "integer",
                    "default": 10
                }
            }
        }
        spec = TaskSpecification(**data)
        # Check that the parameter name was populated
        assert "epochs" in spec.parameters_schema
        epochs_param = spec.parameters_schema["epochs"]
        assert epochs_param.name == "epochs"


class TestTaskDefinition:
    """Test TaskDefinition model."""
    
    def test_valid_task_definition(self, task_definition_data):
        """Test valid task definition."""
        task_def = TaskDefinition(**task_definition_data)
        assert task_def.kind == "Task"
        assert task_def.metadata["name"] == "test-task"
        assert task_def.spec.description == "Test task"
    
    def test_task_definition_invalid_kind(self):
        """Test task definition with invalid kind."""
        data = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "InvalidKind",
            "metadata": {"name": "test"},
            "spec": {"description": "Test"}
        }
        with pytest.raises(ValidationError, match='kind must be "Task"'):
            TaskDefinition(**data)


class TestExperimentPipelineTask:
    """Test ExperimentPipelineTask model."""
    
    def test_valid_experiment_pipeline_task(self):
        """Test valid experiment pipeline task."""
        task = ExperimentPipelineTask(
            name="train-model",
            task_reference="training-task",
            parameters={"epochs": 20},
            inputs={"data": "dataset"},
            depends_on=["preprocess"],
            iterate_over=[
                {"parameters": {"epochs": 10}},
                {"parameters": {"epochs": 20}}
            ]
        )
        assert task.name == "train-model"
        assert task.task_reference == "training-task"
        assert task.parameters["epochs"] == 20
        assert task.depends_on == ["preprocess"]
        assert len(task.iterate_over) == 2


class TestGlobalParameterDefinition:
    """Test GlobalParameterDefinition model."""
    
    def test_valid_global_parameter_definition(self):
        """Test valid global parameter definition."""
        param = GlobalParameterDefinition(
            name="dataset_name",
            type="string",
            value="cifar10",
            description="Name of the dataset"
        )
        assert param.name == "dataset_name"
        assert param.type == "string"
        assert param.value == "cifar10"


class TestExperimentSpecification:
    """Test ExperimentSpecification model."""
    
    def test_valid_experiment_specification(self):
        """Test valid experiment specification."""
        spec = ExperimentSpecification(
            description="Training experiment",
            global_parameter_definitions=[
                GlobalParameterDefinition(
                    name="dataset",
                    type="string",
                    value="cifar10"
                )
            ],
            pipeline=[
                ExperimentPipelineTask(
                    name="train",
                    task_reference="training-task"
                )
            ]
        )
        assert spec.description == "Training experiment"
        assert len(spec.global_parameter_definitions) == 1
        assert len(spec.pipeline) == 1


class TestExperimentDefinition:
    """Test ExperimentDefinition model."""
    
    def test_valid_experiment_definition(self, experiment_definition_data):
        """Test valid experiment definition."""
        exp_def = ExperimentDefinition(**experiment_definition_data)
        assert exp_def.kind == "Experiment"
        assert exp_def.metadata["name"] == "test-experiment"
        assert exp_def.spec.description == "Test experiment"
    
    def test_experiment_definition_invalid_kind(self):
        """Test experiment definition with invalid kind."""
        data = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "InvalidKind",
            "metadata": {"name": "test"},
            "spec": {"description": "Test"}
        }
        with pytest.raises(ValidationError, match='kind must be "Experiment"'):
            ExperimentDefinition(**data)


class TestEnvironmentVariable:
    """Test EnvironmentVariable model."""
    
    def test_valid_environment_variable(self):
        """Test valid environment variable."""
        env_var = EnvironmentVariable(name="PYTHONPATH", value="/workspace")
        assert env_var.name == "PYTHONPATH"
        assert env_var.value == "/workspace"


class TestEnvironmentSpecification:
    """Test EnvironmentSpecification model."""
    
    def test_valid_environment_specification(self):
        """Test valid environment specification."""
        spec = EnvironmentSpecification(
            description="Python environment",
            type="docker",
            image="python:3.9-slim",
            requirements_file="requirements.txt",
            setup_commands=["pip install torch"],
            env_variables=[
                EnvironmentVariable(name="CUDA_VISIBLE_DEVICES", value="0")
            ]
        )
        assert spec.type == "docker"
        assert spec.image == "python:3.9-slim"
        assert len(spec.setup_commands) == 1
        assert len(spec.env_variables) == 1


class TestEnvironmentDefinition:
    """Test EnvironmentDefinition model."""
    
    def test_valid_environment_definition(self):
        """Test valid environment definition."""
        data = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Environment",
            "metadata": {"name": "test-env"},
            "spec": {
                "type": "docker",
                "image": "python:3.9-slim"
            }
        }
        env_def = EnvironmentDefinition(**data)
        assert env_def.kind == "Environment"
        assert env_def.spec.type == "docker"


class TestDataSpecification:
    """Test DataSpecification model."""
    
    def test_valid_data_specification(self):
        """Test valid data specification."""
        spec = DataSpecification(
            description="CIFAR-10 dataset",
            type="torchvision_dataset",
            loader_module="torchvision.datasets.CIFAR10",
            config={"root": "./data", "train": True}
        )
        assert spec.type == "torchvision_dataset"
        assert spec.loader_module == "torchvision.datasets.CIFAR10"


class TestDataDefinition:
    """Test DataDefinition model."""
    
    def test_valid_data_definition(self):
        """Test valid data definition."""
        data = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Data",
            "metadata": {"name": "test-data"},
            "spec": {
                "type": "local_files",
                "uri": "./data"
            }
        }
        data_def = DataDefinition(**data)
        assert data_def.kind == "Data"
        assert data_def.spec.type == "local_files"


class TestModelSpecification:
    """Test ModelSpecification model."""
    
    def test_valid_model_specification(self):
        """Test valid model specification."""
        spec = ModelSpecification(
            description="ResNet-18 model",
            source_type="module",
            module_path="torchvision.models.resnet18",
            config={"pretrained": False, "num_classes": 10}
        )
        assert spec.source_type == "module"
        assert spec.module_path == "torchvision.models.resnet18"


class TestModelDefinition:
    """Test ModelDefinition model."""
    
    def test_valid_model_definition(self):
        """Test valid model definition."""
        data = {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Model",
            "metadata": {"name": "test-model"},
            "spec": {
                "sourceType": "module",
                "modulePath": "torchvision.models.resnet18"
            }
        }
        model_def = ModelDefinition(**data)
        assert model_def.kind == "Model"
        assert model_def.spec.source_type == "module"


class TestInstanceStatus:
    """Test InstanceStatus enum."""
    
    def test_instance_status_values(self):
        """Test instance status enum values."""
        assert InstanceStatus.PENDING == "Pending"
        assert InstanceStatus.RUNNING == "Running"
        assert InstanceStatus.SUCCEEDED == "Succeeded"
        assert InstanceStatus.FAILED == "Failed"


class TestArtifactMetadata:
    """Test ArtifactMetadata model."""
    
    def test_valid_artifact_metadata(self):
        """Test valid artifact metadata."""
        artifact = ArtifactMetadata(
            id="artifact-123",
            name="model_weights",
            uri="artifact://exp-1/job-1/model.pkl",
            job_instance_id="job-123"
        )
        assert artifact.id == "artifact-123"
        assert artifact.name == "model_weights"
        assert artifact.uri == "artifact://exp-1/job-1/model.pkl"
        assert artifact.job_instance_id == "job-123"


class TestJobInstanceContext:
    """Test JobInstanceContext model."""
    
    def test_valid_job_instance_context(self):
        """Test valid job instance context."""
        context = JobInstanceContext(
            experiment_id="exp-123",
            task_instance_id="task-123",
            job_instance_id="job-123",
            job_definition_ref="test.job:TestJob",
            resolved_parameters={"epochs": "10"},
            input_literal_values={"epochs": 10},
            resolved_inputs={"data": "artifact://exp-1/job-0/data.csv"},
            resolved_outputs={"model": "artifact://exp-1/job-1/model.pkl"},
            resources_request={"cpus": 2.0},
            priority=1
        )
        assert context.experiment_id == "exp-123"
        assert context.job_definition_ref == "test.job:TestJob"
        assert context.priority == 1


class TestExperimentInstance:
    """Test ExperimentInstance model."""
    
    def test_valid_experiment_instance(self, sample_experiment_instance):
        """Test valid experiment instance."""
        exp = sample_experiment_instance
        assert exp.id == "exp-123"
        assert exp.name == "test-experiment"
        assert exp.status == InstanceStatus.PENDING
        assert exp.parameters["epochs"] == 10


class TestTaskInstance:
    """Test TaskInstance model."""
    
    def test_valid_task_instance(self, sample_task_instance):
        """Test valid task instance."""
        task = sample_task_instance
        assert task.id == "task-123"
        assert task.experiment_instance_id == "exp-123"
        assert task.status == InstanceStatus.PENDING


class TestJobInstance:
    """Test JobInstance model."""
    
    def test_valid_job_instance(self, sample_job_instance):
        """Test valid job instance."""
        job = sample_job_instance
        assert job.id == "job-123"
        assert job.context.experiment_id == "exp-123"
        assert job.task_instance_id == "task-123"
        assert job.status == InstanceStatus.PENDING


class TestModelValidation:
    """Test model validation edge cases."""
    
    def test_parameter_definition_required_field(self):
        """Test parameter definition with required field."""
        param = ParameterDefinition(name="test", type="string", required=False)
        assert param.required is False
    
    def test_job_step_definition_minimal(self):
        """Test minimal job step definition."""
        step = JobStepDefinition(name="test", executor="test.job:TestJob")
        assert step.name == "test"
        assert step.executor == "test.job:TestJob"
        assert step.priority == 0  # Default value
    
    def test_task_specification_empty_steps(self):
        """Test task specification with empty steps."""
        with pytest.raises(ValidationError, match="steps list cannot be empty"):
            TaskSpecification(steps=[])
    
    def test_experiment_specification_empty_pipeline(self):
        """Test experiment specification with empty pipeline."""
        with pytest.raises(ValidationError, match="pipeline list cannot be empty"):
            ExperimentSpecification(pipeline=[])
    
    def test_environment_specification_invalid_type(self):
        """Test environment specification with invalid type."""
        with pytest.raises(ValidationError, match="Input should be"):
            EnvironmentSpecification(type="invalid_type")
    
    def test_data_specification_invalid_type(self):
        """Test data specification with invalid type."""
        with pytest.raises(ValidationError, match="Input should be"):
            DataSpecification(type="invalid_type")
    
    def test_model_specification_invalid_source_type(self):
        """Test model specification with invalid source type."""
        with pytest.raises(ValidationError, match="Input should be"):
            ModelSpecification(source_type="invalid_type")