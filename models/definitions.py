from typing import Any, Dict, List, Optional, Literal, Union
from datetime import datetime, timezone # Added timezone
from pydantic import BaseModel, Field, model_validator, field_validator

# --- Common Reusable Models for Definitions ---

class EnvironmentVariable(BaseModel):
    """Environment variable definition."""
    name: str = Field(..., description="Environment variable name")
    value: str = Field(..., description="Environment variable value")

class ResourceRequirements(BaseModel):
    cpus: Optional[float] = Field(None, description="Number of CPU cores")
    memory_gb: Optional[float] = Field(None, description="Memory in GiB")
    gpus: Optional[int] = Field(None, description="Number of GPUs")
    custom: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Custom resource requests")

class ArtifactDefinition(BaseModel):
    name: str = Field(..., description="Logical name of the artifact, e.g., 'dataset', 'model_weights'")
    description: Optional[str] = None
    optional: bool = Field(default=False)
    # path_template: Optional[str] = Field(None, description="Template for the artifact path, used by the system if not overridden")

class ParameterDefinition(BaseModel):
    name: str = Field(..., description="Name of the parameter, automatically inferred from the key in parametersSchema")
    description: Optional[str] = None
    type: str = Field(..., description="Parameter type, e.g., 'string', 'integer', 'number', 'boolean', 'object', 'array'")
    default: Optional[Any] = Field(None, description="Default value for the parameter")
    required: bool = Field(True, description="Whether the parameter is required")
    enum: Optional[List[Any]] = None
    items: Optional[Dict[str, Any]] = None  # For array type
    properties: Optional[Dict[str, Any]] = None  # For object type
    schema_definition: Optional[Dict[str, Any]] = Field(default=None, alias="schema", description="JSON schema for complex parameter types like object or array")

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

class JobStepDefinition(BaseModel):
    name: str = Field(..., description="Unique name for this step within the task")
    description: Optional[str] = None
    executor: str = Field(..., description="Reference to the executable for this job, e.g., 'module.submodule.ClassName' or 'docker_image_uri'")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Parameters to pass to the job executor. Can use templating from task parameters or outputs of previous steps.")
    inputs: Optional[Dict[str, str]] = Field(default_factory=dict, description="Mapping of job's input artifact names to task's input artifact names or outputs of other steps (e.g., '{{inputs.task_input_name}}' or '{{steps.prev_step_name.outputs.artifact_name}}')")
    outputs_templates: Optional[Dict[str, str]] = Field(default_factory=dict, description="Mapping of logical output artifact names to their filename templates. E.g., {'model': 'model-{{parameters.version}}.pkl'}")
    depends_on: Optional[List[str]] = Field(default_factory=list, description="List of other JobStepDefinition names within the same task that this step depends on")
    resources_request: Optional[ResourceRequirements] = Field(default=None, alias="resources", description="Resource requirements for this job step. Aliased as 'resources' for YAML manifest.")
    retry_policy: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Policy for retrying the job on failure, e.g., {'max_attempts': 3, 'backoff_seconds': 60}")
    priority: int = Field(default=0, description="Job priority, higher numbers typically mean higher priority")
    # on_condition: Optional[str] = Field(None, description="A condition string to evaluate for executing this step")

# --- Task Definition ---

class TaskSpecification(BaseModel):
    description: Optional[str] = None
    parameters_schema: Optional[Dict[str, ParameterDefinition]] = Field(default_factory=dict, alias="parametersSchema")
    inputs: Optional[Dict[str, ArtifactDefinition]] = Field(default_factory=dict, description="Input artifacts this task expects")
    outputs: Optional[Dict[str, ArtifactDefinition]] = Field(default_factory=dict, description="Output artifacts this task produces")
    steps: List[JobStepDefinition] = Field(..., description="The sequence or DAG of job steps that constitute this task")
    resources: Optional[ResourceRequirements] = Field(None, description="Default resource requirements for job steps in this task, can be overridden at step level")

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

    @model_validator(mode='before')
    @classmethod
    def _populate_parameter_names(cls, data: Any) -> Any:
        if isinstance(data, dict):
            params_schema = data.get('parametersSchema') # Uses alias
            if isinstance(params_schema, dict):
                for param_name, param_def in params_schema.items():
                    if isinstance(param_def, dict) and 'name' not in param_def:
                        param_def['name'] = param_name
        return data

    @field_validator('steps')
    @classmethod
    def validate_steps_not_empty(cls, v):
        """Validate that steps list is not empty."""
        if not v:
            raise ValueError('steps list cannot be empty')
        return v

class TaskDefinition(BaseModel):
    api_version: str = Field(..., alias="apiVersion", description="API version of the TaskDefinition schema")
    kind: Literal["Task"] = Field(..., description="Resource kind, must be 'Task'")
    metadata: Dict[str, Any] = Field(..., description="Standard Kubernetes-like metadata (name, labels, annotations, etc.)")
    spec: TaskSpecification

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

    @model_validator(mode='before')
    @classmethod
    def check_kind(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("kind") != "Task":
            raise ValueError('TaskDefinition kind must be "Task"')
        return data

# --- Experiment Definition ---

class IterateItem(BaseModel):
    parameters: Dict[str, Any] = Field(..., description="A set of parameters for a single iteration.")
    # Можно добавить сюда name или id для итерации, если потребуется в будущем

class ExperimentPipelineTask(BaseModel):
    name: str = Field(..., description="Unique name for this task invocation within the experiment's pipeline")
    task_reference: str = Field(..., alias="taskReference", description="Name of the TaskDefinition to execute")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Specific parameter values for this TaskDefinition invocation. Can use templating from experiment parameters.")
    inputs: Optional[Dict[str, str]] = Field(default_factory=dict, description="Mapping of TaskDefinition's input artifact names to experiment-level artifacts or outputs of previous pipeline tasks (e.g., 'artifact_uri' or '{{tasks.prev_task_name.outputs.artifact_name}}')")
    depends_on: Optional[List[str]] = Field(default_factory=list, alias="dependsOn", description="List of other ExperimentPipelineTask names within the same experiment that this task depends on")
    iterate_over: Optional[List[IterateItem]] = Field(default=None, alias="iterateOver", description="List of parameter sets for iterating this task. Each item in the list generates one TaskInstance with the specified parameters.")
    # on_condition: Optional[str] = Field(None, description="A condition string to evaluate for executing this pipeline task")

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

class GlobalParameterDefinition(BaseModel):
    name: str = Field(..., description="Unique name of the global parameter.")
    type: str = Field(..., description="Data type of the parameter (e.g., string, integer, float, boolean).") # TODO: Consider using Literal for type
    value: Any = Field(..., description="Actual value of the global parameter.")
    description: Optional[str] = Field(default=None, description="Optional description for the global parameter.")

    model_config = {
        "extra": "forbid"
    }

class ExperimentSpecification(BaseModel):
    description: Optional[str] = None
    global_parameter_definitions: Optional[List[GlobalParameterDefinition]] = Field(default_factory=list, alias="globalParameters", description="List of global parameter definitions for the experiment.")
    pipeline: List[ExperimentPipelineTask] = Field(..., description="The DAG of tasks to be executed in this experiment")
    reporting: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Configuration for experiment reporting")
    # artifact_store_config: Optional[Dict[str, Any]] = Field(None, description="Configuration for the artifact store specific to this experiment")

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

    @model_validator(mode='before')
    @classmethod
    def _populate_exp_parameter_names(cls, data: Any) -> Any:
        if isinstance(data, dict):
            params_schema = data.get('parametersSchema') # Uses alias
            if isinstance(params_schema, dict):
                for param_name, param_def in params_schema.items():
                    if isinstance(param_def, dict) and 'name' not in param_def:
                        param_def['name'] = param_name
        return data

    @field_validator('pipeline')
    @classmethod
    def validate_pipeline_not_empty(cls, v):
        """Validate that pipeline list is not empty."""
        if not v:
            raise ValueError('pipeline list cannot be empty')
        return v

class ExperimentDefinition(BaseModel):
    api_version: str = Field(..., alias="apiVersion", description="API version of the ExperimentDefinition schema")
    kind: Literal["Experiment"] = Field(..., description="Resource kind, must be 'Experiment'")
    metadata: Dict[str, Any] = Field(..., description="Standard Kubernetes-like metadata (name, labels, annotations, etc.)")
    spec: ExperimentSpecification

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

    @model_validator(mode='before')
    @classmethod
    def check_kind(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("kind") != "Experiment":
            raise ValueError('ExperimentDefinition kind must be "Experiment"')
        return data

# --- Unified Manifest Model (Optional, for parsing any kind) ---

# --- Environment Definition ---

class EnvironmentSpecification(BaseModel):
    """Environment specification for execution environments."""
    description: Optional[str] = Field(None, description="Environment description")
    type: Literal["docker", "conda", "venv", "bare_metal"] = Field(..., description="Environment type")
    image: Optional[str] = Field(None, description="Docker image name (if type: docker)")
    python_version: Optional[str] = Field(None, alias="pythonVersion", description="Python version (if type: conda or venv)")
    conda_env_name: Optional[str] = Field(None, alias="condaEnvName", description="Conda environment name (if type: conda)")
    venv_path: Optional[str] = Field(None, alias="venvPath", description="Path to virtual environment (if type: venv)")
    requirements_file: Optional[str] = Field(None, alias="requirementsFile", description="Path to dependency file")
    setup_commands: Optional[List[str]] = Field(default_factory=list, alias="setupCommands", description="Additional setup commands")
    env_variables: Optional[List[EnvironmentVariable]] = Field(default_factory=list, alias="envVariables", description="Environment variables")
    
    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

class EnvironmentDefinition(BaseModel):
    """Environment resource definition."""
    api_version: str = Field(..., alias="apiVersion", description="API version")
    kind: Literal["Environment"] = Field(..., description="Resource kind")
    metadata: Dict[str, Any] = Field(..., description="Resource metadata")
    spec: EnvironmentSpecification
    
    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

# --- Data Definition ---

class DataSpecification(BaseModel):
    """Data specification for data sources."""
    description: Optional[str] = Field(None, description="Dataset description")
    type: Literal["torchvision_dataset", "local_files", "s3_bucket", "gcs_bucket", "database", "custom_module"] = Field(..., description="Data source type")
    uri: Optional[str] = Field(None, description="URI to data source")
    format: Optional[str] = Field(None, description="Data format")
    loader_module: Optional[str] = Field(None, alias="loaderModule", description="Python module for loading data")
    loader_function: Optional[str] = Field(None, alias="loaderFunction", description="Function name in loader module")
    config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Configuration for data loader")
    schema: Optional[Dict[str, Any]] = Field(None, description="Data schema description")
    
    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

class DataDefinition(BaseModel):
    """Data resource definition."""
    api_version: str = Field(..., alias="apiVersion", description="API version")
    kind: Literal["Data"] = Field(..., description="Resource kind")
    metadata: Dict[str, Any] = Field(..., description="Resource metadata")
    spec: DataSpecification
    
    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

# --- Model Definition ---

class ModelSpecification(BaseModel):
    """Model specification for ML models."""
    description: Optional[str] = Field(None, description="Model description")
    source_type: Literal["module", "pretrained_hub", "onnx_file"] = Field(..., alias="sourceType", description="Model source type")
    module_path: Optional[str] = Field(None, alias="modulePath", description="Python module path for model")
    function_name: Optional[str] = Field(None, alias="functionName", description="Factory function name")
    config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Model configuration")
    input_signature: Optional[List[Dict[str, Any]]] = Field(None, alias="inputSignature", description="Input tensor signature")
    output_signature: Optional[List[Dict[str, Any]]] = Field(None, alias="outputSignature", description="Output tensor signature")
    
    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

class ModelDefinition(BaseModel):
    """Model resource definition."""
    api_version: str = Field(..., alias="apiVersion", description="API version")
    kind: Literal["Model"] = Field(..., description="Resource kind")
    metadata: Dict[str, Any] = Field(..., description="Resource metadata")
    spec: ModelSpecification
    
    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

# --- Unified Manifest Model (Optional, for parsing any kind) ---

class AnyDefinition(BaseModel):
    api_version: str = Field(..., alias="apiVersion")
    kind: str # Keep as str to allow parsing before knowing the exact type
    metadata: Dict[str, Any]
    spec: Dict[str, Any] # Keep as dict for now, specific parsing will cast to specific specification types

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }
