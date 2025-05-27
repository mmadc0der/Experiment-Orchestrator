from typing import Any, Dict, List, Optional, Literal
from datetime import datetime, timezone # Added timezone
from pydantic import BaseModel, Field, model_validator

# --- Common Reusable Models for Definitions ---

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

class ExperimentPipelineTask(BaseModel):
    name: str = Field(..., description="Unique name for this task invocation within the experiment's pipeline")
    task_reference: str = Field(..., alias="taskReference", description="Name of the TaskDefinition to execute")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Specific parameter values for this TaskDefinition invocation. Can use templating from experiment parameters.")
    inputs: Optional[Dict[str, str]] = Field(default_factory=dict, description="Mapping of TaskDefinition's input artifact names to experiment-level artifacts or outputs of previous pipeline tasks (e.g., 'artifact_uri' or '{{tasks.prev_task_name.outputs.artifact_name}}')")
    depends_on: Optional[List[str]] = Field(default_factory=list, alias="dependsOn", description="List of other ExperimentPipelineTask names within the same experiment that this task depends on")
    iterate_over: Optional[Dict[str, List[Any]]] = Field(default=None, alias="iterateOver", description="Parameter grid for iterating this task, creating multiple TaskInstances. E.g., {'learning_rate': [0.01, 0.001]}")
    # on_condition: Optional[str] = Field(None, description="A condition string to evaluate for executing this pipeline task")

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }

class ExperimentSpecification(BaseModel):
    description: Optional[str] = None
    parameters_schema: Optional[Dict[str, ParameterDefinition]] = Field(default_factory=dict, alias="parametersSchema", description="Global parameters schema for the experiment")
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

class AnyDefinition(BaseModel):
    api_version: str = Field(..., alias="apiVersion")
    kind: str # Keep as str to allow parsing before knowing the exact type
    metadata: Dict[str, Any]
    spec: Dict[str, Any] # Keep as dict for now, specific parsing will cast to TaskSpecification or ExperimentSpecification

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"
    }
