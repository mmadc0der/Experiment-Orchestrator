import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# --- Enums ---

class InstanceStatus(str, Enum):
    PENDING = "Pending"
    READY = "Ready"
    QUEUED = "Queued"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    SKIPPED = "Skipped"

# --- Core Models ---

class ArtifactMetadata(BaseModel):
    id: str = Field(..., description="Unique identifier for the artifact, e.g., 'artifact_name-uuid_suffix'")
    name: str = Field(..., description="Logical name of the artifact within the job, e.g., 'trained_model', 'features_output'")
    uri: str = Field(..., description="Full URI of the artifact, e.g., 'artifact://exp-1/job-1/features.npy' or 'file:///local/path', 's3://bucket/key'")
    job_instance_id: str = Field(..., description="ID of the JobInstance that produced this artifact")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    size_bytes: Optional[int] = Field(None, description="Size of the artifact in bytes")
    checksum: Optional[str] = Field(None, description="Checksum (e.g., md5, sha256) of the artifact content")
    content_type: Optional[str] = Field(None, description="MIME type or format of the artifact, e.g., 'application/octet-stream', 'image/png', 'text/csv'")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom metadata for the artifact")

    class Config:
        from_attributes = True


class JobInstanceContext(BaseModel):
    experiment_id: str = Field(..., description="ID of the parent ExperimentInstance")
    task_instance_id: str = Field(..., description="ID of the parent TaskInstance")
    job_instance_id: str = Field(..., description="ID of this JobInstance")
    
    job_definition_ref: str = Field(..., description="Reference to the definition of this job, e.g., 'my_jobs_package.train_model_job' or a TaskDefinition name")
    resolved_parameters: Dict[str, Any] = Field(default_factory=dict, description="Resolved parameters for this job instance, e.g., {'lr': 0.001}")
    resolved_inputs: Optional[Dict[str, str]] = Field(default=None, description="Resolved input artifact URIs, e.g., {'raw_data': 'artifact://exp-1/job-prev/data.csv'}")
    resolved_outputs: Optional[Dict[str, str]] = Field(default=None, description="Resolved output artifact URIs, e.g., {'trained_model': 'artifact://exp-1/job-this/model.pt'}")
    resources_request: Dict[str, Any] = Field(default_factory=dict, description="Resource requirements, e.g., {'gpus': 1, 'cpus': 2, 'memory_gb': 4}")
    priority: int = Field(default=0, description="Job priority, higher numbers typically mean higher priority")

    class Config:
        from_attributes = True


class JobInstance(BaseModel):
    id: str = Field(..., description="Unique identifier for the job instance, e.g., 'job_name-uuid_suffix'")
    name: str = Field(..., description="Unique runtime name for the job instance, e.g., 'task_name-iteration_info-step_name'")
    task_instance_id: str
    job_definition_ref: str = Field(..., description="Reference to the job's definition, e.g., 'my_module.MyJobClass' or 'task_def_name:train_step'")
    status: InstanceStatus = Field(default=InstanceStatus.PENDING)
    context: JobInstanceContext
    depends_on_job_ids: List[str] = Field(default_factory=list, description="List of JobInstance IDs this job depends on")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    worker_id: Optional[str] = Field(None, description="ID of the worker that executed/is executing this job")
    result_summary: Dict[str, Any] = Field(default_factory=dict)
    output_artifacts: List[ArtifactMetadata] = Field(default_factory=list)
    attempt_count: int = Field(default=0)

    class Config:
        from_attributes = True


class TaskInstance(BaseModel):
    id: str = Field(..., description="Unique identifier for the task instance, e.g., 'task_name-uuid_suffix'")
    name: str = Field(..., description="Unique runtime name for the task instance, incorporating iteration info if any, e.g., 'pipeline_task_name-param1_val1-param2_val2'")
    name_in_pipeline: str = Field(..., description="Name of the task as defined in the experiment pipeline, e.g., 'data-prep'")
    experiment_instance_id: str
    task_definition_ref: str = Field(..., description="Reference to the TaskDefinition from the manifest")
    status: InstanceStatus = Field(default=InstanceStatus.PENDING)
    parameters: Dict[str, Any] = Field(default_factory=dict) # Resolved parameters for this instance
    job_instance_ids: List[str] = Field(default_factory=list)
    depends_on_task_ids: List[str] = Field(default_factory=list, description="List of TaskInstance IDs this task instance depends on")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ExperimentInstance(BaseModel):
    id: str = Field(..., description="Unique identifier for the experiment instance, e.g., 'experiment_name-uuid_suffix'")
    experiment_definition_ref: str = Field(..., description="Reference to the Experiment definition from the manifest")
    name: Optional[str] = Field(None, description="User-friendly name for the experiment instance, can be derived from manifest or generated")
    description: Optional[str] = Field(None, description="Optional description for the experiment instance")
    status: InstanceStatus = Field(default=InstanceStatus.PENDING)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    task_instance_ids: List[str] = Field(default_factory=list)
    tags: Dict[str, str] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    report_data: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        from_attributes = True
