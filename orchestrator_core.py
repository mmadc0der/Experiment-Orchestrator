# m:\Projects\Experiment Orchestrator\orchestrator_core.py
import yaml
import os
from pathlib import Path
import logging as custom_logging
import uvicorn
from fastapi import FastAPI, HTTPException, APIRouter
from contextlib import asynccontextmanager
from pydantic import BaseModel
import redis # For Redis specific exceptions

from logger import init_logging
logger = custom_logging.getLogger(__name__)
from manifest_processing.manifest_parser import ManifestParser
from manifest_processing.manifest_expander import ManifestExpander
from brokers.redis_broker import RedisBroker
from models.instances import InstanceStatus, ExperimentInstance, TaskInstance, JobInstance # Ensure all are imported
from scheduler import Scheduler

CONFIG_FILE_NAME = "config.yaml"
DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(process)d:%(threadName)s] %(funcName)s: %(message)s"
API_VERSION = "v1alpha2"

# --- Pydantic модели ---
class ProcessManifestRequest(BaseModel): # Переименовано из RunExperimentRequest
    manifest_content: str

class StatusResponse(BaseModel):
    status: str
    message: str
    details: dict | None = None

# --- Orchestrator Class ---
class Orchestrator:
    def __init__(self, workspace_path: str | Path = "."):
        self.workspace_path = Path(workspace_path).resolve()
        self.config = self._load_config()
        self._configure_logging()
        logger.info(f"Orchestrator initialized. Workspace: {self.workspace_path}")
        logger.info(f"Configuration loaded: {CONFIG_FILE_NAME}")
        self.manifest_parser = ManifestParser()
        self._ensure_directories()
        self.redis_broker = self._initialize_redis_broker()
        self.scheduler = self._initialize_scheduler()

    def _load_config(self) -> dict:
        config_path = self.workspace_path / CONFIG_FILE_NAME
        if not config_path.exists():
            custom_logging.error(f"Configuration file not found: {config_path}")
            return {}
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            custom_logging.error(f"Error parsing configuration file {config_path}: {e}")
            return {}
        except Exception as e:
            custom_logging.error(f"Could not read configuration file {config_path}: {e}")
            return {}

    def _configure_logging(self):
        logging_config = self.config.get("logging", {})
        paths_config = self.config.get("paths", {})
        file_level_str = logging_config.get("file_level", "INFO").upper()
        console_level_str = logging_config.get("console_level", "WARNING").upper()
        log_format = logging_config.get("format", DEFAULT_LOG_FORMAT)
        file_level = getattr(custom_logging, file_level_str, custom_logging.INFO)
        console_level = getattr(custom_logging, console_level_str, custom_logging.WARNING)
        log_dir_name = paths_config.get("log_dir", "logs")
        self.log_dir = self.workspace_path / log_dir_name
        rotation_config = logging_config.get("rotation", {})
        max_bytes_str = rotation_config.get("max_bytes", "10*1024*1024")
        try:
            max_bytes = int(eval(max_bytes_str))
        except Exception:
            logger.warning(f"Could not evaluate max_bytes_str '{max_bytes_str}', using 10MB default.")
            max_bytes = 10 * 1024 * 1024

        backup_count = int(rotation_config.get("backup_count", 5))
        init_logging(
            file_level=file_level,
            console_level=console_level,
            log_dir=str(self.log_dir),
            log_format=log_format,
            max_bytes=max_bytes,
            backup_count=backup_count
        )

    def _ensure_directories(self):
        paths_config = self.config.get("paths", {})
        all_dirs_to_ensure = {paths_config.get("log_dir", "logs")}
        for dir_key in paths_config:
            if dir_key.endswith("_dir"):
                 all_dirs_to_ensure.add(paths_config[dir_key])
        
        for dir_name in all_dirs_to_ensure:
            dir_path = self.workspace_path / dir_name
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Ensured directory exists: {dir_path}")
            except Exception as e:
                logger.error(f"Could not create directory {dir_path}: {e}")

    def _initialize_redis_broker(self) -> RedisBroker | None:
        redis_config = self.config.get("redis")
        if not redis_config:
            logger.error(f"Redis configuration ('redis:') not found in {CONFIG_FILE_NAME}. RedisBroker will not be initialized.")
            return None
        
        try:
            host = redis_config.get("host", "localhost")
            port = int(redis_config.get("port", 6379))
            db = int(redis_config.get("db", 0))
            username = redis_config.get("username") # Can be None
            password = redis_config.get("password") # Can be None
            key_prefix_user = redis_config.get("key_prefix_user", "")
            
            broker = RedisBroker(
                host=host, 
                port=port, 
                db=db, 
                username=username, 
                password=password, 
                key_prefix_user=key_prefix_user
            )
            logger.info(f"RedisBroker initialized successfully with prefix '{key_prefix_user}'.")
            return broker
        except redis.exceptions.RedisError as e: # Catch generic Redis errors from broker's init
            logger.error(f"Failed to initialize RedisBroker: {e}. Orchestrator will continue without Redis integration for now.")
            return None
        except ValueError as e:
            logger.error(f"Invalid Redis configuration value (e.g., port or db not an int): {e}. RedisBroker not initialized.")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during RedisBroker initialization: {e}. RedisBroker not initialized.", exc_info=True)
            return None

    def _initialize_scheduler(self) -> Scheduler | None:
        if not self.redis_broker:
            logger.error("RedisBroker is not initialized. Scheduler cannot be started.")
            return None
        
        scheduler_config = self.config.get("scheduler", {})
        try:
            scheduler_instance = Scheduler(redis_broker=self.redis_broker, config=scheduler_config)
            scheduler_instance.start()
            logger.info("Scheduler initialized and started.")
            return scheduler_instance
        except Exception as e:
            logger.error(f"Failed to initialize or start Scheduler: {e}", exc_info=True)
            return None

    def process_manifest_from_string_content(self, manifest_yaml_string: str) -> dict: # Переименовано
        """
        Processes a manifest provided as a YAML string.
        Returns a dictionary with operation status.
        """
        logger.info("Attempting to parse manifest from string content.")
        try:
            manifest_data_list = self.manifest_parser.parse_manifest_from_string(manifest_yaml_string)
        except yaml.YAMLError as e:
            logger.error(f"YAML parsing error in manifest string content: {e}")
            return {"status": "error", "message": f"YAML parsing error in manifest string content: {e}"}
        except Exception as e:
            logger.error(f"Failed to parse manifest from string content: {e}")
            return {"status": "error", "message": f"Failed to parse manifest from string content: {e}"}

        return self._process_parsed_manifest_data(manifest_data_list, "string content")

    def _process_parsed_manifest_data(self, manifest_data_list: list[dict], source_description: str) -> dict:
        if not manifest_data_list:
            logger.warning(f"No documents found or parsed from manifest {source_description}")
            return {"status": "warning", "message": f"No documents found or parsed from manifest {source_description}"}

        logger.info(f"Successfully parsed {len(manifest_data_list)} document(s) from {source_description}.")
        
        experiment_instance = None
        task_instances = []
        job_instances = []
        expansion_successful = False
        expansion_error_message = ""
        all_kinds_in_manifest = [doc.get('kind') for doc in manifest_data_list if doc.get('kind')]

        # Attempt to expand if an ExperimentDefinition might be present
        # ManifestExpander will internally check for ExperimentDefinition and raise error if not suitable
        # We only try to expand if 'ExperimentDefinition' is one of the kinds found.
        if "Experiment" in all_kinds_in_manifest:
            try:
                logger.info("Experiment kind found. Initializing ManifestExpander with parsed documents.")
                expander = ManifestExpander(raw_manifest_docs=manifest_data_list)
                
                logger.info("Attempting to expand manifest using ManifestExpander.process_manifest().")
                experiment_instance, task_instances, job_instances = expander.process_manifest()
                
                expansion_successful = True
                logger.info(f"ManifestExpander successfully processed and expanded the experiment part of the manifest.")
                if experiment_instance:
                    logger.info(f"Generated ExperimentInstance: ID={experiment_instance.id}, Name={experiment_instance.name}")
                logger.info(f"Generated {len(task_instances)} TaskInstance(s) and {len(job_instances)} JobInstance(s).")

            except RuntimeError as e:
                logger.warning(f"ManifestExpander could not expand an experiment from the manifest: {e}. This may be expected if the ExperimentDefinition is invalid or incomplete.")
                expansion_error_message = str(e) 
            except Exception as e:
                logger.error(f"Unexpected error during ManifestExpander processing: {e}", exc_info=True)
                expansion_error_message = f"Unexpected internal error during expansion: {e}"
        else:
            logger.info("No Experiment kind found in manifest. Skipping experiment expansion.")
            expansion_error_message = "No Experiment kind found in manifest to expand."

        # Process/report on all documents based on expansion outcome
        processed_doc_details = []
        for i, doc_data in enumerate(manifest_data_list):
            kind = doc_data.get('kind')
            name = doc_data.get('metadata', {}).get('name', 'N/A')

            if kind == "Experiment":
                if expansion_successful:
                    status_msg = "expanded_as_part_of_experiment"
                    logger.info(f"Document {i+1} (Kind: {kind}, Name: {name}) was part of the successful experiment expansion.")
                else:
                    status_msg = "expansion_failed_or_not_applicable"
                    logger.warning(f"Document {i+1} (Kind: {kind}, Name: {name}) found, but expansion failed. Error: {expansion_error_message}")
                processed_doc_details.append({"kind": kind, "name": name, "status": status_msg, "error_if_any": expansion_error_message if not expansion_successful else None})
            elif kind == "Task" and "Experiment" in all_kinds_in_manifest:
                 # If there was an attempt to expand an experiment, TaskDefinitions are considered 'used' or 'related'
                status_msg = "used_by_expander_attempt" if expansion_successful else "related_to_failed_expansion_attempt"
                logger.info(f"Document {i+1} (Kind: {kind}, Name: {name}) was potentially used by ManifestExpander (success: {expansion_successful}).")
                processed_doc_details.append({"kind": kind, "name": name, "status": status_msg})
            else:
                logger.info(f"Document {i+1} (Kind: {kind}, Name: {name}) acknowledged. Specific processing for this kind TBD.")
                processed_doc_details.append({"kind": kind, "name": name, "status": "acknowledged_other_kind"})
        
        if expansion_successful and self.scheduler:
            logger.info("Submitting expanded job details to Scheduler...")
            try:
                self.scheduler.submit_experiment_jobs(experiment_instance, task_instances, job_instances)
                logger.info("Successfully submitted job details to Scheduler.")
                # We'll rely on the scheduler's logs for individual job submission status
                # and update the redis_storage_status based on whether scheduler accepted it.
                # For now, assume success if no exception from submit_experiment_jobs.
                redis_ops_summary = ["Scheduler accepted jobs."] # Simplified summary
            except Exception as e:
                logger.error(f"Error submitting jobs to Scheduler: {e}", exc_info=True)
                redis_ops_summary = [f"ERROR submitting to Scheduler: {e}"]
        elif expansion_successful and not self.scheduler:
            logger.warning("Scheduler not available. Cannot submit jobs for processing.")
            redis_ops_summary = ["Scheduler not available."]
        else:
            redis_ops_summary = ["Expansion not successful or no jobs to submit."]

        if expansion_successful:
            return {
                "status": "accepted_experiment_expanded",
                "message": "Experiment successfully expanded. Other resources acknowledged.",
                "expansion_details": {
                    "experiment_instance_id": experiment_instance.id if experiment_instance else None,
                    "task_instance_count": len(task_instances),
                    "job_instance_count": len(job_instances),
                    "submission_status": "success" if self.scheduler and "Scheduler accepted jobs." in redis_ops_summary else ("scheduler_unavailable" if not self.scheduler else "error_during_submission")
                },
                "processed_documents": processed_doc_details,
                "all_kinds_in_manifest": list(set(all_kinds_in_manifest))
            }
        elif "Experiment" in all_kinds_in_manifest: # An experiment was defined but couldn't be expanded
             return {
                "status": "error_experiment_expansion",
                "message": f"Failed to expand ExperimentDefinition from manifest. Error: {expansion_error_message or 'Unknown expansion error'}",
                "processed_documents": processed_doc_details,
                "all_kinds_in_manifest": list(set(all_kinds_in_manifest))
            }
        else: # No ExperimentDefinition, other kinds processed
            return {
                "status": "accepted_other_kinds",
                "message": "Manifest processed. No Experiment kind found to expand; other kinds acknowledged.",
                "processed_documents": processed_doc_details,
                "all_kinds_in_manifest": list(set(all_kinds_in_manifest))
            }

    def _handle_experiment_resource(self, experiment_data: dict): # Переименовано
        exp_name = experiment_data.get("metadata", {}).get("name", "unnamed_experiment")
        logger.info(f"Handling Experiment resource: {exp_name}")
        # TODO: Заменить на реальную логику планирования/выполнения эксперимента
        logger.warning(f"Actual processing/scheduling logic for Experiment '{exp_name}' is not yet implemented.")

# --- FastAPI приложение ---
@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    # The scheduler is started during Orchestrator initialization.
    # We could add other startup logic here if needed.
    logger.info("Application startup: Orchestrator initialized, scheduler should be running.")
    yield
    # Code to execute during shutdown
    logger.info("Application shutdown: Attempting to stop scheduler...")
    if orchestrator_service and orchestrator_service.scheduler:
        try:
            orchestrator_service.scheduler.stop()
            logger.info("Scheduler stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}", exc_info=True)
    else:
        logger.warning("Scheduler instance not found or not initialized, skipping stop.")

app = FastAPI(title="Experiment Orchestrator API", lifespan=lifespan)
api_router = APIRouter()
orchestrator_service = Orchestrator()

@api_router.post("/manifests/process", response_model=StatusResponse, tags=["Manifests"]) # Изменен путь и тег
async def process_manifest_api(request: ProcessManifestRequest): # Переименована функция и тип запроса
    """
    Accepts a manifest (YAML content) for processing by the orchestrator.
    The orchestrator will parse the manifest and schedule the defined resources.
    """
    logger.info(f"API call to /api/{API_VERSION}/manifests/process with manifest content.")
    try:
        result = orchestrator_service.process_manifest_from_string_content(request.manifest_content) # Вызов переименованного метода
        
        if result["status"] == "error": # Ошибка парсинга или другая критическая ошибка до принятия в обработку
            raise HTTPException(status_code=400, detail=result["message"])
        elif result["status"] == "warning": # Например, пустой манифест
             return StatusResponse(status="warning", message=result["message"], details=result)
        # Для статуса "accepted"
        return StatusResponse(status=result["status"], message=result["message"], details=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unhandled exception in /api/{API_VERSION}/manifests/process: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@app.get("/", response_model=StatusResponse, tags=["General"])
async def root():
    """
    Root endpoint for checking API availability.
    """
    return StatusResponse(status="ok", message="Experiment Orchestrator API is running.")

app.include_router(api_router, prefix=f"/api/{API_VERSION}")

# uvicorn orchestrator_core:app --reload
