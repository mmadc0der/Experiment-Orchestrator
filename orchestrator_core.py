# m:\Projects\Experiment Orchestrator\orchestrator_core.py
import yaml
import os
from pathlib import Path
import logging as custom_logging
import uvicorn
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel

from logger import init_logging
logger = custom_logging.getLogger(__name__)
from manifest_parser import ManifestParser

CONFIG_FILE_NAME = "config.example.yaml"
DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(process)d:%(threadName)s] %(funcName)s: %(message)s"
API_VERSION = "v1alpha1"

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
        logger.info(f"Configuration loaded: {self.config}")
        self.manifest_parser = ManifestParser()
        self._ensure_directories()

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
        """
        Common logic for processing parsed manifest data.
        This method will eventually trigger the actual scheduling/execution of resources.
        """
        if not manifest_data_list:
            logger.warning(f"No documents found or parsed from manifest {source_description}")
            return {"status": "warning", "message": f"No documents found or parsed from manifest {source_description}"}

        logger.info(f"Successfully parsed {len(manifest_data_list)} document(s) from {source_description}.")
        
        processed_kinds = []
        # TODO: Main logic for processing each document in the manifest
        # This will involve identifying the 'kind' and dispatching to appropriate handlers/schedulers.
        for i, doc_data in enumerate(manifest_data_list):
            kind = doc_data.get('kind')
            name = doc_data.get('metadata', {}).get('name')
            logger.info(f"Processing document {i+1} from {source_description}: kind='{kind}', name='{name}'")
            
            # Здесь будет логика маршрутизации в зависимости от 'kind'
            # Например, если kind == "Experiment", вызвать _schedule_experiment(doc_data)
            # если kind == "Dataset", вызвать _register_dataset(doc_data) и т.д.
            if kind == "Experiment": # Оставим пока для примера
                self._handle_experiment_resource(doc_data) # Переименовано для ясности
            # Другие виды ресурсов
            # elif kind == "Model":
            #     self._handle_model_resource(doc_data)
            else:
                logger.info(f"Resource kind '{kind}' (name: '{name}') acknowledged. Specific processing TBD.")

            processed_kinds.append(kind)
        
        logger.info(f"Finished initial processing of manifest from {source_description}.")
        # Возвращаем более общий ответ
        return {
            "status": "accepted", # Изменено на "accepted"
            "message": f"Manifest from {source_description} with {len(manifest_data_list)} document(s) accepted for processing.",
            "processed_doc_count": len(manifest_data_list),
            "processed_kinds": processed_kinds # Оставляем для информации
        }

    def _handle_experiment_resource(self, experiment_data: dict): # Переименовано
        exp_name = experiment_data.get("metadata", {}).get("name", "unnamed_experiment")
        logger.info(f"Handling Experiment resource: {exp_name}")
        # TODO: Заменить на реальную логику планирования/выполнения эксперимента
        logger.warning(f"Actual processing/scheduling logic for Experiment '{exp_name}' is not yet implemented.")

# --- FastAPI приложение ---
app = FastAPI(title="Experiment Orchestrator API")
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
