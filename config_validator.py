"""
Configuration validation module for the Experiment Orchestrator.
Provides schema validation and default configuration management.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, validator, root_validator
import logging

logger = logging.getLogger(__name__)

class LoggingConfig(BaseModel):
    """Logging configuration schema."""
    file_level: str = Field(default="INFO", description="Log level for file output")
    console_level: str = Field(default="WARNING", description="Log level for console output")
    format: str = Field(
        default="%(asctime)s %(levelname)s [%(process)d:%(module)s] %(funcName)s: %(message)s",
        description="Log format string"
    )
    rotation: Optional[Dict[str, Any]] = Field(default=None, description="Log rotation settings")
    
    @validator('file_level', 'console_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()

class PathsConfig(BaseModel):
    """Paths configuration schema."""
    modules_root: str = Field(default="modules", description="Directory for user-defined Python modules")
    artifacts_root: str = Field(default="artifacts", description="Root directory for storing experiment artifacts")
    runtime_root: str = Field(default="runtime", description="Root directory for storing runtime data")
    log_dir: str = Field(default="log", description="Directory for log files")
    
    @validator('*')
    def validate_paths(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError("Path must be a non-empty string")
        return v

class RedisConfig(BaseModel):
    """Redis configuration schema."""
    host: str = Field(default="localhost", description="Redis server host")
    port: int = Field(default=6379, description="Redis server port")
    db: int = Field(default=0, description="Redis database number")
    username: Optional[str] = Field(default=None, description="Redis username")
    password: Optional[str] = Field(default=None, description="Redis password")
    key_prefix_user: str = Field(default="expdb@", description="User-specific prefix for Redis keys")
    
    @validator('port')
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

class SchedulerConfig(BaseModel):
    """Scheduler configuration schema."""
    polling_interval_seconds: int = Field(default=5, description="How often the scheduler checks for pending jobs")
    default_worker_queue: str = Field(default="orchestrator_default_worker_queue", description="Default Redis list name for jobs")
    pending_jobs_set_key: str = Field(default="orchestrator_pending_jobs", description="Redis Set key for pending jobs")
    job_status_updates_channel: str = Field(default="job_status_updates", description="Redis Pub/Sub channel for job status updates")
    worker_resources_updates_channel: str = Field(default="worker_resources_updates", description="Redis Pub/Sub channel for worker resource updates")
    max_concurrent_resolutions: Optional[int] = Field(default=None, description="Maximum concurrent dependency resolutions")
    
    @validator('polling_interval_seconds')
    def validate_polling_interval(cls, v):
        if v < 1:
            raise ValueError("Polling interval must be at least 1 second")
        return v

class OrchestratorConfig(BaseModel):
    """Main orchestrator configuration schema."""
    logging: LoggingConfig = Field(default_factory=LoggingConfig, description="Logging configuration")
    paths: PathsConfig = Field(default_factory=PathsConfig, description="Paths configuration")
    redis: RedisConfig = Field(default_factory=RedisConfig, description="Redis configuration")
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig, description="Scheduler configuration")
    
    @root_validator
    def validate_paths_exist(cls, values):
        """Validate that configured paths can be created."""
        paths_config = values.get('paths')
        if paths_config:
            # Check if paths are absolute or relative
            for field_name, path_value in paths_config.dict().items():
                if field_name == 'modules_root' and not os.path.exists(path_value):
                    logger.warning(f"Modules root path does not exist: {path_value}")
                elif field_name == 'artifacts_root' and not os.path.exists(path_value):
                    logger.warning(f"Artifacts root path does not exist: {path_value}")
                elif field_name == 'runtime_root' and not os.path.exists(path_value):
                    logger.warning(f"Runtime root path does not exist: {path_value}")
                elif field_name == 'log_dir' and not os.path.exists(path_value):
                    logger.warning(f"Log directory does not exist: {path_value}")
        return values

class ConfigValidator:
    """Configuration validator and manager."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config.yaml"
        self.config: Optional[OrchestratorConfig] = None
    
    def load_config(self, workspace_path: str = ".") -> OrchestratorConfig:
        """Load and validate configuration from file."""
        config_file_path = Path(workspace_path) / self.config_path
        
        if not config_file_path.exists():
            logger.warning(f"Configuration file not found: {config_file_path}. Using defaults.")
            self.config = OrchestratorConfig()
            return self.config
        
        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            if not config_data:
                logger.warning("Configuration file is empty. Using defaults.")
                self.config = OrchestratorConfig()
                return self.config
            
            # Validate configuration
            self.config = OrchestratorConfig(**config_data)
            logger.info(f"Configuration loaded and validated from {config_file_path}")
            return self.config
            
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration file: {e}")
            raise ValueError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise ValueError(f"Configuration validation failed: {e}")
    
    def get_config(self) -> OrchestratorConfig:
        """Get the current configuration."""
        if self.config is None:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        return self.config
    
    def validate_redis_connection(self) -> bool:
        """Validate Redis connection using current configuration."""
        try:
            import redis
            redis_config = self.config.redis
            
            client = redis.StrictRedis(
                host=redis_config.host,
                port=redis_config.port,
                db=redis_config.db,
                username=redis_config.username,
                password=redis_config.password,
                decode_responses=False
            )
            client.ping()
            logger.info("Redis connection validated successfully")
            return True
        except Exception as e:
            logger.error(f"Redis connection validation failed: {e}")
            return False
    
    def create_directories(self, workspace_path: str = ".") -> None:
        """Create necessary directories based on configuration."""
        if not self.config:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        
        base_path = Path(workspace_path)
        paths_config = self.config.paths
        
        directories_to_create = [
            base_path / paths_config.modules_root,
            base_path / paths_config.artifacts_root,
            base_path / paths_config.runtime_root,
            base_path / paths_config.log_dir,
        ]
        
        for directory in directories_to_create:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def get_default_config_dict(self) -> Dict[str, Any]:
        """Get default configuration as dictionary."""
        return OrchestratorConfig().dict()
    
    def save_default_config(self, workspace_path: str = ".") -> None:
        """Save default configuration to file."""
        config_file_path = Path(workspace_path) / self.config_path
        
        if config_file_path.exists():
            logger.warning(f"Configuration file already exists: {config_file_path}")
            return
        
        default_config = self.get_default_config_dict()
        
        with open(config_file_path, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, default_flow_style=False, indent=2)
        
        logger.info(f"Default configuration saved to {config_file_path}")

# Convenience function for backward compatibility
def load_config(workspace_path: str = ".") -> OrchestratorConfig:
    """Load configuration with validation."""
    validator = ConfigValidator()
    return validator.load_config(workspace_path)