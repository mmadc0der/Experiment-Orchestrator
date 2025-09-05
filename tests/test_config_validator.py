"""
Tests for the configuration validator module.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from pydantic import ValidationError

from config_validator import (
    ConfigValidator, OrchestratorConfig, LoggingConfig, 
    PathsConfig, RedisConfig, SchedulerConfig
)


class TestLoggingConfig:
    """Test LoggingConfig validation."""
    
    def test_valid_logging_config(self):
        """Test valid logging configuration."""
        config = LoggingConfig(
            file_level="DEBUG",
            console_level="INFO",
            format="%(asctime)s %(levelname)s %(message)s"
        )
        assert config.file_level == "DEBUG"
        assert config.console_level == "INFO"
    
    def test_invalid_log_level(self):
        """Test invalid log level raises validation error."""
        with pytest.raises(ValidationError):
            LoggingConfig(file_level="INVALID")
    
    def test_log_level_case_insensitive(self):
        """Test that log levels are converted to uppercase."""
        config = LoggingConfig(file_level="debug", console_level="info")
        assert config.file_level == "DEBUG"
        assert config.console_level == "INFO"


class TestPathsConfig:
    """Test PathsConfig validation."""
    
    def test_valid_paths_config(self):
        """Test valid paths configuration."""
        config = PathsConfig(
            modules_root="modules",
            artifacts_root="artifacts",
            runtime_root="runtime",
            log_dir="logs"
        )
        assert config.modules_root == "modules"
        assert config.artifacts_root == "artifacts"
    
    def test_empty_path_raises_error(self):
        """Test that empty paths raise validation error."""
        with pytest.raises(ValidationError):
            PathsConfig(modules_root="")


class TestRedisConfig:
    """Test RedisConfig validation."""
    
    def test_valid_redis_config(self):
        """Test valid Redis configuration."""
        config = RedisConfig(
            host="localhost",
            port=6379,
            db=0,
            username="user",
            password="pass",
            key_prefix_user="test@"
        )
        assert config.host == "localhost"
        assert config.port == 6379
    
    def test_invalid_port_raises_error(self):
        """Test that invalid port raises validation error."""
        with pytest.raises(ValidationError):
            RedisConfig(port=70000)
    
    def test_default_values(self):
        """Test default values."""
        config = RedisConfig()
        assert config.host == "localhost"
        assert config.port == 6379
        assert config.db == 0


class TestSchedulerConfig:
    """Test SchedulerConfig validation."""
    
    def test_valid_scheduler_config(self):
        """Test valid scheduler configuration."""
        config = SchedulerConfig(
            polling_interval_seconds=5,
            default_worker_queue="test_queue",
            pending_jobs_set_key="test_pending"
        )
        assert config.polling_interval_seconds == 5
    
    def test_invalid_polling_interval_raises_error(self):
        """Test that invalid polling interval raises validation error."""
        with pytest.raises(ValidationError):
            SchedulerConfig(polling_interval_seconds=0)


class TestOrchestratorConfig:
    """Test OrchestratorConfig validation."""
    
    def test_valid_orchestrator_config(self):
        """Test valid orchestrator configuration."""
        config = OrchestratorConfig()
        assert isinstance(config.logging, LoggingConfig)
        assert isinstance(config.paths, PathsConfig)
        assert isinstance(config.redis, RedisConfig)
        assert isinstance(config.scheduler, SchedulerConfig)
    
    def test_config_with_custom_values(self):
        """Test configuration with custom values."""
        config = OrchestratorConfig(
            logging=LoggingConfig(file_level="DEBUG"),
            redis=RedisConfig(host="redis.example.com", port=6380)
        )
        assert config.logging.file_level == "DEBUG"
        assert config.redis.host == "redis.example.com"
        assert config.redis.port == 6380


class TestConfigValidator:
    """Test ConfigValidator functionality."""
    
    def test_load_config_from_file(self, temp_workspace, test_config):
        """Test loading configuration from file."""
        # Create config file
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(test_config.dict(), f)
        
        validator = ConfigValidator()
        loaded_config = validator.load_config(str(temp_workspace))
        
        assert loaded_config.logging.file_level == test_config.logging.file_level
        assert loaded_config.redis.host == test_config.redis.host
    
    def test_load_config_file_not_found(self, temp_workspace):
        """Test loading configuration when file doesn't exist."""
        validator = ConfigValidator()
        config = validator.load_config(str(temp_workspace))
        
        # Should return default configuration
        assert isinstance(config, OrchestratorConfig)
        assert config.redis.host == "localhost"
    
    def test_load_config_invalid_yaml(self, temp_workspace):
        """Test loading configuration with invalid YAML."""
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        validator = ConfigValidator()
        with pytest.raises(ValueError, match="Invalid YAML"):
            validator.load_config(str(temp_workspace))
    
    def test_load_config_validation_error(self, temp_workspace):
        """Test loading configuration with validation errors."""
        config_path = temp_workspace / "config.yaml"
        invalid_config = {
            "redis": {
                "port": "invalid_port"  # Should be integer
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(invalid_config, f)
        
        validator = ConfigValidator()
        with pytest.raises(ValueError, match="Configuration validation failed"):
            validator.load_config(str(temp_workspace))
    
    def test_create_directories(self, temp_workspace, test_config):
        """Test directory creation."""
        validator = ConfigValidator()
        validator.config = test_config
        validator.create_directories(str(temp_workspace))
        
        # Check that directories were created
        assert (temp_workspace / "modules").exists()
        assert (temp_workspace / "artifacts").exists()
        assert (temp_workspace / "runtime").exists()
        assert (temp_workspace / "log").exists()
    
    def test_get_default_config_dict(self):
        """Test getting default configuration as dictionary."""
        validator = ConfigValidator()
        default_config = validator.get_default_config_dict()
        
        assert isinstance(default_config, dict)
        assert "logging" in default_config
        assert "paths" in default_config
        assert "redis" in default_config
        assert "scheduler" in default_config
    
    def test_save_default_config(self, temp_workspace):
        """Test saving default configuration."""
        validator = ConfigValidator()
        validator.save_default_config(str(temp_workspace))
        
        config_path = temp_workspace / "config.yaml"
        assert config_path.exists()
        
        # Verify the file contains valid YAML
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        assert "logging" in config_data
    
    def test_validate_redis_connection_mock(self, test_config):
        """Test Redis connection validation with mock."""
        validator = ConfigValidator()
        validator.config = test_config
        
        # Mock redis import and client
        with pytest.raises(ImportError):
            validator.validate_redis_connection()
    
    def test_config_with_empty_file(self, temp_workspace):
        """Test configuration with empty file."""
        config_path = temp_workspace / "config.yaml"
        with open(config_path, 'w') as f:
            f.write("")
        
        validator = ConfigValidator()
        config = validator.load_config(str(temp_workspace))
        
        # Should return default configuration
        assert isinstance(config, OrchestratorConfig)
    
    def test_config_with_none_values(self, temp_workspace):
        """Test configuration with None values."""
        config_path = temp_workspace / "config.yaml"
        config_data = {
            "redis": {
                "username": None,
                "password": None
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
        
        validator = ConfigValidator()
        config = validator.load_config(str(temp_workspace))
        
        assert config.redis.username is None
        assert config.redis.password is None