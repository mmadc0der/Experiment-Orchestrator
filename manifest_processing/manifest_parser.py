# m:\Projects\Experiment Orchestrator\manifest_parser.py
import yaml
import os
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

from models.definitions import (
    AnyDefinition, TaskDefinition, ExperimentDefinition, 
    EnvironmentDefinition, DataDefinition, ModelDefinition
)

logger = logging.getLogger(__name__)

class ManifestParseError(Exception):
    """Custom exception for manifest parsing errors."""
    def __init__(self, message: str, file_path: Optional[str] = None, line_number: Optional[int] = None):
        self.message = message
        self.file_path = file_path
        self.line_number = line_number
        super().__init__(self.message)

class ManifestValidationError(Exception):
    """Custom exception for manifest validation errors."""
    def __init__(self, message: str, resource_kind: Optional[str] = None, resource_name: Optional[str] = None, file_path: Optional[str] = None):
        self.message = message
        self.resource_kind = resource_kind
        self.resource_name = resource_name
        self.file_path = file_path
        super().__init__(self.message)

class ManifestParser:
    """
    A parser for YAML manifest files with validation.
    Supports multi-document YAML files and resource validation.
    """
    
    # Resource kind to definition class mapping
    RESOURCE_KINDS = {
        "Task": TaskDefinition,
        "Experiment": ExperimentDefinition,
        "Environment": EnvironmentDefinition,
        "Data": DataDefinition,
        "Model": ModelDefinition,
    }
    
    def __init__(self, validate_schemas: bool = True):
        """
        Initialize the manifest parser.
        
        Args:
            validate_schemas: Whether to validate parsed resources against their schemas
        """
        self.validate_schemas = validate_schemas

    def parse_manifest(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Loads and parses a YAML manifest file with optional validation.

        Args:
            file_path (str): Path to the manifest file.

        Returns:
            List[Dict[str, Any]]: List of parsed and validated resource dictionaries.

        Raises:
            ManifestParseError: If file parsing fails.
            ManifestValidationError: If resource validation fails.
        """
        if not os.path.exists(file_path):
            raise ManifestParseError(f"Manifest file not found: {file_path}", file_path=file_path)
        if not os.path.isfile(file_path):
            raise ManifestParseError(f"The specified path is not a file: {file_path}", file_path=file_path)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                documents = list(yaml.safe_load_all(f))
            
            # Filter out None documents
            documents = [doc for doc in documents if doc is not None]
            
            if not documents:
                logger.warning(f"No valid documents found in manifest file: {file_path}")
                return []
            
            # Validate documents if requested
            if self.validate_schemas:
                validated_documents = []
                for i, doc in enumerate(documents):
                    validated_doc = self._validate_resource(doc, file_path, i + 1)
                    validated_documents.append(validated_doc)
                return validated_documents
            else:
                return documents
                
        except yaml.YAMLError as e:
            raise ManifestParseError(f"YAML parsing error in file {file_path}: {e}", file_path=file_path)
        except ManifestValidationError:
            # Re-raise validation errors as-is
            raise
        except Exception as e:
            raise ManifestParseError(f"Failed to read manifest file {file_path}: {e}", file_path=file_path)

    def parse_manifest_from_string(self, yaml_string: str) -> List[Dict[str, Any]]:
        """
        Parses a YAML string with optional validation.

        Args:
            yaml_string (str): The YAML string content.

        Returns:
            List[Dict[str, Any]]: List of parsed and validated resource dictionaries.

        Raises:
            ManifestParseError: If YAML parsing fails.
            ManifestValidationError: If resource validation fails.
        """
        if not yaml_string or not yaml_string.strip():
            logger.warning("Empty YAML string provided")
            return []
        
        try:
            documents = list(yaml.safe_load_all(yaml_string))
            documents = [doc for doc in documents if doc is not None]
            
            if not documents:
                logger.warning("No valid documents found in YAML string")
                return []
            
            # Validate documents if requested
            if self.validate_schemas:
                validated_documents = []
                for i, doc in enumerate(documents):
                    validated_doc = self._validate_resource(doc, "string_input", i + 1)
                    validated_documents.append(validated_doc)
                return validated_documents
            else:
                return documents
                
        except yaml.YAMLError as e:
            raise ManifestParseError(f"YAML parsing error in string content: {e}")
        except ManifestValidationError:
            # Re-raise validation errors as-is
            raise
        except Exception as e:
            raise ManifestParseError(f"Failed to parse manifest from string: {e}")
    
    def _validate_resource(self, resource_dict: Dict[str, Any], file_path: str, document_number: int) -> Dict[str, Any]:
        """
        Validate a resource dictionary against its schema.
        
        Args:
            resource_dict: The resource dictionary to validate
            file_path: Path to the manifest file (for error reporting)
            document_number: Document number in the file (for error reporting)
            
        Returns:
            The validated resource dictionary
            
        Raises:
            ManifestValidationError: If validation fails
        """
        if not isinstance(resource_dict, dict):
            raise ManifestValidationError(
                f"Resource must be a dictionary, got {type(resource_dict).__name__}",
                file_path=file_path
            )
        
        # Check required fields
        if 'apiVersion' not in resource_dict:
            raise ManifestValidationError(
                "Resource missing required field 'apiVersion'",
                file_path=file_path
            )
        
        if 'kind' not in resource_dict:
            raise ManifestValidationError(
                "Resource missing required field 'kind'",
                file_path=file_path
            )
        
        if 'metadata' not in resource_dict:
            raise ManifestValidationError(
                "Resource missing required field 'metadata'",
                file_path=file_path
            )
        
        if 'spec' not in resource_dict:
            raise ManifestValidationError(
                "Resource missing required field 'spec'",
                file_path=file_path
            )
        
        # Get resource kind and validate
        kind = resource_dict['kind']
        resource_name = resource_dict.get('metadata', {}).get('name', 'unknown')
        
        if kind not in self.RESOURCE_KINDS:
            raise ManifestValidationError(
                f"Unknown resource kind: {kind}. Supported kinds: {list(self.RESOURCE_KINDS.keys())}",
                resource_kind=kind,
                resource_name=resource_name
            )
        
        # Validate against specific schema
        try:
            definition_class = self.RESOURCE_KINDS[kind]
            validated_resource = definition_class(**resource_dict)
            logger.debug(f"Successfully validated {kind} resource: {resource_name}")
            return validated_resource.model_dump()
        except Exception as e:
            raise ManifestValidationError(
                f"Schema validation failed for {kind} resource '{resource_name}': {e}",
                resource_kind=kind,
                resource_name=resource_name
            )
    
    def get_supported_kinds(self) -> List[str]:
        """Get list of supported resource kinds."""
        return list(self.RESOURCE_KINDS.keys())
    
    def validate_single_resource(self, resource_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single resource dictionary.
        
        Args:
            resource_dict: The resource dictionary to validate
            
        Returns:
            The validated resource dictionary
            
        Raises:
            ManifestValidationError: If validation fails
        """
        return self._validate_resource(resource_dict, "single_resource", 1)
