# m:\Projects\Experiment Orchestrator\manifest_parser.py
import yaml
import os

class ManifestParser:
    """
    A parser for YAML manifest files.
    Supports multi-document YAML files.
    """

    def parse_manifest(self, file_path: str) -> list[dict]:
        """
        Loads and parses a YAML manifest file.

        Args:
            file_path (str): Path to the manifest file.

        Returns:
            list[dict]: List of dictionaries, where each dictionary represents
                        one document from the YAML file.

        Raises:
            FileNotFoundError: If the file is not found at the specified path.
            yaml.YAMLError: If an error occurs during YAML parsing.
            Exception: Other unexpected errors during file reading.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Manifest file not found: {file_path}")
        if not os.path.isfile(file_path):
            raise ValueError(f"The specified path is not a file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Используем safe_load_all для поддержки многодокументных файлов
                documents = list(yaml.safe_load_all(f))
            # Фильтруем пустые документы, которые могут появиться из-за '---' в конце файла
            return [doc for doc in documents if doc is not None]
        except yaml.YAMLError as e:
            # Добавляем имя файла к сообщению об ошибке для лучшей диагностики
            raise yaml.YAMLError(f"YAML parsing error in file {file_path}: {e}")
        except Exception as e:
            raise Exception(f"Failed to read manifest file {file_path}: {e}")

    def parse_manifest_from_string(self, yaml_string: str) -> list[dict]:
        """
        Parses a YAML string. Supports multi-document YAML strings.

        Args:
            yaml_string (str): The YAML string content.

        Returns:
            list[dict]: List of dictionaries, where each dictionary represents
                        one document from the YAML string.

        Raises:
            yaml.YAMLError: If an error occurs during YAML parsing.
        """
        if not yaml_string or not yaml_string.strip():
            # Возвращаем пустой список, если строка пустая или состоит только из пробельных символов
            return []
        try:
            documents = list(yaml.safe_load_all(yaml_string))
            # Фильтруем пустые документы
            return [doc for doc in documents if doc is not None]
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"YAML parsing error in string content: {e}")
        except Exception as e: # Ловим другие возможные ошибки, хотя safe_load_all обычно кидает YAMLError
            raise Exception(f"Failed to parse manifest from string: {e}")
