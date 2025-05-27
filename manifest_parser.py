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

if __name__ == "__main__":
    parser = ManifestParser()
    
    # Тестирование парсинга из строки
    print("--- Testing parsing from string ---")
    test_yaml_string_single = """
kind: Test
apiVersion: v1
metadata:
  name: test-resource
spec:
  param: value
"""
    test_yaml_string_multi = """
kind: TestDoc1
apiVersion: v1
metadata:
  name: test-doc1
---
kind: TestDoc2
apiVersion: v1
metadata:
  name: test-doc2
spec:
  data: example
"""
    test_yaml_string_empty = ""
    test_yaml_string_invalid = "kind: Test\n  bad-indent: true"

    try:
        print("\nParsing single document string:")
        parsed_single = parser.parse_manifest_from_string(test_yaml_string_single)
        print(yaml.dump(parsed_single, allow_unicode=True, sort_keys=False, indent=2))
        
        print("\nParsing multi-document string:")
        parsed_multi = parser.parse_manifest_from_string(test_yaml_string_multi)
        print(yaml.dump(parsed_multi, allow_unicode=True, sort_keys=False, indent=2))

        print("\nParsing empty string:")
        parsed_empty = parser.parse_manifest_from_string(test_yaml_string_empty)
        print(f"Parsed empty string result: {parsed_empty}")

        print("\nParsing invalid string (expecting error):")
        parser.parse_manifest_from_string(test_yaml_string_invalid) # Это должно вызвать ошибку

    except yaml.YAMLError as e:
        print(f"Caught expected YAML error: {e}")
    except Exception as e:
        print(f"Unexpected error during string parsing test: {e}")
    
    print("\n--- Finished testing parsing from string ---\n")

    # Существующий код для тестирования parse_manifest из файла
    try:
        manifest_path = input("Enter the path to the manifest file (e.g., manifests/simple.yaml or configs/example.manifest.yaml, press Enter for default): ")
        
        if not manifest_path:
            # Попробуем найти simple.yaml или example.manifest.yaml в стандартных местах
            default_paths_to_try = [
                os.path.join("manifests", "simple.yaml"),
                os.path.join("manifests", "example.manifest.yaml"), # Если simple.yaml нет
                os.path.join("configs", "example.manifest.yaml") # Старый путь
            ]
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir) # Предполагаем, что скрипт в корне или поддиректории

            found_default = False
            for dp in default_paths_to_try:
                # Сначала проверяем относительно корня проекта
                abs_dp_project = os.path.join(project_root, dp)
                if os.path.exists(abs_dp_project):
                    manifest_path = abs_dp_project
                    found_default = True
                    break
                # Затем относительно текущей рабочей директории (если вдруг запускают из корня)
                abs_dp_cwd = os.path.join(os.getcwd(), dp)
                if os.path.exists(abs_dp_cwd):
                    manifest_path = abs_dp_cwd
                    found_default = True
                    break
            
            if found_default:
                print(f"Path not specified, using default value: {manifest_path}")
            else:
                print(f"Path not specified, and no default manifest found in expected locations.")
                exit()


        if not os.path.isabs(manifest_path) and not (manifest_path.startswith("./") or manifest_path.startswith(".\\")):
            # Если путь не абсолютный и не явно относительный, попробуем сделать его абсолютным от корня проекта
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir) 
            
            potential_path_from_root = os.path.join(project_root, manifest_path)
            potential_path_from_cwd = os.path.join(os.getcwd(), manifest_path)

            if os.path.exists(potential_path_from_root):
                manifest_path = potential_path_from_root
                print(f"Relative path '{os.path.basename(manifest_path)}' resolved to '{manifest_path}' (project root).")
            elif os.path.exists(potential_path_from_cwd):
                 manifest_path = potential_path_from_cwd
                 print(f"Relative path '{os.path.basename(manifest_path)}' resolved to '{manifest_path}' (current dir).")
            # Если не нашли, оставим как есть, FileNotFoundError сработает позже

        if not os.path.exists(manifest_path):
            print(f"Error: File '{manifest_path}' not found.")
        else:
            print(f"\nParsing file: {manifest_path}\n")
            parsed_data = parser.parse_manifest(manifest_path)
            
            print("Parsing result:")
            for i, doc in enumerate(parsed_data):
                print(f"\n--- Document {i+1} ---")
                print(yaml.dump(doc, allow_unicode=True, sort_keys=False, indent=2))
            
            print(f"\nSuccessfully parsed {len(parsed_data)} documents.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Error: {e}")
    except yaml.YAMLError as e:
        print(f"YAML parsing error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
