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
            raise yaml.YAMLError(f"YAML parsing error in file {file_path}: {e}")
        except Exception as e:
            raise Exception(f"Failed to read manifest file {file_path}: {e}")

if __name__ == "__main__":
    parser = ManifestParser()
    
    try:
        manifest_path = input("Enter the path to the manifest file (e.g., configs/example.manifest.yaml): ")
        
        # For convenience, if the path is not specified, use the default value
        if not manifest_path:
            default_path = os.path.join("configs", "example.manifest.yaml")
            print(f"Path not specified, using default value: {default_path}")
            manifest_path = default_path

        if not os.path.isabs(manifest_path):
            # Convert relative path to absolute path from the script directory
            # This is useful if the script is not run from the root of the project
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir) # Assuming the script is in a subdirectory of the project
            if manifest_path.startswith("configs/") or manifest_path.startswith("configs\\"):
                 # If the path starts with configs/, consider it from the root of the project
                manifest_path_abs = os.path.join(project_root, manifest_path)
            else:
                # Otherwise, consider it from the directory where the script is located
                manifest_path_abs = os.path.join(script_dir, manifest_path)
            
            # Check if the file exists at the absolute path, if not - try from the root of the project
            if not os.path.exists(manifest_path_abs) and (manifest_path.startswith("configs/") or manifest_path.startswith("configs\\")):
                 # If the path starts with configs/, consider it from the root of the project
                manifest_path_abs = os.path.join(os.getcwd(), manifest_path) # Пробуем от CWD, если запуск из корня
            
            print(f"Relative path '{manifest_path}' transformed to '{manifest_path_abs}'")
            manifest_path = manifest_path_abs


        if not os.path.exists(manifest_path):
            print(f"Error: File '{manifest_path}' not found.")
        else:
            print(f"\nParsing file: {manifest_path}\n")
            parsed_data = parser.parse_manifest(manifest_path)
            
            print("Parsing result:")
            for i, doc in enumerate(parsed_data):
                print(f"\n--- Document {i+1} ---")
                # Use yaml.dump to print the dictionary as YAML
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
