import click
import requests
import os
import glob
import yaml

ORCHESTRATOR_API_BASE_URL = "http://127.0.0.1:8000/api/v1alpha2"  # Updated to v1alpha2

@click.group()
def expctl():
    """A CLI tool to interact with the Experiment Orchestrator."""
    pass

@expctl.command()
@click.option('-f', '--filename', 
              type=click.Path(exists=True), 
              required=True, 
              help='Path to a YAML manifest file or a directory containing YAML manifests.')
@click.option('--api-url', default=ORCHESTRATOR_API_BASE_URL, help='Base URL for the Orchestrator API.')
def apply(filename: str, api_url: str):
    """Apply a manifest or all manifests in a directory to the orchestrator."""
    files_to_process = []
    if os.path.isdir(filename):
        click.echo(f"Processing directory: {filename}")
        yaml_files = glob.glob(os.path.join(filename, '*.yaml'))
        yml_files = glob.glob(os.path.join(filename, '*.yml'))
        files_to_process.extend(yaml_files)
        files_to_process.extend(yml_files)
        if not files_to_process:
            click.secho(f"No YAML or YML files found in directory: {filename}", fg='yellow')
            return
    elif os.path.isfile(filename):
        if not (filename.lower().endswith('.yaml') or filename.lower().endswith('.yml')):
            click.secho(f"File '{filename}' is not a YAML or YML file.", fg='red')
            return
        files_to_process.append(filename)
    else:
        click.secho(f"Path '{filename}' is not a valid file or directory.", fg='red')
        return

    for file_path in files_to_process:
        click.echo(f"--- Processing file: {file_path} ---")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                manifest_content_str = f.read()
            
            # YAML может содержать несколько документов, разделенных '---'
            # requests будет отправлять строку, а серверный парсер разберет ее на документы
            
            headers = {"Content-Type": "application/json"}
            body = {"manifest_content": manifest_content_str}
            
            process_url = f"{api_url}/manifests/process"
            click.echo(f"Sending manifest to {process_url}")
            
            response = requests.post(process_url, json=body, headers=headers)
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    click.secho(f"Successfully applied: {file_path}", fg='green')
                    click.echo(f"Server response: {response_data.get('message', 'OK')}")
                    if response_data.get('details'):
                        click.echo(f"Details: {response_data['details']}")
                except requests.exceptions.JSONDecodeError:
                     click.secho(f"Successfully applied (status 200), but could not decode JSON response: {file_path}", fg='yellow')
                     click.echo(f"Raw response: {response.text[:200]}...") # Показать часть ответа
            else:
                click.secho(f"Error applying {file_path}. Status: {response.status_code}", fg='red')
                try:
                    error_data = response.json()
                    click.echo(f"Error details: {error_data.get('detail', response.text)}")
                except requests.exceptions.JSONDecodeError:
                    click.echo(f"Raw error response: {response.text}")
        
        except FileNotFoundError:
            click.secho(f"File not found: {file_path}", fg='red')
        except yaml.YAMLError as e:
            click.secho(f"Error parsing YAML file {file_path}: {e}", fg='red')
        except requests.exceptions.RequestException as e:
            click.secho(f"Error connecting to orchestrator API at {api_url}: {e}", fg='red')
        except Exception as e:
            click.secho(f"An unexpected error occurred while processing {file_path}: {e}", fg='red')
        click.echo("--- End of file processing ---\n")

if __name__ == '__main__':
    expctl()
