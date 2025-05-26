# Experiment Orchestrator

A project for creating a scalable experiment orchestrator for machine learning.

## Main Concepts

The orchestrator operates on a set of declarative API resources that describe various aspects of an ML experiment. Users define these resources in YAML manifests.

### Resources

Central elements of the system are resources. Each resource has:
- `apiVersion`: API version to which the resource belongs (e.g., `orchestrator.windsurf.ai/v1alpha1`).
- `kind`: Resource type (e.g., `Environment`, `Model`, `Experiment`).
- `metadata`: Resource metadata, including `name` (unique name) and `labels` (tags for grouping and filtering).
- `spec`: Resource specification containing its configuration and parameters.

### Definitions vs. Instances

- **Definitions/Templates**: These are resources that describe *how* something should be created or configured. For example, a `Model` definition describes the architecture of a model but does not contain trained weights. Examples: `Environment`, `Data`, `Model`, `TaskDefinition`.
- **Instances**: These are specific results obtained during the execution of tasks (e.g., tasks from `Experiment`). They represent "materialized" definitions with saved state (e.g., a trained model with weights, processed dataset). The orchestrator typically creates them automatically. Example: `ModelInstance`.

### Experiments and Parameterization

The `Experiment` resource allows defining and running complex workflows, including:
- Running multiple tasks with different parameter sets (e.g., for hyperparameter tuning).
- Organizing tasks in sequences or dependency graphs (pipelines).
- Automatic creation and naming of resource instances based on task execution results.

## API Resources

Below is the description of the main API resources defined in the system. Complete examples can be found in `configs/example.manifest.yaml`.

### Common Manifest Structure

Each resource in a YAML file is defined with the following structure:

```yaml
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: <KindName>
metadata:
  name: <unique-resource-name>
  labels:
    key1: value1
    key2: value2
spec:
  # ... resource specification ...
```

---

### 1. Environment

Describes the execution environment for experiment tasks.

- **`kind: Environment`**
- **`spec` fields:**
  - `description` (string, optional): Environment description.
  - `type` (string, required): Environment type. Possible values: `docker`, `conda`, `venv`, `bare_metal`.
  - `image` (string, optional): Имя Docker-образа (если `type: "docker"`).
  - `pythonVersion` (string, optional): Python version (if `type: "conda"` or `"venv"`).
  - `condaEnvName` (string, optional): Conda environment name (if `type: "conda"`).
  - `venvPath` (string, optional): Path to virtual environment (if `type: "venv"`).
  - `requirementsFile` (string, optional): Path to dependency file (e.g., `requirements.txt`).
  - `setupCommands` (list of strings, optional): Additional commands for environment setup.
  - `envVariables` (list of objects, optional): Environment variables for tasks.
    - `name` (string, required): Variable name.
    - `value` (string, required): Variable value.

Example:
```yaml
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Environment
metadata:
  name: python-3.9-pytorch-cuda11
spec:
  type: "docker"
  image: "pytorch/pytorch:1.10.1-cuda11.3-cudnn8-runtime"
  requirementsFile: "requirements/pytorch_env.txt"
```

---

### 2. Data

Describes the data source, its loading method, and configuration.

- **`kind: Data`**
- **`spec` fields:**
  - `description` (string, optional): Dataset description.
  - `type` (string, required): Type of data source (e.g., `torchvision_dataset`, `local_files`, `s3_bucket`).
  - `uri` (string, optional): URI to data (e.g., path to file/directory, S3 URI).
  - `format` (string, optional): Data format (e.g., `parquet`, `csv`).
  - `loaderModule` (string, optional): Path to Python module/class for loading data (e.g., `torchvision.datasets.CIFAR10`).
  - `loaderFunction` (string, optional): Name of function in `loaderModule` for loading data.
  - `config` (object, optional): Parameters passed to `loaderModule` or `loaderFunction`.
  - `schema` (object, optional): Data schema description.

Example:
```yaml
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Data
metadata:
  name: cifar10-dataset
spec:
  type: "torchvision_dataset"
  loaderModule: "torchvision.datasets.CIFAR10"
  config:
    root: "./data_cache/cifar10"
    train: true
    download: true
    transform:
      modulePath: "torchvision.transforms.Compose"
      # ... параметры для transform ...
```

---

### 3. Model

Describes the architecture of the model, its initialization method, and configuration. This is a model definition, not a trained instance.

- **`kind: Model`**
- **`spec` fields:**
  - `description` (string, optional): Model description.
  - `sourceType` (string, required): Type of model source (e.g., `module`, `pretrained_hub`, `onnx_file`).
  - `modulePath` (string, optional): Path to Python module/class for model initialization (if `sourceType: "module"`).
  - `functionName` (string, optional): Name of factory function in `modulePath`.
  - `config` (object, optional): Parameters passed to model constructor or factory function.
  - `input_signature` (list of objects, optional): Description of expected input tensors.
  - `output_signature` (list of objects, optional): Description of output tensors.

Example:
```yaml
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Model
metadata:
  name: resnet18-cifar10
spec:
  sourceType: "module"
  modulePath: "torchvision.models.resnet18"
  config:
    pretrained: false
    num_classes: 10
```

---

### 4. TaskDefinition

Describes a template for a specific operation (task), such as training, evaluation, data preprocessing, etc. It is not the task itself, but its definition.

- **`kind: TaskDefinition`**
- **`spec` fields:**
  - `description` (string, optional): Task description.
  - `executorModule` (string, required): Path to Python module and function that will execute the task logic (e.g., `my_tasks.training:run_train_task`).
  - `parametersSchema` (object, optional): JSON Schema, describing parameters accepted by the task (e.g., `epochs`, `learning_rate`).
  - `inputsSchema` (object, optional): JSON Schema, describing expected input resources/artifacts (e.g., links to `Model`, `Data`, `Environment`).
  - `outputsSchema` (object, optional): JSON Schema, describing artifacts produced by the task (e.g., path to trained model, metrics).

Example:
```yaml
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: TaskDefinition
metadata:
  name: pytorch-train-task
spec:
  executorModule: "orchestrator_tasks.pytorch.train:run_training_task"
  parametersSchema:
    type: object
    properties:
      epochs: { type: "integer", default: 10 }
      # ...
  inputsSchema:
    type: object
    properties:
      model: { type: "string" }
      # ...
  outputsSchema:
    type: object
    properties:
      trained_model_path: { type: "string" }
      # ...
```

---

### 5. Experiment

Defines one or more task runs, possibly with parameterization, for conducting an ML experiment.

- **`kind: Experiment`**
- **`spec` fields:**
  - `description` (string, optional): Experiment description.
  - `commonResourceRefs` (object, optional): Links (by `metadata.name`) to common resources (`Model`, `Data`, `Environment`, `TaskDefinition`), used in experiment tasks.
  - `parameterSpace` (object, optional): Defines parameter space for creating multiple runs (e.g., `matrix` for parameter combinations).
  - `instanceNameTemplate` (string, optional): Template for naming created resource instances (e.g., `ModelInstance`).
  - `pipeline` (list of objects, required): Sequence or DAG of tasks to execute.
    - `name` (string, required): Logical name of the step in the pipeline.
    - `taskDefinitionRef` (string, required): Link to `TaskDefinition`.
    - `inputs` (object, optional): Input resources/artifacts for the task (links to `commonResourceRefs` or outputs of previous steps).
    - `parameters` (object, optional): Concrete parameters for the task, may use values from `parameterSpace`.
    - `outputsToRegister` (object, optional): Description of which task outputs should be registered as new resource instances (e.g., `ModelInstance`).

Example (fragment):
```yaml
apiVersion: orchestrator.windsurf.ai/v1alpha1
kind: Experiment
metadata:
  name: resnet18-lr-sweep-cifar10-01
spec:
  commonResourceRefs:
    modelDefinition: "resnet18-cifar10"
    trainingTaskDefinition: "pytorch-train-task"
    # ...
  parameterSpace:
    matrix:
      - learning_rate: 0.01
      - learning_rate: 0.001
  pipeline:
    - name: "train-resnet"
      taskDefinitionRef: $(commonResourceRefs.trainingTaskDefinition)
      inputs:
        model: $(commonResourceRefs.modelDefinition)
        # ...
      parameters:
        learning_rate: $(Param.learning_rate)
        epochs: 20
      outputsToRegister:
        trained_model_path:
          kind: ModelInstance
```

---

### 6. ModelInstance (and other instances)

*Conceptual resource created by the orchestrator.* Represents a specific, trained (or being trained/processed) instance of a resource, obtained as a result of executing a task from `Experiment`.

- **`kind: ModelInstance`** (or `DataInstance`, `Artifact`, etc.)
- **`metadata`:**
  - `name`: Generated by the orchestrator, often based on `instanceNameTemplate` from `Experiment`.
  - `labels`: May include `experimentName`, `sourceModelDefinition`, `taskRunId`.
- **`spec` fields:**
  - `modelDefinitionRef` (string): Link to the original `Model` definition.
  - `environmentRef` (string): Link to the used `Environment`.
  - `effectiveConfig` (object): Snapshot of the full configuration used to create this instance (including parameters from `ModelDefinition` and `Experiment`).
  - `artifacts` (object): Paths to saved artifacts (e.g., `weightsPath`, `logsPath`).
  - `metrics` (object): Metrics recorded for this instance.
  - `inputs` (object, optional): Links to used input data (e.g., `DataInstanceRef`).
- **`status` fields:**
  - `phase` (string): Current phase of the lifecycle (e.g., `Pending`, `Running`, `Completed`, `Failed`).
  - `startTime` (timestamp): Start time.
  - `completionTime` (timestamp, optional): Completion time.
  - `message` (string, optional): Status message.


## Installation

(Will be added later)

## CLI Usage

(Will be added later)

## Architecture

(Will be added later)
