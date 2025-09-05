# Experiment Orchestrator - Project Documentation

## Project Overview

The Experiment Orchestrator is a scalable machine learning experiment management system designed to handle complex ML workflows through declarative YAML manifests. The system provides a Kubernetes-like API for defining and managing ML experiments, data sources, models, and task definitions.

## Architecture

### Core Components

1. **Orchestrator Core** (`orchestrator_core.py`)
   - Main FastAPI application
   - REST API endpoints for manifest processing
   - Configuration management
   - Service orchestration

2. **Scheduler** (`scheduler.py`)
   - Job scheduling and queue management
   - Redis-based job distribution
   - Pub/Sub messaging for status updates
   - Worker resource management

3. **Manifest Processing**
   - **Parser** (`manifest_processing/manifest_parser.py`): YAML manifest parsing
   - **Expander** (`manifest_processing/manifest_expander.py`): Parameter expansion and templating

4. **Data Models**
   - **Definitions** (`models/definitions.py`): Resource definitions (Environment, Data, Model, TaskDefinition, Experiment)
   - **Instances** (`models/instances.py`): Runtime instances (ExperimentInstance, TaskInstance, JobInstance)

5. **Broker System** (`brokers/redis_broker.py`)
   - Redis-based message broker
   - Job queue management
   - Status tracking and persistence

6. **Job System** (`modules/jobs/`)
   - Abstract job interface
   - Example implementations (SumJob)
   - Job context and parameter validation

## API Resources

### Resource Types

1. **Environment**: Execution environment definitions (Docker, Conda, venv, bare metal)
2. **Data**: Data source definitions and loading configurations
3. **Model**: Model architecture definitions and initialization
4. **TaskDefinition**: Task templates with parameter schemas
5. **Experiment**: Workflow definitions with parameterization and pipelines
6. **Instances**: Runtime instances created from definitions

### API Version
- Current: `orchestrator.windsurf.ai/v1alpha1`
- Target: `orchestrator.windsurf.ai/v1alpha2`

## Key Features

### Declarative Configuration
- YAML-based resource definitions
- Multi-document manifest support
- Parameter templating with Jinja2
- Resource references and dependencies

### Scalable Execution
- Redis-based job queuing
- Worker pool management
- Resource-aware scheduling
- Priority-based job execution

### Experiment Management
- Parameter space exploration
- Pipeline orchestration
- Artifact management
- Status tracking and monitoring

## Current Implementation Status

### ✅ Completed
- Basic project structure and architecture
- YAML manifest parsing
- Pydantic data models for definitions and instances
- Redis broker implementation
- Basic scheduler framework
- CLI tool (expctl) for manifest application
- Example job implementations
- Configuration management

### 🚧 In Progress
- Manifest expansion and parameter resolution
- Job execution and scheduling
- Worker system implementation

### ❌ Missing
- Complete worker system
- Job execution engine
- Error handling and recovery
- Monitoring and observability
- Comprehensive testing
- Documentation and examples
- CI/CD pipeline

## Dependencies

### Core Dependencies
- `PyYAML>=6.0`: YAML processing
- `fastapi>=0.100.0`: Web framework
- `uvicorn[standard]>=0.22.0`: ASGI server
- `Jinja2>=3.0`: Template engine
- `click>=8.0`: CLI framework
- `requests>=2.20`: HTTP client
- `colorama>=0.4`: Terminal colors

### Missing Dependencies
- `redis`: Redis client (currently missing from requirements.txt)
- `pydantic`: Data validation (likely installed as FastAPI dependency)

## Configuration

The system uses `config.yaml` for configuration with the following sections:

- **logging**: Log levels and formatting
- **paths**: Directory structure for modules, artifacts, runtime data
- **redis**: Redis connection settings
- **scheduler**: Job scheduling configuration

## File Structure

```
.
├── orchestrator_core.py          # Main FastAPI application
├── scheduler.py                  # Job scheduler
├── expctl.py                     # CLI tool
├── logger.py                     # Logging configuration
├── requirements.txt              # Python dependencies
├── config.example.yaml          # Configuration template
├── README.md                     # Project documentation
├── brokers/
│   └── redis_broker.py          # Redis message broker
├── manifest_processing/
│   ├── manifest_parser.py       # YAML parser
│   └── manifest_expander.py     # Parameter expansion
├── models/
│   ├── definitions.py           # Resource definitions
│   └── instances.py             # Runtime instances
├── modules/
│   └── jobs/
│       ├── job.py               # Abstract job interface
│       └── sum_job.py           # Example job implementation
└── manifests/
    └── example.manifest.yaml    # Example manifests
```

## Known Issues and TODOs

1. **Missing Redis dependency** in requirements.txt
2. **Incomplete manifest expansion** logic with parameter resolution
3. **Missing worker system** for actual job execution
4. **Incomplete job execution** and scheduling implementation
5. **Missing error handling** and recovery mechanisms
6. **No monitoring** or observability features
7. **Missing test suite**
8. **Incomplete documentation** and examples

## Development Notes

- The project follows a microservices-like architecture with clear separation of concerns
- Uses Pydantic for data validation and serialization
- Implements a plugin-based job system for extensibility
- Designed for horizontal scaling through Redis-based job distribution
- Follows Kubernetes-like resource management patterns