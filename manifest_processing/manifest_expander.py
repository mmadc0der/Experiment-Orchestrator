import itertools
import uuid
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Union
import jinja2
from jinja2 import Environment, select_autoescape

# Предполагаем, что models находится на том же уровне, что и manifest_processing
import os

ARTIFACT_BASE_PATH = "./outputs/artifacts"

logger = logging.getLogger(__name__)

def generate_artifact_uri(
    experiment_id: str,
    task_instance_id: str,
    job_instance_id: str,
    output_name_template: str
) -> str:
    """
    Generates a URI-like path for an artifact.
    Example: ./outputs/artifacts/exp-id/task-id/job-id/model.pkl
    """
    if output_name_template.startswith('/'):
        output_name_template = output_name_template[1:]
    return os.path.join(
        ARTIFACT_BASE_PATH,
        experiment_id,
        task_instance_id,
        job_instance_id,
        output_name_template
    ).replace('\\', '/')

from models.definitions import (
    ExperimentDefinition,
    TaskDefinition,
    AnyDefinition, # Для предварительного парсинга
    # ... другие необходимые модели из definitions
)
from models.instances import (
    ExperimentInstance,
    TaskInstance,
    JobInstance,
    JobInstanceContext,
    InstanceStatus,
    # ... другие необходимые модели из instances
)
# Если ManifestParser будет перемещен сюда же:
# from .manifest_parser import ManifestParser

class ManifestExpander:
    # Class-level constants for reference handling
    REFERENCE_PREFIX = "ref::"
    PARAM_REF_PATTERN = re.compile(r"^{{\s*parameters\.(?P<name>[a-zA-Z0-9_]+)\s*}}$")
    JOB_OUTPUT_REF_PATTERN = re.compile(r"^{{\s*job\.outputs\.(?P<name>[a-zA-Z0-9_]+)\s*}}$")
    STEP_OUTPUT_REF_PATTERN = re.compile(r"^{{\s*(?P<step_name>[a-zA-Z0-9_-]+)\.outputs\.(?P<output_name>[a-zA-Z0-9_]+)\s*}}$")

    def __init__(self, raw_manifest_docs: List[Dict[str, Any]]):
        """
        Initializes the ManifestExpander with raw documents parsed from a YAML manifest.

        Args:
            raw_manifest_docs: A list of dictionaries, where each dictionary
                               represents a single document from the YAML manifest.
        """
        self.raw_docs = raw_manifest_docs
        self.experiment_def: Optional[ExperimentDefinition] = None
        self.task_defs: Dict[str, TaskDefinition] = {}
        self.override_params: Dict[str, Any] = {} # Added to store override_params
        self.experiment_instance_id: Optional[str] = None # Added to store experiment_instance_id

        # Initialize Jinja2 environment
        self.jinja_env = Environment(undefined=jinja2.Undefined)  # Changed from StrictUndefined

        self._parse_and_categorize_definitions()

    def _construct_reference(self, context_parts: List[str]) -> str:
        """
        Constructs a standardized reference string.
        Example: ref::experiment_id:task_id:step_name:outputs:output_name
        """
        return f"{ManifestExpander.REFERENCE_PREFIX}{':'.join(context_parts)}"

    def _generate_instance_id(self, name_from_metadata: str, entity_type_prefix: str = "") -> str:
        """
        Generates a unique instance ID in the format 'name-suffix' or 'prefix-name-suffix'.
        Example: 'my-experiment-a1b2c3d4'
        """
        # Обеспечиваем, чтобы имя было безопасным для использования в ID
        safe_name = "".join(c if c.isalnum() or c == '-' else '-' for c in name_from_metadata.lower()).strip('-')
        
        # Генерируем короткий UUID-суффикс
        suffix = uuid.uuid4().hex[:8] # Можно настроить длину

        if entity_type_prefix:
            return f"{entity_type_prefix}-{safe_name}-{suffix}"
        return f"{safe_name}-{suffix}"

    def _parse_and_categorize_definitions(self):
        """
        Parses raw documents into Pydantic models (ExperimentDefinition, TaskDefinition)
        and categorizes them.
        """
        found_experiment_defs = []
        
        for doc_data in self.raw_docs:
            # Сначала парсим как AnyDefinition, чтобы получить kind и metadata.name
            # Это позволяет избежать ошибок, если spec не соответствует ожидаемому сразу
            try:
                any_def = AnyDefinition(**doc_data)
            except Exception as e:
                logger.error(f"Error parsing document '{doc_data.get('metadata', {}).get('name', 'Unnamed Document')}': {e}", exc_info=True)
                continue

            kind = any_def.kind
            metadata_name = any_def.metadata.get("name")

            if not metadata_name:
                logger.warning(f"Skipping document of kind '{kind}' because it has no metadata.name.")
                continue

            try:
                if kind == "Experiment": # Должно соответствовать Literal в ExperimentDefinition
                    exp_def = ExperimentDefinition(**doc_data)
                    found_experiment_defs.append(exp_def)
                elif kind == "Task":
                    task_def = TaskDefinition(**doc_data)
                    if metadata_name in self.task_defs:
                        logger.warning(f"Duplicate TaskDefinition name '{metadata_name}'. Overwriting.")
                    self.task_defs[metadata_name] = task_def
                    # DEBUG PRINT for TaskDefinition parsing
                    if metadata_name == 'prepare-data-iter-v1': # Specific to our test case
                        if len(task_def.spec.steps) > 1 and task_def.spec.steps[1].name == 'process-step':
                            logger.debug(f"Task 'prepare-data-iter-v1', process-step inputs: {task_def.spec.steps[1].inputs}")
                else:
                    logger.warning(f"Unsupported kind '{kind}' for document '{metadata_name}'. Skipping.")
            except Exception as e:
                logger.error(f"Error parsing document '{metadata_name}' (kind: {kind}) due to Pydantic validation error: {e}", exc_info=True)
        
        if not found_experiment_defs:
            raise ValueError("No ExperimentDefinition found in the manifest.")
        if len(found_experiment_defs) > 1:
            # TODO: Решить, как обрабатывать несколько ExperimentDefinition. Пока берем первый.
            logger.warning(f"Multiple ExperimentDefinitions found. Using the first one: '{found_experiment_defs[0].metadata.get('name')}'.")
        
        self.experiment_def = found_experiment_defs[0]

        # Initialize resolved_global_params and resolved_global_param_literals
        # Global params will be resolved in expand_experiment, once experiment_instance_id is known.

        # TODO: Добавить валидацию, что все task_reference в ExperimentDefinition существуют в self.task_defs

    def _resolve_global_params(self, experiment_def: ExperimentDefinition) -> None:
        """Resolves global parameters, handling precedence and storing them as references
        with their literal values stored separately.
        Precedence: overrides > experiment_def.parameters (with defaults) > experiment_def.spec.global_parameter_definitions.
        """
        if not self.experiment_instance_id:
            logger.error("_resolve_global_params called before experiment_instance_id is set. Cannot create valid references.")
            # Raising an error might be better as valid references are critical.
            temp_exp_id_for_ref = "UNKNOWN_EXPERIMENT_ID" # Fallback, though ideally this state is prevented.
        else:
            temp_exp_id_for_ref = self.experiment_instance_id

        # These will hold the final resolved parameters and their literals.
        resolved_refs: Dict[str, str] = {}
        resolved_literals: Dict[str, Any] = {}

        # 1. Process experiment_def.spec.global_parameter_definitions (base values)
        # These are List[GlobalParameterValue] from the manifest's spec section.
        if experiment_def.spec and experiment_def.spec.global_parameter_definitions:
            for gpd_val in experiment_def.spec.global_parameter_definitions:
                param_name = gpd_val.name
                param_value = gpd_val.value
                ref_parts = [temp_exp_id_for_ref, "global_params", param_name]
                param_ref = self._construct_reference(ref_parts)

                if param_name in self.override_params:
                    value_to_store = self.override_params[param_name]
                    has_value = True
                    source_info = "override"
                elif param_def.default is not None:
                    value_to_store = param_def.default
                    has_value = True
                    source_info = "default from experiment_def.spec.global_parameter_definitions"
                elif param_name in resolved_literals: # Value already set from spec.global_parameter_definitions
                    # If 'required' is true here, it's satisfied by the value from spec.
                    # No need to update value_to_store, keep the one from spec unless overridden.
                    # This 'elif' ensures we don't raise 'required' error if spec provided it.
                    pass # Value from spec is already in resolved_literals and will be kept if not overridden
                elif param_def.required:
                    raise ValueError(
                        f"Required global parameter '{param_name}' from experiment_def.spec.global_parameter_definitions "
                        f"not provided via override, has no default, and not found in spec.global_parameter_definitions."
                    )
                
                if has_value: # If override or default was applied for this parameter definition
                    logger.debug(f"Global parameter '{param_name}' value set/updated from {source_info}: {value_to_store}")
                    ref_parts = [temp_exp_id_for_ref, "global_params", param_name]
                    param_ref = self._construct_reference(ref_parts)
                    resolved_refs[param_name] = param_ref      # Update/overwrite ref from spec if name collision
                    resolved_literals[param_name] = value_to_store # Update/overwrite literal from spec
        
        # 2. Process experiment_def.spec.parameters (structured definitions with defaults, required flags)
        # These are Dict[str, GlobalParameterDefinitionModel] from the manifest's parameters section.
        # These can override or supplement values from spec.global_parameter_definitions.
        # Overrides from self.override_params take highest precedence here.
        if hasattr(experiment_def, 'spec') and experiment_def.spec and hasattr(experiment_def.spec, 'parameters') and experiment_def.spec.parameters:
            for name, definition_model in experiment_def.spec.parameters.items():
                value_to_store = None
                has_value = False
                source_info = ""

                if name in self.override_params:
                    value_to_store = self.override_params[name]
                    has_value = True
                    source_info = "override"
                elif definition_model.default is not None:
                    value_to_store = definition_model.default
                    has_value = True
                    source_info = "default from experiment_def.spec.parameters"
                elif name in resolved_literals: # Value already set from spec.global_parameter_definitions
                    # If 'required' is true here, it's satisfied by the value from spec.
                    # No need to update value_to_store, keep the one from spec unless overridden.
                    # This 'elif' ensures we don't raise 'required' error if spec provided it.
                    pass # Value from spec is already in resolved_literals and will be kept if not overridden
                elif definition_model.required:
                    raise ValueError(
                        f"Required global parameter '{name}' from experiment_def.spec.parameters "
                        f"not provided via override, has no default, and not found in spec.global_parameter_definitions."
                    )
                
                if has_value: # If override or default was applied for this parameter definition
                    logger.debug(f"Global parameter '{name}' value set/updated from {source_info}: {value_to_store}")
                    ref_parts = [temp_exp_id_for_ref, "global_params", name]
                    param_ref = self._construct_reference(ref_parts)
                    resolved_refs[name] = param_ref      # Update/overwrite ref from spec if name collision
                    resolved_literals[name] = value_to_store # Update/overwrite literal from spec
        
        # 3. Process any override_params that were not part of experiment_def.parameters
        # These are considered additional or ad-hoc overrides.
        for name, value in self.override_params.items():
            if name not in resolved_refs: # If not already processed via experiment_def.parameters
                logger.debug(f"Global parameter '{name}' (ad-hoc override) set with value: {value}")
                ref_parts = [temp_exp_id_for_ref, "global_params", name]
                param_ref = self._construct_reference(ref_parts)
                resolved_refs[name] = param_ref
                resolved_literals[name] = value

        self.resolved_global_params = resolved_refs
        self.resolved_global_param_literals = resolved_literals
        
        logger.info(f"Final resolved global parameters (as references): {self.resolved_global_params}")
        logger.debug(f"Final global parameter literals: {self.resolved_global_param_literals}")

    def process_manifest(self) -> Tuple[ExperimentInstance, List[TaskInstance], List[JobInstance]]:
        """
        Processes the loaded manifest documents by expanding the experiment definition.

        Returns:
            A tuple containing the ExperimentInstance, a list of TaskInstances,
            and a list of JobInstances.

        Raises:
            RuntimeError: If no ExperimentDefinition was loaded or found.
        """
        if not self.experiment_def:
            # Эта ошибка также может быть вызвана из _parse_and_categorize_definitions,
            # но дублируем проверку для ясности и как публичный контракт метода.
            raise RuntimeError("Cannot process manifest: No ExperimentDefinition loaded. Ensure the manifest contains a valid 'Experiment' document.")
        
        logger.info(f"Processing manifest for experiment: {self.experiment_def.metadata.get('name')}")
        return self.expand_experiment()

    def expand_experiment(self) -> Tuple[ExperimentInstance, List[TaskInstance], List[JobInstance]]:
        """
        Expands the loaded ExperimentDefinition into a graph of instances,
        handling 'iterate_over' for task parallelization.
        """
        if not self.experiment_def:
            raise RuntimeError("ExperimentDefinition not loaded or parsed correctly.")

        exp_instance_name = self.experiment_def.metadata.get("name", "default-experiment")
        exp_instance_id = self._generate_instance_id(exp_instance_name, "exp")
        self.experiment_instance_id = exp_instance_id # Set the instance ID for the expander context

        # Resolve global parameters now that we have the experiment_instance_id
        self._resolve_global_params(self.experiment_def)
        
        experiment_instance = ExperimentInstance(
            id=exp_instance_id,
            experiment_definition_ref=self.experiment_def.metadata.get("name"),
            name=exp_instance_name,
            description=self.experiment_def.spec.description,
            parameters=self.resolved_global_params or {},
            status=InstanceStatus.PENDING,
        )
        logger.info(f"Created ExperimentInstance: ID={experiment_instance.id}, Name='{experiment_instance.name}'")
        
        all_task_instances: List[TaskInstance] = []
        all_job_instances: List[JobInstance] = []
        
        # Карта для отслеживания созданных TaskInstance по их имени в конвейере.
        # Значение - список ID созданных TaskInstance (может быть несколько из-за iterate_over)
        created_pipeline_task_instances_map: Dict[str, List[str]] = {}

        for pipeline_task_def in self.experiment_def.spec.pipeline:
            task_def_name = pipeline_task_def.task_reference
            task_def = self.task_defs.get(task_def_name)

            if not task_def:
                logger.warning(f"TaskDefinition '{task_def_name}' referenced in pipeline task '{pipeline_task_def.name}' not found. Skipping.")
                continue

            # Параметры, переданные при вызове задачи в конвейере эксперимента
            task_call_params = pipeline_task_def.parameters or {}
            
            # Глобальные параметры уже разрешены и хранятся в self.resolved_global_params
            # Этот блок больше не нужен в таком виде, но оставим комментарий как напоминание о контексте

            iteration_parameter_sets = []
            if pipeline_task_def.iterate_over:
                # Новый код для обработки List[IterateItem]
                for iterate_item_model in pipeline_task_def.iterate_over:
                    iteration_parameter_sets.append(iterate_item_model.parameters)
            
            # Если iterate_over пуст или его не было, добавляем один пустой набор параметров,
            # чтобы цикл for i, iter_params in enumerate(iteration_parameter_sets) ниже
            # выполнился хотя бы один раз (для задач без итерации).
            if not iteration_parameter_sets:
                iteration_parameter_sets.append({})

            current_task_instance_ids_for_pipeline_task: List[str] = []

            for i, iter_params in enumerate(iteration_parameter_sets):
                # 1. Параметры, определенные в самом pipeline.task (если есть)
                base_pipeline_task_params = dict(pipeline_task_def.parameters or {})
                
                # 2. Объединяем с параметрами текущей итерации (iter_params имеют приоритет)
                current_merged_params = {**base_pipeline_task_params, **iter_params}

                # 3. Разрешаем ссылки global:// и формируем final_task_params
                final_task_params = {}
                for key, value in current_merged_params.items():
                    if isinstance(value, str) and value.startswith("global://"):
                        global_param_name = value[len("global://"):]
                        if global_param_name in self.resolved_global_params:
                            final_task_params[key] = self.resolved_global_params[global_param_name]
                            logger.debug(f"TaskInstance '{pipeline_task_def.name}-iter{i}': Resolved global param reference '{key}: {value}' to '{final_task_params[key]}'")
                        else:
                            logger.warning(f"TaskInstance '{pipeline_task_def.name}-iter{i}': Global parameter reference '{value}' for parameter '{key}' not found. Using string as value.")
                            final_task_params[key] = value # Оставляем как есть или можно вызвать ошибку
                    else:
                        final_task_params[key] = value
                
                # Добавляем глобальные параметры, которые не были переопределены на уровне задачи или итерации
                # Это обеспечивает, что все глобальные параметры доступны, если не указаны локально.
                for gp_name, gp_value in self.resolved_global_params.items():
                    if gp_name not in final_task_params:
                        final_task_params[gp_name] = gp_value
                        logger.debug(f"TaskInstance '{pipeline_task_def.name}-iter{i}': Added unreferenced global param '{gp_name}: {gp_value}'")


                # Имя для TaskInstance: если есть итерация, добавляем суффикс
                task_instance_base_name = f"{exp_instance_name}-{pipeline_task_def.name}"
                task_instance_name_for_id = task_instance_base_name
                if pipeline_task_def.iterate_over:
                    task_instance_name_for_id += f"-iter{i}"
                
                task_instance_id = self._generate_instance_id(task_instance_name_for_id, "task")

                # Определение зависимостей для TaskInstance
                # TODO: Это предварительная логика, нужно будет уточнить.
                # Если задача B зависит от A, и A породила A1, A2, то B должна зависеть от A1 и A2.
                depends_on_task_instance_ids: List[str] = []
                if pipeline_task_def.depends_on:
                    for dep_pipeline_task_name in pipeline_task_def.depends_on:
                        if dep_pipeline_task_name in created_pipeline_task_instances_map:
                            depends_on_task_instance_ids.extend(created_pipeline_task_instances_map[dep_pipeline_task_name])
                        else:
                            logger.warning(f"Dependency '{dep_pipeline_task_name}' for pipeline task '{pipeline_task_def.name}' not yet processed or not found. Check pipeline order.")
                
                # Теперь final_task_params содержит исходные значения.
                # Преобразуем их в ссылки и сохраним литералы отдельно.
                final_task_params_as_refs: Dict[str, str] = {}
                task_input_literals: Dict[str, Any] = {}
                exp_id_for_ref = experiment_instance.id # Используем ID уже созданного ExperimentInstance

                for param_key, param_value in final_task_params.items():
                    # Проверяем, является ли значение простым литералом (str, int, float, bool)
                    # Сложные структуры (dict, list) пока не оборачиваем в ссылки на этом уровне,
                    # предполагая, что они будут обработаны внутри _expand_task_into_jobs, если это параметры шагов.
                    # Если это параметр уровня задачи, который является словарем/списком, его обработка как единого литерала может быть сложной.
                    # TODO: Продумать обработку сложных структур как литералов на уровне TaskInstance.
                    if isinstance(param_value, (str, int, float, bool)):
                        # Если это уже существующая ссылка от global://, не пересоздаем
                        if isinstance(param_value, str) and param_value.startswith(self.REFERENCE_PREFIX):
                            final_task_params_as_refs[param_key] = param_value
                        else:
                            literal_ref = self._construct_reference([exp_id_for_ref, task_instance_id, "parameters", param_key])
                            final_task_params_as_refs[param_key] = literal_ref
                            task_input_literals[param_key] = param_value
                    else: # Handles complex types (dict, list) and None
                        # Convert complex types and None to references as well.
                        # Their actual value will be stored in task_input_literals.
                        # This ensures TaskInstance.parameters remains Dict[str, str].
                        logger.debug(f"TaskInstance '{task_instance_name_for_id}' parameter '{param_key}' is a complex type ({type(param_value)}) or None. Converting to reference and storing literal.")
                        literal_ref = self._construct_reference([exp_id_for_ref, task_instance_id, "parameters", param_key])
                        final_task_params_as_refs[param_key] = literal_ref
                        task_input_literals[param_key] = param_value

                task_instance = TaskInstance(
                    id=task_instance_id,
                    experiment_instance_id=exp_id_for_ref,
                    task_definition_ref=task_def_name,
                    name=task_instance_name_for_id, 
                    name_in_pipeline=pipeline_task_def.name, 
                    parameters=final_task_params_as_refs, # Теперь это Dict[str, str] (в идеале)
                    input_literal_values=task_input_literals,
                    status=InstanceStatus.PENDING,
                    depends_on_task_ids=depends_on_task_instance_ids
                )
                logger.info(f"Created TaskInstance: ID={task_instance.id}, Name='{task_instance.name}'")
                all_task_instances.append(task_instance)
                current_task_instance_ids_for_pipeline_task.append(task_instance.id)

                # Для каждого TaskInstance создаем его JobInstances
                # experiment_params передаются как есть (пока не ссылки), task_call_params теперь ссылки
                job_instances_for_task = self._expand_task_into_jobs(
                    task_def=task_def,
                    task_instance=task_instance, # task_instance уже содержит параметры как ссылки и литералы
                    experiment_params=self.resolved_global_params, # Глобальные параметры эксперимента (пока не ссылки)
                    task_call_params=task_instance.parameters # Это уже Dict[str, str] ссылок
                )
                all_job_instances.extend(job_instances_for_task)
            
            created_pipeline_task_instances_map[pipeline_task_def.name] = current_task_instance_ids_for_pipeline_task

        return experiment_instance, all_task_instances, all_job_instances

    def _render_template(
        self,
        template_string: Any,
        context: Dict[str, Any],
        job_instance_context: Optional[JobInstanceContext] = None,
        task_instance: Optional[TaskInstance] = None,
        global_param_literals: Optional[Dict[str, Any]] = None,
        current_job_id: Optional[str] = None, 
        current_step_name: Optional[str] = None, 
        is_output_template: bool = False,
        template_type: str = "parameter"
    ) -> Tuple[str, Optional[Any]]:
        if not isinstance(template_string, str):
            return template_string, None

        if not ("{{" in template_string and "}}" in template_string):
            return template_string, None

        exp_id = context.get("experiment", {}).get("id", "unknown_exp")
        task_id = context.get("task", {}).get("id", "unknown_task")

        if is_output_template:
            match = self.JOB_OUTPUT_REF_PATTERN.match(template_string)
            if match:
                output_name = match.group("name")
                if not current_job_id:
                    logger.error(f"Cannot construct self-output reference for '{template_string}': current_job_id is missing.")
                    return template_string, None
                ref = self._construct_reference([exp_id, task_id, current_job_id, "outputs", output_name])
                logger.debug(f"Rendered self-output template '{template_string}' to reference: '{ref}' for job {current_job_id}")
                return ref, None

        match = self.STEP_OUTPUT_REF_PATTERN.match(template_string)
        if match:
            referenced_step_name = match.group("step_name")
            output_name = match.group("output_name")
            if referenced_step_name in context and isinstance(context[referenced_step_name], dict) and \
               "outputs" in context[referenced_step_name] and output_name in context[referenced_step_name]["outputs"]:
                resolved_ref = context[referenced_step_name]["outputs"][output_name]
                if isinstance(resolved_ref, str) and resolved_ref.startswith(self.REFERENCE_PREFIX):
                    logger.debug(f"Rendered step output template '{template_string}' to existing reference: '{resolved_ref}'")
                    return resolved_ref, None
                else:
                    logger.warning(
                        f"Step '{referenced_step_name}' output '{output_name}' found in context for template '{template_string}' "
                        f"but is not a valid reference: '{resolved_ref}'. This might indicate an issue in how "
                        f"processed_steps_context is populated.")
            else:
                logger.debug(
                    f"Prospective reference for '{template_string}'. Referenced step '{referenced_step_name}' or its output "
                    f"'{output_name}' not found as a direct reference in current rendering context. "
                    f"Context keys for steps: {[k for k in context if isinstance(context[k], dict) and 'outputs' in context[k]]}")
            ref = self._construct_reference([exp_id, task_id, referenced_step_name, "outputs", output_name])
            logger.debug(f"Rendered step output template '{template_string}' to prospective reference: '{ref}'")
            return ref, None

        match = self.PARAM_REF_PATTERN.match(template_string)
        if match:
            param_name = match.group("name")
            ref = self._construct_reference([exp_id, task_id, "parameters", param_name])
            logger.debug(f"Rendered task parameter template '{template_string}' to reference: '{ref}'")
            return ref, None

        logger.debug(
            f"Template '{template_string}' (type: {template_type}) did not match known reference patterns. "
            f"Returning original string. It will be treated as a literal if not already a ref.")
        return template_string, None

    def _expand_task_into_jobs(
        self,
        task_def: TaskDefinition,
        task_instance: TaskInstance,
        experiment_params: Optional[Dict[str, Any]],
        task_call_params: Optional[Dict[str, Any]],
    ) -> List[JobInstance]:

        # Helper functions to extract dependencies from template strings
        # Defined here for atomicity of the change, ideally would be private class methods
        def _extract_step_dependencies_from_str(template_string: str) -> List[str]:
            if not isinstance(template_string, str):
                return []
            # Regex to find {{<step_name>.outputs...}}
            # It captures the <step_name>
            dependencies = re.findall(r"\{\{\s*([a-zA-Z0-9_.-]+)\.outputs\.", template_string)
            return list(set(dependencies))

        def _collect_dependencies_recursive(value: Any, collected_deps_set: set):
            if isinstance(value, str):
                for dep_name in _extract_step_dependencies_from_str(value):
                    collected_deps_set.add(dep_name)
            elif isinstance(value, list):
                for item in value:
                    _collect_dependencies_recursive(item, collected_deps_set)
            elif isinstance(value, dict):
                for sub_value in value.values():
                    _collect_dependencies_recursive(sub_value, collected_deps_set)
        
        job_instances_for_task: List[JobInstance] = []
        step_name_to_job_id_map: Dict[str, str] = {} # Maps step_def.name to JobInstance.id
        # This context will store resolved output values (or templates if not fully resolvable now)
        # Structure: {"step_name": {"outputs": {"output_key": value}}}
        processed_steps_context: Dict[str, Dict[str, Any]] = {}

        for step_def in task_def.spec.steps:
            job_instance_id = f"job-{uuid.uuid4().hex[:16]}" 
            job_instance_display_name = f"{task_instance.name}-{step_def.name}-{job_instance_id[-8:]}"
            job_definition_reference = f"{task_def.metadata['name']}:{step_def.name}"

            current_job_dependency_names: set[str] = set(step_def.depends_on or [])

            exp_id_for_ref = task_instance.experiment_instance_id
            task_id_for_ref = task_instance.id

            # Инициализация JobInstanceContext для текущего джоба
            current_job_instance_context = JobInstanceContext(
                experiment_id=exp_id_for_ref, # Corrected field name
                task_instance_id=task_id_for_ref,
                job_instance_id=job_instance_id,
                job_definition_ref=job_definition_reference, # Added missing field
                resolved_parameters={}, # Будет заполнено позже
                input_literal_values={}, # Будет заполняться по мере разрешения
                resolved_inputs={}, # Будет заполнено позже
                resolved_outputs={} # Будет заполнено позже
            )

            # 1. Контекст для рендеринга параметров и входов ТЕКУЩЕГО шага.
            current_step_render_context = {
                "parameters": {**(experiment_params or {}), **(task_call_params or {})}, 
                "experiment": {"id": exp_id_for_ref},
                "task": {"id": task_id_for_ref, "name": task_instance.name},
                "job": {"id": job_instance_id}, 
                **processed_steps_context 
            }

            # 2. Разрешение параметров текущего шага
            resolved_current_step_params_as_refs = {}
            if step_def.parameters:
                for key, value_template in step_def.parameters.items():
                    _collect_dependencies_recursive(value_template, current_job_dependency_names)
                    
                    # Рекурсивная функция для обработки вложенных структур (списки/словари)
                    def _process_param_value(template_val, param_key_path: List[str]):
                        if isinstance(template_val, str):
                            # current_job_instance_context из внешней области видимости цикла
                            # task_instance из аргументов _expand_task_into_jobs
                            # self.resolved_global_param_literals из атрибутов экземпляра ManifestExpander
                            ref_str, literal_val = self._render_template(
                                template_string=template_val, 
                                context=current_step_render_context, 
                                job_instance_context=current_job_instance_context,
                                task_instance=task_instance,
                                global_param_literals=self.resolved_global_param_literals,
                                current_job_id=job_instance_id,
                                current_step_name=step_def.name,
                                is_output_template=False,
                                template_type=f"parameter:{'.'.join(param_key_path)}"
                            )
                            if literal_val is not None:
                                # Сохраняем разрешенный литерал в контексте джоба, используя ссылку как ключ
                                current_job_instance_context.input_literal_values[ref_str] = literal_val
                            return ref_str # Возвращаем ссылку
                        elif isinstance(template_val, list):
                            return [_process_param_value(item, param_key_path + [str(idx)]) for idx, item in enumerate(template_val)]
                        elif isinstance(template_val, dict):
                            return {k: _process_param_value(v, param_key_path + [k]) for k, v in template_val.items()}
                        return template_val # Не строка, не список, не словарь - возвращаем как есть (например, число, bool)

                    resolved_current_step_params_as_refs[key] = _process_param_value(value_template, [key])
            
            # Итоговые параметры для JobInstanceContext (только ссылки)
            # Глобальные и задачные параметры также должны быть представлены как ссылки
            # Это более сложный момент, так как они приходят уже "разрешенными". 
            # Пока что для них оставим прямое значение, но в будущем их тоже нужно будет обернуть в ссылки.
            # TODO: Обернуть experiment_params и task_call_params в ссылки.
            final_resolved_parameters_as_refs = {
                **(experiment_params or {}), # ВРЕМЕННО: оставляем как есть
                **(task_call_params or {}),  # ВРЕМЕННО: оставляем как есть
                **resolved_current_step_params_as_refs
            }

            # 3. Рендеринг ВЫХОДНЫХ ЗНАЧЕНИЙ (шаблонов) для текущего шага
            job_instance_rendered_outputs_as_refs: Dict[str, str] = {}
            # Контекст для рендеринга выходов текущего джоба.
            # Важно: параметры здесь должны быть уже в виде ссылок или литералов, которые _render_template сможет обработать.
            job_self_output_render_context = {
                "parameters": final_resolved_parameters_as_refs, # Используем параметры в виде ссылок
                "experiment": {"id": exp_id_for_ref},
                "task": {"id": task_id_for_ref, "name": task_instance.name},
                "job": {"id": job_instance_id}
                # Не включаем сюда processed_steps_context, так как job.outputs ссылается на самого себя
            }
            if step_def.outputs_templates:
                for output_logical_name, output_value_template in step_def.outputs_templates.items():
                    ref_str, literal_val = self._render_template(
                        template_string=output_value_template, 
                        context=job_self_output_render_context, 
                        job_instance_context=current_job_instance_context,
                        task_instance=task_instance,
                        global_param_literals=self.resolved_global_param_literals,
                        current_job_id=job_instance_id,
                        current_step_name=step_def.name,
                        is_output_template=True, # Ключевой флаг
                        template_type=f"output:{output_logical_name}"
                    )
                    job_instance_rendered_outputs_as_refs[output_logical_name] = ref_str
                    if literal_val is not None:
                        # Сохраняем "литеральный" выход (если такое возможно) в input_literal_values
                        # или, возможно, в current_job_instance_context.resolved_outputs если бы оно принимало Any
                        current_job_instance_context.input_literal_values[ref_str] = literal_val 
                        logger.debug(f"Output template '{output_value_template}' for output '{output_logical_name}' in step '{step_def.name}' resolved to reference '{ref_str}' with literal value '{literal_val}'.")
                    
                    if not ref_str.startswith(self.REFERENCE_PREFIX):
                        logger.error(f"Output template '{output_value_template}' for output '{output_logical_name}' in step '{step_def.name}' did not resolve to a reference string. Got: '{ref_str}'. This is an issue.")
            
            # Обновляем общий контекст выходных данных шагов задачи (для следующих шагов)
            # Также обновляем resolved_outputs в текущем JobInstanceContext
            processed_steps_context[step_def.name] = {"outputs": job_instance_rendered_outputs_as_refs}
            current_job_instance_context.resolved_outputs = job_instance_rendered_outputs_as_refs.copy()

            # 4. Разрешение ВХОДНЫХ артефактов
            current_step_resolved_inputs: Dict[str, str] = {}
            if step_def.inputs:
                for input_logical_name, input_uri_template in step_def.inputs.items():
                    _collect_dependencies_recursive(input_uri_template, current_job_dependency_names)
                    ref_str, literal_val = self._render_template(
                        template_string=input_uri_template, 
                        context=current_step_render_context, 
                        job_instance_context=current_job_instance_context,
                        task_instance=task_instance,
                        global_param_literals=self.resolved_global_param_literals,
                        current_job_id=job_instance_id,
                        current_step_name=step_def.name,
                        is_output_template=False,
                        template_type=f"input:{input_logical_name}"
                    )
                    current_step_resolved_inputs[input_logical_name] = ref_str
                    if literal_val is not None:
                        current_job_instance_context.input_literal_values[ref_str] = literal_val
                        logger.debug(f"Input template '{input_uri_template}' for input '{input_logical_name}' in step '{step_def.name}' resolved to reference '{ref_str}' with literal value '{literal_val}'.")
                    
                    if not ref_str.startswith(self.REFERENCE_PREFIX):
                        logger.warning(f"Input template '{input_uri_template}' for input '{input_logical_name}' in step '{step_def.name}' resolved to a non-reference string: '{ref_str}'. This might be intended (literal URI) or an issue if a reference to another step's output was expected.")

            # Заполняем оставшиеся поля в current_job_instance_context
            current_job_instance_context.resolved_parameters = {
                **(experiment_params or {}), 
                **(task_call_params or {}),  
                **resolved_current_step_params_as_refs
            }
            current_job_instance_context.resources_request = getattr(step_def, 'resources', None) or {}
            current_job_instance_context.priority = getattr(step_def, 'priority', 0)
            current_job_instance_context.resolved_inputs = current_step_resolved_inputs
            # current_job_instance_context.resolved_outputs уже был заполнен ранее
            # current_job_instance_context.input_literal_values наполнялся по ходу дела

            # 7. Создание JobInstance
            actual_job_dependency_ids = [
                step_name_to_job_id_map[dep_name]
                for dep_name in current_job_dependency_names
                if dep_name in step_name_to_job_id_map # Убедимся, что зависимость существует
            ]

            job_instance = JobInstance(
                id=job_instance_id,
                name=job_instance_display_name,
                task_instance_id=task_id_for_ref,
                job_definition_ref=job_definition_reference,
                status=InstanceStatus.PENDING, 
                depends_on_job_ids=actual_job_dependency_ids,
                command_template=step_def.command if hasattr(step_def, 'command') else None, 
                image=step_def.image if hasattr(step_def, 'image') else None,
                node_selector=step_def.node_selector if hasattr(step_def, 'node_selector') else None,
                retry_policy=step_def.retry_policy if hasattr(step_def, 'retry_policy') else None,
                timeout_seconds=step_def.timeout_seconds if hasattr(step_def, 'timeout_seconds') else None,
                environment_variables=step_def.environment_variables if hasattr(step_def, 'environment_variables') else None,
                context=current_job_instance_context,
                resolved_outputs=job_instance_rendered_outputs_as_refs
            )
            job_instances_for_task.append(job_instance)
            step_name_to_job_id_map[step_def.name] = job_instance_id

        return job_instances_for_task # Добавлен return

if __name__ == '__main__':
    from logger import init_logging
    init_logging(console_level=logging.INFO) # Initialize custom logging, set console to INFO
    logger.info("ManifestExpander basic test")

    # Test logging levels were here and removed, which is correct.

    sample_raw_docs = [
        {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Experiment",
            "metadata": {"name": "my-iter-experiment"}, # Изменил имя для ясности
            "spec": {
                "description": "A test experiment with iteration.",
                "pipeline": [
                    {
                        "name": "iterative-data-prep", # Имя задачи в конвейере
                        "taskReference": "prepare-data-iter-v1", # Ссылка на определение задачи
                        "parameters": { # Базовые параметры для этой задачи
                            "base_param": "value_for_all_iterations"
                        },
                        "iterateOver": [{
                            "parameters": { # <--- Параметры итерации теперь вложены сюда
                                "learning_rate": [0.01, 0.001],
                                "optimizer": ["adam", "sgd"],
                                "version_suffix": ["iter1", "iter2"] # Добавляем итерацию по version_suffix
                            }
                        }]
                    },
                    # Можно добавить еще одну задачу, которая зависит от первой,
                    # чтобы потом протестировать depends_on
                    # {
                    #     "name": "analysis-task",
                    #     "taskReference": "analyze-data-v1",
                    #     "dependsOn": ["iterative-data-prep"] 
                    # }
                ]
            }
        },
        {
            "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
            "kind": "Task", # Убедись, что kind соответствует модели TaskDefinition
            "metadata": {"name": "prepare-data-iter-v1"}, # Имя определения задачи
            "spec": {
                "description": "Prepares data, can be iterated, and passes artifacts.",
                # Добавим параметры в схему задачи, чтобы видеть их разрешение
                "parametersSchema": {
                    "learning_rate": {"type": "number", "description": "Learning rate for training."},
                    "optimizer": {"type": "string", "description": "Optimizer to use."},
                    "base_param": {"type": "string", "description": "A base parameter."},
                    "version_suffix": {"type": "string", "default": "v1"} 
                },
                "steps": [
                    {
                        "name": "download-step",
                        "executor": "downloader_job.Downloader",
                        "parameters": {
                             # Пример использования параметра задачи в шаге
                            "source_url": "http://example.com/data?opt={{parameters.optimizer}}"
                        },
                        "outputs_templates": { 
                            "raw_data_file": "downloaded_data_{{parameters.optimizer}}_{{parameters.version_suffix}}.csv"
                        }
                    },
                    {
                        "name": "process-step",
                        "executor": "processor_job.Processor",
                        "depends_on": ["download-step"],
                        "inputs": { 
                            "input_dataset": "{{steps['download-step'].outputs.raw_data_file}}"
                        },
                        "parameters": {
                            "rate": "{{parameters.learning_rate}}"
                        },
                        "outputs_templates": { 
                            "processed_data_file": "processed_data_lr{{parameters.learning_rate}}_{{parameters.version_suffix}}.parquet"
                        }
                    }
                ]
            }
        },
        # { # Определение для analyze-data-v1, если будешь добавлять зависимую задачу
        #     "apiVersion": "orchestrator.windsurf.ai/v1alpha1",
        #     "kind": "Task",
        #     "metadata": {"name": "analyze-data-v1"},
        #     "spec": {
        #         "description": "Analyzes data.",
        #         "steps": [{"name": "analyze", "executor": "analyzer.Analyzer"}]
        #     }
        # }
    ]

    try:
        expander = ManifestExpander(sample_raw_docs)
        logger.info(f"Loaded ExperimentDefinition: {expander.experiment_def.metadata['name'] if expander.experiment_def else 'None'}")
        logger.info(f"Loaded TaskDefinitions: {list(expander.task_defs.keys())}")

        if expander.experiment_def:
            exp_instance, task_instances, job_instances = expander.expand_experiment()
            
            logger.info(f"\n--- Generated ExperimentInstance ---")
            logger.info(f"ID: {exp_instance.id}, Name: {exp_instance.name}")
            logger.debug(f"Parameters: {exp_instance.parameters}") # Пока не разрешаются, будет пусто или схема

            logger.info(f"\n--- Generated TaskInstances ({len(task_instances)}) ---")
            for ti in task_instances:
                logger.info(f"  TaskInstance Created: ID={ti.id}, Name='{ti.name}', Pipeline Name='{ti.name_in_pipeline}'") # INFO for creation
                logger.debug(f"    Definition: {ti.task_definition_ref}")
                logger.debug(f"    Parameters: {ti.parameters}")
                logger.debug(f"    Depends on Task IDs: {ti.depends_on_task_ids}")
                # Считаем джобы для этой задачи
                jobs_for_this_task = [j for j in job_instances if j.task_instance_id == ti.id]
                logger.debug(f"    Number of jobs: {len(jobs_for_this_task)}")

            logger.info(f"\n--- Generated JobInstances ({len(job_instances)}) ---")
            for ji in job_instances:
                logger.info(f"  JobInstance Created: ID={ji.id}, Name='{ji.name}'") # INFO for creation
                logger.debug(f"    TaskInstance ID: {ji.task_instance_id}")
                logger.debug(f"    Job Definition Ref: {ji.job_definition_ref}")
                logger.debug(f"    Resolved Parameters: {ji.context.resolved_parameters}")
                logger.debug(f"    Resolved Inputs: {ji.context.resolved_inputs}")
                logger.debug(f"    Resolved Outputs (at expansion): {ji.resolved_outputs}") # This is the direct field on JobInstance with actual values
                logger.debug(f"    Resources: {ji.context.resources_request}")
                logger.debug(f"    Priority: {ji.context.priority}")
                logger.debug(f"    Depends on Job IDs: {ji.depends_on_job_ids}")
                logger.debug(f"    Created At: {ji.created_at}")
                
    except ValueError as ve:
        logger.error(f"Error during expansion: {ve}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
