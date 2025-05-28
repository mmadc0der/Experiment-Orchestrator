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

        self._parse_and_categorize_definitions()

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

        # Initialize resolved_global_params
        self.resolved_global_params: Dict[str, Any] = {}
        if self.experiment_def and self.experiment_def.spec.global_parameter_definitions:
            for gpd in self.experiment_def.spec.global_parameter_definitions:
                # TODO: Add type validation/casting based on gpd.type if necessary
                self.resolved_global_params[gpd.name] = gpd.value
            logger.debug(f"Initialized resolved_global_params: {self.resolved_global_params}")

        # TODO: Добавить валидацию, что все task_reference в ExperimentDefinition существуют в self.task_defs

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
                
                task_instance = TaskInstance(
                    id=task_instance_id,
                    experiment_instance_id=experiment_instance.id,
                    task_definition_ref=task_def_name,
                    name=task_instance_name_for_id, # Уникальное имя экземпляра задачи
                    name_in_pipeline=pipeline_task_def.name, # Имя задачи как в конвейере эксперимента
                    parameters=final_task_params, # Разрешенные параметры для этой конкретной задачи
                    status=InstanceStatus.PENDING,
                    depends_on_task_ids=depends_on_task_instance_ids,
                    # inputs/outputs будут разрешаться позже или при выполнении
                )
                logger.info(f"Created TaskInstance: ID={task_instance.id}, Name='{task_instance.name}'")
                all_task_instances.append(task_instance)
                current_task_instance_ids_for_pipeline_task.append(task_instance.id)

                # Для каждого TaskInstance создаем его JobInstances
                job_instances_for_task = self._expand_task_into_jobs(
                    task_def=task_def,
                    task_instance=task_instance,
                    experiment_params=self.resolved_global_params, # Передаем глобальные параметры эксперимента
                    task_call_params=final_task_params      # Передаем разрешенные параметры задачи
                )
                all_job_instances.extend(job_instances_for_task)
            
            created_pipeline_task_instances_map[pipeline_task_def.name] = current_task_instance_ids_for_pipeline_task

        return experiment_instance, all_task_instances, all_job_instances

    def _render_template(self, template_string: str, context: Dict[str, Any]) -> str:
        """
        Renders a Jinja2 template string with the given context.
        """
        logger.debug(f"Attempting to render template '{template_string}' with context keys: {list(context.keys())}")
        try:
            template = jinja2.Template(template_string)
            rendered_string = template.render(context)
            logger.debug(f"Successfully rendered template to: '{rendered_string}'")
            return rendered_string
        except jinja2.exceptions.UndefinedError as e:
            undefined_name = getattr(e, 'name', 'Unknown')
            logger.debug(f"Jinja2 UndefinedError ('{undefined_name}' is undefined) for template: '{template_string}'. Keeping original.")
            return template_string
        except Exception as e:
            logger.error(f"General error rendering template='{template_string}': {e}", exc_info=True)
            return template_string

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
            # Regex to find {{steps.<step_name>.outputs...}} or {{<step_name>.outputs...}}
            # It captures the <step_name>
            dependencies = re.findall(r"\{\{\s*(?:steps\.)?([a-zA-Z0-9_.-]+)\.outputs\.", template_string)
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

            # 1. Контекст для рендеринга параметров и входов ТЕКУЩЕГО шага.
            # Он включает выходы ПРЕДЫДУЩИХ обработанных шагов.
            current_step_render_context = {
                "parameters": {**(experiment_params or {}), **(task_call_params or {})}, # Global and task-level params
                "experiment": {"id": task_instance.experiment_instance_id},
                "task": {"id": task_instance.id, "name": task_instance.name},
                "job": {"id": job_instance_id}, # ID of the JobInstance being created
                "steps": processed_steps_context # Outputs of previously processed steps
            }

            # 2. Разрешение параметров текущего шага
            resolved_current_step_params = {}
            if step_def.parameters:
                for key, value_template in step_def.parameters.items():
                    _collect_dependencies_recursive(value_template, current_job_dependency_names)
                    # Recursively render templates within complex structures (lists/dicts)
                    def _render_param_value(template_val):
                        if isinstance(template_val, str):
                            return self._render_template(template_val, current_step_render_context)
                        elif isinstance(template_val, list):
                            return [_render_param_value(item) for item in template_val]
                        elif isinstance(template_val, dict):
                            return {k: _render_param_value(v) for k, v in template_val.items()}
                        return template_val

                    resolved_current_step_params[key] = _render_param_value(value_template)
            
            # Итоговые разрешенные параметры для текущего шага (глобальные + задачные + специфичные для шага)
            final_resolved_parameters = {
                **(experiment_params or {}),
                **(task_call_params or {}),
                **resolved_current_step_params
            }

            # 3. Разрешение ВЫХОДНЫХ ЗНАЧЕНИЙ/ШАБЛОНОВ для текущего шага
            current_job_produced_outputs: Dict[str, Any] = {}
            if step_def.outputs_templates:
                # Контекст для рендеринга шаблонов выходных значений текущего шага.
                # Использует final_resolved_parameters (включая разрешенные параметры текущего шага).
                # Не должен включать "steps" во избежание циклических ссылок на самого себя.
                output_value_render_context = {
                    "parameters": final_resolved_parameters,
                    "experiment": current_step_render_context["experiment"],
                    "task": current_step_render_context["task"],
                    "job": current_step_render_context["job"]
                }
                for output_logical_name, output_value_template in step_def.outputs_templates.items():
                    # Рендерим шаблон выходного значения.
                    # Это может быть простой тип, строка-шаблон для дальнейшего разрешения, или шаблон URI артефакта.
                    resolved_value = self._render_template(output_value_template, output_value_render_context)
                    current_job_produced_outputs[output_logical_name] = resolved_value
                    # generate_artifact_uri НЕ вызывается здесь по умолчанию.
                    # Если output_value_template предназначен для генерации URI, он должен сам это сделать
                    # или быть обернут в специальную функцию в шаблоне, например, {{ artifact_path(...) }})
            
            # Обновляем общий контекст выходных данных шагов задачи (для следующих шагов)
            processed_steps_context[step_def.name] = {"outputs": current_job_produced_outputs}

            # 4. Разрешение ВХОДНЫХ артефактов
            current_step_resolved_inputs: Dict[str, str] = {}
            # logger.debug(f"For step '{step_def.name}', step_def.inputs is: {step_def.inputs}")
            if step_def.inputs:
                # Контекст для рендеринга входов (current_step_render_context) уже определен
                # и содержит "steps": processed_steps_context (выходы предыдущих шагов)
                pass # Используем current_step_render_context
                for input_logical_name, input_uri_template in step_def.inputs.items():
                    _collect_dependencies_recursive(input_uri_template, current_job_dependency_names)
                    try:
                        resolved_input_uri = self._render_template(input_uri_template, current_step_render_context)
                        current_step_resolved_inputs[input_logical_name] = resolved_input_uri
                    except jinja2.exceptions.UndefinedError as e:
                        logger.warning(f"Input template '{input_uri_template}' for '{input_logical_name}' in step '{step_def.name}' contains undefined variables (likely a future dependency): {e}. Keeping original template.")
                        current_step_resolved_inputs[input_logical_name] = input_uri_template
                    except Exception as e:
                        logger.error(f"Error rendering input template '{input_uri_template}' for '{input_logical_name}' in step '{step_def.name}': {e}. Keeping original template.", exc_info=True)
                        current_step_resolved_inputs[input_logical_name] = input_uri_template

            # Блоки для JobInstanceContext и старой логики depends_on_job_ids_list удалены,
            # так как новый JobInstance (ниже) напрямую использует необходимые поля
            # и depends_on_job_ids вычисляются из current_job_dependency_names.

            # 7. Создание JobInstance
            actual_job_dependency_ids = [
                step_name_to_job_id_map[dep_name]
                for dep_name in current_job_dependency_names
                if dep_name in step_name_to_job_id_map
            ]
            # Log if a declared dependency was not found (e.g., typo or future step not yet processed - though loop order should prevent latter)
            for dep_name in current_job_dependency_names:
                if dep_name not in step_name_to_job_id_map:
                    logger.warning(f"Step '{step_def.name}' has an unresolved dependency on step '{dep_name}'. It might be a typo or a step defined later in the task which is not supported for direct dependency resolution during expansion.")

            job_instance = JobInstance(
                id=job_instance_id,
                name=job_instance_display_name,
                task_instance_id=task_instance.id,
                experiment_instance_id=task_instance.experiment_instance_id,
                job_definition_ref=job_definition_reference,
                parameters=final_resolved_parameters,
                inputs=current_step_resolved_inputs,
                resolved_outputs=current_job_produced_outputs, # Outputs resolved at expansion time
                depends_on_job_ids=list(set(actual_job_dependency_ids)), # Actual JobInstance IDs
                status=InstanceStatus.PENDING
                # 'outputs' field in JobInstance might be used for artifact URIs generated at runtime by worker
            )
            job_instances_for_task.append(job_instance)
            step_name_to_job_id_map[step_def.name] = job_instance.id

        return job_instances_for_task # Добавлен return

if __name__ == '__main__':
    from logger import init_logging
    init_logging() # Initialize custom logging
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
                        "iterateOver": { # <--- ДОБАВЛЯЕМ ITERATE_OVER
                            "learning_rate": [0.01, 0.001],
                            "optimizer": ["adam", "sgd"],
                            "version_suffix": ["iter1", "iter2"] # Добавляем итерацию по version_suffix
                        }
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
                logger.debug(f"    Context Job Def Ref: {ji.context.job_definition_ref}")
                logger.debug(f"    Context Resolved Params: {ji.context.resolved_parameters}")
                logger.debug(f"    Context Resolved Inputs: {ji.context.resolved_inputs}")
                logger.debug(f"    Context Resolved Outputs: {ji.context.resolved_outputs}")
                logger.debug(f"    Context Resources: {ji.context.resources_request}")
                logger.debug(f"    Context Priority: {ji.context.priority}")
                logger.debug(f"    Depends on Job IDs: {ji.depends_on_job_ids}")
                logger.debug(f"    Created At: {ji.created_at}")
                
    except ValueError as ve:
        logger.error(f"Error during expansion: {ve}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
