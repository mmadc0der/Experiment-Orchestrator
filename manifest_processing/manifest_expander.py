import itertools
import uuid
import logging
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

        # TODO: Добавить валидацию, что все task_reference в ExperimentDefinition существуют в self.task_defs

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
            parameters=self.experiment_def.spec.parameters_schema or {}, # TODO: Разрешить параметры эксперимента
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
            
            # Параметры самого эксперимента (глобальные)
            # TODO: Разрешить глобальные параметры эксперимента, если они есть
            experiment_global_params = {} # Placeholder

            iteration_parameter_sets = []
            if pipeline_task_def.iterate_over:
                param_names = list(pipeline_task_def.iterate_over.keys())
                param_value_lists = [pipeline_task_def.iterate_over[name] for name in param_names]
                
                for combination_values in itertools.product(*param_value_lists):
                    iteration_params = dict(zip(param_names, combination_values))
                    iteration_parameter_sets.append(iteration_params)
            else:
                # Если нет iterate_over, будет одна "итерация" с пустым набором итерационных параметров
                iteration_parameter_sets.append({})

            current_task_instance_ids_for_pipeline_task: List[str] = []

            for i, iter_params in enumerate(iteration_parameter_sets):
                # Объединяем параметры: итерационные имеют приоритет над параметрами вызова задачи
                final_task_params = {**task_call_params, **iter_params}

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
                    experiment_params=experiment_global_params, # Передаем глобальные параметры эксперимента
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
        
        job_instances_for_task: List[JobInstance] = []
        
        job_name_to_id_map: Dict[str, str] = {} # Карта для ID джобов по имени шага
        current_task_instance_step_outputs: Dict[str, Dict[str, str]] = {} # Карта выходов шагов

        for step_def in task_def.spec.steps:
            job_instance_id = f"job-{uuid.uuid4().hex[:16]}" 
            job_instance_display_name = f"{task_instance.name}-{step_def.name}-{job_instance_id[-8:]}"
            job_definition_reference = f"{task_def.metadata['name']}:{step_def.name}"

            # 1. Базовый контекст для рендеринга
            base_render_context = {
                "parameters": {**(experiment_params or {}), **(task_call_params or {})},
                "experiment": {"id": task_instance.experiment_instance_id},
                "task": {"id": task_instance.id, "name": task_instance.name},
                "job": {"id": job_instance_id}
            }

            # 2. Разрешение параметров шага
            resolved_step_params = {}
            if step_def.parameters:
                for key, value_template in step_def.parameters.items():
                    if isinstance(value_template, str):
                        resolved_step_params[key] = self._render_template(value_template, base_render_context)
                    elif isinstance(value_template, list):
                        resolved_step_params[key] = [
                            self._render_template(item, base_render_context) if isinstance(item, str) else item
                            for item in value_template
                        ]
                    else:
                        resolved_step_params[key] = value_template
            
            # Итоговые разрешенные параметры (эксперимент + задача/итерация + шаг)
            final_resolved_parameters = {
                **(experiment_params or {}),
                **(task_call_params or {}), 
                **resolved_step_params      
            }

            # 3. Разрешение ВЫХОДНЫХ артефактов
            current_step_resolved_outputs: Dict[str, str] = {}
            if step_def.outputs_templates:
                output_render_context = {
                    "parameters": final_resolved_parameters, 
                    "experiment": base_render_context["experiment"],
                    "task": base_render_context["task"],
                    "job": base_render_context["job"]
                    # "steps" не нужен для имен выходных файлов
                }
                for output_logical_name, output_filename_template in step_def.outputs_templates.items():
                    rendered_output_filename = self._render_template(output_filename_template, output_render_context)
                    current_step_resolved_outputs[output_logical_name] = generate_artifact_uri(
                        experiment_id=task_instance.experiment_instance_id,
                        task_instance_id=task_instance.id,
                        job_instance_id=job_instance_id,
                        output_name_template=rendered_output_filename 
                    )
            
            # Сохраняем выходы текущего шага для использования следующими шагами
            if current_step_resolved_outputs:
                 current_task_instance_step_outputs[step_def.name] = {
                     "outputs": current_step_resolved_outputs
                 }

            # 4. Разрешение ВХОДНЫХ артефактов
            current_step_resolved_inputs: Dict[str, str] = {}
            # logger.debug(f"For step '{step_def.name}', step_def.inputs is: {step_def.inputs}")
            if step_def.inputs:
                input_render_context = {
                    "parameters": final_resolved_parameters,
                    "experiment": base_render_context["experiment"],
                    "task": base_render_context["task"],
                    "job": base_render_context["job"],
                    "steps": current_task_instance_step_outputs
                }
                for input_logical_name, input_uri_template in step_def.inputs.items():
                    # logger.debug(f"Processing input '{input_logical_name}' with template '{input_uri_template}' for step '{step_def.name}'")
                    try:
                        resolved_input_uri = self._render_template(input_uri_template, input_render_context)
                        current_step_resolved_inputs[input_logical_name] = resolved_input_uri
                    except Exception as e:
                        logger.error(f"Error rendering input '{input_logical_name}' with template '{input_uri_template}' for step '{step_def.name}': {e}. Keeping original template.", exc_info=True)
                        current_step_resolved_inputs[input_logical_name] = input_uri_template 

            # 5. Создание JobInstanceContext
            job_context = JobInstanceContext(
                experiment_id=task_instance.experiment_instance_id,
                task_instance_id=task_instance.id,
                job_instance_id=job_instance_id,
                job_definition_ref=job_definition_reference,
                resolved_parameters=final_resolved_parameters, 
                resolved_inputs=current_step_resolved_inputs if current_step_resolved_inputs else None,
                resolved_outputs=current_step_resolved_outputs if current_step_resolved_outputs else None,
                resources_request=step_def.resources_request.model_dump(exclude_none=True) if step_def.resources_request else {},
                priority=step_def.priority
            )
            
            # 6. Определение зависимостей JobInstance
            depends_on_job_ids_list: List[str] = []
            if step_def.depends_on:
                for dep_step_name in step_def.depends_on:
                    if dep_step_name in job_name_to_id_map: # Используем job_name_to_id_map
                        depends_on_job_ids_list.append(job_name_to_id_map[dep_step_name])
                    else:
                        logger.warning(f"Intra-task job dependency '{dep_step_name}' for job step "
                              f"'{step_def.name}' in task '{task_instance.name}' not found. "
                              f"Ensure steps are defined before being depended upon or check for circular dependencies.")
            
            # 7. Создание JobInstance
            job_instance = JobInstance(
                id=job_instance_id,
                name=job_instance_display_name, 
                task_instance_id=task_instance.id,
                job_definition_ref=job_definition_reference, 
                context=job_context,
                status=InstanceStatus.PENDING, 
                depends_on_job_ids=depends_on_job_ids_list, # Исправлено на depends_on_job_ids_list
                created_at=datetime.now(timezone.utc) 
            )
            logger.info(f"Created JobInstance: ID={job_instance.id}, Name='{job_instance.name}'")
            
            job_instances_for_task.append(job_instance) # Исправлено на job_instances_for_task
            job_name_to_id_map[step_def.name] = job_instance.id # Исправлено на job_name_to_id_map

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
