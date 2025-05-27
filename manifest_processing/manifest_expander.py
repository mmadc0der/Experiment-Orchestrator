import itertools
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional
from jinja2 import Environment, select_autoescape

# Предполагаем, что models находится на том же уровне, что и manifest_processing
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
                # TODO: Более гранулированная обработка ошибок и логирование
                print(f"Warning: Skipping document due to initial parsing error: {e}. Document: {doc_data.get('metadata', {}).get('name', 'Unknown')}")
                continue

            kind = any_def.kind
            metadata_name = any_def.metadata.get("name")

            if not metadata_name:
                print(f"Warning: Skipping document of kind '{kind}' because it has no metadata.name.")
                continue

            try:
                if kind == "Experiment": # Должно соответствовать Literal в ExperimentDefinition
                    exp_def = ExperimentDefinition(**doc_data)
                    found_experiment_defs.append(exp_def)
                elif kind == "Task":
                    task_def = TaskDefinition(**doc_data)
                    if metadata_name in self.task_defs:
                        print(f"Warning: Duplicate TaskDefinition name '{metadata_name}'. Overwriting.")
                    self.task_defs[metadata_name] = task_def
                else:
                    print(f"Warning: Unsupported kind '{kind}' for document '{metadata_name}'. Skipping.")
            except Exception as e:
                # TODO: Более гранулированная обработка ошибок и логирование
                print(f"Warning: Skipping document '{metadata_name}' (kind: {kind}) due to Pydantic validation error: {e}")
        
        if not found_experiment_defs:
            raise ValueError("No ExperimentDefinition found in the manifest.")
        if len(found_experiment_defs) > 1:
            # TODO: Решить, как обрабатывать несколько ExperimentDefinition. Пока берем первый.
            print(f"Warning: Multiple ExperimentDefinitions found. Using the first one: '{found_experiment_defs[0].metadata.get('name')}'.")
        
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
        
        all_task_instances: List[TaskInstance] = []
        all_job_instances: List[JobInstance] = []
        
        # Карта для отслеживания созданных TaskInstance по их имени в конвейере.
        # Значение - список ID созданных TaskInstance (может быть несколько из-за iterate_over)
        created_pipeline_task_instances_map: Dict[str, List[str]] = {}

        for pipeline_task_def in self.experiment_def.spec.pipeline:
            task_def_name = pipeline_task_def.task_reference
            task_def = self.task_defs.get(task_def_name)

            if not task_def:
                print(f"Warning: TaskDefinition '{task_def_name}' referenced in pipeline task '{pipeline_task_def.name}' not found. Skipping.")
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
                            print(f"Warning: Dependency '{dep_pipeline_task_name}' for pipeline task '{pipeline_task_def.name}' not yet processed or not found. Check pipeline order.")
                
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
                all_task_instances.append(task_instance)
                current_task_instance_ids_for_pipeline_task.append(task_instance.id)

                # Для каждого TaskInstance создаем его JobInstances
                job_instances_for_task = self._expand_task_into_jobs(
                    task_instance=task_instance,
                    task_def=task_def,
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
        if not isinstance(template_string, str):
            return template_string
        
        if '{{' not in template_string and '{%' not in template_string:
            return template_string

        try:
            env = Environment(
                loader=None, 
                autoescape=select_autoescape(['html', 'xml'])
            )
            template = env.from_string(template_string)
            return template.render(context)
        except Exception as e:
            print(f"Warning: Failed to render template: {template_string}. Error: {e}. Returning original string.")
            return template_string

    def _expand_task_into_jobs(
        self,
        task_instance: TaskInstance,
        task_def: TaskDefinition,
        experiment_params: Dict[str, Any], # Общие параметры эксперимента
        task_call_params: Dict[str, Any]   # Параметры из iterateOver или pipeline.parameters для этого TaskInstance
    ) -> List[JobInstance]:
        all_job_instances: List[JobInstance] = []
        created_job_instance_map: Dict[str, str] = {} # step_name -> job_instance_id

        if not task_def.spec.steps:
            print(f"Warning: TaskDefinition '{task_def.metadata['name']}' for TaskInstance '{task_instance.name}' has no steps defined. No jobs will be created.")
            return []

        for step_def in task_def.spec.steps:
            # 1. Генерируем ID для JobInstance заранее, так как он нужен в контексте
            job_instance_id = self._generate_instance_id(f"job-{task_instance.name}-{step_def.name}", "job")
            job_definition_reference = f"{task_def.metadata['name']}:{step_def.name}"

            # 2. Формируем и разрешаем параметры для JobContext
            # Контекст, доступный для рендеринга шаблонов в параметрах шага
            # job_instance_id уже сгенерирован на этом этапе
            render_context = {
                "parameters": {**(experiment_params or {}), **(task_call_params or {})},
                "experiment": {"id": task_instance.experiment_instance_id}, # Используем ID из task_instance
                "task": {"id": task_instance.id, "name": task_instance.name},
                "job": {"id": job_instance_id}
            }

            # Параметры в порядке приоритета: step_def.parameters > task_call_params > experiment_params
            # Сначала объединяем их, чтобы получить окончательный набор параметров перед рендерингом.
            merged_params_before_render = {
                **(experiment_params or {}),
                **(task_call_params or {}),
                **(step_def.parameters or {}) # step_def.parameters это Dict, может быть пустым
            }

            resolved_job_params = {}
            for key, value in merged_params_before_render.items():
                if isinstance(value, str):
                    # Если значение - строка, пытаемся его отрендерить
                    resolved_job_params[key] = self._render_template(value, render_context)
                elif isinstance(value, list):
                    # Если значение - список, пытаемся отрендерить каждый строковый элемент
                    resolved_job_params[key] = [
                        self._render_template(item, render_context) if isinstance(item, str) else item
                        for item in value
                    ]
                # TODO: Добавить рекурсивную обработку для словарей, если их значения также могут быть шаблонами.
                #       Например, если параметр сам по себе является словарем с шаблонами внутри.
                #       Пока что словари передаются как есть.
                else:
                    # Для других типов (числа, булевы и т.д.) оставляем как есть
                    resolved_job_params[key] = value

            # 3. Разрешение входных артефактов (пока заглушка)
            # resolved_job_input_templates: Dict[str, str] = {}
            # if step_def.inputs:
            #     for input_name, artifact_ref in step_def.inputs.items():
            #         resolved_job_input_templates[input_name] = f"placeholder_uri_for_{artifact_ref}"
            
            # 4. Формирование output_templates для JobContext (пока заглушка)
            # output_artifact_templates: Dict[str, str] = {}
            # if step_def.outputs: # JobStepDefinition.outputs это Dict[str, ArtifactDefinition]
            #     for output_logical_name in step_def.outputs.keys():
            #         output_artifact_templates[output_logical_name] = \
            #             f"artifact_placeholder/{task_instance.experiment_instance_id}/" \
            #             f"{task_instance.id}/{job_instance_id}/outputs/{output_logical_name}"

            # 5. Создание JobInstanceContext
            job_context = JobInstanceContext(
                experiment_id=task_instance.experiment_instance_id,
                task_instance_id=task_instance.id,
                job_instance_id=job_instance_id,
                job_definition_ref=job_definition_reference,
                parameters=resolved_job_params,
                # inputs=resolved_job_input_templates, # Раскомментировать, когда будет готово
                # outputs_templates=output_artifact_templates, # Раскомментировать, когда будет готово
                # resources_request=step_def.resources # Если поле resources есть в JobStepDefinition
            )

            job_instance_name_for_id = f"{task_instance.name}-{step_def.name}"
            
            # 6. Определение зависимостей для JobInstance (внутри задачи)
            current_step_dependencies: List[str] = []
            if step_def.depends_on: # JobStepDefinition.depends_on
                for dep_step_name in step_def.depends_on:
                    if dep_step_name in created_job_instance_map:
                        current_step_dependencies.append(created_job_instance_map[dep_step_name])
                    else:
                        print(f"Warning: Intra-task job dependency '{dep_step_name}' for job step "
                              f"'{step_def.name}' in task '{task_instance.name}' not found. "
                              f"Ensure steps are defined before being depended upon or check for circular dependencies.")
            
            # 7. Создание JobInstance
            job_instance = JobInstance(
                id=job_instance_id,
                name=job_instance_name_for_id,
                task_instance_id=task_instance.id,
                job_definition_ref=job_definition_reference, # Дублирует контекст, но пока оставим
                context=job_context,
                status=InstanceStatus.PENDING, # Статус по умолчанию
                depends_on_job_ids=current_step_dependencies,
                created_at=datetime.now(timezone.utc)
            )
            
            all_job_instances.append(job_instance)
            created_job_instance_map[step_def.name] = job_instance.id # Сохраняем ID джоба

        return all_job_instances

if __name__ == '__main__':
    print("ManifestExpander basic test")
    
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
                            "optimizer": ["adam", "sgd"]
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
                "description": "Prepares data, can be iterated.",
                # Добавим параметры в схему задачи, чтобы видеть их разрешение
                "parametersSchema": {
                    "learning_rate": {"type": "number", "description": "Learning rate for training."},
                    "optimizer": {"type": "string", "description": "Optimizer to use."},
                    "base_param": {"type": "string", "description": "A base parameter."}
                },
                "steps": [
                    {
                        "name": "download-step",
                        "executor": "downloader_job.Downloader",
                        "parameters": {
                             # Пример использования параметра задачи в шаге
                            "source_url": "http://example.com/data?opt={{parameters.optimizer}}"
                        }
                    },
                    {
                        "name": "process-step",
                        "executor": "processor_job.Processor",
                        "depends_on": ["download-step"],
                        "parameters": {
                            "rate": "{{parameters.learning_rate}}"
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
        print(f"Loaded ExperimentDefinition: {expander.experiment_def.metadata['name'] if expander.experiment_def else 'None'}")
        print(f"Loaded TaskDefinitions: {list(expander.task_defs.keys())}")

        if expander.experiment_def:
            exp_instance, task_instances, job_instances = expander.expand_experiment()
            
            print(f"\n--- Generated ExperimentInstance ---")
            print(f"ID: {exp_instance.id}, Name: {exp_instance.name}")
            print(f"Parameters: {exp_instance.parameters}") # Пока не разрешаются, будет пусто или схема

            print(f"\n--- Generated TaskInstances ({len(task_instances)}) ---")
            for ti in task_instances:
                print(f"  ID: {ti.id}, Name: {ti.name}, Pipeline Name: {ti.name_in_pipeline}")
                print(f"    Definition: {ti.task_definition_ref}")
                print(f"    Parameters: {ti.parameters}")
                print(f"    Depends on Task IDs: {ti.depends_on_task_ids}")
                # Считаем джобы для этой задачи
                jobs_for_this_task = [j for j in job_instances if j.task_instance_id == ti.id]
                print(f"    Number of jobs: {len(jobs_for_this_task)}")

            print(f"\n--- Generated JobInstances ({len(job_instances)}) ---")
            for ji in job_instances:
                print(f"  ID: {ji.id}, Name: {ji.name}")
                print(f"    TaskInstance ID: {ji.task_instance_id}")
                # print(f"    Executor: {ji.executor}")
                print(f"    Context Params: {ji.context.parameters}")
                print(f"    Depends on Job IDs: {ji.depends_on_job_ids}")
                
    except ValueError as ve:
        print(f"Error during expansion: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}", type(e))
        import traceback
        traceback.print_exc()
