from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class JobContext:
    """
    A context object that can be passed to a Job during execution.
    It can hold job-specific configurations, paths, or other utilities.
    """
    def __init__(self, job_id: str, config: Optional[Dict[str, Any]] = None, artifacts_path: Optional[str] = None):
        self.job_id = job_id
        self.config = config or {}
        self.artifacts_path = artifacts_path
        # Можно добавить сюда ссылки на логгер, сервисы и т.д.


class Job(ABC):
    """
    Abstract base class for all executable jobs in the orchestrator.
    """

    def __init__(self, context: Optional[JobContext] = None):
        """
        Initializes the job.
        Args:
            context: Optional context for the job, containing job_id, config, etc.
        """
        self.context = context if context else JobContext(job_id="unknown_job")

    @abstractmethod
    def execute(self, params: Any) -> Any:
        """
        The main execution logic of the job.
        This method must be overridden by concrete job implementations.

        Args:
            params: Input parameters for the job. The structure of params
                    is specific to each job implementation.

        Returns:
            Any: The result of the job execution. The structure of the result
                 is specific to each job implementation.
        """
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} (job_id: {self.context.job_id if self.context else 'N/A'})>"
