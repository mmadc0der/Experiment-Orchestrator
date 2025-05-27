from typing import Any
from pydantic import BaseModel, ValidationError
import logging

from job import Job, JobContext

logger = logging.getLogger(__name__)

class SumJobParams(BaseModel):
    a: int
    b: int

class SumJobResult(BaseModel):
    sum: int

class SumJob(Job):
    """
    A simple job that calculates the sum of two numbers.
    """

    def execute(self, params: Any) -> SumJobResult:
        """
        Executes the sum operation.

        Args:
            params: Must be a dictionary or an object convertible to SumJobParams,
                    e.g., {"a": 10, "b": 5} or SumJobParams(a=10, b=5).

        Returns:
            SumJobResult: An object containing the sum.

        Raises:
            ValueError: If params are not in the expected format or type.
        """
        logger.info(f"SumJob (job_id: {self.context.job_id}): Starting execution with params: {params}")
        try:
            if isinstance(params, dict):
                validated_params = SumJobParams(**params)
            elif isinstance(params, SumJobParams):
                validated_params = params
            else:
                raise ValueError("Input params must be a dictionary or SumJobParams instance.")
            
            logger.debug(f"SumJob (job_id: {self.context.job_id}): Validated params: a={validated_params.a}, b={validated_params.b}")
            
            result_sum = validated_params.a + validated_params.b
            logger.info(f"SumJob (job_id: {self.context.job_id}): Calculation complete. Result: {result_sum}")
            
            return SumJobResult(sum=result_sum)

        except ValidationError as e:
            logger.error(f"SumJob (job_id: {self.context.job_id}): Parameter validation failed: {e}")
            # В реальной системе здесь можно было бы вернуть специальный объект ошибки или re-raise кастомное исключение
            raise ValueError(f"Invalid parameters for SumJob: {e}")
        except Exception as e:
            logger.error(f"SumJob (job_id: {self.context.job_id}): An unexpected error occurred: {e}", exc_info=True)
            raise # Re-raise непредвиденные ошибки
