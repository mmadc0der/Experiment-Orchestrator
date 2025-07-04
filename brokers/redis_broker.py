import json
import logging
from typing import Any, Dict, List, Optional

import redis # type: ignore

from models.instances import InstanceStatus, JobInstance

logger = logging.getLogger(__name__)

class RedisBroker:
    placeholder: Any = "__PENDING__"
    empty_hash_placeholder_field: str = "__empty_hash__"
    empty_hash_placeholder_value: str = "1"

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, 
                 username: Optional[str] = None, password: Optional[str] = None, 
                 key_prefix_user: str = ""):
        """
        Initializes the RedisBroker.

        Args:
            host: Redis server host.
            port: Redis server port.
            db: Redis database number.
            username: Username for Redis authentication (optional).
            password: Password for Redis authentication (optional).
            key_prefix_user: Prefix to be added to all keys (e.g., 'user@').
        """
        self.key_prefix_user = key_prefix_user
        try:
            self.redis_client = redis.StrictRedis(
                host=host, 
                port=port, 
                db=db, 
                username=username, 
                password=password, 
                decode_responses=False
            )
            self.redis_client.ping() # Check connection
            logger.info(f"Successfully connected to Redis at {host}:{port}, db {db} (user: {username or 'default'})")
        except redis.exceptions.AuthenticationError as e:
            logger.error(f"Redis authentication failed for user '{username}': {e}")
            raise
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Failed to connect to Redis at {host}:{port}, db {db}: {e}")
            raise

    def _get_prefixed_key(self, key_name: str) -> str:
        """
        Applies the user-specific prefix to a key if the prefix is defined.
        """
        if self.key_prefix_user:
            # Assuming self.key_prefix_user (e.g., 'expdb@') already contains
            # the desired separator if one is needed.
            return f"{self.key_prefix_user}{key_name}"
        return key_name

    def _serialize(self, value: Any) -> bytes:
        """Serializes a Python object to bytes for Redis storage."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, (int, float)):
            return str(value).encode('utf-8')
        if isinstance(value, str):
            return value.encode('utf-8')
        return json.dumps(value).encode('utf-8')

    def _deserialize(self, value_bytes: Optional[bytes]) -> Any:
        """Deserializes bytes from Redis to a Python object."""
        if value_bytes is None:
            return None
        try:
            # Attempt to decode as UTF-8 string first, then try JSON
            decoded_str = value_bytes.decode('utf-8')
            try:
                return json.loads(decoded_str)
            except json.JSONDecodeError:
                # If it's not JSON, it might be a simple string, int, or float stored as string
                if decoded_str.isdigit():
                    return int(decoded_str)
                try:
                    return float(decoded_str)
                except ValueError:
                    return decoded_str # Return as string if not int/float
        except UnicodeDecodeError:
            logger.warning("Could not decode bytes as UTF-8, returning raw bytes.")
            return value_bytes # Fallback for non-UTF-8 binary data

    def store_job_details(self, job_id: str, job_instance: JobInstance) -> None:
        """Stores the full JobInstance object as a JSON string in Redis."""
        key = self._get_prefixed_key(f"job:{job_id}:details")
        try:
            job_instance_json = job_instance.model_dump_json()
            self.redis_client.set(key, job_instance_json)
            logger.debug(f"Stored details for job {job_id} at key {key}")
        except Exception as e:
            logger.error(f"Error storing details for job {job_id}: {e}", exc_info=True)
            raise

    def get_job_details(self, job_id: str) -> Optional[str]:
        """Retrieves the JobInstance JSON string from Redis."""
        key = self._get_prefixed_key(f"job:{job_id}:details")
        try:
            value_bytes = self.redis_client.get(key)
            if value_bytes:
                logger.debug(f"Retrieved details for job {job_id} from key {key}")
                return value_bytes.decode('utf-8')
            logger.warning(f"No details found for job {job_id} at key {key}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving details for job {job_id}: {e}", exc_info=True)
            return None

    def set_job_input_literals(self, job_id: str, literals: Dict[str, Any]) -> None:
        """Stores the input literal values for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:input_literals")
        serialized_literals = {k: self._serialize(v) for k, v in literals.items()}
        if serialized_literals: # Redis HMSET requires a non-empty mapping
            self.redis_client.hmset(key, serialized_literals)
            logger.debug(f"Stored input literals for job {job_id} at key {key}")
        else:
            logger.debug(f"No input literals to store for job {job_id} at key {key}")

    def get_job_input_literals(self, job_id: str) -> Dict[str, Any]:
        """Retrieves the input literal values for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:input_literals")
        serialized_literals = self.redis_client.hgetall(key)
        return {k.decode('utf-8'): self._deserialize(v) for k, v in serialized_literals.items()}

    def set_job_resolved_inputs(self, job_id: str, resolved_inputs: Dict[str, Any]) -> None:
        """Stores the resolved input parameters for a job."""
        # key variable is defined below for both branches
        if not resolved_inputs:
            key = self._get_prefixed_key(f"job:{job_id}:resolved_inputs") # Define key here for logging
            prefixed_key = self._get_prefixed_key(f"job:{job_id}:resolved_inputs") # Get key for delete and hmset
            self.redis_client.delete(prefixed_key) # Ensure clean state
            self.redis_client.hmset(prefixed_key, {self.empty_hash_placeholder_field: self.empty_hash_placeholder_value})
            logger.debug(f"Stored empty placeholder for resolved inputs for job {job_id} at {key}")
            return
        try:
            key = self._get_prefixed_key(f"job:{job_id}:resolved_inputs") # Define key here for this branch
            serialized_inputs = {k: self._serialize(v) for k, v in resolved_inputs.items()}
            self.redis_client.hmset(key, serialized_inputs)
            logger.debug(f"Stored resolved inputs for job {job_id} at {key}: {list(resolved_inputs.keys())}")
        except Exception as e:
            logger.error(f"Error setting resolved inputs for job {job_id}: {e}", exc_info=True)
            raise

    def get_job_resolved_inputs(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves the resolved input parameters for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:resolved_inputs")
        try:
            value_dict_bytes = self.redis_client.hgetall(key)
            if not value_dict_bytes:
                logger.warning(f"No resolved inputs found for job {job_id} at key {key}")
                return None
            
            if self.empty_hash_placeholder_field.encode('utf-8') in value_dict_bytes and \
               len(value_dict_bytes) == 1:
                logger.debug(f"Resolved inputs for job {job_id} is an intentionally empty set.")
                return {}

            deserialized_inputs = {k.decode('utf-8'): self._deserialize(v) 
                                   for k, v in value_dict_bytes.items()}
            logger.debug(f"Retrieved resolved inputs for job {job_id} from {key}")
            return deserialized_inputs
        except Exception as e:
            logger.error(f"Error getting resolved inputs for job {job_id}: {e}", exc_info=True)
            return None

    def set_job_output_value(self, job_id: str, output_name: str, value: Any) -> None:
        """Stores a specific output value for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:outputs:{output_name}")
        self.redis_client.set(key, self._serialize(value))
        logger.debug(f"Stored output '{output_name}' for job {job_id} at key {key}")

    def get_job_output_value(self, job_id: str, output_name: str) -> Any:
        """Retrieves a specific output value for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:outputs:{output_name}")
        value_bytes = self.redis_client.get(key)
        return self._deserialize(value_bytes)

    def register_job_outputs(self, job_id: str, output_names: List[str], placeholder: Any = None) -> None:
        if placeholder is None:
            placeholder = self.placeholder
        """Registers output keys for a job with a placeholder value (e.g., to signify they are pending)."""
        # This could also be implemented using a set for job_id:outputs_pending or similar
        # For simplicity, we'll set each output key with a placeholder.
        serialized_placeholder = self._serialize(placeholder)
        for output_name in output_names:
            key = self._get_prefixed_key(f"job:{job_id}:outputs:{output_name}")
            # SETNX ensures we only set it if it doesn't exist, useful if registration can happen multiple times
            self.redis_client.setnx(key, serialized_placeholder)
            logger.debug(f"Registered output '{output_name}' for job {job_id} with placeholder at key {key}")

    def set_job_status(self, job_id: str, status: str) -> None:
        """Sets the status for a job. Expects status to be the string value of the enum.
        Example: InstanceStatus.PENDING.value
        """
        key = self._get_prefixed_key(f"job:{job_id}:status")
        self.redis_client.set(key, status) # Store enum by its value (status is already a string value)
        logger.debug(f"Set status for job {job_id} to {status} at key {key}")

    def set_job_error_info(self, job_id: str, error_message: str) -> None:
        """Stores error information for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:error")
        try:
            self.redis_client.set(key, error_message)
            logger.debug(f"Error info for job {job_id} set at key {key}")
        except Exception as e:
            logger.error(f"Error setting error info for job {job_id}: {e}", exc_info=True)

    def get_job_error_info(self, job_id: str) -> Optional[str]:
        """Retrieves error information for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:error")
        try:
            value_bytes = self.redis_client.get(key)
            if value_bytes:
                return value_bytes.decode('utf-8')
            return None
        except Exception as e:
            logger.error(f"Error retrieving error info for job {job_id}: {e}", exc_info=True)
            return None

    def get_job_status(self, job_id: str) -> Optional[InstanceStatus]:
        """Retrieves the status for a job."""
        key = self._get_prefixed_key(f"job:{job_id}:status")
        status_value_bytes = self.redis_client.get(key)
        if status_value_bytes:
            status_value = status_value_bytes.decode('utf-8')
            try:
                return InstanceStatus(status_value)
            except ValueError:
                logger.error(f"Invalid status value '{status_value}' retrieved for job {job_id}")
                return None
        return None

    # --- Scheduler-specific Sets and Queues ---

    def add_job_to_pending_set(self, set_key_name: str, job_id: str) -> None:
        """Adds a job ID to a Redis set (e.g., for pending jobs)."""
        # Note: set_key_name is the base name, prefix is applied by _get_prefixed_key.
        prefixed_key = self._get_prefixed_key(set_key_name)
        try:
            self.redis_client.sadd(prefixed_key, job_id)
            logger.debug(f"Added job {job_id} to set {prefixed_key}")
        except Exception as e:
            logger.error(f"Error adding job {job_id} to set {key}: {e}", exc_info=True)
            raise

    def get_job_ids_from_set(self, set_key_name: str) -> List[str]:
        """Retrieves all job IDs from a Redis set."""
        prefixed_key = self._get_prefixed_key(set_key_name)
        try:
            job_ids_bytes = self.redis_client.smembers(prefixed_key)
            job_ids = [jid.decode('utf-8') for jid in job_ids_bytes]
            logger.debug(f"Retrieved {len(job_ids)} job IDs from set {prefixed_key}")
            return job_ids
        except Exception as e:
            logger.error(f"Error retrieving job IDs from set {key}: {e}", exc_info=True)
            return []

    def remove_job_from_set(self, set_key_name: str, job_id: str) -> None:
        """Removes a job ID from a Redis set."""
        prefixed_key = self._get_prefixed_key(set_key_name)
        try:
            self.redis_client.srem(prefixed_key, job_id)
            logger.debug(f"Removed job {job_id} from set {prefixed_key}")
        except Exception as e:
            logger.error(f"Error removing job {job_id} from set {key}: {e}", exc_info=True)
            raise

    def push_job_to_worker_queue(self, queue_key_name: str, job_id: str) -> None:
        """Pushes a job ID to a Redis list (worker queue)."""
        # Note: queue_key_name is the base name, prefix is applied by _get_prefixed_key.
        prefixed_key = self._get_prefixed_key(queue_key_name)
        try:
            self.redis_client.rpush(prefixed_key, job_id)
            logger.debug(f"Pushed job {job_id} to queue {prefixed_key}")
        except Exception as e:
            logger.error(f"Error pushing job {job_id} to queue {key}: {e}", exc_info=True)
            raise

    # --- Generic Queue Operations (can be used by worker or other components) ---
    def get_all_job_outputs(self, job_id: str) -> Dict[str, Any]:
        """Retrieves all output values for a given job."""
        outputs_pattern = self._get_prefixed_key(f"job:{job_id}:outputs:*")
        output_keys = [key.decode('utf-8') for key in self.redis_client.keys(outputs_pattern)]
        
        outputs = {}
        for key in output_keys:
            output_name = key.split(':')[-1]
            value_bytes = self.redis_client.get(key)
            outputs[output_name] = self._deserialize(value_bytes)
        return outputs

    def add_job_to_queue(self, queue_name: str, job_id: str) -> None:
        """Adds a job ID to a specific queue (e.g., 'pending_jobs')."""
        prefixed_queue_name = self._get_prefixed_key(queue_name)
        self.redis_client.rpush(prefixed_queue_name, job_id)
        logger.info(f"Added job {job_id} to queue {queue_name}")

    def get_job_from_queue(self, queue_name: str, timeout: int = 0) -> Optional[str]:
        """Retrieves a job ID from a specific queue. Blocks if timeout > 0."""
        # BLPOP is a blocking list pop operation.
        # It returns a tuple (list_name, item_popped) or None if timeout occurs.
        prefixed_queue_name = self._get_prefixed_key(queue_name)
        result = self.redis_client.blpop([prefixed_queue_name], timeout=timeout)
        if result:
            job_id_bytes = result[1]
            job_id = job_id_bytes.decode('utf-8')
            logger.info(f"Retrieved job {job_id} from queue {queue_name}")
            return job_id
        return None

    # Example of a more complex operation: checking if all dependencies for a job are met
    # This is illustrative and might live in the scheduler or a different service logic
    def check_dependencies_met(self, job_id: str, dependency_job_ids: List[str], required_output_name: str = "result") -> bool:
        """Checks if all specified dependency jobs have successfully completed and produced a specific output."""
        if not dependency_job_ids:
            return True # No dependencies, so they are met

        for dep_job_id in dependency_job_ids:
            dep_status = self.get_job_status(dep_job_id)
            if dep_status != InstanceStatus.SUCCEEDED:
                logger.debug(f"Dependency job {dep_job_id} for job {job_id} not SUCCEEDED (status: {dep_status}).")
                return False
            
            # Optionally, check if a specific output from the dependency is available
            # This depends on how outputs are named and registered.
            # For this example, let's assume a common output name like 'result'.
            dep_output = self.get_job_output_value(dep_job_id, required_output_name)
            if dep_output is None or dep_output == self.placeholder:
                logger.debug(f"Dependency job {dep_job_id} for job {job_id} has not produced output '{required_output_name}'.")
                return False
        return True

if __name__ == '__main__':
    # Basic test and usage example
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s')

    try:
        broker = RedisBroker(host='localhost', port=6379, db=0, key_prefix_user="test_user@") # Add username/password if testing auth

        # Test data
        test_job_id = "job_test_123"
        test_dep_job_id = "job_dep_456"

        # Simulate dependency completion
        broker.set_job_status(test_dep_job_id, InstanceStatus.SUCCEEDED)
        broker.set_job_output_value(test_dep_job_id, "result", {"data": "dependency output data"})
        broker.set_job_output_value(test_dep_job_id, "another_output", 12345)

        # Test job registration and status
        broker.set_job_status(test_job_id, InstanceStatus.PENDING)
        print(f"Status of {test_job_id}: {broker.get_job_status(test_job_id)}")

        # Test input literals
        literals_to_set = {"param1": "hello", "param2": 123, "param3": [1, 2, "world"], "param4": {"nested": True}}
        broker.set_job_input_literals(test_job_id, literals_to_set)
        retrieved_literals = broker.get_job_input_literals(test_job_id)
        print(f"Retrieved literals for {test_job_id}: {retrieved_literals}")
        assert literals_to_set == retrieved_literals

        # Test output registration and setting/getting values
        broker.register_job_outputs(test_job_id, ["main_result", "aux_data"])
        print(f"Output 'main_result' for {test_job_id} (before set): {broker.get_job_output_value(test_job_id, 'main_result')}")
        broker.set_job_output_value(test_job_id, "main_result", {"value": "final result"})
        print(f"Output 'main_result' for {test_job_id} (after set): {broker.get_job_output_value(test_job_id, 'main_result')}")
        
        all_outputs = broker.get_all_job_outputs(test_dep_job_id)
        print(f"All outputs for {test_dep_job_id}: {all_outputs}")
        assert all_outputs.get("result") == {"data": "dependency output data"}
        assert all_outputs.get("another_output") == 12345

        # Test dependency check
        can_run = broker.check_dependencies_met(test_job_id, [test_dep_job_id], required_output_name="result")
        print(f"Can job {test_job_id} run based on dependencies? {can_run}")
        assert can_run == True

        # Test queue operations
        queue = "test_job_queue"
        broker.add_job_to_queue(queue, test_job_id)
        broker.add_job_to_queue(queue, "job_another_789")
        
        popped_job1 = broker.get_job_from_queue(queue, timeout=1)
        print(f"Popped from queue: {popped_job1}")
        assert popped_job1 == test_job_id

        popped_job2 = broker.get_job_from_queue(queue, timeout=1)
        print(f"Popped from queue: {popped_job2}")
        assert popped_job2 == "job_another_789"

        popped_job3 = broker.get_job_from_queue(queue, timeout=1) # Should be None
        print(f"Popped from queue (should be None): {popped_job3}")
        assert popped_job3 is None

        print("RedisBroker basic tests passed.")

    except redis.exceptions.ConnectionError:
        logger.error("Could not connect to Redis. Ensure Redis server is running and accessible.")
    except Exception as e:
        logger.error(f"An error occurred during RedisBroker testing: {e}", exc_info=True)

