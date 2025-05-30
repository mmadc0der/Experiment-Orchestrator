import threading
import time
import logging
import json # For Pub/Sub messages
import redis # For redis.exceptions
from typing import List, Dict, Any, Optional

from brokers.redis_broker import RedisBroker
from models.instances import InstanceStatus, ExperimentInstance, TaskInstance, JobInstance

logger = logging.getLogger(__name__)

DEFAULT_POLLING_INTERVAL = 5  # Will be used for Pub/Sub get_message timeout
DEFAULT_WORKER_QUEUE = "default_worker_queue"
DEFAULT_JOB_STATUS_UPDATES_CHANNEL = "job_status_updates" # Default Redis Pub/Sub channel for job status updates from workers
DEFAULT_WORKER_RESOURCES_UPDATES_CHANNEL = "worker_resources_updates" # Default Redis Pub/Sub channel for worker resource updates and heartbeats
DEFAULT_PENDING_JOBS_SET_KEY = "orchestrator_pending_jobs" # Default Redis Set key for pending jobs

class Scheduler:
    def __init__(self, redis_broker: RedisBroker, config: Optional[Dict[str, Any]] = None):
        self.redis_broker = redis_broker
        self.config = config if config else {}
        
        scheduler_config = self.config.get("scheduler", {})
        self.polling_interval = scheduler_config.get("polling_interval_seconds", DEFAULT_POLLING_INTERVAL)
        
        # Get base names from config or use defaults
        base_worker_queue_name = scheduler_config.get("default_worker_queue", DEFAULT_WORKER_QUEUE)
        base_pending_jobs_set_key = scheduler_config.get("pending_jobs_set_key", DEFAULT_PENDING_JOBS_SET_KEY)

        self.job_status_updates_channel_base = scheduler_config.get("job_status_updates_channel", DEFAULT_JOB_STATUS_UPDATES_CHANNEL)
        self.worker_resources_updates_channel_base = scheduler_config.get("worker_resources_updates_channel", DEFAULT_WORKER_RESOURCES_UPDATES_CHANNEL)

        logger.debug(f"Scheduler.__init__: RedisBroker key_prefix_user: '{self.redis_broker.key_prefix_user}'")

        self.job_status_updates_channel = self.redis_broker._get_prefixed_key(self.job_status_updates_channel_base)
        logger.debug(f"Scheduler.__init__: job_status_updates_channel_base: '{self.job_status_updates_channel_base}', job_status_updates_channel (prefixed): '{self.job_status_updates_channel}'")

        self.worker_resources_updates_channel = self.redis_broker._get_prefixed_key(self.worker_resources_updates_channel_base)
        logger.debug(f"Scheduler.__init__: worker_resources_updates_channel_base: '{self.worker_resources_updates_channel_base}', worker_resources_updates_channel (prefixed): '{self.worker_resources_updates_channel}'") 

        # RedisBroker handles prefixing for queue and set names internally when its methods are called.
        # Scheduler should store and pass base names to RedisBroker for these.
        self.worker_queue_name = base_worker_queue_name
        self.pending_jobs_set_key = base_pending_jobs_set_key

        self._stop_event = threading.Event()
        self._pubsub_thread: Optional[threading.Thread] = None
        self.pubsub = None # Initialize later in start() to ensure Redis client is ready
        
        logger.info(
            f"Scheduler initialized. "
            f"Worker queue: '{self.worker_queue_name}', "
            f"Pending jobs set: '{self.pending_jobs_set_key}', "
            f"Job status updates channel (subscribe): '{self.job_status_updates_channel}', "
            f"Worker resources updates channel (subscribe): '{self.worker_resources_updates_channel}', "
            f"PubSub poll timeout: {self.polling_interval}s"
        )

    def submit_experiment_jobs(
        self,
        experiment_instance: ExperimentInstance,
        task_instances: List[TaskInstance],
        job_instances: List[JobInstance]
    ) -> None:
        logger.info(f"Submitting jobs for experiment: {experiment_instance.id}")
        if not self.redis_broker:
            logger.error("RedisBroker is not available. Cannot submit jobs.")
            return

        initial_pending_jobs_to_check = []
        try:
            for job_instance in job_instances:
                job_id = job_instance.id
                logger.debug(f"Submitting job {job_id} for experiment {experiment_instance.id}")

                self.redis_broker.store_job_details(job_id, job_instance)
                logger.debug(f"Stored full job details for job {job_id} in Redis.")

                output_names = []
                if job_instance.context and job_instance.context.resolved_outputs:
                    output_names = list(job_instance.context.resolved_outputs.keys())
                elif job_instance.spec and job_instance.spec.outputs:
                    output_names = [out_param.name for out_param in job_instance.spec.outputs]
                
                if output_names:
                    self.redis_broker.register_job_outputs(job_id, output_names)
                
                self.redis_broker.set_job_status(job_id, InstanceStatus.PENDING)
                self.redis_broker.add_job_to_pending_set(self.pending_jobs_set_key, job_id)
                logger.info(f"Job {job_id} submitted with status PENDING and added to pending set '{self.pending_jobs_set_key}'.")
                
                # If a job has no input references, it might be ready immediately
                if not (job_instance.context and job_instance.context.resolved_inputs):
                    initial_pending_jobs_to_check.append(job_id)

            logger.info(f"Successfully submitted {len(job_instances)} jobs for experiment {experiment_instance.id}.")

            if initial_pending_jobs_to_check:
                logger.info(f"Performing initial check for jobs with no input references: {initial_pending_jobs_to_check}")
                self._evaluate_specific_pending_jobs(initial_pending_jobs_to_check)

        except Exception as e:
            logger.error(f"Error submitting jobs for experiment {experiment_instance.id}: {e}", exc_info=True)

    def start(self) -> None:
        if self._pubsub_thread is not None and self._pubsub_thread.is_alive():
            logger.warning("Scheduler PubSub listener thread is already running.")
            return
        
        logger.info(f"Starting scheduler PubSub listener thread for channels '{self.job_status_updates_channel}' and '{self.worker_resources_updates_channel}'...")
        self._stop_event.clear()
        try:
            # Initialize pubsub client here to ensure it's fresh if start is called after a stop
            self.pubsub = self.redis_broker.redis_client.pubsub()
            self.pubsub.subscribe(self.job_status_updates_channel, self.worker_resources_updates_channel)
            subscribed_channel_names = [self.job_status_updates_channel, self.worker_resources_updates_channel]
            logger.info(f"Subscribed to Redis Pub/Sub channels: {', '.join(subscribed_channel_names)}")
        except Exception as e:
            logger.error(f"Failed to subscribe to Pub/Sub channels: {e}", exc_info=True)
            if self.pubsub: # Attempt to close if partially initialized
                try: self.pubsub.close() 
                except: pass
            self.pubsub = None # Ensure it's None if subscription failed
            return

        self._pubsub_thread = threading.Thread(target=self._pubsub_listener_loop, name="SchedulerPubSubListener", daemon=True)
        self._pubsub_thread.start()
        logger.info("Scheduler PubSub listener thread started.")

    def stop(self) -> None:
        if self._pubsub_thread is None or not self._pubsub_thread.is_alive():
            logger.info("Scheduler PubSub listener thread is not running or already stopped.")
            return

        logger.info("Stopping scheduler PubSub listener thread...")
        self._stop_event.set() # Signal the loop to stop
        
        if self.pubsub:
            try:
                channels_to_unsubscribe = []
                if hasattr(self, 'job_status_updates_channel') and self.job_status_updates_channel:
                    channels_to_unsubscribe.append(self.job_status_updates_channel)
                if hasattr(self, 'worker_resources_updates_channel') and self.worker_resources_updates_channel:
                    channels_to_unsubscribe.append(self.worker_resources_updates_channel)
                
                if channels_to_unsubscribe:
                    self.pubsub.unsubscribe(*channels_to_unsubscribe)
                    logger.info(f"Unsubscribed from Redis Pub/Sub channels: {', '.join(channels_to_unsubscribe)}")
                
                self.pubsub.close()
                logger.info("PubSub connection closed.")
            except redis.exceptions.RedisError as e:
                logger.error(f"Error during PubSub cleanup: {e}")
            self.pubsub = None # Ensure pubsub client is cleared

        self._pubsub_thread.join(timeout=self.polling_interval + 2) 
        if self._pubsub_thread.is_alive():
            logger.warning("Scheduler PubSub listener thread did not stop in time.")
        else:
            logger.info("Scheduler PubSub listener thread stopped.")
        self._pubsub_thread = None

    def _pubsub_listener_loop(self) -> None:
        logger.info(f"PubSub listener loop started for channels '{self.job_status_updates_channel}' and '{self.worker_resources_updates_channel}'.")
        while not self._stop_event.is_set():
            if not self.pubsub:
                logger.error("PubSub client not initialized. Stopping listener loop.")
                break
            try:
                message = self.pubsub.get_message(timeout=self.polling_interval)
                if message and message['type'] == 'message':
                    channel_str = message['channel'].decode('utf-8')
                    data_str = message['data'].decode('utf-8') if isinstance(message['data'], bytes) else message['data']
                    logger.debug(f"Received PubSub message on '{channel_str}': {data_str}")

                    if channel_str == self.job_status_updates_channel:
                        self._handle_job_status_update(message)
                    elif channel_str == self.worker_resources_updates_channel:
                        self._handle_worker_resource_update(message)
                    else:
                        logger.warning(f"Received message on unhandled channel '{channel_str}': {data_str}")

                elif message:
                    logger.debug(f"Received control message on PubSub: {message}")

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logger.error(f"PubSub listener: Redis connection/timeout error: {e}. Attempting to re-establish...", exc_info=False)
                if self._stop_event.is_set(): break # Exit if stop was signaled during error handling
                time.sleep(self.polling_interval) 
                try:
                    if self.pubsub: self.pubsub.close()
                    self.pubsub = self.redis_broker.redis_client.pubsub()
                    self.pubsub.subscribe(self.job_status_updates_channel, self.worker_resources_updates_channel)
                    logger.info(f"Re-subscribed to Pub/Sub channels: {self.job_status_updates_channel}, {self.worker_resources_updates_channel}")
                except Exception as re_e:
                    logger.error(f"PubSub listener: Failed to re-subscribe after error: {re_e}. Stopping listener.", exc_info=True)
                    self._stop_event.set() # Signal stop to prevent tight loop on errors
            except Exception as e:
                logger.error(f"Error in PubSub listener loop: {e}", exc_info=True)
                if self._stop_event.is_set(): break
                time.sleep(1) 
        logger.info("PubSub listener loop finished.")

    def _handle_job_status_update(self, message: Dict[str, Any]) -> None:
        """Handles a job status update received from the Pub/Sub channel."""
        try:
            data_str = message['data'].decode('utf-8') if isinstance(message['data'], bytes) else message['data']
            logger.info(f"Processing job status update: {data_str} on channel {message['channel'].decode('utf-8')}")
            # TODO: Deserialize event_data (e.g., JSON) and process it
            # Example: update = json.loads(data_str)
            # job_id = update.get("job_id")
            # new_status = update.get("status")
            # error_info = update.get("error")
            # if job_id and new_status:
            #     self.redis_broker.set_job_status(job_id, InstanceStatus(new_status))
            #     if new_status == InstanceStatus.FAILED and error_info:
            #         self.redis_broker.set_job_error_info(job_id, str(error_info))
            #     logger.info(f"Updated status for job {job_id} to {new_status}.")

        except Exception as e:
            logger.error(f"Error processing job status update: {e}. Data: {message.get('data')}")

    def _handle_worker_resource_update(self, message: Dict[str, Any]) -> None:
        """Handles a worker resource update received from the Pub/Sub channel."""
        try:
            data_str = message['data'].decode('utf-8') if isinstance(message['data'], bytes) else message['data']
            logger.info(f"Processing worker resource update: {data_str} on channel {message['channel'].decode('utf-8')}")
            # TODO: Deserialize event_data (e.g., JSON) and process it
            # Example: update = json.loads(data_str)
            # worker_id = update.get("worker_id")
            # resources = update.get("resources") # e.g., {'cpu': 4, 'gpu': 1, 'available_slots': 2}
            # status = update.get("status") # e.g., 'available', 'busy', 'offline'
            # if worker_id:
            #    self.redis_broker.update_worker_status(worker_id, status, resources)
        except Exception as e:
            logger.error(f"Error processing worker resource update: {e}. Data: {message.get('data')}", exc_info=True)

    def _evaluate_specific_pending_jobs(self, job_ids_to_check: List[str]) -> None:
        if not self.redis_broker:
            logger.warning("RedisBroker not available, skipping job evaluation.")
            return
        logger.debug(f"Evaluating specific pending jobs: {job_ids_to_check}")

        for job_id in job_ids_to_check:
            current_status = self.redis_broker.get_job_status(job_id)
            if current_status != InstanceStatus.PENDING:
                logger.debug(f"Job {job_id} is no longer PENDING (status: {current_status}). Skipping evaluation.")
                # If it's not PENDING but still in the set (e.g. processed by another thread/instance, or error), remove it.
                if self.redis_broker.redis_client.sismember(self.redis_broker.key_prefix_user + self.pending_jobs_set_key, job_id):
                     logger.warning(f"Job {job_id} has status {current_status} but was found in pending set. Removing.")
                     self.redis_broker.remove_job_from_set(self.pending_jobs_set_key, job_id)
                continue

            logger.debug(f"Processing pending job for potential dispatch: {job_id}")
            job_instance_data = self.redis_broker.get_job_details(job_id)
            if not job_instance_data:
                logger.warning(f"Could not retrieve details for pending job {job_id}. Skipping.")
                continue
            
            try:
                job_instance = JobInstance.model_validate_json(job_instance_data)
            except Exception as e:
                logger.error(f"Failed to deserialize JobInstance for job {job_id}: {e}", exc_info=True)
                self.redis_broker.set_job_status(job_id, InstanceStatus.FAILED)
                self.redis_broker.set_job_error_info(job_id, f"Internal error: Failed to deserialize JobInstance: {e}")
                self.redis_broker.remove_job_from_set(self.pending_jobs_set_key, job_id)
                continue

            all_dependencies_met = True
            # This dictionary will store values resolved from outputs of other dependent jobs.
            # Literal values are already in job_instance.context.input_literal_values.
            # Artifact URIs are already in job_instance.context.resolved_inputs.
            resolved_dependency_outputs = {}

            # Process resolved_parameters for dependencies on other jobs' outputs
            if job_instance.context and job_instance.context.resolved_parameters:
                logger.debug(f"Job {job_id}: Checking resolved_parameters for dependencies: {job_instance.context.resolved_parameters}")
                for param_name, ref_str in job_instance.context.resolved_parameters.items():
                    if not isinstance(ref_str, str) or not ref_str.startswith("ref::"):
                        # This parameter is not a reference or not in the expected format for a job output dependency.
                        # It might be a literal value that ManifestExpander should have placed in input_literal_values,
                        # or a reference to something other than a job output (e.g., task parameter).
                        # Scheduler only resolves job output dependencies.
                        logger.debug(f"Job {job_id}: Parameter '{param_name}' value '{ref_str}' (type: {type(ref_str)}) is not a resolvable job output 'ref::' dependency. Skipping.")
                        continue

                    try:
                        parts = ref_str.split(':')
                        if not (len(parts) == 7 and parts[0] == "ref" and parts[1] == "" and parts[5] == "outputs"):
                            logger.warning(f"Job {job_id}: Parameter '{param_name}' has reference '{ref_str}' that is not a resolvable job output dependency type. Skipping.")
                            continue
                        
                        dep_job_id = parts[4]
                        dep_output_name = parts[6]
                        logger.debug(f"Job {job_id}: Parameter '{param_name}' depends on Job '{dep_job_id}' output '{dep_output_name}'.")

                    except Exception as e:
                        logger.error(f"Job {job_id}: Critical error parsing dependency reference '{ref_str}' for param '{param_name}': {e}. Marking as FAILED.", exc_info=True)
                        self.redis_broker.set_job_status(job_id, InstanceStatus.FAILED.value)
                        self.redis_broker.set_job_error_info(job_id, f"Critical error parsing dependency: {ref_str} for {param_name}")
                        self.redis_broker.remove_job_from_set(self.pending_jobs_set_key, job_id)
                        all_dependencies_met = False
                        break # Break from resolved_parameters loop for this job_id
                    
                    dep_status = self.redis_broker.get_job_status(dep_job_id)
                    if dep_status != InstanceStatus.SUCCEEDED.value:
                        logger.debug(f"Job {job_id}: Dependency job {dep_job_id} for param '{param_name}' is not SUCCEEDED (status: {dep_status}). Waiting.")
                        all_dependencies_met = False
                        break
                    
                    dep_output_value = self.redis_broker.get_job_output_value(dep_job_id, dep_output_name)
                    if dep_output_value == self.redis_broker.placeholder:
                        logger.debug(f"Job {job_id}: Dependency job {dep_job_id} output '{dep_output_name}' for param '{param_name}' is still placeholder. Waiting.")
                        all_dependencies_met = False
                        break
                    
                    resolved_dependency_outputs[param_name] = dep_output_value
                    logger.debug(f"Job {job_id}: Resolved dependency for param '{param_name}' from Job '{dep_job_id}' output '{dep_output_name}'. Value type: {type(dep_output_value)}")

                if not all_dependencies_met:
                    logger.debug(f"Job {job_id}: Not all job output dependencies from resolved_parameters met. Will re-check later.")
                    continue
            
            # If all_dependencies_met is still true, all *job output dependencies* are met.
            # Literal inputs are in job_instance.context.input_literal_values.
            # Artifact URIs are in job_instance.context.resolved_inputs.
            # The worker will be responsible for combining these three sources.

            if not all_dependencies_met:
                logger.debug(f"Job {job_id}: Not all dependencies met. Will re-check upon next relevant event.")
                continue 

            logger.info(f"Job {job_id}: All job output dependencies met. Resolved dependency outputs: {list(resolved_dependency_outputs.keys())}")
            try:
                # Store only the outputs resolved from other jobs. 
                # Literals and artifact URIs are already in JobInstanceContext.
                self.redis_broker.set_job_resolved_inputs(job_id, resolved_dependency_outputs)
                self.redis_broker.set_job_status(job_id, InstanceStatus.READY.value)
                self.redis_broker.push_job_to_worker_queue(self.worker_queue_name, job_id)
                self.redis_broker.remove_job_from_set(self.pending_jobs_set_key, job_id)
                logger.info(f"Job {job_id} is now READY and pushed to queue '{self.worker_queue_name}'. Removed from pending set.")
            except Exception as e:
                logger.error(f"Job {job_id}: Error during final dispatch steps: {e}", exc_info=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(name)s - [%(threadName)s] - %(funcName)s - %(message)s')
    
    try:
        test_redis_broker = RedisBroker(host='localhost', port=6379, db=9, key_prefix_user="test_scheduler_pubsub@")
        test_redis_broker.redis_client.flushdb()
        logger.info("Connected to test Redis and flushed DB 9.")

        scheduler_config_dict = {
            "scheduler": {
                "polling_interval_seconds": 1, # Used for pubsub timeout
                "default_worker_queue": "test_worker_q_pubsub",
                "pending_jobs_set_key": "test_pending_set_pubsub",
                "job_events_channel": "test_job_events_pubsub"
            }
        }
        scheduler = Scheduler(redis_broker=test_redis_broker, config=scheduler_config_dict)
        scheduler.start()

        mock_exp_id = "exp_ps_001"
        mock_exp_instance = ExperimentInstance(id=mock_exp_id, name="PubSub Test Exp", kind="Experiment", apiVersion="v1", metadata={"name":"PubSub Test Exp"}, spec={})
        
        # Job A: No dependencies
        job_a_outputs = [{"name": "outA"}]
        job_a_context = {"input_literal_values": {"val": 10}, "resolved_outputs": {o['name']: test_redis_broker.placeholder for o in job_a_outputs}}
        job_a = JobInstance(id=f"{mock_exp_id}:job_A", name="Job A", kind="JobInstance", apiVersion="v1", metadata={}, spec={"outputs": job_a_outputs}, context=job_a_context)

        # Job B: Depends on Job A's outA
        job_b_outputs = [{"name": "outB"}]
        job_b_context = {"input_references": {"inB": f"{mock_exp_id}:job_A:outA"}, "resolved_outputs": {o['name']: test_redis_broker.placeholder for o in job_b_outputs}}
        job_b = JobInstance(id=f"{mock_exp_id}:job_B", name="Job B", kind="JobInstance", apiVersion="v1", metadata={}, spec={"outputs": job_b_outputs}, context=job_b_context)
        
        # Job C: No dependencies
        job_c_outputs = [{"name": "outC"}]
        job_c_context = {"input_literal_values": {"val": 20}, "resolved_outputs": {o['name']: test_redis_broker.placeholder for o in job_c_outputs}}
        job_c = JobInstance(id=f"{mock_exp_id}:job_C", name="Job C", kind="JobInstance", apiVersion="v1", metadata={}, spec={"outputs": job_c_outputs}, context=job_c_context)

        mock_job_instances_list = [job_a, job_b, job_c]
        scheduler.submit_experiment_jobs(mock_exp_instance, [], mock_job_instances_list)

        logger.info("--- Initial submission complete. Job A and C should become READY soon. ---")
        time.sleep(3) # Give scheduler time for initial evaluation

        logger.info(f"Job A status: {test_redis_broker.get_job_status(job_a.id)}")
        logger.info(f"Job B status: {test_redis_broker.get_job_status(job_b.id)}") # Should be PENDING
        logger.info(f"Job C status: {test_redis_broker.get_job_status(job_c.id)}")
        
        logger.info(f"Pending set content: {test_redis_broker.get_job_ids_from_set(scheduler.pending_jobs_set_key)}")
        logger.info(f"Worker queue content: {test_redis_broker.redis_client.lrange(test_redis_broker.key_prefix_user + scheduler.worker_queue_name, 0, -1)}")

        # Simulate Job A completing and publishing an event
        logger.info("--- Simulating Job A completion --- ")
        test_redis_broker.set_job_output_value(job_a.id, "outA", "value_from_A_simulation")
        test_redis_broker.set_job_status(job_a.id, InstanceStatus.COMPLETED)
        # Worker would publish this:
        event_message_A = json.dumps({"job_id": job_a.id, "status": InstanceStatus.COMPLETED.value})
        test_redis_broker.redis_client.publish(scheduler.job_events_channel, event_message_A)
        logger.info(f"Published completion event for Job A to {scheduler.job_events_channel}")

        time.sleep(3) # Give scheduler time to process the event for Job A and then Job B

        logger.info(f"Job A status after event: {test_redis_broker.get_job_status(job_a.id)}")
        logger.info(f"Job B status after A's event: {test_redis_broker.get_job_status(job_b.id)}") # Should now be READY
        logger.info(f"Job C status after A's event: {test_redis_broker.get_job_status(job_c.id)}")
        
        logger.info(f"Pending set after A's event: {test_redis_broker.get_job_ids_from_set(scheduler.pending_jobs_set_key)}")
        logger.info(f"Worker queue after A's event: {test_redis_broker.redis_client.lrange(test_redis_broker.key_prefix_user + scheduler.worker_queue_name, 0, -1)}")

    except Exception as e:
        logger.error("Error in scheduler Pub/Sub test setup: %s", e, exc_info=True)
    finally:
        if 'scheduler' in locals() and scheduler:
            scheduler.stop()
        logger.info("Scheduler Pub/Sub test finished.")
