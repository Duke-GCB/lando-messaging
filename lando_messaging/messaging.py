"""
Defines all commands/payloads and a message receiver which allow lando and lando_worker to communicate over AMQP.
"""

from lando_messaging.workqueue import WorkQueueProcessor, DisconnectingWorkQueueProcessor


class JobCommands(object):
    """
    Names of all the commands that are sent through the queue.
    """
    START_JOB = 'start_job'                                  # webserver -> lando
    RESTART_JOB = 'restart_job'                              # webserver -> lando
    CANCEL_JOB = 'cancel_job'                                # webserver -> lando and lando -> lando_worker

    WORKER_STARTED = 'worker_started'                        # lando_worker -> lando
    STAGE_JOB = 'stage_job'                                  # lando -> lando_worker
    STAGE_JOB_COMPLETE = 'stage_job_complete'                # lando_worker -> lando
    STAGE_JOB_ERROR = 'stage_job_error'                      # lando_worker -> lando

    RUN_JOB = 'run_job'                                      # lando -> lando_worker
    RUN_JOB_COMPLETE = 'run_job_complete'                    # lando_worker -> lando
    RUN_JOB_ERROR = 'run_job_error'                          # lando_worker -> lando

    ORGANIZE_OUTPUT = 'organize_output'                      # lando -> lando_worker
    ORGANIZE_OUTPUT_COMPLETE = 'organize_output_complete'    # lando_worker/watcher -> lando
    ORGANIZE_OUTPUT_ERROR = 'organize_output_error'          # lando_worker/watcher -> lando

    STORE_JOB_OUTPUT = 'store_job_output'                    # lando -> lando_worker
    STORE_JOB_OUTPUT_COMPLETE = 'store_job_output_complete'  # lando_worker -> lando
    STORE_JOB_OUTPUT_ERROR = 'store_job_output_error'        # lando_worker -> lando

    # only for use with k8s lando/watcher
    RECORD_OUTPUT_PROJECT_COMPLETE = 'record_output_project_complete'  # watcher -> lando
    RECORD_OUTPUT_PROJECT_ERROR = 'record_output_project_error'        # watcher -> lando


# Commands that lando will receive.
VM_LANDO_INCOMING_MESSAGES = [
    JobCommands.START_JOB,
    JobCommands.RESTART_JOB,
    JobCommands.CANCEL_JOB,
    JobCommands.WORKER_STARTED,
    JobCommands.STAGE_JOB_COMPLETE,
    JobCommands.STAGE_JOB_ERROR,
    JobCommands.RUN_JOB_COMPLETE,
    JobCommands.RUN_JOB_ERROR,
    JobCommands.ORGANIZE_OUTPUT_COMPLETE,
    JobCommands.ORGANIZE_OUTPUT_ERROR,
    JobCommands.STORE_JOB_OUTPUT_COMPLETE,
    JobCommands.STORE_JOB_OUTPUT_ERROR,
]

# Commands that lando_worker will receive.
VM_LANDO_WORKER_INCOMING_MESSAGES = [
    JobCommands.STAGE_JOB,
    JobCommands.RUN_JOB,
    JobCommands.ORGANIZE_OUTPUT,
    JobCommands.STORE_JOB_OUTPUT,
]

K8S_LANDO_INCOMING_MESSAGES = [
    JobCommands.START_JOB,
    JobCommands.RESTART_JOB,
    JobCommands.CANCEL_JOB,
    JobCommands.STAGE_JOB_COMPLETE,
    JobCommands.STAGE_JOB_ERROR,
    JobCommands.RUN_JOB_COMPLETE,
    JobCommands.RUN_JOB_ERROR,
    JobCommands.ORGANIZE_OUTPUT_COMPLETE,
    JobCommands.ORGANIZE_OUTPUT_ERROR,
    JobCommands.STORE_JOB_OUTPUT_COMPLETE,
    JobCommands.STORE_JOB_OUTPUT_ERROR,
    JobCommands.RECORD_OUTPUT_PROJECT_COMPLETE,
    JobCommands.RECORD_OUTPUT_PROJECT_ERROR,
]


class MessageRouter(object):
    """
    Listens for messages on the AMQP queue and runs the appropriate method on an object.
    """
    def __init__(self, config, obj, queue_name, command_names, processor_constructor=WorkQueueProcessor):
        """
        Setup for listening on queue_name for command_names and calling methods on obj when commands come in.
        :param config: WorkerConfig/ServerConfig: settings for connecting to the queue
        :param obj: object: lando/lando_worker object that will have methods run on
        :param queue_name: str: name of the queue we should listen on
        :param command_names: [str]: list of JobCommands that obj has implemented
        :param func(config, queue_name): constructor for a WorkQueueProcessor type object
        """
        self.queue_name = queue_name
        self.processor = processor_constructor(config, queue_name)
        for command in command_names:
            self.processor.add_command_by_method_name(command, obj)

    def run(self):
        """
        Blocking loop that will call commands as messages come in.
        Delete the queue we are listening on or call processor.shutdown() to end loop.
        """
        self.processor.process_messages_loop()

    def shutdown(self):
        """
        Method that can be called from a command to terminate the message processing loop
        """
        self.processor.shutdown()

    @staticmethod
    def make_lando_router(config, obj, queue_name):
        """
        Makes MessageRouter which can listen to queue_name sending messages to the VM version of lando.
        :param config: WorkerConfig/ServerConfig: settings for connecting to the queue
        :param obj: object: implements lando specific methods
        :param queue_name: str: name of the queue we will listen on.
        :return MessageRouter
        """
        return MessageRouter(config, obj, queue_name, VM_LANDO_INCOMING_MESSAGES,
                             processor_constructor=WorkQueueProcessor)

    @staticmethod
    def make_k8s_lando_router(config, obj, queue_name):
        """
        Makes MessageRouter which can listen to queue_name sending messages to the k8s version of lando.
        :param config: WorkerConfig/ServerConfig: settings for connecting to the queue
        :param obj: object: implements lando specific methods
        :param queue_name: str: name of the queue we will listen on.
        :return MessageRouter
        """
        return MessageRouter(config, obj, queue_name, K8S_LANDO_INCOMING_MESSAGES,
                             processor_constructor=WorkQueueProcessor)

    @staticmethod
    def make_worker_router(config, obj, queue_name):
        """
        Makes MessageRouter which can listen to queue_name sending lando_worker specific messages to obj.
        :param config: WorkerConfig/ServerConfig: settings for connecting to the queue
        :param obj: object: implements lando_worker specific methods
        :param queue_name: str: name of the queue we will listen on.
        """
        return MessageRouter(config, obj, queue_name, VM_LANDO_WORKER_INCOMING_MESSAGES,
                             processor_constructor=DisconnectingWorkQueueProcessor)


class StartJobPayload(object):
    """
    Payload to be sent with JobCommands.START_JOB to lando.
    """
    def __init__(self, job_id):
        """
        :param job_id: int: job id we want to have lando start.
        """
        self.job_id = job_id


class RestartJobPayload(object):
    """
    Payload to be sent with JobCommands.RESTART_JOB to lando.
    """
    def __init__(self, job_id):
        """
        :param job_id: int: job id we want to have lando restart.
        """
        self.job_id = job_id


class CancelJobPayload(object):
    """
    Payload to be sent with JobCommands.CANCEL_JOB to lando.
    """
    def __init__(self, job_id):
        """
        :param job_id: int: job id we want to have lando cancel.
        """
        self.job_id = job_id


class WorkerStartedPayload(object):
    """
    Payload to be sent with JobCommands.WORKER_STARTED to lando.
    """
    def __init__(self, worker_queue_name):
        """
        :param worker_queue_name: str: name of the AMQP queue for the worker who just launched
        """
        self.worker_queue_name = worker_queue_name


class StageJobPayload(object):
    """
    Payload to be sent with JobCommands.STAGE_JOB to lando_worker.
    """
    def __init__(self, credentials, job_details, input_files, vm_instance_name):
        """
        :param credentials: jobapi.Credentials: keys used to download files
        :param job_details: object: details about job(id, name, created date, workflow version)
        :param input_files: [InputFile]: list of files to download
        :param vm_instance_name: str: name of the instance lando_worker is running on (this passed back in the response)
        """
        self.credentials = credentials
        self.job_id = job_details.id
        self.job_details = job_details
        self.input_files = input_files
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.STAGE_JOB_COMPLETE
        self.error_command = JobCommands.STAGE_JOB_ERROR
        self.job_description = "Staging files"


class RunJobPayload(object):
    """
    Payload to be sent with JobCommands.RUN_JOB to lando_worker.
    """
    def __init__(self, job_details, workflow, vm_instance_name):
        """
        :param job_details: object: details about job(id, name, created date, workflow version)
        :param workflow: jobapi.Workflow: url to workflow and parameters to use
        :param vm_instance_name: name of the instance lando_worker is running on (this passed back in the response)
        """
        self.job_id = job_details.id
        self.job_details = job_details
        self.workflow_url = workflow.workflow_url
        self.workflow_path = workflow.workflow_path
        self.workflow_type = workflow.workflow_type
        self.job_order = workflow.job_order
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.RUN_JOB_COMPLETE
        self.error_command = JobCommands.RUN_JOB_ERROR
        self.job_description = "Running workflow"


class OrganizeOutputProjectPayload(object):
    """
    Payload to be sent with JobCommands.ORGANIZE_OUTPUT to lando_worker.
    """
    def __init__(self, job_details, vm_instance_name):
        """
        :param job_details: object: details about job(id, name, created date, workflow version)
        :param vm_instance_name: name of the instance lando_worker is running on (this passed back in the response)
        """
        self.job_id = job_details.id
        self.job_details = job_details
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.ORGANIZE_OUTPUT_COMPLETE
        self.error_command = JobCommands.ORGANIZE_OUTPUT_ERROR
        self.job_description = "Organize output project"


class StoreJobOutputPayload(object):
    """
    Payload to be sent with JobCommands.STORE_JOB_OUTPUT to lando_worker.
    """
    def __init__(self, credentials, job_details, vm_instance_name):
        """
        :param credentials: jobapi.Credentials: user's credentials used to upload resulting files
        :param job_details: object: details about job(id, name, created date, workflow version)
        :param vm_instance_name: name of the instance lando_worker is running on (this passed back in the response)
        """
        self.credentials = credentials
        self.job_id = job_details.id
        self.job_details = job_details
        self.vm_instance_name = vm_instance_name
        self.success_command = JobCommands.STORE_JOB_OUTPUT_COMPLETE
        self.error_command = JobCommands.STORE_JOB_OUTPUT_ERROR
        self.job_description = "Storing output files"


class JobStepCompletePayload(object):
    """
    Payload that will be sent to the *_job_complete methods
    """
    def __init__(self, payload):
        self.job_id = payload.job_id
        self.vm_instance_name = payload.vm_instance_name


class JobStepStoreOutputCompletePayload(object):
    """
    Payload that will be sent to the store_job_output_complete method
    """
    def __init__(self, payload, output_project_info):
        """
        :param payload: StoreJobOutputPayload: payload sent with JobCommands.STORE_JOB_OUTPUT
        :param output_project_info: object: info about the project created
        """
        self.job_id = payload.job_id
        self.vm_instance_name = payload.vm_instance_name
        self.output_project_info = output_project_info


class JobStepErrorPayload(object):
    """
    Payload that will be sent to the *_job_error methods
    """
    def __init__(self, payload, message):
        self.job_id = payload.job_id
        self.vm_instance_name = payload.vm_instance_name
        self.message = message
