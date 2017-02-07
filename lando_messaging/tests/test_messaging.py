from __future__ import absolute_import
from unittest import TestCase
from lando_messaging.dockerutil import DockerRabbitmq
from lando_messaging.messaging import MessageRouter, LANDO_INCOMING_MESSAGES, LANDO_WORKER_INCOMING_MESSAGES
from lando_messaging.messaging import StageJobPayload, RunJobPayload, StoreJobOutputPayload
from lando_messaging.clients import LandoClient, LandoWorkerClient


class FakeConfig(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.work_queue_config = self


class FakeLando(object):
    def __init__(self):
        self.router = None

    def start_job(self, payload):
        self.start_job_payload = payload

    def restart_job(self, payload):
        self.restart_job_payload = payload

    def cancel_job(self, payload):
        self.cancel_job_payload = payload

    def worker_started(self, payload):
        self.worker_started_payload = payload

    def stage_job_complete(self, payload):
        self.stage_job_complete_payload = payload

    def stage_job_error(self, payload):
        self.stage_job_error_payload = payload

    def run_job_complete(self, payload):
        self.run_job_complete_payload = payload

    def run_job_error(self, payload):
        self.run_job_error_payload = payload

    def store_job_output_complete(self, payload):
        self.store_job_output_complete_payload = payload

    def store_job_output_error(self, payload):
        self.store_job_output_error_payload = payload
        self.router.processor.shutdown()


class FakeLandoWorker(object):
    def __init__(self):
        self.router = None

    def stage_job(self, payload):
        self.stage_job_payload = payload

    def run_job(self, payload):
        self.run_job_payload = payload

    def store_job_output(self, payload):
        self.store_job_output_payload = payload
        self.router.processor.shutdown()


class FakeWorkflow(object):
    def __init__(self):
        self.job_order = ''
        self.url = ''
        self.object_name = ''
        self.output_directory = ''


class FakeOutputDirectory(object):
    def __init__(self):
        self.dir_name = ''
        self.project_id = ''
        self.dds_user_credentials = ''


class TestMessagingAndClients(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rabbit_vm = DockerRabbitmq()
        cls.config = FakeConfig(DockerRabbitmq.HOST, DockerRabbitmq.USER, DockerRabbitmq.PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls.rabbit_vm.destroy()

    def test_lando_client_and_router(self):
        """
        Test to verify that messages sent from LandoClient through rabbit make it to Lando
        """
        queue_name = "lando"
        lando_client = LandoClient(self.config, queue_name)
        fake_lando = FakeLando()
        router = MessageRouter(self.config, fake_lando, queue_name=queue_name, command_names=LANDO_INCOMING_MESSAGES)
        fake_lando.router = router

        # Messages sent to lando from a user
        # Send message to fake_lando to start job 1
        lando_client.start_job(job_id=1)
        # Send message to fake_lando to cancel job 2
        lando_client.cancel_job(job_id=2)
        # Send message to fake_lando to restart job 33
        lando_client.restart_job(job_id=33)

        # Send message to fake_lando that the worker VM has finished launching
        lando_client.worker_started("test")

        # Messages sent to lando from a lando_worker after receiving a message from lando
        stage_job_payload = StageJobPayload(credentials=None, job_id=3, input_files=[], vm_instance_name='test')
        # Send message to fake_lando that some stage job is complete
        lando_client.job_step_complete(stage_job_payload)
        # Send message to fake_lando that some stage job had an error
        lando_client.job_step_error(stage_job_payload, "Oops1")

        run_job_payload = RunJobPayload(job_id=4, workflow=FakeWorkflow(), vm_instance_name='test')
        # Send message to fake_lando that a job has been run
        lando_client.job_step_complete(run_job_payload)
        # Send message to fake_lando that a job failed while running
        lando_client.job_step_error(run_job_payload, "Oops2")

        store_job_ouput_payload = StoreJobOutputPayload(None, 5, FakeOutputDirectory(), vm_instance_name='test')
        # Send message to fake_lando that we finished storing output
        lando_client.job_step_complete(store_job_ouput_payload)
        # Send message to fake_lando that we had an error while storing output
        lando_client.job_step_error(store_job_ouput_payload, "Oops3")

        router.run()
        self.assertEqual(fake_lando.start_job_payload.job_id, 1)
        self.assertEqual(fake_lando.cancel_job_payload.job_id, 2)
        self.assertEqual(fake_lando.restart_job_payload.job_id, 33)

        self.assertEqual(fake_lando.stage_job_complete_payload.job_id, 3)
        self.assertEqual(fake_lando.stage_job_error_payload.message, "Oops1")

        self.assertEqual(fake_lando.run_job_complete_payload.job_id, 4)
        self.assertEqual(fake_lando.run_job_error_payload.message, "Oops2")

        self.assertEqual(fake_lando.store_job_output_complete_payload.job_id, 5)
        self.assertEqual(fake_lando.store_job_output_error_payload.message, "Oops3")

    def test_lando_worker_client_and_router(self):
        """
        Test to verify that messages sent from LandoWorkerClient through rabbit make it to LandoWorker
        """
        queue_name = "lando_worker"
        lando_worker_client = LandoWorkerClient(self.config, queue_name)
        fake_lando_worker = FakeLandoWorker()
        router = MessageRouter(self.config, fake_lando_worker, queue_name=queue_name,
                               command_names=LANDO_WORKER_INCOMING_MESSAGES)
        fake_lando_worker.router = router

        lando_worker_client.stage_job(credentials=None, job_id=1, input_files=[], vm_instance_name='test1')
        lando_worker_client.run_job(job_id=2, workflow=FakeWorkflow(), vm_instance_name='test2')
        lando_worker_client.store_job_output(credentials=None, job_id=3, output_directory=FakeOutputDirectory(),
                                             vm_instance_name='test3')

        router.run()
        self.assertEqual(fake_lando_worker.stage_job_payload.job_id, 1)
        self.assertEqual(fake_lando_worker.stage_job_payload.vm_instance_name, 'test1')
        self.assertEqual(fake_lando_worker.run_job_payload.job_id, 2)
        self.assertEqual(fake_lando_worker.run_job_payload.vm_instance_name, 'test2')
        self.assertEqual(fake_lando_worker.store_job_output_payload.job_id, 3)
        self.assertEqual(fake_lando_worker.store_job_output_payload.vm_instance_name, 'test3')

