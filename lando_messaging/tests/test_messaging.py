
from unittest import TestCase, skipIf
from unittest.mock import patch, Mock, call
import os
from lando_messaging.messaging import MessageRouter, LANDO_INCOMING_MESSAGES, LANDO_WORKER_INCOMING_MESSAGES
from lando_messaging.messaging import StageJobPayload, RunJobPayload, StoreJobOutputPayload, JobCommands
from lando_messaging.clients import LandoClient, LandoWorkerClient
from lando_messaging.workqueue import Config

INTEGRATION_TEST = os.environ.get('INTEGRATION_TEST') == 'true'


class FakeJobDetails(object):
    def __init__(self, job_id):
        self.id = job_id


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

    def organize_output_complete(self, payload):
        self.organize_output_complete_payload = payload

    def organize_output_error(self, payload):
        self.organize_output_error_payload = payload

    def store_job_output_complete(self, payload):
        self.store_job_output_complete_payload = payload

    def store_job_output_error(self, payload):
        self.store_job_output_error_payload = payload
        self.router.shutdown()


class FakeLandoWorker(object):
    def __init__(self):
        self.router = None

    def stage_job(self, payload):
        self.stage_job_payload = payload

    def run_job(self, payload):
        self.run_job_payload = payload

    def store_job_output(self, payload):
        self.store_job_output_payload = payload
        self.router.shutdown()


class FakeWorkflow(object):
    def __init__(self):
        self.job_order = ''
        self.url = ''
        self.object_name = ''


class MessageRouterTestCase(TestCase):
    @patch('lando_messaging.messaging.WorkQueueProcessor')
    def test_make_lando_router_organize_output_setup(self, mock_work_queue_processor):
        mock_obj = Mock()
        mock_config = Mock()
        MessageRouter.make_lando_router(mock_config, mock_obj, queue_name='somequeue')
        mock_work_queue_processor.return_value.add_command_by_method_name.assert_has_calls([
            call(JobCommands.ORGANIZE_OUTPUT_COMPLETE, mock_obj),
            call(JobCommands.ORGANIZE_OUTPUT_ERROR, mock_obj),
        ])


@skipIf(not INTEGRATION_TEST, 'Integration tests require a local rabbitmq instance')
class TestMessagingAndClients(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config('localhost', 'guest', 'guest')

    def test_lando_client_and_router(self):
        """
        Test to verify that messages sent from LandoClient through rabbit make it to Lando
        """
        queue_name = "lando"
        lando_client = LandoClient(self.config, queue_name)
        fake_lando = FakeLando()
        router = MessageRouter.make_lando_router(self.config, fake_lando, queue_name)
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
        stage_job_payload = StageJobPayload(credentials=None, job_details=FakeJobDetails(3), input_files=[],
                                            vm_instance_name='test')
        # Send message to fake_lando that some stage job is complete
        lando_client.job_step_complete(stage_job_payload)
        # Send message to fake_lando that some stage job had an error
        lando_client.job_step_error(stage_job_payload, "Oops1")

        run_job_payload = RunJobPayload(job_details=FakeJobDetails(4), workflow=FakeWorkflow(), vm_instance_name='test')
        # Send message to fake_lando that a job has been run
        lando_client.job_step_complete(run_job_payload)
        # Send message to fake_lando that a job failed while running
        lando_client.job_step_error(run_job_payload, "Oops2")

        store_job_output_payload = StoreJobOutputPayload(None, FakeJobDetails(5), vm_instance_name='test')
        # Send message to fake_lando that we finished storing output
        lando_client.job_step_store_output_complete(store_job_output_payload, output_project_info='project_id')
        # Send message to fake_lando that we had an error while storing output
        lando_client.job_step_error(store_job_output_payload, "Oops3")

        router.run()
        self.assertEqual(fake_lando.start_job_payload.job_id, 1)
        self.assertEqual(fake_lando.cancel_job_payload.job_id, 2)
        self.assertEqual(fake_lando.restart_job_payload.job_id, 33)

        self.assertEqual(fake_lando.stage_job_complete_payload.job_id, 3)
        self.assertEqual(fake_lando.stage_job_error_payload.message, "Oops1")

        self.assertEqual(fake_lando.run_job_complete_payload.job_id, 4)
        self.assertEqual(fake_lando.run_job_error_payload.message, "Oops2")

        self.assertEqual(fake_lando.store_job_output_complete_payload.job_id, 5)
        self.assertEqual(fake_lando.store_job_output_complete_payload.output_project_info, 'project_id')
        self.assertEqual(fake_lando.store_job_output_error_payload.message, "Oops3")

    def test_organize_output_messages(self):
        queue_name = "lando"
        lando_client = LandoClient(self.config, queue_name)

        fake_lando = Mock()
        self.organize_output_complete_payload = None
        def record_complete_payload(payload):
            self.organize_output_complete_payload = payload
        self.organize_output_error_payload = None
        def record_error_payload(payload):
            self.organize_output_error_payload = payload
            fake_lando.router.shutdown()
        fake_lando.organize_output_complete = record_complete_payload
        fake_lando.organize_output_error = record_error_payload

        router = MessageRouter.make_lando_router(self.config, fake_lando, queue_name)
        fake_lando.router = router

        run_job_payload = RunJobPayload(job_details=FakeJobDetails(5), workflow=FakeWorkflow(), vm_instance_name='test')
        run_job_payload.success_command = JobCommands.ORGANIZE_OUTPUT_COMPLETE
        run_job_payload.error_command = JobCommands.ORGANIZE_OUTPUT_ERROR
        lando_client.job_step_complete(run_job_payload)
        lando_client.job_step_error(run_job_payload, "Oops3")

        router.run()

        self.assertEqual(self.organize_output_complete_payload.job_id, 5)
        self.assertEqual(self.organize_output_error_payload.message, "Oops3")

    def test_raises_for_mismatch_job_step_complete(self):
        # job_step_complete should not be used with StoreJobOutputPayload
        queue_name = "lando"
        lando_client = LandoClient(self.config, queue_name)
        store_job_output_payload = StoreJobOutputPayload(None, FakeJobDetails(5), vm_instance_name='test')
        with self.assertRaises(ValueError):
            lando_client.job_step_complete(store_job_output_payload)

        # job_step_store_output_complete should not be used with non-StoreJobOutputPayload
        run_job_payload = RunJobPayload(job_details=FakeJobDetails(4), workflow=FakeWorkflow(), vm_instance_name='test')
        with self.assertRaises(ValueError):
            lando_client.job_step_store_output_complete(run_job_payload, 'stuff')

    def test_lando_worker_client_and_router(self):
        """
        Test to verify that messages sent from LandoWorkerClient through rabbit make it to LandoWorker
        """
        queue_name = "lando_worker"
        lando_worker_client = LandoWorkerClient(self.config, queue_name)
        fake_lando_worker = FakeLandoWorker()
        router = MessageRouter.make_worker_router(self.config, fake_lando_worker, queue_name)
        fake_lando_worker.router = router

        lando_worker_client.stage_job(credentials=None, job_details=FakeJobDetails(1), input_files=[],
                                      vm_instance_name='test1')
        lando_worker_client.run_job(job_details=FakeJobDetails(2), workflow=FakeWorkflow(), vm_instance_name='test2')
        lando_worker_client.store_job_output(credentials=None, job_details=FakeJobDetails(3), vm_instance_name='test3')

        router.run()
        #self.assertEqual(fake_lando_worker.stage_job_payload.job_id, 1)
        #self.assertEqual(fake_lando_worker.stage_job_payload.vm_instance_name, 'test1')
        #self.assertEqual(fake_lando_worker.run_job_payload.job_id, 2)
        #self.assertEqual(fake_lando_worker.run_job_payload.vm_instance_name, 'test2')
        #self.assertEqual(fake_lando_worker.store_job_output_payload.job_id, 3)
        #self.assertEqual(fake_lando_worker.store_job_output_payload.vm_instance_name, 'test3')

