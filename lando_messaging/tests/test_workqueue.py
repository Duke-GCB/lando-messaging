from __future__ import absolute_import
from unittest import TestCase
import pickle
from lando_messaging.dockerutil import DockerRabbitmq
from lando_messaging.workqueue import WorkQueueConnection, WorkQueueProcessor, WorkQueueClient, WorkProgressQueue, \
    WorkRequest, get_version_str
from mock import MagicMock, patch


class FakeConfig(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.work_queue_config = self


class TestWorkQueue(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rabbit_vm = DockerRabbitmq()
        cls.config = FakeConfig(DockerRabbitmq.HOST, DockerRabbitmq.USER, DockerRabbitmq.PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls.rabbit_vm.destroy()

    def test_work_queue_connection_single_message(self):
        """
        Test that we can send a message through rabbit and receive it on our end.
        """
        my_queue_name = "testing1"
        my_payload = "HEYME"
        work_queue_connection = WorkQueueConnection(self.config)
        work_queue_connection.connect()
        work_queue_connection.send_durable_message(queue_name=my_queue_name, body=my_payload)

        def processor(ch, method, properties, body):
            self.assertEqual(my_payload, body)
            # Terminate receive loop
            work_queue_connection.delete_queue(my_queue_name)
        work_queue_connection.receive_loop_with_callback(queue_name=my_queue_name, callback=processor)

    def test_work_queue_processor(self):
        """
        Make sure we can send and receive messages using higher level WorkQueueProcessor/WorkQueueClient.
        """
        my_queue_name = "testing2"
        client = WorkQueueClient(self.config, my_queue_name)
        processor = WorkQueueProcessor(self.config, my_queue_name)

        # Add three messages processor will run functions for
        processor.add_command("one", self.save_one_value)
        processor.add_command_by_method_name("save_two_value", self)

        def close_queue(payload):
            client.delete_queue()
        processor.add_command("stop", close_queue)

        # Send messages to through rabbitmq
        self.one_value = None
        self.two_value = None
        client.send("one", "oneValue")
        client.send("save_two_value", {'two': 2})
        client.send("stop", '')

        # Wait until close_queue message is processed
        processor.process_messages_loop()
        self.assertEqual(self.one_value, "oneValue")
        self.assertEqual(self.two_value, {'two': 2})

    def save_one_value(self, payload):
        # Saves value for test_work_queue_processor
        self.one_value = payload

    def save_two_value(self, payload):
        # Saves value for test_work_queue_processor
        self.two_value = payload


class TestWorkProgressQueue(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rabbit_vm = DockerRabbitmq()
        cls.config = FakeConfig(DockerRabbitmq.HOST, DockerRabbitmq.USER, DockerRabbitmq.PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls.rabbit_vm.destroy()

    def test_work_progress_queue_can_send_json_message(self):
        wpq = WorkProgressQueue(self.config, "job_status")
        result = wpq.send('{"job":16, "status":"GOOD"}')
        self.assertEqual(True, result)


class TestGetVersionStr(TestCase):
    def test_version(self):
        version_str = get_version_str()
        self.assertEqual(type(version_str), str)
        parts = version_str.split(".")
        self.assertRegexpMatches(version_str, '^\d+.\d+.\d+$')


class TestWorkQueueProcessor(TestCase):
    def setUp(self):
        self.payload = None

    def do_work(self, payload):
        self.payload = payload

    def test_default_mismatch_version_raises(self):
        processor = WorkQueueProcessor(MagicMock(), 'test')
        processor.add_command('dowork', self.do_work)
        request = WorkRequest('dowork', 'somedata')
        request.version = '0.0.0'
        with self.assertRaises(ValueError) as err_context:
            processor.process_message(MagicMock(), MagicMock(), None, pickle.dumps(request))
        self.assertEqual(str(err_context.exception), 'Received version mismatch.')

    def upgrade_payload(self, work_request, our_version):
        work_request.payload = 'evenbetter'
        work_request.version = our_version

    def test_default_mismatch_version_with_override(self):
        # We pretend to upgrade the request payload
        processor = WorkQueueProcessor(MagicMock(), 'test', self.upgrade_payload)
        processor.add_command('dowork', self.do_work)
        request = WorkRequest('dowork', 'somedata')
        request.version = '0.0.0'
        processor.process_message(MagicMock(), MagicMock(), None, pickle.dumps(request))
        self.assertEqual('evenbetter', self.payload)
