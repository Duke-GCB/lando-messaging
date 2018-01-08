from __future__ import absolute_import
from unittest import TestCase, skipIf
import os
import pickle
from lando_messaging.workqueue import WorkQueueConnection, WorkQueueProcessor, WorkQueueClient, WorkProgressQueue, \
    WorkRequest, get_version_str, Config, DisconnectingWorkQueueProcessor
from mock import MagicMock, patch

INTEGRATION_TEST = os.environ.get('INTEGRATION_TEST') == 'true'


@skipIf(not INTEGRATION_TEST, 'Integration tests require a local rabbitmq instance')
class TestWorkQueue(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config('localhost', 'guest', 'guest')

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


@skipIf(not INTEGRATION_TEST, 'Integration tests require a local rabbitmq instance')
class TestWorkProgressQueue(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config('localhost', 'guest', 'guest')

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

    @patch('lando_messaging.workqueue.WorkQueueConnection')
    def test_process_messages_loop_calls_blocking_method(self, mock_work_queue_connection):
        processor = WorkQueueProcessor(MagicMock(), 'test')
        processor.process_messages_loop()  # falls through because we mocked the connection
        processor.connection.receive_loop_with_callback.assert_called_with('test', processor.process_message)
        self.assertEqual(processor.receiving_messages, True)

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

    def test_process_messages_loop_with_shutdown_call(self):
        """
        Test that the loop exits when a command runs shutdown
        """
        processor = WorkQueueProcessor(MagicMock(), 'test2')

        def mock_receive_loop_with_callback(queue_name, func):
            processor.shutdown()

        # replace AMQP looping method with mock above
        processor.connection.receive_loop_with_callback = mock_receive_loop_with_callback

        processor.process_messages_loop()


class TestDisconnectingWorkQueueProcessor(TestCase):
    def setUp(self):
        self.payload = None

    def do_work(self, payload):
        self.payload = payload

    def test_default_mismatch_version_raises(self):
        processor = DisconnectingWorkQueueProcessor(MagicMock(), 'test')
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
        processor = DisconnectingWorkQueueProcessor(MagicMock(), 'test', self.upgrade_payload)
        processor.add_command('dowork', self.do_work)
        request = WorkRequest('dowork', 'somedata')
        request.version = '0.0.0'
        processor.process_message(MagicMock(), MagicMock(), None, pickle.dumps(request))
        self.assertEqual('evenbetter', self.payload)

    @patch('lando_messaging.workqueue.WorkQueueConnection')
    @patch('lando_messaging.workqueue.pickle')
    def test_process_messages_loop_disconnects(self, mock_pickle, mock_work_queue_connection):
        processor = DisconnectingWorkQueueProcessor(MagicMock(), 'test2')
        mock_pickle.loads.return_value = MagicMock(version=processor.version)

        def mock_receive_loop_with_callback(queue_name, func):
            self.assertEqual(func, processor.save_work_request_and_close)
            mock_channel = MagicMock()
            func(ch=mock_channel, method=MagicMock(), properties=None, body=None)
            self.assertEqual(processor.receiving_messages, True)
            mock_channel.stop_consuming.assert_called()
            # Manually Force loop to exit (so we can cleanly test that the connection was closed)
            processor.receiving_messages = False

        # replace AMQP looping method with mock above
        processor.connection.receive_loop_with_callback = mock_receive_loop_with_callback

        processor.process_messages_loop()
        processor.connection.close.assert_called()

    def test_process_messages_loop_with_shutdown_call(self):
        """
        Test that the loop exits when a command runs shutdown
        """
        processor = DisconnectingWorkQueueProcessor(MagicMock(), 'test2')

        def mock_receive_loop_with_callback(queue_name, func):
            processor.shutdown()

        # replace AMQP looping method with mock above
        processor.connection.receive_loop_with_callback = mock_receive_loop_with_callback

        processor.process_messages_loop()
