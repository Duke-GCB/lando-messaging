from __future__ import absolute_import
from unittest import TestCase
import pickle
from lando_messaging.dockerutil import DockerRabbitmq
from lando_messaging.workqueue import WorkQueueConnection, WorkQueueProcessor, WorkQueueClient, WorkProgressQueue,\
    DelayedMessageQueue


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


class TestDelayedMessageQueue(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rabbit_vm = DockerRabbitmq()
        cls.config = FakeConfig(DockerRabbitmq.HOST, DockerRabbitmq.USER, DockerRabbitmq.PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls.rabbit_vm.destroy()

    def test_single_delayed_message(self):
        """
        Test that we can send a message through rabbit and receive it on our end.
        """
        my_queue_name = "testing1"
        delay_queue_name = "testing1delay"
        my_payload = "HEYME"
        work_queue_connection = WorkQueueConnection(self.config)
        delayed_message_queue = DelayedMessageQueue(work_queue_connection, my_queue_name, delay_queue_name)
        delayed_message_queue.send_delayed_message(my_payload, 500)

        work_queue_connection.connect()

        def processor(ch, method, properties, body):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            content = pickle.loads(body)
            self.assertEqual(content, my_payload)
            # Terminate receive loop
            work_queue_connection.delete_queue(my_queue_name)
        work_queue_connection.receive_loop_with_callback(queue_name=my_queue_name, callback=processor)

        delayed_message_queue.delete_queue()

    def test_two_delayed_messages(self):
        """
        Test that we can send a message through rabbit and receive it on our end.
        """
        self.messages_received = 0
        my_queue_name = "testing1"
        delay_queue_name = "testing1delay"
        my_payload1 = "HEY"
        my_payload2 = "THERE"
        work_queue_connection = WorkQueueConnection(self.config)
        delayed_message_queue = DelayedMessageQueue(work_queue_connection, my_queue_name, delay_queue_name)
        delayed_message_queue.send_delayed_message(my_payload1, 400)
        delayed_message_queue.send_delayed_message(my_payload2, 200)
        work_queue_connection.connect()

        def processor(ch, method, properties, body):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            content = pickle.loads(body)
            self.assertIn(content, [my_payload1, my_payload2])
            self.messages_received += 1
            # Terminate receive loop
            if self.messages_received == 2:
                work_queue_connection.delete_queue(my_queue_name)
        work_queue_connection.receive_loop_with_callback(queue_name=my_queue_name, callback=processor)

        delayed_message_queue.delete_queue()
        self.assertEqual(self.messages_received, 2)
