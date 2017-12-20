from unittest import TestCase
from lando_messaging.consumer import AsyncQueueConsumer, AsyncExchangeConsumer
from mock import MagicMock, patch


class TestAsyncQueueConsumer(TestCase):
    def setUp(self):
        self.on_message_payload = ''

    @patch('lando_messaging.consumer.pika')
    def test_run(self, mock_pika):
        consumer = AsyncQueueConsumer(host='somehost',
                                      username='guest',
                                      password='pass',
                                      queue_name='myqueue',
                                      message_consumer_func=self.message_consumer_func)
        consumer.run()

        # when we call run we should setup our pika connection and start ioloop
        mock_pika.PlainCredentials.assert_called_with('guest', 'pass')
        mock_credentials = mock_pika.PlainCredentials.return_value
        mock_pika.ConnectionParameters.assert_called_with(host='somehost', credentials=mock_credentials)
        mock_connection_params = mock_pika.ConnectionParameters.return_value
        mock_pika.SelectConnection.assert_called_with(mock_connection_params,
                                                      consumer.on_connection_open,
                                                      stop_ioloop_on_close=False)
        mock_select_connection = mock_pika.SelectConnection.return_value
        mock_select_connection.ioloop.start.assert_called()

        # then pika should call on_connection_open and we will create a channel
        consumer.on_connection_open(MagicMock())
        # we will then create a channel
        mock_select_connection.channel.assert_called()

        # then pika should call on_channel_open and we will declare our queue
        mock_channel = MagicMock()
        consumer.on_channel_open(channel=mock_channel)
        mock_channel.queue_declare.assert_called_with(consumer.start_consuming, 'myqueue', durable=True)

        # then pika should call start_consuming and we will ask the broker to send us messages for the queue
        consumer.start_consuming(MagicMock())
        mock_channel.basic_consume.assert_called_with(consumer.on_message, 'myqueue')

        # then the broker sends pika a message and pika calls on_message which acknowledges the message and calls our func
        consumer.on_message(mock_channel, MagicMock(), MagicMock(), 'payload')
        mock_channel.basic_ack.assert_called()
        self.assertEqual(self.on_message_payload, 'payload')

        # when we shutdown it should cleanup
        consumer.stop()
        mock_channel.basic_cancel.assert_called()

        # then pika should call on_cancelok
        consumer.on_cancelok(MagicMock())
        mock_channel.close.assert_called()




    def message_consumer_func(self, channel, basic_deliver, properties, body):
        self.on_message_payload = body
