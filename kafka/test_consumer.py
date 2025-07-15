import unittest
from unittest.mock import patch, MagicMock
import kafka.consumer as consumer

class TestKafkaConsumer(unittest.TestCase):
    @patch('kafka.consumer.Consumer')
    def test_init_consumer_success(self, mock_consumer_class):
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        consumer.consumer = None

        consumer.init_consumer('test-group')

        self.assertIs(consumer.consumer, mock_consumer)
        mock_consumer_class.assert_called_once()

    def test_subscribe_to_topic(self):
        mock_consumer = MagicMock()
        consumer.consumer = mock_consumer

        consumer.subscribe_to_topic('test-topic')

        mock_consumer.subscribe.assert_called_once_with(['test-topic'])

    def test_poll_message(self):
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = 'msg'
        consumer.consumer = mock_consumer

        result = consumer.poll_message()

        self.assertEqual(result, 'msg')
        mock_consumer.poll.assert_called_once_with(1.0)

    def test_close_consumer(self):
        mock_consumer = MagicMock()
        consumer.consumer = mock_consumer

        consumer.close_consumer()

        mock_consumer.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()