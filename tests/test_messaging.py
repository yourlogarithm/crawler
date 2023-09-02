import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from constants import URLS_TOPIC
from src.messaging import Producer


class TestProducer(unittest.IsolatedAsyncioTestCase):
    @patch('aiokafka.AIOKafkaProducer', new_callable=MagicMock)
    async def asyncSetUp(self, MockAIOKafkaProducer):
        self.mock_kafka_producer = MockAIOKafkaProducer()
        self.producer = Producer("kafka://localhost:9092")
        self.producer._inner = self.mock_kafka_producer

    async def test_send_to_queue(self):
        self.mock_kafka_producer.send_and_wait = AsyncMock()
        topic = "test_topic"
        message = b"test_message"
        timestamp_ms = 1632431343

        await self.producer.send_to_queue(topic, message, timestamp_ms)
        self.mock_kafka_producer.send_and_wait.assert_called_with(topic, message, timestamp_ms=timestamp_ms)

    async def test_send_urls_batch(self):
        self.mock_kafka_producer.partitions_for = AsyncMock(return_value=[0, 1, 2])
        self.mock_kafka_producer.send_batch = AsyncMock()
        self.mock_kafka_producer.create_batch = MagicMock()
        batch = AsyncMock()
        batch.append = AsyncMock()
        self.mock_kafka_producer.create_batch.return_value = batch
        urls = ["http://example1.com", "http://example2.com"]
        timestamp_ms = 1632431343

        await self.producer.send_urls_batch(urls, timestamp_ms)

        # Check if partitions_for is called with the correct topic
        self.mock_kafka_producer.partitions_for.assert_called_with(URLS_TOPIC)

        # Check if send_batch is called (at least once)
        self.mock_kafka_producer.send_batch.assert_called()

        # Check if append is called with the correct parameters
        batch.append.assert_called()
