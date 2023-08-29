from aiokafka import AIOKafkaProducer
from pyngleton import singleton

from constants import URLS_TOPIC


@singleton
class Producer:
    def __init__(self, kafka_uri: str):
        self._inner = AIOKafkaProducer(bootstrap_servers=kafka_uri)

    @property
    def inner(self):
        return self._inner

    async def send_to_queue(self, topic: str, message: bytes, timestamp_ms: int):
        await self._inner.send_and_wait(topic, message, timestamp_ms=timestamp_ms)

    async def send_urls_batch(self, urls: list[str], timestamp_ms: int):
        partitions = tuple(await self._inner.partitions_for(URLS_TOPIC))
        num_partitions = len(partitions)
        partition_counter = 0

        batch = self._inner.create_batch()
        while urls:
            msg = urls.pop().encode('utf-8')
            metadata = batch.append(key=None, value=msg, timestamp=timestamp_ms)
            if metadata is None:
                partition = partitions[partition_counter % num_partitions]
                await self._inner.send_batch(batch, URLS_TOPIC, partition=partition)
                batch = self._inner.create_batch()
                partition_counter += 1
        partition = partitions[partition_counter % num_partitions]
        await self._inner.send_batch(batch, URLS_TOPIC, partition=partition)
