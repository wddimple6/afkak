import unittest2

from mock import MagicMock

#from afkak import KafkaClient
from afkak.consumer import Consumer
# from afkak.common import (
#     ProduceRequest, BrokerMetadata, PartitionMetadata,
#     TopicAndPartition, KafkaUnavailableError,
#     LeaderUnavailableError, PartitionUnavailableError
# )
# from afkak.kafkacodec import (
#     create_message
# )


class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = Consumer(MagicMock(), 'group', 'topic',
                                partitions=['0'])
            consumer.queue_low_watermark = 32  # STFU pyflakes
