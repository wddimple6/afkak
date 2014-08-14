import os
import random
import struct
import unittest2

from mock import MagicMock, patch

from afkak import KafkaClient
from afkak.consumer import SimpleConsumer
from afkak.common import (
    ProduceRequest, BrokerMetadata, PartitionMetadata,
    TopicAndPartition, KafkaUnavailableError,
    LeaderUnavailableError, PartitionUnavailableError
)
from afkak.kafkacodec import (
    create_message
)

class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])
