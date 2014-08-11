import os
import random
import struct
import unittest2

from mock import MagicMock, patch

from kafkatwisted import KafkaClient
from kafkatwisted.consumer import SimpleConsumer
from kafkatwisted.common import (
    ProduceRequest, BrokerMetadata, PartitionMetadata,
    TopicAndPartition, KafkaUnavailableError,
    LeaderUnavailableError, PartitionUnavailableError
)
from kafkatwisted.kafkacodec import (
    create_message
)

class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])
