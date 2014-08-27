import unittest2

from mock import MagicMock

from afkak.consumer import Consumer


class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = Consumer(MagicMock(), 'group', 'topic',
                                auto_commit=False, partitions=['0'])
            consumer.queue_low_watermark = 32  # STFU pyflakes
