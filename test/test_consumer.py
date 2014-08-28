import unittest2

from mock import MagicMock

from afkak.consumer import Consumer


class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = Consumer(MagicMock(), 'group', 'topic',
                                auto_commit=False, partitions=['0'])
            consumer.queue_low_watermark = 32  # STFU pyflakes

    def test_consumer_init(self):
        consumer = Consumer(
            MagicMock(), 'tGroup', 'tTopic', False, [0, 1, 2, 3],
            5, 60, 4096, 1000, 256 * 1024, 8 * 1024 * 1024, 128,
            256)
        self.assertIsNone(consumer.commit_looper)
        self.assertIsNone(consumer.commit_looper_d)

    def test_consumer_buffer_size_err(self):
        with self.assertRaises(ValueError):
            consumer = Consumer(MagicMock(), 'group', 'topic', [0],
                                buffer_size=8192, max_buffer_size=4096)
            consumer.queue_low_watermark = 32  # STFU pyflakes

#    def test_consumer_setup_loopingcall(self):
#        with
