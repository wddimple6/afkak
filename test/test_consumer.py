import logging
log = logging.getLogger('afkak_test.test_consumer')
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')

import nose.twistedtools
import unittest2

from mock import MagicMock, patch, ANY

from twisted.python.failure import Failure
from twisted.internet.defer import setDebugging  # , inlineCallbacks

from afkak.consumer import Consumer, AUTO_COMMIT_INTERVAL
from afkak.common import (KafkaUnavailableError, OffsetFetchResponse,
                          LeaderNotAvailableError)
# from afkak.kafkacodec import (create_message)

DEBUGGING = True
setDebugging(DEBUGGING)
from twisted.internet.base import DelayedCall
DelayedCall.debug = DEBUGGING


class TestKafkaConsumer(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        # Startup the twisted reactor in a thread. We need this for the
        # @nose.twistedtools.deferred(timeout=) decorator to work for
        # testing @inlineCallbacks functions
        cls.reactor, cls.thread = nose.twistedtools.threaded_reactor()

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
        consumer._setupFetchOffsets(
            [OffsetFetchResponse('tTopic', 0, 50, 'meta1', 0),
             OffsetFetchResponse('tTopic', 1, 150, 'meta2', 0),
             OffsetFetchResponse('tTopic', 2, 350, 'meta3', 0),
             OffsetFetchResponse('tTopic', 3, 950, 'meta4', 3)])
        offsets = {0: 50, 1: 150, 2: 350, 3: 0}
        self.assertEqual(consumer.offsets, offsets)

    def test_consumer_buffer_size_err(self):
        with self.assertRaises(ValueError):
            consumer = Consumer(MagicMock(), 'group', 'topic', [0],
                                buffer_size=8192, max_buffer_size=4096)
            consumer.queue_low_watermark = 32  # STFU pyflakes

    @patch('afkak.consumer.LoopingCall')
    def test_consumer_setup_loopingcall(self, lcMock):
        mockClient = MagicMock()
        mockClient.topic_partitions.__getitem__.return_value = [0]
        consumer = Consumer(mockClient, 'group', 'topic')
        assert lcMock.called
        consumer.commit_looper.start.assert_called_once_with(
            AUTO_COMMIT_INTERVAL / 1000.0, now=False)

    @patch('afkak.consumer.LoopingCall')
    @patch('afkak.consumer.log')
    def test_consumer_empty_partitions(self, mockLog, _):
        mockClient = MagicMock()
        mockClient.topic_partitions.__getitem__.return_value = []
        consumer = Consumer(mockClient, 'group', 'topic')
        self.assertEqual({}, consumer.offsets)
        mockLog.warning.assert_called_once_with(
            'setupPartitionOffsets got empty partition list.')

    def test_consumer_load_topic_metadata(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'topic', False)
        mockClient.load_metadata_for_topics.assert_called_once_with('topic')
        consumer.offsetFetchD.addCallbacks.assert_called_once_with(
            consumer._handleClientLoadMetadata,
            consumer._handleClientLoadMetadataError)

    def test_consumer_loadmetadata(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'topic', False)
        mockClient.topic_partitions.__getitem__.return_value = [0, 1, 2]
        consumer._handleClientLoadMetadata(None)
        offsets = {0: 0, 1: 0, 2: 0}
        self.assertEqual(consumer.offsets, offsets)
        self.assertEqual(consumer.fetch_offsets, offsets)

    def test_consumer_loadmetadata_with_autocommit(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = True
        mockClient.topic_partitions.__getitem__.return_value = [0, 3, 5, 7]
        consumer = Consumer(mockClient, 'group', 'clwa_topic',
                            auto_commit_every_t=None)
        consumer._setupFetchOffsets(
            [OffsetFetchResponse('clwa_topic', 0, 25, 'meta1', 0),
             OffsetFetchResponse('clwa_topic', 3, 50, 'meta2', 0),
             OffsetFetchResponse('clwa_topic', 5, 75, 'meta3', 0),
             OffsetFetchResponse('clwa_topic', 7, 95, 'meta4', 0)])
        offsets = {0: 25, 3: 50, 5: 75, 7: 95}
        self.assertEqual(consumer.offsets, offsets)
        self.assertEqual(consumer.fetch_offsets, offsets)

    @patch('afkak.consumer.log')
    def test_consumer_loadmetadata_fail(self, mockLog):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'topic', False)
        f = Failure(
            KafkaUnavailableError(
                "Unable to load metadata from configured hosts"))
        consumer._handleClientLoadMetadataError(f)
        offsets = {}
        self.assertEqual(consumer.offsets, offsets)
        self.assertEqual(consumer.fetch_offsets, offsets)
        mockLog.error.assert_called_once_with(
            'Client failed to load metadata:%r', f)

    def test_consumer_getClock(self):
        from twisted.internet import reactor
        consumer = Consumer(MagicMock(), 'group', 'topic', False, [0])
        clock = consumer._getClock()
        self.assertEqual(clock, reactor)
        self.assertEqual(consumer._clock, reactor)
        mockClock = MagicMock()
        consumer._clock = mockClock
        clock = consumer._getClock()
        self.assertEqual(clock, mockClock)
        self.assertEqual(consumer._clock, mockClock)

    def test_consumer_repr(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'Grues', 'Diet', auto_commit=False)
        mockClient.topic_partitions.__getitem__.return_value = [0, 1, 2]
        consumer._handleClientLoadMetadata(None)
        self.assertEqual(
            consumer.__repr__(),
            '<afkak.Consumer group=Grues, topic=Diet, partitions=[0, 1, 2]>')

    @patch('afkak.consumer.log')
    @patch('afkak.consumer.LoopingCall')
    def test_commit_timer_callbacks(self, lcMock, mockLog):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = True
        mockClient.topic_partitions.__getitem__.return_value = [0, 3, 5, 7]
        consumer = Consumer(mockClient, 'Zork', 'Eaten',
                            auto_commit_every_t=1200)
        assert lcMock.called
        consumer.commit_looper.start.assert_called_once_with(1.2, now=False)
        consumer.commit_looper.start.reset_mock()
        f = Failure(LeaderNotAvailableError())
        consumer._commitTimerFailed(f)
        # Check we log the failure, and restart...
        mockLog.warning.assert_called_once_with('commitTimerFailed:%r: %s',
                                                f, ANY)
        consumer.commit_looper.start.assert_called_once_with(1.2, now=False)
        mockLog.warning.reset_mock()
        # Check the stop call checks for wrong looping call
        cl = consumer.commit_looper
        cld = consumer.commit_looper_d
        s = 'Not the droids you are looking for'
        consumer._commitTimerStopped(s)
        mockLog.warning.assert_called_once_with(
            'commitTimerStopped with wrong timer:%s not:%s', s, cl)
        self.assertEqual(cl, consumer.commit_looper)
        self.assertEqual(cld, consumer.commit_looper_d)
        # Now for success
        consumer._commitTimerStopped(cl)
        self.assertEqual(None, consumer.commit_looper)
        self.assertEqual(None, consumer.commit_looper_d)

    # @nose.twistedtools.deferred(timeout=5)
    # @inlineCallbacks
    # @patch('afkak.consumer.log')
    # def test_consumer_check_fetch(self, mockLog):
    #     mockClient = MagicMock()
    #     T1 = 'CCF_Topic'
    #     # Setup so no deferred/requests needed on Consumer.__init__()
    #     mockClient.topic_partitions.__getitem__.return_value = [1, 2]
    #     responses = self.makeFetchResponses(T1, [1, 2], 30)
    #     mockClient.send_fetch_request.return_value = responses
    #     consumer = Consumer(mockClient, 'group', T1, False)
    #     self.assertIsNone(consumer._fetchD)
    #     consumer._check_fetch_msgs()
    #     # should log and start a fetch via the mockClient
    #     mockLog.debug.assert_called_once_with(
    #    'Initiating new fetch:%d <= %d',
    #    consumer.queue.pending,
    #    consumer.queue_low_watermark)


    # @patch('afkak.consumer.LoopingCall')
    # @patch('afkak.consumer.log')
    # def test_consumer_commit(self, mockLog, mockLC):
    #     mockClient = MagicMock()
    #     mockClient.topic_partitions.__getitem__.return_value = []
    #     consumer = Consumer(mockClient, 'group', 'topic')
    #     self.assertEqual({}, consumer.offsets)
    #     mockLog.warning.assert_called_once_with(
    #         'setupPartitionOffsets got empty partition list.')

    # def makeFetchResponses(topic, partitions, messagesPerPart):
    #     """

    #     """
