import logging
log = logging.getLogger('afkak_test.test_consumer')
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')

from nose.twistedtools import threaded_reactor, deferred

from mock import MagicMock, patch, ANY

from twisted.python.failure import Failure
from twisted.internet.defer import setDebugging, inlineCallbacks, Deferred

from twisted.trial.unittest import TestCase

from afkak.consumer import (Consumer, AUTO_COMMIT_INTERVAL,
                            TOPIC_LOAD_RETRY_DELAY)
from afkak.common import (KafkaUnavailableError, OffsetFetchResponse,
                          LeaderNotAvailableError, OffsetCommitRequest,
                          OffsetCommitResponse, FailedPayloadsError)
# from afkak.kafkacodec import (create_message)

DEBUGGING = True
setDebugging(DEBUGGING)
from twisted.internet.base import DelayedCall
DelayedCall.debug = DEBUGGING


class TestKafkaConsumer(TestCase):
    @classmethod
    def setUpClass(cls):
        # Startup the twisted reactor in a thread. We need this for the
        # @nose.twistedtools.deferred(timeout=) decorator to work for
        # testing @inlineCallbacks functions
        cls.reactor, cls.thread = threaded_reactor()

    # NOTE: This must be called from the reactor thread (ie, something
    # wrapped with @deferred
    @inlineCallbacks
    def cleanupConsumer(self, consumer):
        # Handle cleaning up the passed consumer. Adds an errback to
        # the readyD, if it's not None, then calls consumer.stop()
        loading = consumer.waitForReady()
        if loading:
            # Prepare to handle the errback that will happen when we stop()
            loading.addErrback(lambda _: None)
        yield consumer.stop()

    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = Consumer(MagicMock(), 'group', 'topic',
                                auto_commit=False, partitions=['0'])
            consumer.queue_low_watermark = 32  # STFU pyflakes
            # No need to call consumer.stop, it didn't get far enough

    def test_consumer_init(self):
        consumer = Consumer(
            MagicMock(), 'tGroup', 'tTopic', False, [0, 1, 2, 3],
            None, None, 4096, 1000, 256 * 1024, 8 * 1024 * 1024, 128, 256)
        self.assertIsNone(consumer.commit_looper)
        self.assertIsNone(consumer.commit_looper_d)
        # Call the callback directly...
        consumer._setupFetchOffsets(
            [OffsetFetchResponse('tTopic', 0, 50, 'meta1', 0),
             OffsetFetchResponse('tTopic', 1, 150, 'meta2', 0),
             OffsetFetchResponse('tTopic', 2, 350, 'meta3', 0),
             OffsetFetchResponse('tTopic', 3, 950, 'meta4', 3)])
        offsets = {0: 50, 1: 150, 2: 350, 3: 0}
        self.assertEqual(consumer.offsets, offsets)
        # No need to call consumer.stop, no real client, partitions supplied

    def test_consumer_buffer_size_err(self):
        with self.assertRaises(ValueError):
            consumer = Consumer(MagicMock(), 'group', 'topic', [0],
                                buffer_size=8192, max_buffer_size=4096)
            consumer.queue_low_watermark = 32  # STFU pyflakes
        # No need to call consumer.stop, it didn't get far enough

    @patch('afkak.consumer.LoopingCall')
    def test_consumer_setup_loopingcall(self, lcMock):
        mockClient = MagicMock()
        mockClient.topic_partitions.__getitem__.return_value = [0]
        consumer = Consumer(mockClient, 'group', 'topic')
        assert lcMock.called
        consumer.commit_looper.start.assert_called_once_with(
            AUTO_COMMIT_INTERVAL / 1000.0, now=False)
        # No need to call consumer.stop, enough is mocked away

    @patch('afkak.consumer.LoopingCall')
    @patch('afkak.consumer.log')
    def test_consumer_empty_partitions(self, mockLog, _):
        mockClient = MagicMock()
        mockClient.topic_partitions.__getitem__.return_value = []
        consumer = Consumer(mockClient, 'group', 'topic')
        self.assertEqual({}, consumer.offsets)
        mockLog.warning.assert_called_once_with(
            'setupPartitions: Metadata for topic:%s is empty partition list.',
            'topic')
        # No need to call consumer.stop, enough is mocked away

    def test_consumer_load_topic_metadata(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        d = MagicMock()
        mockClient.load_metadata_for_topics.return_value = d
        consumer = Consumer(mockClient, 'group', 'topic', False)
        mockClient.load_metadata_for_topics.assert_called_once_with('topic')
        d.addCallbacks.assert_called_once_with(
            consumer._handleClientLoadMetadata,
            consumer._handleClientLoadMetadataError)
        # No need to call consumer.stop, enough is mocked away

    def test_consumer_load_topic_metadata_retry(self):
        mockClock = MagicMock()
        reloadCall = MagicMock()
        mockClock.callLater.return_value = reloadCall
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        d = MagicMock()
        mockClient.load_metadata_for_topics.return_value = d
        consumer = Consumer(mockClient, 'group', 'aTopic', False)
        consumer._clock = mockClock
        mockClient.load_metadata_for_topics.assert_called_once_with('aTopic')
        d.addCallbacks.assert_called_once_with(
            consumer._handleClientLoadMetadata,
            consumer._handleClientLoadMetadataError)
        # Ok, now fake the deferred being called-back
        consumer._handleClientLoadMetadata(None)
        # Consumer should have asked it's clock to callLater()
        mockClock.callLater.assert_called_once_with(
            TOPIC_LOAD_RETRY_DELAY,
            consumer._setupPartitions)
        # No need to call consumer.stop, enough is mocked away

    def test_consumer_setupPartitions(self):
        mockClient = MagicMock()
        mockD = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        mockClient.load_metadata_for_topics.return_value = mockD
        consumer = Consumer(mockClient, 'group', 'topic', False)
        mockClient.load_metadata_for_topics.assert_called_once_with('topic')
        mockD.addCallbacks.assert_called_once_with(
            consumer._handleClientLoadMetadata,
            consumer._handleClientLoadMetadataError)
        mockClient.topic_partitions.__getitem__.return_value = [0, 1, 2]
        mockClient.has_metadata_for_topic.return_value = True
        consumer._handleClientLoadMetadata(None)
        offsets = {0: 0, 1: 0, 2: 0}
        self.assertEqual(consumer.offsets, offsets)
        self.assertEqual(consumer.fetch_offsets, offsets)
        # No need to call consumer.stop, enough is mocked away

    @patch('afkak.consumer.log')
    def test_consumer_setupPartitionOffsetsRetry(self, mockLog):
        mockClient = MagicMock()
        mockClock = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'topic2', True,
                            auto_commit_every_t=None)
        consumer._clock = mockClock
        mockClient.load_metadata_for_topics.assert_called_once_with('topic2')
        consumer._setupPartitionOffsets([])
        mockLog.warning.assert_called_once_with(
            'setupPartitionOffsets got empty partition list.')
        mockClock.callLater.assert_called_once_with(
            TOPIC_LOAD_RETRY_DELAY,
            consumer._setupPartitions)
        # No need to call consumer.stop, enough is mocked away

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
        # No need to call consumer.stop, enough is mocked away

    @patch('afkak.consumer.log')
    def test_consumer_setupFetchOffsetsRetry(self, mockLog):
        mockClock = MagicMock()
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = True
        mockClient.topic_partitions.__getitem__.return_value = [0, 3, 5, 7]
        consumer = Consumer(mockClient, 'group', 'csfor_topic',
                            auto_commit_every_t=None)
        consumer._clock = mockClock
        theOffsets = [OffsetFetchResponse('csfor_topic', 0, 25, 'meta9', 0),
                      OffsetFetchResponse('csfor_topic', 3, 50, 'meta8', 0),
                      OffsetFetchResponse('csfor_topic', 5, 75, 'meta7', 0),
                      OffsetFetchResponse('csfor_topic', 7, 0, 'meta6', 5)]
        consumer._setupFetchOffsets(theOffsets)
        offsets = {0: 25, 3: 50, 5: 75}
        self.assertEqual(consumer.offsets, offsets)
        self.assertEqual(consumer.fetch_offsets, {})
        mockLog.error.assert_called_once_with(
            '_setupFetchOffsets got unexpected KafkaError:%r', ANY)
        mockClock.callLater.assert_called_once_with(
            TOPIC_LOAD_RETRY_DELAY,
            consumer._setupPartitions)
        # No need to call consumer.stop, enough is mocked away

    def test_consumer_check_fetch_msgs(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'cfm_topic',
                            auto_commit=False)

        consumer.fetch = MagicMock()
        # Check we don't fetch when stopping
        consumer.stopping = True
        consumer._check_fetch_msgs()
        self.assertFalse(consumer.fetch.called)

        # And if we're not stopping, but we're waiting on an outstanding fetch
        # we still don't fetch
        consumer.stopping = False
        consumer._fetchD = True
        consumer._check_fetch_msgs()
        self.assertFalse(consumer.fetch.called)

        # And if we're not stopping, and not waiting on an outstanding fetch
        # but we have enough messages, we still don't fetch
        consumer._fetchD = None
        consumer.queue.pending = range(consumer.queue_low_watermark + 1)
        consumer._check_fetch_msgs()
        self.assertFalse(consumer.fetch.called)

        # But if we don't have enough messages, we will fetch
        consumer.queue.pending = range(consumer.queue_low_watermark)
        consumer._check_fetch_msgs()
        self.assertTrue(consumer.fetch.called)
        # No need to call consumer.stop, enough is mocked away

    def test_consumer_commit(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'tcc_group', 'cfm_topic',
                            auto_commit=False)
        # test commit without anything to do (count_since_commit == 0)
        self.assertEqual(self.successResultOf(consumer.commit()), None)
        self.assertFalse(mockClient.send_offset_commit_request.called)
        # Test that with something to do that we call waitForReady()
        consumer.waitForReady = MagicMock()
        consumer.waitForReady.return_value = True
        consumer.count_since_commit += 1
        self.assertEqual(self.successResultOf(consumer.commit()), None)
        self.assertFalse(mockClient.send_offset_commit_request.called)
        consumer.waitForReady.assert_called_once_with()
        # Ok, now test with some partitions setup
        consumer.waitForReady.reset_mock()
        consumer.offsets = {0: 50, 1: 150, 2: 350, 3: 75}
        respD = Deferred()
        mockClient.send_offset_commit_request.return_value = respD
        d = consumer.commit()
        self.assertNoResult(d)
        mockClient.send_offset_commit_request.assert_called_once_with(
            'tcc_group', [OffsetCommitRequest(topic='cfm_topic', partition=0,
                                              offset=50, metadata=None),
                          OffsetCommitRequest(topic='cfm_topic', partition=1,
                                              offset=150, metadata=None),
                          OffsetCommitRequest(topic='cfm_topic', partition=2,
                                              offset=350, metadata=None),
                          OffsetCommitRequest(topic='cfm_topic', partition=3,
                                              offset=75, metadata=None)])
        # Send the responses
        resps = [OffsetCommitResponse('cfm_topic', 0, 0),
                 OffsetCommitResponse('cfm_topic', 1, 0),
                 OffsetCommitResponse('cfm_topic', 2, 0),
                 OffsetCommitResponse('cfm_topic', 3, 0)]

        resultList = []

        def _checkResult(result):
            resultList.append(result)

        d.addBoth(_checkResult)
        respD.callback(resps)
        self.assertEqual(resps, resultList[0])

    def test_consumer_commit_partitions(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'tcc_group', 'cfm_topic',
                            auto_commit=False)
        consumer.waitForReady = MagicMock()
        consumer.waitForReady.return_value = True
        consumer.count_since_commit += 1
        self.assertEqual(self.successResultOf(consumer.commit()), None)
        self.assertFalse(mockClient.send_offset_commit_request.called)
        consumer.waitForReady.assert_called_once_with()
        # Ok, now test with some partitions setup
        consumer.waitForReady.reset_mock()
        consumer.commit_looper = MagicMock()
        consumer.offsets = {0: 50, 1: 150, 2: 350, 3: 75}
        respD = Deferred()
        mockClient.send_offset_commit_request.return_value = respD
        d = consumer.commit(partitions=[0, 3])
        self.assertNoResult(d)
        mockClient.send_offset_commit_request.assert_called_once_with(
            'tcc_group', [OffsetCommitRequest(topic='cfm_topic', partition=0,
                                              offset=50, metadata=None),
                          OffsetCommitRequest(topic='cfm_topic', partition=3,
                                              offset=75, metadata=None)])
        # Send the responses
        resps = [OffsetCommitResponse('cfm_topic', 0, 0),
                 OffsetCommitResponse('cfm_topic', 3, 0)]

        resultList = []

        def _checkResult(result):
            resultList.append(result)

        d.addBoth(_checkResult)
        respD.callback(resps)
        self.assertEqual(resps, resultList[0])
        # Make sure the commit_looper got reset
        consumer.commit_looper.reset.assert_called_once_with()

    def test_consumer_auto_commit(self):
        # setup 'nuetered' consumer
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'tcc_group', 'cfm_topic',
                            auto_commit=False)
        consumer.commit = MagicMock()
        commitD = Deferred()
        consumer.commit.return_value = commitD

        # Make sure it does nothing with auto_commit == False
        consumer._auto_commit()
        self.assertFalse(consumer.commit.called)
        # Turn on auto commit, still nothing (no messages yet)
        consumer.auto_commit = True
        consumer._auto_commit()
        self.assertFalse(consumer.commit.called)
        # Now fake some messages having been processed
        consumer.count_since_commit = consumer.auto_commit_every_n
        consumer._auto_commit()
        # Did we start commit()?
        consumer.commit.assert_called_once_with()
        # Ok, check for deferred
        self.assertEqual(consumer._autoCommitD, commitD)
        # Check we don't try to start another commit until last one done
        consumer._auto_commit()
        consumer.commit.assert_called_once_with()
        # Fire the deferred and see that the autoCommitD got cleared
        commitD.callback(None)
        self.assertEqual(consumer._autoCommitD, None)

    @deferred(timeout=15)
    @patch('afkak.consumer.log')
    @inlineCallbacks
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
        yield self.cleanupConsumer(consumer)

    @deferred(timeout=15)
    @inlineCallbacks
    def test_consumer_stop_with_auto_by_t(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'topic', True)
        consumer._commitTimerStopped = MagicMock()
        self.assertNotEqual(consumer.commit_looper, None)
        self.assertNotEqual(consumer.commit_looper_d, None)
        yield self.cleanupConsumer(consumer)
        self.assertEqual(consumer.commit_looper, None)
        self.assertEqual(consumer.commit_looper_d, None)

    @patch('afkak.consumer.log')
    def test_consumer_stop_with_background_fetch(self, mockLog):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = False
        consumer = Consumer(mockClient, 'group', 'topic', False)
        # Pretend there was an error...
        f = FailedPayloadsError()
        consumer._fetchD = Failure(f)
        consumer.readyD = None
        consumer._autoCommitD = True
        consumer.stop()
        mockLog.debug.assert_called_once_with(
            'Background fetch failed during stop:%r', f)

    # def test_consumer_pending(self):
    #     mockClient = MagicMock()
    #     mockClient.has_metadata_for_topic.return_value = False
    #     consumer = Consumer(mockClient, 'group', 'topic', False)
    #     respD = Deferred()
    #     mockClient.send_offset_request.return_value = respD
    #     consumer.offsets = {5: 50, 6: 60, 7: 70, 8: 80}
    #     d = consumer.pending()
    #     self.assertNoResult(d)

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
        # No need to call consumer.stop, enough is mocked away

    def test_consumer_repr(self):
        mockClient = MagicMock()
        mockClient.has_metadata_for_topic.return_value = True
        consumer = Consumer(mockClient, 'Grues', 'Diet', auto_commit=False)
        mockClient.topic_partitions.__getitem__.return_value = [0, 1, 2]
        consumer._handleClientLoadMetadata(None)
        self.assertEqual(
            consumer.__repr__(),
            '<afkak.Consumer group=Grues, topic=Diet, partitions=[0, 1, 2]>')
        # No need to call consumer.stop, enough is mocked away

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
        # No need to call consumer.stop, enough is mocked away

    # @deferred(timeout=5)
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
