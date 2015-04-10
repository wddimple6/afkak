import logging
import uuid

from mock import Mock, ANY, patch

# from twisted.python.failure import Failure
from twisted.internet.defer import (setDebugging, Deferred)
from twisted.internet.base import DelayedCall
from twisted.internet.task import LoopingCall
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial import unittest
# from twisted.python.failure import Failure

from afkak.producer import (Producer)
import afkak.producer as aProducer

from afkak.common import (
    UnsupportedCodecError,
    ProduceRequest,
    ProduceResponse,
    UnknownTopicOrPartitionError,
    KafkaUnavailableError,
    OffsetOutOfRangeError,
    BrokerNotAvailableError,
    )
#
#     OffsetOutOfRangeError,
#     InvalidConsumerGroupError,
#     ConsumerFetchSizeTooSmall,
#     OffsetFetchRequest,
#     OffsetFetchResponse,
#     OffsetRequest,
#     OffsetResponse,
#     FetchRequest,
#     FetchResponse,
#     KAFKA_SUCCESS,
#     OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_COMMITTED)

from afkak.kafkacodec import (create_message_set)
from testutil import (random_string)

log = logging.getLogger(__name__)
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')

DEBUGGING = True
setDebugging(DEBUGGING)
DelayedCall.debug = DEBUGGING


def dummyProc(messages):
    return None


class TestAfkakProducer(unittest.TestCase):
    _messages = {}
    topic = None

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))
        return self._messages[s]

    def setUp(self):
        super(unittest.TestCase, self).setUp()
        if not self.topic:
            self.topic = "%s-%s" % (
                self.id()[self.id().rindex(".") + 1:], random_string(10))

    def test_producer_init_simplest(self):
        producer = Producer(Mock())
        self.assertEqual(
            producer.__repr__(),
            "<Producer <class 'afkak.partitioner.RoundRobinPartitioner'>:"
            "Unbatched:1:1000>")
        d = producer.stop()
        self.successResultOf(d)

    def test_producer_init_batch(self):
        producer = Producer(Mock(), batch_send=True)
        looper = producer.sendLooper
        self.assertEqual(type(looper), LoopingCall)
        self.assertTrue(looper.running)
        d = producer.stop()
        self.successResultOf(d)
        self.assertFalse(looper.running)
        self.assertEqual(
            producer.__repr__(),
            "<Producer <class 'afkak.partitioner.RoundRobinPartitioner'>:"
            "10cnt/32768bytes/30secs:1:1000>")

    def test_producer_bad_codec_value(self):
        with self.assertRaises(UnsupportedCodecError):
            p = Producer(Mock(), codec=99)
            p.__repr__()  # STFU pyflakes

    def test_producer_bad_codec_type(self):
        with self.assertRaises(TypeError):
            p = Producer(Mock(), codec='bogus')
            p.__repr__()  # STFU pyflakes

    def test_producer_send_empty_messages(self):
        client = Mock()
        producer = Producer(client)
        d = producer.send_messages(self.topic, msgs=[])
        self.failureResultOf(d, ValueError)
        d = producer.stop()
        self.successResultOf(d)

    def test_producer_send_messages(self):
        client = Mock()
        ret = Deferred()
        client.send_produce_request.return_value = ret
        client.topic_partitions = {self.topic: [0, 1, 2, 3]}
        msgs = [self.msg("one"), self.msg("two")]
        ack_timeout = 5

        producer = Producer(client, ack_timeout=ack_timeout)
        d = producer.send_messages(self.topic, msgs=msgs)
        # Check the expected request was sent
        msgSet = create_message_set(msgs, producer.codec)
        req = ProduceRequest(self.topic, ANY, msgSet)
        client.send_produce_request.assert_called_once_with(
            [req], acks=producer.req_acks, timeout=ack_timeout)
        # Check results when "response" fires
        self.assertNoResult(d)
        ret.callback(None)
        self.successResultOf(d)

        d = producer.stop()
        self.successResultOf(d)

    def test_producer_send_messages_batched(self):
        client = Mock()
        ret = Deferred()
        client.send_produce_request.return_value = ret
        client.topic_partitions = {self.topic: [0, 1, 2, 3]}
        msgs = [self.msg("one"), self.msg("two")]
        batch_t = 5
        clock = MemoryReactorClock()

        producer = Producer(client, batch_every_t=batch_t, batch_send=True,
                            clock=clock)
        d = producer.send_messages(self.topic, msgs=msgs)
        # Check no request was yet sent
        self.assertFalse(client.send_produce_request.called)
        # Advance the clock
        clock.advance(batch_t)
        # Check the expected request was sent
        msgSet = create_message_set(msgs, producer.codec)
        req = ProduceRequest(self.topic, ANY, msgSet)
        client.send_produce_request.assert_called_once_with(
            [req], acks=producer.req_acks, timeout=producer.ack_timeout)
        # Check results when "response" fires
        self.assertNoResult(d)
        ret.callback([ProduceResponse(self.topic, 0, 0, 10L)])
        self.successResultOf(d)

        d = producer.stop()
        self.successResultOf(d)

    def test_producer_send_messages_batched_fail(self):
        client = Mock()
        ret = Deferred()
        client.send_produce_request.return_value = ret
        client.topic_partitions = {self.topic: [0, 1, 2, 3]}
        msgs = [self.msg("one"), self.msg("two")]
        batch_t = 5
        clock = MemoryReactorClock()

        producer = Producer(client, batch_every_t=batch_t, batch_send=True,
                            clock=clock)
        d = producer.send_messages(self.topic, msgs=msgs)
        # Check no request was yet sent
        self.assertFalse(client.send_produce_request.called)
        # Advance the clock
        clock.advance(batch_t)
        # Check the expected request was sent
        msgSet = create_message_set(msgs, producer.codec)
        req = ProduceRequest(self.topic, ANY, msgSet)
        client.send_produce_request.assert_called_once_with(
            [req], acks=producer.req_acks, timeout=producer.ack_timeout)
        # Check results when "response" fires
        self.assertNoResult(d)
        ret.errback(OffsetOutOfRangeError(
            'test_producer_send_messages_batched_fail'))
        self.failureResultOf(d, OffsetOutOfRangeError)

        d = producer.stop()
        self.successResultOf(d)

    def test_producer_send_messages_unknown_topic(self):
        client = Mock()
        ds = [Deferred()]
        client.load_metadata_for_topics.return_value = ds[0]
        client.topic_partitions = {}
        msgs = [self.msg("one"), self.msg("two")]
        ack_timeout = 5

        producer = Producer(client, ack_timeout=ack_timeout)
        d = producer.send_messages(self.topic, msgs=msgs)
        # d is waiting on result from ds[0] for load_metadata_for_topics
        # Check results when "response" fires
        self.assertNoResult(d)
        # fire it with client still reporting no metadata for topic
        ds[0].callback(None)
        self.failureResultOf(d, UnknownTopicOrPartitionError)

        d = producer.stop()
        self.successResultOf(d)

    def test_producer_send_messages_fail(self):
        m = Mock()
        m.addCallbacks.side_effect = KafkaUnavailableError
        client = Mock()
        client.send_produce_request.side_effect = [BrokerNotAvailableError()]
        client.topic_partitions = {self.topic: [0, 1, 2, 3]}
        msgs = [self.msg("one"), self.msg("two")]

        with patch.object(aProducer, 'log') as klog:
            producer = Producer(client)
            d = producer.send_messages(self.topic, msgs=msgs)
            # Check the expected request was sent
            msgSet = create_message_set(msgs, producer.codec)
            req = ProduceRequest(self.topic, ANY, msgSet)
            client.send_produce_request.assert_called_once_with(
                [req], acks=producer.req_acks, timeout=producer.ack_timeout)
            klog.exception.assert_called_once_with("Unable to send messages")
            self.failureResultOf(d, BrokerNotAvailableError)

        d = producer.stop()
        self.successResultOf(d)

    def test_producer_sendWaiting_fail(self):
        m = Mock()
        m.addCallbacks.side_effect = KafkaUnavailableError
        client = Mock()
        client.send_produce_request.return_value = m
        client.topic_partitions = {self.topic: [0, 1, 2, 3]}
        msgs = [self.msg("one"), self.msg("two")]
        batch_t = 5
        clock = MemoryReactorClock()

        with patch.object(aProducer, 'log') as klog:
            producer = Producer(client, batch_every_t=batch_t, batch_send=True,
                                clock=clock)
            d = producer.send_messages(self.topic, msgs=msgs)
            # Check no request was yet sent
            self.assertFalse(client.send_produce_request.called)
            # Advance the clock
            clock.advance(batch_t)
            # Check the expected request was sent
            msgSet = create_message_set(msgs, producer.codec)
            req = ProduceRequest(self.topic, ANY, msgSet)
            client.send_produce_request.assert_called_once_with(
                [req], acks=producer.req_acks, timeout=producer.ack_timeout)
            klog.warning.assert_called_once_with('_sendTimerFailed:%r: %s',
                                                 ANY, ANY)

        d = producer.stop()
        self.successResultOf(d)

    def test_producer_sendTimeStopped_error(self):
        # Purely for coverage
        client = Mock()
        producer = Producer(client, batch_send=True)
        with patch.object(aProducer, 'log') as klog:
            producer._sendTimerStopped('Borg')
            klog.warning.assert_called_once_with(
                'commitTimerStopped with wrong timer:%s not:%s', 'Borg',
                producer.sendLooper)

        d = producer.stop()
        self.successResultOf(d)
