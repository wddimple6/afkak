import logging

from nose.twistedtools import threaded_reactor, deferred

from mock import Mock, patch, ANY

from twisted.python.failure import Failure
from twisted.internet.defer import setDebugging, inlineCallbacks, Deferred
from twisted.internet.base import DelayedCall
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase

from afkak.consumer import (Consumer, FETCH_BUFFER_SIZE_BYTES)
from afkak.common import (
    KafkaUnavailableError,
    OffsetOutOfRangeError,
    InvalidConsumerGroupError,
    ConsumerFetchSizeTooSmall,
    FailedPayloadsError,
    OffsetFetchRequest,
    OffsetFetchResponse,
    OffsetRequest,
    OffsetResponse,
    FetchRequest,
    FetchResponse,
    KAFKA_SUCCESS,
    OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_COMMITTED)

from afkak.kafkacodec import (create_message, KafkaCodec)
import afkak.consumer as kconsumer

log = logging.getLogger(__name__)
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')

DEBUGGING = True
setDebugging(DEBUGGING)
DelayedCall.debug = DEBUGGING


def dummyProc(messages):
    return None


class TestKafkaConsumer(TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = Consumer(Mock(), 'topic', '0', dummyProc)
            consumer.__repr__()  # STFU pyflakes

    def test_consumer_init(self):
        consumer = Consumer(
            Mock(), 'tTopic', 9, dummyProc, None,
            4096, 1000, 256 * 1024, 8 * 1024 * 1024, 1.0, 30)
        del(consumer)

    def test_consumer_buffer_size_err(self):
        with self.assertRaises(ValueError):
            consumer = Consumer(None, 'Grues', 99, dummyProc,
                                buffer_size=8192, max_buffer_size=4096)

    def test_consumer_getClock(self):
        from twisted.internet import reactor
        consumer = Consumer(Mock(), 'topic', 23, dummyProc)
        clock = consumer._getClock()
        self.assertEqual(clock, reactor)
        self.assertEqual(consumer._clock, reactor)
        mockClock = Mock()
        consumer._clock = mockClock
        clock = consumer._getClock()
        self.assertEqual(clock, mockClock)
        self.assertEqual(consumer._clock, mockClock)

    def test_consumer_repr(self):
        mockClient = Mock()
        processor = '<function dummyProc at 0x12345678>'
        consumer = Consumer(mockClient, 'Grues', 99, processor)
        self.assertEqual(
            consumer.__repr__(),
            '<afkak.Consumer topic=Grues, partition=99, '
            'processor=<function dummyProc at 0x12345678>>')

    def test_consumer_start_offset(self):
        mockclient = Mock()
        mockback = Mock()
        consumer = Consumer(mockclient, 'offset22Topic', 18, dummyProc)
        d = consumer.start(22)
        request = FetchRequest('offset22Topic', 18, 22, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        d.addCallback(mockback)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_start_earliest(self):
        mockclient = Mock()
        mockback = Mock()
        consumer = Consumer(mockclient, 'earliestTopic', 9, dummyProc)
        d = consumer.start(OFFSET_EARLIEST)
        request = OffsetRequest('earliestTopic', 9, OFFSET_EARLIEST, 1)
        mockclient.send_offset_request.assert_called_once_with([request])
        d.addCallback(mockback)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_start_latest(self):
        offset = 2346  # arbitrary
        topic = 'latestTopic'
        part = 10
        reqs_ds = [Deferred(), Deferred(),]
        mockclient = Mock()
        mockclient.send_offset_request.return_value = reqs_ds[0]
        mockclient.send_offset_fetch_request.return_value = reqs_ds[1]
        mockback = Mock()
        consumer = Consumer(mockclient, topic , part, dummyProc)
        d = consumer.start(OFFSET_LATEST)
        # Make sure request was made
        request = OffsetRequest(topic , part, OFFSET_LATEST, 1)
        mockclient.send_offset_request.assert_called_once_with([request])
        # Deliver the response
        response = OffsetResponse(topic, part, KAFKA_SUCCESS, [offset])
        reqs_ds[0].callback([response])
        self.assertEqual(offset, consumer._fetch_offset)
        # Check that the message fetch was started
        request = FetchRequest(topic, part, offset, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        # Stop the consumer to cleanup any outstanding operations
        d.addCallback(mockback)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_start_committed(self):
        offset = 2996  # arbitrary
        topic = 'committedTopic'
        part = 23
        reqs_ds = [Deferred(), Deferred(),]
        mockclient = Mock()
        mockclient.send_offset_fetch_request.return_value = reqs_ds[0]
        mockclient.send_fetch_request.return_value = reqs_ds[1]
        mockback = Mock()
        consumer = Consumer(mockclient, topic, part, dummyProc,
            group_id="myGroup")
        d = consumer.start(OFFSET_COMMITTED)
        # Make sure request was made
        request = OffsetFetchRequest(topic, part)
        mockclient.send_offset_fetch_request.assert_called_once_with(
            'myGroup', [request])
        # Deliver the response
        response = OffsetFetchResponse(topic, part, offset, "METADATA",
                                       KAFKA_SUCCESS)
        reqs_ds[0].callback([response])
        self.assertEqual(offset, consumer._fetch_offset)
        # Check that the message fetch was started
        request = FetchRequest(topic, part, offset, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        # Stop the consumer to cleanup any outstanding operations
        d.addCallback(mockback)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_start_committed_bad_group(self):
        mockclient = Mock()
        mockback = Mock()
        consumer = Consumer(mockclient, 'committedTopic', 11, dummyProc)
        d = consumer.start(OFFSET_COMMITTED)
        request = OffsetFetchRequest('committedTopic', 11)
        self.assertFalse(mockclient.called)
        self.assertFailure(d, InvalidConsumerGroupError)

    def test_consumer_start_twice(self):
        mockclient = Mock()
        mockback = Mock()
        consumer = Consumer(mockclient, 'twice_start', 12, dummyProc)
        d = consumer.start(0)
        self.assertRaises(RuntimeError, consumer.start, 0)

    def test_consumer_stop_during_offset(self):
        offset = 999  # arbitrary
        topic = 'stop_during_offset'
        part = 101
        reqs_ds = [Deferred(),]
        mockclient = Mock()
        mockclient.send_offset_request.return_value = reqs_ds[0]
        mockback = Mock()
        consumer = Consumer(mockclient, topic , part, dummyProc)
        d = consumer.start(OFFSET_LATEST)
        # Make sure request was made
        request = OffsetRequest(topic , part, OFFSET_LATEST, 1)
        mockclient.send_offset_request.assert_called_once_with([request])
        # Stop the consumer to cleanup any outstanding operations
        d.addCallback(mockback)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_stop_during_fetch_retry(self):
        fetch_ds = [Deferred()]
        mockclient = Mock()
        mockback = Mock()
        mockclient.send_fetch_request.side_effect = fetch_ds
        consumer = Consumer(mockclient, 'committedTopic', 11, "FakeProc",
            group_id="myGroup")
        d = consumer.start(0)
        d.addCallback(mockback)
        with patch.object(kconsumer, 'log') as klog:
            f = Failure(OffsetOutOfRangeError())
            fetch_ds[0].errback(f)
            klog.error.assert_called_once_with(
                "%r: Failure fetching from kafka: %r", consumer, f)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_stop_during_processing(self):
        fetch_ds = [Deferred()]
        mockclient = Mock()
        mockback = Mock()
        mockerrback = Mock()
        proc_d = Deferred(Mock())

        topic = 'proc_stop'
        part = 33
        offset = 67

        mockclient.send_fetch_request.side_effect = fetch_ds
        consumer = Consumer(mockclient, topic, part, lambda _: proc_d)
        d = consumer.start(offset)
        d.addCallback(mockback)
        d.addErrback(mockerrback)
        request = FetchRequest(topic, part, offset, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        # create & deliver the response
        messages = [
            create_message("v9", "k9"),
            create_message("v10", "k10")
        ]
        message_set = KafkaCodec._encode_message_set(messages, offset)
        message_iter = KafkaCodec._decode_message_set_iter(message_set)
        response = FetchResponse(topic, part, KAFKA_SUCCESS, 486, message_iter)
        fetch_ds[0].callback(response)
        # Make sure the processor was called
        self.assertEqual(proc_d, consumer._processor_d)
        # stop consumer mid processing
        consumer.stop()
        # Make sure the processor callback was cancelled
        proc_d._canceller.assert_called_once_with(proc_d)

        # Make sure the start callback was called, and the errback wasn't
        mockback.assert_called_once_with('Stopped')
        self.assertFalse(mockerrback.called)

    def test_consumer_stop_not_started(self):
        mockclient = Mock()
        mockback = Mock()
        consumer = Consumer(mockclient, 'stop_no_start', 12, dummyProc)
        self.assertRaises(RuntimeError, consumer.stop)

    def test_consumer_processor_error(self):
        reqs_ds = [Deferred()]
        mockclient = Mock()
        mockback = Mock()
        proc_d = Deferred()

        topic = 'proc_error'
        part = 30
        offset = 38

        mockclient.send_fetch_request.side_effect = reqs_ds
        consumer = Consumer(mockclient, topic, part, lambda _: proc_d)
        d = consumer.start(offset)
        d.addErrback(mockback)
        request = FetchRequest(topic, part, offset, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        # create & deliver the response
        messages = [
            create_message("v1", "k1"),
            create_message("v2", "k2")
        ]
        message_set = KafkaCodec._encode_message_set(messages, offset)
        message_iter = KafkaCodec._decode_message_set_iter(message_set)
        response = FetchResponse(topic, part, KAFKA_SUCCESS, 99, message_iter)
        reqs_ds[0].callback(response)
        # Make sure the processor was called
        self.assertEqual(proc_d, consumer._processor_d)

        # Errback the processor deferred
        f = Failure(KeyError())  # Pick some random failure mode
        proc_d.errback(f)
        # Ensure the start() deferred was errback'd
        mockback.assert_called_once_with(f)

        consumer.stop()

    def test_consumer_error_during_offset(self):
        offset = 1001  # arbitrary
        topic = 'error_during_offset'
        part = 991
        reqs_ds = [Deferred(), Deferred()]
        mockclient = Mock()
        mockclient.send_offset_request.side_effect = reqs_ds
        mockback = Mock()
        consumer = Consumer(mockclient, topic , part, dummyProc)
        consumer._clock = MemoryReactorClock()
        d = consumer.start(OFFSET_LATEST)
        # Make sure request for offset was made
        request = OffsetRequest(topic , part, OFFSET_LATEST, 1)
        mockclient.send_offset_request.assert_called_once_with([request])
        # Errback the first request
        f = Failure(KafkaUnavailableError())  # Perhaps kafka wasn't up yet...
        with patch.object(kconsumer, 'log') as klog:
            reqs_ds[0].errback(f)
        # Advance the clock to trigger the 2nd request
        consumer._clock.advance(consumer.retry_delay + 1)  # fire the callLater
        self.assertEqual(2, mockclient.send_offset_request.call_count)

        # Stop the consumer to cleanup any outstanding operations
        d.addCallback(mockback)
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_errors_during_offset(self):
        topic = 'all_errors_during_offset'
        part = 991
        mockback = Mock()
        mockclient = Mock()
        mockclient.send_offset_request.side_effect = Deferred
        # The request we expect...
        request = OffsetRequest(topic , part, OFFSET_EARLIEST, 1)
        # The error we'll return
        f = Failure(KafkaUnavailableError())  # Perhaps kafka wasn't up yet...

        consumer = Consumer(mockclient, topic , part, dummyProc)
        consumer._clock = MemoryReactorClock()
        d = consumer.start(OFFSET_EARLIEST)
        d.addErrback(mockback)
        # Make sure request for offset was made with correct request
        mockclient.send_offset_request.assert_called_once_with([request])
        call_count = 0
        while not mockback.called:
            # Make sure more requests are made each time the timer expires
            call_count += 1
            self.assertEqual(call_count,
                             mockclient.send_offset_request.call_count)
            # Errback the request
            with patch.object(kconsumer, 'log') as klog:
                consumer._request_d.errback(f)
            # Advance the clock to trigger the 2nd request
            consumer._clock.advance(consumer.retry_delay + 0.01)

        # Make sure the start() deferred was errbacked with the failure
        mockback.assert_called_once_with(f)
        # Stop the consumer to cleanup any outstanding operations
        consumer.stop()

    def test_consumer_fetch_reply_during_processing(self):
        fetch_ds = [Deferred(), Deferred(), Deferred()]
        mockclient = Mock()
        mockback = Mock()
        proc_ds = [Deferred(), Deferred()]
        deferMaker = Mock()
        deferMaker.side_effect = proc_ds

        topic = 'repl_during_proc'
        part = 42
        offset = 1967

        mockclient.send_fetch_request.side_effect = fetch_ds
        consumer = Consumer(mockclient, topic, part, deferMaker)
        consumer._clock = MemoryReactorClock()
        d = consumer.start(offset)
        d.addCallback(mockback)
        messages = [create_message("v9", "k9"),
                    create_message("v10", "k10")
        ]

        # Return a message set starting 1 before the requested offest to
        # exercise the message-skipping code in Consumer._handle_fetch_response
        # which deals with the fact that Kafka can return messages with offests
        # less than requested due to messages being compressed as a set, and
        # the whole compressed set being returned together
        message_set = KafkaCodec._encode_message_set(messages, offset-1)
        message_iter = KafkaCodec._decode_message_set_iter(message_set)
        response = FetchResponse(topic, part, KAFKA_SUCCESS, 486, message_iter)
        with patch.object(kconsumer, 'log') as klog:
            fetch_ds[0].callback(response)
        # Make sure the processor was called
        self.assertEqual(proc_ds[0], consumer._processor_d)
        # Trigger another fetch
        consumer._clock.advance(0.01)
        # Make sure the consumer made a 2nd fetch request
        self.assertEqual(fetch_ds[1], consumer._request_d)
        # Make sure the consumer is still waiting on the 1st processor deferred
        self.assertEqual(proc_ds[0], consumer._processor_d)
        # Deliver the 2nd fetch result
        message_set = KafkaCodec._encode_message_set(messages, offset+1)
        message_iter = KafkaCodec._decode_message_set_iter(message_set)
        response = FetchResponse(topic, part, KAFKA_SUCCESS, 486, message_iter)
        fetch_ds[1].callback(response)
        # Make sure the consumer is STILL waiting on the 1st processor deferred
        self.assertEqual(proc_ds[0], consumer._processor_d)
        # And is STILL waiting on the 2nd fetch reply
        self.assertEqual(fetch_ds[1], consumer._request_d)

        # Deliver the processing result
        proc_ds[0].callback(None)
        # Confirm the consumer is now waiting on the 2nd processor deferred,
        # and isn't waiting on any fetch result
        self.assertEqual(proc_ds[1], consumer._processor_d)
        self.assertEqual(None, consumer._request_d)

        # stop consumer to clean up
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_fetch_large_message(self):
        topic = 'fetch_large_message'
        part = 676
        offset = 0
        mockback = Mock()
        mock_proc = Mock()
        mockclient = Mock()
        reqs_ds = [Deferred() for x in range(10)]
        mockclient.send_fetch_request.side_effect = reqs_ds

        consumer = Consumer(mockclient, topic, part, mock_proc)
        consumer._clock = MemoryReactorClock()
        messages = [create_message('*' * (consumer.buffer_size * 9))]
        message_set = KafkaCodec._encode_message_set(messages, offset)
        d = consumer.start(offset)
        d.addCallback(mockback)

        # Ok, we deliver only part of the message_set, up to the size requested
        while not mock_proc.called:
            # Get the buffer size from the last call
            request = mockclient.send_fetch_request.call_args[0][0][0]
            # Create a response only as large as the 'max_bytes' request param
            message_iter = KafkaCodec._decode_message_set_iter(
                message_set[0:request.max_bytes])
            response = FetchResponse(topic, part, KAFKA_SUCCESS, 486,
                                     message_iter)
            with patch.object(kconsumer, 'log') as klog:
                consumer._request_d.callback(response)
            # Advance the clock to trigger the next request
            consumer._clock.advance(0.01)

        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_consumer_fetch_too_large_message(self):
        topic = 'fetch_too_large_message'
        part = 676
        offset = 0
        mockback = Mock()
        mock_proc = Mock()
        mockclient = Mock()
        reqs_ds = [Deferred() for x in range(10)]
        mockclient.send_fetch_request.side_effect = reqs_ds

        consumer = Consumer(mockclient, topic, part, mock_proc,
                            max_buffer_size=8 * FETCH_BUFFER_SIZE_BYTES)
        consumer._clock = MemoryReactorClock()
        messages = [create_message('X' * (consumer.buffer_size * 9))]
        message_set = KafkaCodec._encode_message_set(messages, offset)
        d = consumer.start(offset)
        d.addErrback(mockback)

        # Ok, we deliver only part of the message_set, up to the size requested
        while not mockback.called:
            # Get the buffer size from the last call
            request = mockclient.send_fetch_request.call_args[0][0][0]
            # Create a response only as large as the 'max_bytes' request param
            message_iter = KafkaCodec._decode_message_set_iter(
                message_set[0:request.max_bytes])
            response = FetchResponse(topic, part, KAFKA_SUCCESS, 486,
                                     message_iter)
            with patch.object(kconsumer, 'log') as klog:
                consumer._request_d.callback(response)
            # Advance the clock to trigger the next request
            consumer._clock.advance(0.01)

        consumer.stop()
        self.assertTrue(mockback.call_args[0][0].check(
            ConsumerFetchSizeTooSmall))

    def test_do_fetch_not_reentrant(self):
        # This test is a bit of a hack to get coverage
        mockclient = Mock()
        mockback = Mock()
        consumer = Consumer(mockclient, 'do_fetch_not_reentrant', 8, dummyProc)
        d = consumer.start(0)
        request = FetchRequest('do_fetch_not_reentrant', 8, 0, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        d.addCallback(mockback)

        # I think _do_fetch() cannot possibly (normally) be called when there's
        # an outstanding request, so force it
        with patch.object(kconsumer, 'log') as klog:
            consumer._do_fetch()
            klog.debug.assert_called_once_with(
                "_do_fetch: Outstanding request: %r", consumer._request_d)

        # And make sure no additional fetch request was made
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        # clean up
        consumer.stop()
        mockback.assert_called_once_with('Stopped')

    def test_do_fetch_before_retry_call(self):
        # This test is a bit of a hack to get coverage
        mockclient = Mock()
        mockclient.send_fetch_request.return_value = Deferred()
        mockback = Mock()
        consumer = Consumer(mockclient, 'do_fetch_before_retry_call', 8, dummyProc)
        d = consumer.start(0)
        request = FetchRequest('do_fetch_before_retry_call', 8, 0, consumer.buffer_size)
        mockclient.send_fetch_request.assert_called_once_with(
            [request], max_wait_time=consumer.fetch_max_wait_time,
            min_bytes=consumer.fetch_min_bytes)
        d.addCallback(mockback)
        # The error we'll return
        f = Failure(KafkaUnavailableError())  # Perhaps kafka wasn't up yet...

        # errback the request so the Consumer will create a _retry_call
        with patch.object(kconsumer, 'log') as klog:
            consumer._request_d.errback(f)

        # I think _do_fetch() cannot possibly (normally) be called before the
        # retry_call fires, so force it
        with patch.object(kconsumer, 'log') as klog:
            consumer._do_fetch()

        # clean up
        consumer.stop()
        mockback.assert_called_once_with('Stopped')
