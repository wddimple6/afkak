import os

import logging

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.defer import inlineCallbacks, returnValue, setDebugging
from twisted.internet.base import DelayedCall

from afkak import (Consumer, create_message, )
from afkak.common import (
    ProduceRequest,  ConsumerFetchSizeTooSmall,
    OFFSET_EARLIEST,
    )
from afkak.consumer import FETCH_BUFFER_SIZE_BYTES
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase, asyncDelay,
    random_string,
    )

from twisted.trial import unittest

log = logging.getLogger(__name__)
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')


class TestConsumerIntegration(KafkaIntegrationTestCase, unittest.TestCase):

    # Default partition
    partition = 0

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        DEBUGGING = True
        setDebugging(DEBUGGING)
        DelayedCall.debug = DEBUGGING

        cls.zk = ZookeeperFixture.instance()
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port)
        # cls.server3 = KafkaFixture.instance(2, cls.zk.host, cls.zk.port)
        # cls.server4 = KafkaFixture.instance(3, cls.zk.host, cls.zk.port)

        cls.server = cls.server1  # Bootstrapping server

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        cls.server1.close()
        cls.server2.close()
        # cls.server3.close()
        # cls.server4.close()
        cls.zk.close()

    @inlineCallbacks
    def send_messages(self, partition, messages):
        messages = [create_message(self.msg(str(msg))) for msg in messages]
        produce = ProduceRequest(self.topic, partition, messages=messages)
        resp, = yield self.client.send_produce_request([produce])

        self.assertEquals(resp.error, 0)

        returnValue([x.value for x in messages])

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEquals(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEquals(len(set(messages)), num_messages)

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_consumer(self):
        yield self.send_messages(self.partition, range(0, 100))

        # Create a consumer.
        consumer = self.consumer()

        # Check for messages on the processor
        self.assertFalse(consumer.processor._messages)

        # Start the consumer from the beginning
        d = consumer.start(OFFSET_EARLIEST)

        # Send some more messages
        yield self.send_messages(self.partition, range(100, 200))

        # Loop waiting for all the messages to show up
        while len(consumer.processor._messages) < 200:
            # Wait a bit for them to arrive
            yield asyncDelay()

        # Make sure we got all 200
        self.assertEqual(len(consumer.processor._messages), 200)

        # Send some more messages
        yield self.send_messages(self.partition, range(200, 250))
        # Loop waiting for the new message
        while len(consumer.processor._messages) < 250:
            # Wait a bit for them to arrive
            yield asyncDelay()

        # make sure we got them all
        self.assert_message_count(consumer.processor._messages, 250)

        # Clean up
        consumer.stop()
        self.successResultOf(d)

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_large_messages(self):
        # Produce 10 "normal" size messages
        small_messages = yield self.send_messages(
            0, [str(x) for x in range(10)])

        # Produce 10 messages that are large (bigger than default fetch size)
        large_messages = yield self.send_messages(
          0, [random_string(FETCH_BUFFER_SIZE_BYTES * 3) for x in range(10)])

        # Consumer should still get all of them
        consumer = self.consumer()

        # Start the consumer from the beginning
        d = consumer.start(OFFSET_EARLIEST)

        # Loop waiting for all the messages to show up
        while len(consumer.processor._messages) < 20:
            # Wait a bit for them to arrive
            yield asyncDelay()

        expected_messages = set(small_messages + large_messages)
        actual_messages = set([x.message.value for x in
                               consumer.processor._messages])
        self.assertEqual(expected_messages, actual_messages)

        # Clean up
        consumer.stop()
        self.successResultOf(d)

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_huge_messages(self):
        # Produce 10 "normal" size messages
        yield self.send_messages(0, [str(x) for x in range(10)])

        # Setup a max buffer size for the consumer, and put a message in
        # Kafka that's bigger than that
        MAX_FETCH_BUFFER_SIZE_BYTES = (256 * 1024) - 10
        huge_message, = yield self.send_messages(
            0, [random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)])

        # Create a consumer with the (smallish) max buffer size
        consumer = self.consumer(max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES)

        # This consumer fails to get the message, and errbacks the start
        # deferred
        d = consumer.start(OFFSET_EARLIEST)

        # Loop waiting for the errback to be called
        while not d.called:
            # Wait a bit for them to arrive
            yield asyncDelay()
        # Make sure the failure is as expected
        self.failureResultOf(d, ConsumerFetchSizeTooSmall)

        # Make sure the smaller, earlier messages were delivered
        self.assert_message_count(consumer.processor._messages, 10)

        # last offset seen
        last_offset = consumer.processor._messages[-1].offset

        # Stop the consumer: d already errbacked, but stop still must be called
        consumer.stop()

        # Create a consumer with no fetch size limit
        big_consumer = self.consumer()
        # Start just past the last message processed
        d = big_consumer.start(last_offset + 1)
        # Consume giant message successfully
        while not big_consumer.processor._messages:
            # Wait a bit for it to arrive
            yield asyncDelay()

        self.assertEquals(big_consumer.processor._messages[0].message.value,
                          huge_message)

        # Clean up
        big_consumer.stop()
        self.successResultOf(d)

    @kafka_versions("0.8.1", "0.8.1.1", "0.8.2.1")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_consumer_restart(self):
        sent_messages = yield self.send_messages(self.partition, range(0, 100))

        # Create & start a consumer.
        consumer = self.consumer()

        # Check for messages on the processor
        self.assertFalse(consumer.processor._messages)

        # Start the consumer from the beginning
        d = consumer.start(OFFSET_EARLIEST)

        # Send some more messages
        sent_messages += yield self.send_messages(
            self.partition, range(100, 200))

        # Loop waiting for all the messages to show up
        while len(consumer.processor._messages) < 200:
            # Wait a bit for them to arrive
            yield asyncDelay()

        # Make sure we got all 200
        self.assertEqual(len(consumer.processor._messages), 200)

        # Stop the consumer and record it's fetch offset
        offset = consumer.stop()
        self.successResultOf(d)

        # Send some more messages
        sent_messages += yield self.send_messages(
            self.partition, range(200, 250))
        # Restart the consumer at the returned offset
        d = consumer.start(offset)
        # Loop waiting for the new message
        while len(consumer.processor._messages) < 250:
            # Wait a bit for them to arrive
            yield asyncDelay()

        # make sure we got them all
        self.assert_message_count(consumer.processor._messages, 250)
        expected_messages = set(sent_messages)
        actual_messages = set([x.message.value for x in
                               consumer.processor._messages])
        self.assertEqual(expected_messages, actual_messages)

        # Clean up
        consumer.stop()
        self.successResultOf(d)

    def consumer(self, **kwargs):
        def make_processor():
            def default_message_proccessor(messages):
                """Default message processing function

                   Strictly for testing.
                   Just adds the messages to its own _messages attr
                """
                default_message_proccessor._messages.extend(messages)
                return None

            # Setup a list property '_messages' on the processor function
            default_message_proccessor._messages = []

            return default_message_proccessor

        group = kwargs.pop('group_id', self.id())
        topic = kwargs.pop('topic', self.topic)
        partition = kwargs.pop('partition', self.partition)
        processor = kwargs.pop('processor', make_processor())

        return Consumer(self.client, topic, partition, processor, group,
                        **kwargs)
