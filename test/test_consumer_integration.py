import os

import logging

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.defer import inlineCallbacks, returnValue, setDebugging
from twisted.internet.base import DelayedCall

from afkak import (Consumer, create_message, )
from afkak.common import (
    ProduceRequest,  # ConsumerFetchSizeTooSmall,
    OFFSET_EARLIEST,
    )
# from afkak.consumer import FETCH_BUFFER_SIZE_BYTES
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase, asyncDelay,
    #    random_string,
    )

from twisted.trial.unittest import TestCase as TrialTestCase

log = logging.getLogger('afkak_test.test_consumer_integration')
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')


class TestConsumerIntegration(KafkaIntegrationTestCase, TrialTestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
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
        if not os.environ.get('KAFKA_VERSION'):
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
        yield self.send_messages(0, range(0, 100))
        yield self.send_messages(0, range(100, 200))

        # Create & start a consumer.
        consumer = self.consumer()

        # Check for messages on the processor
        log.debug("ZORG1:%r", consumer.processor._messages)
        # Start the consumer from the beginning
        d = consumer.start(OFFSET_EARLIEST)

        # Wait a bit for them to arrive
        yield asyncDelay(2)
        # Check again for messages on the processor
        log.debug("ZORG2:%r", consumer.processor._messages)

        # Send another message
        yield self.send_messages(0, [200])
        # Wait a bit for them to arrive
        yield asyncDelay(2)
        # Check again for messages on the processor
        log.debug("ZORG3:%r", consumer.processor._messages)

        # make sure we got them all
        self.assert_message_count(consumer.processor._messages, 201)
        # Clean up
        consumer.stop()
        self.successResultOf(d)

    # @kafka_versions("all")
    # @deferred(timeout=15)
    # @inlineCallbacks
    # def test_large_messages(self):
    #     # Produce 10 "normal" size messages
    #     small_messages = yield self.send_messages(
    #         0, [str(x) for x in range(10)])

    #     # Produce 10 messages that are large (bigger than default fetch size)
    #     large_messages = yield self.send_messages(
    #       0, [random_string(FETCH_BUFFER_SIZE_BYTES * 3) for x in range(10)])

    #     # Consumer should still get all of them
    #     consumer = self.consumer()

    #     expected_messages = set(small_messages + large_messages)
    #     msgs = []
    #     ds = consumer.get_messages(count=20)
    #     for d in ds:
    #         part, oAm = yield d
    #         msgs.append(oAm.message.value)
    #     actual_messages = set(msgs)
    #     self.assertEqual(expected_messages, actual_messages)

    #     yield self.cleanupConsumer(consumer)

    # @kafka_versions("all")
    # @deferred(timeout=15)
    # @inlineCallbacks
    # def test_huge_messages(self):
    #     # Setup a max buffer size for the consumer, and put a message in
    #     # Kafka that's bigger than that
    #     MAX_FETCH_BUFFER_SIZE_BYTES = (256 * 1024) - 10
    #     huge_message, = yield self.send_messages(
    #         0, [random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)])

    #     # Create a consumer with the default buffer size
    #     consumer = self.consumer(max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES)

    #     # This consumer failes to get the message
    #     with self.assertRaises(ConsumerFetchSizeTooSmall):
    #         yield consumer.get_message()

    #     yield self.cleanupConsumer(consumer)

    #     # Create a consumer with no fetch size limit
    #     big_consumer = self.consumer(
    #         max_buffer_size=None, partition=[0])
    #     # Seek to the last message
    #     yield big_consumer.seek(-1, 2)
    #     # Consume giant message successfully
    #     part, message = yield big_consumer.get_message()
    #     self.assertIsNotNone(message)
    #     self.assertEquals(message.message.value, huge_message)

    #     yield self.cleanupConsumer(big_consumer)

    # @kafka_versions("0.8.1", "0.8.1.1")
    # @deferred(timeout=15)
    # @inlineCallbacks
    # def test_offset_behavior_resuming_behavior(self):
    #     yield self.send_messages(0, range(0, 100))
    #     yield self.send_messages(1, range(100, 200))

    #     # Start a consumer
    #     consumer1 = self.consumer()

    #     # Grab the first 195 messages' deferreds
    #     output_ds1 = consumer1.get_messages(count=195)
    #     output_msgs1 = []
    #     for d in output_ds1:
    #         msg = yield d
    #         output_msgs1.append(msg)
    #     self.assert_message_count(output_msgs1, 195)

    #     # The total offset across both partition should be at 180
    #     consumer2 = self.consumer()

    #     # 181-200
    #     output_ds2 = consumer2.get_messages(count=20)
    #     output_msgs2 = []
    #     for d in output_ds2:
    #         msg = yield d
    #         output_msgs2.append(msg)

    #     self.assert_message_count(output_msgs2, 20)

    #     yield self.cleanupConsumer(consumer1)
    #     yield self.cleanupConsumer(consumer2)

    def consumer(self, **kwargs):
        def make_processor(consumer):
            def default_message_proccessor(messages):
                """Default message processing function

                   Strictly for testing.
                   Just adds the messages to its own _messages attr
                """
                default_message_proccessor._messages.extend(messages)
                return None
            default_message_proccessor._messages = []

            return default_message_proccessor

        group = kwargs.pop('group_id', self.id())
        partition = kwargs.pop('partition', self.partition())
        topic = kwargs.pop('topic', self.topic)
        processor = kwargs.pop('processor', make_processor())

        return Consumer(self.client, topic, partition, processor, group,
                        **kwargs)
