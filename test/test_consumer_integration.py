import os

import logging
log = logging.getLogger('afkak_test.test_consumer_integration')
logging.basicConfig(level=1, format='%(asctime)s %(message)s')

import nose.twistedtools
from twisted.internet.defer import inlineCallbacks, returnValue

from afkak import (Consumer, create_message, )
from afkak.common import (
    ProduceRequest, ConsumerFetchSizeTooSmall
    )
from afkak.consumer import FETCH_BUFFER_SIZE_BYTES
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase,
    random_string,
    )

from twisted.trial.unittest import TestCase as TrialTestCase


class TestConsumerIntegration(KafkaIntegrationTestCase, TrialTestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port)

        cls.server = cls.server1  # Bootstrapping server

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        nose.twistedtools.threaded_reactor()

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        cls.server1.close()
        cls.server2.close()
        cls.zk.close()

        # Shutdown the twisted reactor
        nose.twistedtools.stop_reactor()

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
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_simple_consumer(self):
        yield self.send_messages(0, range(0, 100))
        yield self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer = self.consumer()

        # get the first 200 messages from the consumer and ensure they
        # yield a real result
        msgCount = 0
        for d in consumer:
            yield d
            msgCount += 1
            if msgCount >= 200:
                break

        # Now try to get one more message from the consumer and make sure
        # we don't already have a result for it.
        respD = consumer.get_message()
        self.assertNoResult(respD)
        # send another message
        yield self.send_messages(0, [200])
        # and make sure we can get the result
        yield respD

        yield consumer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=500)
    @inlineCallbacks
    def test_simple_consumer_seek(self):
        print "ZORG:test_simple_consumer_seek_1:"
        yield self.send_messages(0, range(0, 100))
        print "ZORG:test_simple_consumer_seek_2:"
        yield self.send_messages(1, range(100, 200))

        print "ZORG:test_simple_consumer_seek_3:"
        consumer = self.consumer()
        print "ZORG:test_simple_consumer_seek_4:"

        # Rewind 10 messages from the end
        yield consumer.seek(-10, 2)
        print "ZORG:test_simple_consumer_seek_5:"
        msgs = consumer.get_messages(count=10)
        print "ZORG:test_simple_consumer_seek_5.5:", type(msgs), msgs
        self.assert_message_count(msgs, 10)
        # make sure they all yield
        count = 1
        for msg in msgs:
            print "ZORG:test_simple_consumer_seek_6:", count
            yield msg
            print "ZORG:test_simple_consumer_seek_7:", count
            count += 1

        print "ZORG:test_simple_consumer_seek_PART2_1:"
        # Rewind 13 messages from the end
        yield consumer.seek(-13, 2)
        print "ZORG:test_simple_consumer_seek_PART2_5:"
        msgs = yield consumer.get_messages(count=13)
        print "ZORG:test_simple_consumer_seek_PART2_5.5:", type(msgs), msgs
        self.assert_message_count(msgs, 13)
        print "ZORG:test_simple_consumer_seek_PART2_5:"
        # make sure they all yield
        count = 1
        for msg in msgs:
            print "ZORG:test_simple_consumer_seek_PART2_6:", count
            yield msg
            print "ZORG:test_simple_consumer_seek_PART2_7:", count
            count += 1

        print "ZORG:test_simple_consumer_seek_DONE:"

        yield consumer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_simple_consumer_pending(self):
        # Produce 10 messages to partitions 0 and 1
        yield self.send_messages(0, range(0, 10))
        yield self.send_messages(1, range(10, 20))

        consumer = self.consumer()

        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

        yield consumer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_large_messages(self):
        print "ZORG:test_large_messages_1:"
        # Produce 10 "normal" size messages
        small_messages = yield self.send_messages(
            0, [str(x) for x in range(10)])
        print "ZORG:test_large_messages_2:"

        # Produce 10 messages that are large (bigger than default fetch size)
        large_messages = yield self.send_messages(
            0, [random_string(FETCH_BUFFER_SIZE_BYTES * 3) for x in range(10)])

        print "ZORG:test_large_messages_3:"
        # Consumer should still get all of them
        consumer = self.consumer()

        expected_messages = set(small_messages + large_messages)
        print "ZORG:test_large_messages_4:", expected_messages
        msgs = []
        for d in consumer:
            oAm = yield d
            msgs.append(oAm.message.value)
        actual_messages = set(msgs)
        print "ZORG:test_large_messages_5:", actual_messages
        self.assertEqual(expected_messages, actual_messages)
        self.fail()

        yield consumer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=100)
    @inlineCallbacks
    def test_huge_messages(self):
        # 1000000 is default Kafka message.max.bytes
        MAX_FETCH_BUFFER_SIZE_BYTES = (256 * 1024) - 10
        huge_message, = yield self.send_messages(
            0, [random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)])

        # Create a consumer with the default buffer size
        consumer = self.consumer(max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES)

        # Make sure it's completed it's initialization...
        yield consumer.waitForReady()

        # This consumer failes to get the message
        with self.assertRaises(ConsumerFetchSizeTooSmall):
            yield consumer.get_message()

        yield consumer.stop()

        # Create a consumer with no fetch size limit
        big_consumer = self.consumer(
            max_buffer_size=None, partitions=[0])

        # Seek to the last message
        yield big_consumer.seek(-1, 2)

        # Consume giant message successfully
        message = yield big_consumer.get_message()
        self.assertIsNotNone(message)
        self.assertEquals(message.message.value, huge_message)

        big_consumer.stop()

    @kafka_versions("0.8.1")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_offset_behavior__resuming_behavior(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer1 = self.consumer(
            auto_commit_every_t=None, auto_commit_every_n=20)

        # Make sure it's completed it's initialization...
        yield consumer1.waitForReady()

        # Grab the first 195 messages' deferreds
        output_ds1 = consumer1.get_messages(count=195)
        output_msgs1 = []
        for d in output_ds1:
            msg = yield d
            output_msgs1.append(msg)
        self.assert_message_count(output_msgs1, 195)

        # The total offset across both partitions should be at 180
        consumer2 = self.consumer(
            auto_commit_every_t=None, auto_commit_every_n=20)

        # Make sure it's completed it's initialization...
        yield consumer2.waitForReady()

        # 181-200
        output_ds2 = consumer2.get_messages(count=20)
        output_msgs2 = []
        for d in output_ds2:
            msg = yield d
            output_msgs2.append(msg)

        self.assert_message_count(output_msgs2, 20)

        yield consumer1.stop()
        yield consumer2.stop()

    def consumer(self, **kwargs):
        if os.environ['KAFKA_VERSION'] == "0.8.0":
            # Kafka 0.8.0 simply doesn't support offset requests,
            # so hard code it being off
            kwargs['auto_commit'] = False
        else:
            kwargs.setdefault('auto_commit', True)

        consumer_class = kwargs.pop('consumer', Consumer)
        group = kwargs.pop('group', self.id())
        topic = kwargs.pop('topic', self.topic)

        return consumer_class(self.client, group, topic, **kwargs)
