# -*- coding: utf-8 -*-
# Copyright (C) 2017 Ciena, Inc.

from __future__ import print_function

import os
import sys
import logging
from random import randint
import time

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.defer import inlineCallbacks, returnValue, setDebugging
from twisted.internet.base import DelayedCall
from twisted.trial import unittest

from afkak import (
    Consumer, create_message,
    create_message_set, Producer,
    RoundRobinPartitioner, HashedPartitioner,
    CODEC_GZIP, CODEC_SNAPPY,
    )
from afkak.common import (
    ProduceRequest, ConsumerFetchSizeTooSmall,
    OFFSET_EARLIEST, OFFSET_COMMITTED,
    PRODUCER_ACK_NOT_REQUIRED,
    PRODUCER_ACK_ALL_REPLICAS,
    PRODUCER_ACK_LOCAL_WRITE,
    )
from afkak.codec import has_snappy
from afkak.consumer import FETCH_BUFFER_SIZE_BYTES
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase, async_delay,
    random_string, make_send_requests, stat
    )

log = logging.getLogger(__name__)

DEBUGGING = True
setDebugging(DEBUGGING)
DelayedCall.debug = DEBUGGING
PARTITION_COUNT = 11
MESSAGE_BLOCK_SIZE = 100
PRODUCE_TIME = 20

class TestPerformanceIntegration(KafkaIntegrationTestCase, unittest.TestCase):

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

        # Single zookeeper, 3 kafka brokers
        zk_chroot = random_string(10)
        replicas = 3
        partitions = PARTITION_COUNT

        cls.zk = ZookeeperFixture.instance()
        kk_args = [cls.zk.host, cls.zk.port, zk_chroot, replicas, partitions]
        cls.kafka_brokers = [
            KafkaFixture.instance(i, *kk_args) for i in range(replicas)]
        # server is used by our superclass when creating the client...
        cls.server = cls.kafka_brokers[0]

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    def tearDownClass(cls):
        log.info("Tearing down class: %r", cls)
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        for broker in cls.kafka_brokers:
            log.info("Closing broker: %r", broker)
            broker.close()
        log.info("Closing ZK: %r", cls.zk)
        cls.zk.close()

    @kafka_versions("all")
    @deferred(timeout=(PRODUCE_TIME * 3 + 5))
    @inlineCallbacks
    def test_throughput(self):
        yield async_delay(3)  # 0.8.1.1 fails otherwise

        # Flag to shutdown
        keep_running = True
        # Count of messages sent
        sent_msgs_count = [0]
        total_messages_size = [0]
        # setup MESSAGE_BLOCK_SIZEx1024-ish byte messages to send over and over
        constant_messages = [
            self.msg(s) for s in [
                random_string(1024) for x in range(MESSAGE_BLOCK_SIZE)]]
        large_messages = [
            self.msg(s) for s in [random_string(FETCH_BUFFER_SIZE_BYTES * 3)
                for x in range(MESSAGE_BLOCK_SIZE)]]

        constant_messages_size = len(constant_messages[0]) * MESSAGE_BLOCK_SIZE
        large_messages_size = len(large_messages[0]) * MESSAGE_BLOCK_SIZE

        # Create a producer and send some messages
        producer = Producer(self.client)

        # Create consumers (1/partition)
        consumers = [self.consumer(partition=p, fetch_max_wait_time=50)
                         for p in range(PARTITION_COUNT)]

        def log_error(failure):
            log.exception("Failure sending messages: %r", failure)  # pragma: no cover

        def sent_msgs(resps):
            log.info("Messages Sent: %r", resps)
            sent_msgs_count[0] += MESSAGE_BLOCK_SIZE
            return resps

        def send_msgs():
            # randomly, 1/20 of the time, send large messages
            if randint(0, 19):
            ## if True:
                messages = constant_messages
                large=''
                total_messages_size[0] += constant_messages_size
            else:
                messages = large_messages
                large=' large'
                total_messages_size[0] += large_messages_size

            log.info("Sending: %d%s messages", len(messages), large)
            d = producer.send_messages(self.topic,
                msgs=messages)
            # As soon as we get a response from the broker, count them
            # and if we're still supposed to, send more
            d.addCallback(sent_msgs)
            if keep_running:
                d.addCallback(lambda _: self.reactor.callLater(0, send_msgs))
                ## d.addCallback(lambda _: send_msgs())
            d.addErrback(log_error)

        # Start sending messages, MESSAGE_BLOCK_SIZE at a time, 1K or 384K each
        send_msgs()

        # Start the consumers from the beginning
        fetch_start = time.time()
        start_ds = [consumer.start(OFFSET_EARLIEST)
                        for consumer in consumers]

        # Let them all run for awhile...
        log.info("Waiting %d seconds...", PRODUCE_TIME)
        yield async_delay(PRODUCE_TIME)
        # Tell the producer to stop
        keep_running = False
        # Wait up to PRODUCE_TIME for the consumers to catch up
        log.info("Waiting up to %d seconds for "
                     "consumers to finish consuming...", PRODUCE_TIME)
        deadline = time.time() + PRODUCE_TIME * 2
        while time.time() < deadline:
            consumed = sum([len(consumer.processor._messages)
                                              for consumer in consumers])
            log.debug("Consumed %d messages.", consumed)
            if sent_msgs_count[0] == consumed:
                break
            yield async_delay(1)
        fetch_time = time.time() - fetch_start
        consumed_bytes = sum([c.processor._messages_bytes[0]
                                  for c in consumers])

        result_msg = (
            "Sent: {} messages ({:,} total bytes) in ~{} seconds"
            " ({}/sec), Consumed: {} in {:.2f} seconds."
            .format(sent_msgs_count[0], total_messages_size[0],
                        PRODUCE_TIME, sent_msgs_count[0]/PRODUCE_TIME,
                        consumed, fetch_time))
        # Log the result, and print to stderr to get around nose capture
        log.info(result_msg)
        print("\n\t Performance Data: " + result_msg, file=sys.stderr)
        # And print data as stats
        stat('Production_Time', PRODUCE_TIME)
        stat('Consumption_Time', fetch_time)
        stat('Messages_Produced', sent_msgs_count[0])
        stat('Messages_Consumed', consumed)
        stat('Messages_Bytes_Produced', total_messages_size[0])
        stat('Messages_Bytes_Consumed', consumed_bytes)
        stat('Messages_Produced_Per_Second', sent_msgs_count[0]/PRODUCE_TIME)
        stat('Messages_Consumed_Per_Second', consumed/fetch_time)
        stat('Message_Bytes_Produced_Per_Second',
                 total_messages_size[0]/PRODUCE_TIME)
        stat('Message_Bytes_Consumed_Per_Second', consumed_bytes/fetch_time)

        # Clean up
        log.debug('Stopping producer: %r', producer)
        yield producer.stop()
        log.debug('Stopping consumers: %r', consumers)
        for consumer in consumers:
            consumer.stop()
        [self.successResultOf(start_d) for start_d in start_ds]
        # make sure we got all the messages we sent
        self.assertEqual(sent_msgs_count[0],
                         sum([len(consumer.processor._messages)
                                  for consumer in consumers]))
        # self.fail("Failing so Nose will emit logging.")

    def consumer(self, **kwargs):
        def make_processor():
            def default_message_proccessor(consumer_instance, messages):
                """Default message processing function

                   Strictly for testing.
                   Just adds the messages to its own _messages attr
                """
                log.debug(
                    'Processor for Consumer: %r Got block of %d messages.\n'
                    'Sizes: %r',
                    consumer_instance, len(messages),
                    [len(m.message.value) for m in messages])
                default_message_proccessor._messages.extend(messages)
                default_message_proccessor._messages_bytes[0] += sum(
                    [len(m.message.value) for m in messages])
                return None

            # Setup a list property '_messages' on the processor function
            default_message_proccessor._messages = []
            default_message_proccessor._messages_bytes = [0]

            return default_message_proccessor

        topic = kwargs.pop('topic', self.topic)
        partition = kwargs.pop('partition', self.partition)
        processor = kwargs.pop('processor', make_processor())
        group = kwargs.pop('consumer_group', None)

        return Consumer(self.client, topic, partition, processor, group,
                        **kwargs)
