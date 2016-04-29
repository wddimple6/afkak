# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import os
import logging
import time

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.defer import inlineCallbacks, returnValue, setDebugging
from twisted.internet.base import DelayedCall

from mock import patch

from afkak import (KafkaClient, Producer)
import afkak.client as kclient

from afkak.common import (
    TopicAndPartition, check_error, FetchRequest, NotLeaderForPartitionError,
    RequestTimedOutError, UnknownTopicOrPartitionError, FailedPayloadsError,
    KafkaUnavailableError,
    )

from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase,
    random_string, ensure_topic_creation, async_delay,
    )

log = logging.getLogger(__name__)


class TestFailover(KafkaIntegrationTestCase):
    create_client = False
    # Default partition
    partition = 0

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            return

        DEBUGGING = True
        setDebugging(DEBUGGING)
        DelayedCall.debug = DEBUGGING

        zk_chroot = random_string(10)
        replicas = 2
        partitions = 7

        # mini zookeeper, 2 kafka brokers
        cls.zk = ZookeeperFixture.instance()
        kk_args = [cls.zk.host, cls.zk.port, zk_chroot, replicas, partitions]
        cls.kafka_brokers = [
            KafkaFixture.instance(i, *kk_args) for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in cls.kafka_brokers]
        # We want a short timeout on message sending for this test, since
        # we are expecting failures when we take down the brokers
        cls.client = KafkaClient(hosts, timeout=1000, clientId=__name__)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    @deferred(timeout=180)
    @inlineCallbacks
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            return

        log.debug("Closing client:%r", cls.client)
        yield cls.client.close()
        log.debug("Client close complete.")
        # Check for outstanding delayedCalls.
        dcs = cls.reactor.getDelayedCalls()
        if dcs:  # pragma: no cover
            log.error("Outstanding Delayed Calls at tearDown: %s\n\n",
                      ' '.join([str(dc) for dc in dcs]))
        assert(len(dcs) == 0)
        log.debug("Stopping Kafka brokers.")
        for broker in cls.kafka_brokers:
            log.debug("Stopping broker: %r", broker)
            broker.close()
            log.debug("Broker: %r stopped.", broker)
        log.debug("Stopping Zookeeper.")
        cls.zk.close()
        log.debug("Zookeeper Stopped.")

    # 0.8.0 fails because it seems to remove the kafka.properties file? WTF?
    # 0.8.1 & 0.8.1.1: never seem to resync the killed/restarted broker
    @kafka_versions("0.8.2.1", "0.8.2.2", "0.9.0.1")
    @deferred(timeout=600)
    @inlineCallbacks
    def test_switch_leader(self):
        producer = Producer(self.client)
        topic = self.topic
        try:
            for index in range(1, 3):
                # cause the client to establish connections to all the brokers
                log.debug("Pass: %d. Sending 10 random messages", index)
                yield self._send_random_messages(producer, topic, 10)

                # Ensure that the follower is in sync
                log.debug("Ensuring topic/partition is replicated.")
                part_meta = self.client.partition_meta[TopicAndPartition(
                    self.topic, 0)]
                # Ensure the all the replicas are in-sync before proceeding
                while len(part_meta.isr) != 2:  # pragma: no cover
                    log.debug("Waiting for Kafka replica to become synced")
                    if len(part_meta.replicas) != 2:
                        log.error("Kafka replica 'disappeared'!"
                                  "Partitition Meta: %r", part_meta)
                    yield async_delay(1.0)
                    yield self.client.load_metadata_for_topics(self.topic)
                    part_meta = self.client.partition_meta[TopicAndPartition(
                        self.topic, 0)]

                # kill leader for partition 0
                log.debug("Killing leader of partition 0")
                broker, kill_time = self._kill_leader(topic, 0)

                log.debug("Sending 1 more message: 'part 1'")
                yield producer.send_messages(topic, msgs=['part 1'])
                log.debug("Sending 1 more message: 'part 2'")
                yield producer.send_messages(topic, msgs=['part 2'])

                # send to new leader
                log.debug("Sending 10 more messages")
                yield self._send_random_messages(producer, topic, 10)

                # Make sure the ZK ephemeral time (~6 seconds) has elapsed
                wait_time = (kill_time + 6.5) - time.time()
                if wait_time > 0:
                    log.debug("Waiting: %4.2f for ZK timeout", wait_time)
                    yield async_delay(wait_time)
                # restart the kafka broker
                log.debug("Restarting the broker")
                broker.restart()

                # count number of messages
                log.debug("Getting message count")
                count = yield self._count_messages(topic)
                self.assertEqual(count, 22 * index)
        finally:
            log.debug("Stopping the producer")
            yield producer.stop()
            log.debug("Producer stopped")

        log.debug("Test complete.")

    @inlineCallbacks
    def _send_random_messages(self, producer, topic, n):
        for j in range(n):
            resp = yield producer.send_messages(
                topic, msgs=[random_string(10)])

            self.assertFalse(isinstance(resp, Exception))

            if resp:
                self.assertEqual(resp.error, 0)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[
            TopicAndPartition(topic, partition)]
        broker = self.kafka_brokers[leader.node_id]
        broker.stop()
        return (broker, time.time())

    @inlineCallbacks
    def _count_messages(self, topic):
        messages = []
        hosts = '%s:%d,%s:%d' % (self.kafka_brokers[0].host,
                                 self.kafka_brokers[0].port,
                                 self.kafka_brokers[1].host,
                                 self.kafka_brokers[1].port)
        client = KafkaClient(hosts, clientId="CountMessages", timeout=500)

        try:
            yield ensure_topic_creation(client, topic,
                                        reactor=self.reactor)

            # Need to retry this until we have a leader...
            while True:
                # Ask the client to load the latest metadata. This may avoid a
                # NotLeaderForPartitionError I was seeing upon re-start of the
                # broker.
                yield client.load_metadata_for_topics(topic)
                # if there is an error on the metadata for the topic, raise
                if check_error(
                        client.metadata_error_for_topic(topic), False) is None:
                    break
            # Ok, should be safe to get the partitions now...
            partitions = client.topic_partitions[topic]

            requests = [FetchRequest(topic, part, 0, 1024 * 1024)
                        for part in partitions]
            resps = []
            while not resps:
                try:
                    log.debug("_count_message: Fetching messages")
                    # Prevent log.error() call from causing test failure
                    with patch.object(kclient, 'log'):
                        resps = yield client.send_fetch_request(
                            requests, max_wait_time=400)
                except (NotLeaderForPartitionError,
                        UnknownTopicOrPartitionError,
                        KafkaUnavailableError):  # pragma: no cover
                    log.debug("_count_message: Metadata err, retrying...")
                    yield client.load_metadata_for_topics(topic)
                except FailedPayloadsError as e:  # pragma: no cover
                    if not e.args[1][0][1].check(RequestTimedOutError):
                        raise
                    log.debug("_count_message: Timed out err, retrying...")
        finally:
            yield client.close()
        for fetch_resp in resps:
            messages.extend(list(fetch_resp.messages))

        log.debug("Got %d messages:%r", len(messages), messages)

        returnValue(len(messages))
