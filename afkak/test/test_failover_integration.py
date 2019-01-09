# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2018, 2019 Ciena Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
from unittest import TestCase

from nose.twistedtools import deferred, threaded_reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from .. import KafkaClient, Producer
from ..common import (
    PRODUCER_ACK_ALL_REPLICAS, FailedPayloadsError, FetchRequest,
    KafkaUnavailableError, NotLeaderForPartitionError, RequestTimedOutError,
    TopicAndPartition, UnknownTopicOrPartitionError, _check_error,
)
from .fixtures import KafkaHarness
from .testutil import (
    async_delay, ensure_topic_creation, kafka_versions, random_string,
)

log = logging.getLogger(__name__)


def assertNoDelayedCalls(reactor):
    """
    Check for outstanding delayed calls in the reactor.
    """
    dcs = reactor.getDelayedCalls()
    assert len(dcs) == 0, "Found {} outstanding delayed calls:\n{}".format(
        len(dcs),
        '\n'.join(str(dc) for dc in dcs),
    )


class TestFailover(TestCase):
    @kafka_versions("all")
    @deferred(timeout=600)
    @inlineCallbacks
    def test_switch_leader(self):
        """
        Produce messages while killing the coordinator broker.

        Note that in order to avoid loss of acknowledged writes the producer
        must request acks of -1 (`afkak.common.PRODUCER_ACK_ALL_REPLICAS`).
        """
        self.harness = KafkaHarness.start(
            replicas=3,
            partitions=7,
        )
        self.addCleanup(self.harness.halt)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        self.reactor, self.thread = threaded_reactor()

        # We want a short timeout on message sending for this test, since
        # we are expecting failures when we take down the brokers
        self.client = KafkaClient(
            self.harness.bootstrap_hosts,
            timeout=1000,
            clientId=__name__,
            reactor=self.reactor,
        )

        producer = Producer(
            self.client,
            req_acks=PRODUCER_ACK_ALL_REPLICAS,
            max_req_attempts=100,
        )
        topic = self.id()
        try:
            for index in range(1, 3):
                # cause the client to establish connections to all the brokers
                log.debug("Pass: %d. Sending 10 random messages", index)
                yield self._send_random_messages(producer, topic, 10)

                # kill leader for partition 0
                log.debug("Killing leader of partition 0")
                broker, kill_time = self._kill_leader(topic, 0)

                log.debug("Sending 1 more message: 'part 1'")
                yield producer.send_messages(topic, msgs=[b'part 1'])
                log.debug("Sending 1 more message: 'part 2'")
                yield producer.send_messages(topic, msgs=[b'part 2'])

                # send to new leader
                log.debug("Sending 10 more messages")
                yield self._send_random_messages(producer, topic, 10)

                # Make sure the ZK ephemeral time (~6 seconds) has elapsed
                wait_time = (kill_time + 6.5) - time.time()
                if wait_time > 0:
                    log.debug("Waiting: %4.2f for ZK timeout", wait_time)
                    yield async_delay(wait_time)
                # restart the kafka broker
                log.debug("Restarting leader broker %r", broker)
                broker.restart()

                # count number of messages
                log.debug("Getting message count")
                count = yield self._count_messages(topic)
                self.assertGreaterEqual(count, 22 * index)
        finally:
            log.debug("Stopping the producer")
            yield producer.stop()
            log.debug("Producer stopped")

        log.debug("Closing client")
        yield self.client.close()
        assertNoDelayedCalls(self.reactor)
        log.debug("Test complete.")

    @inlineCallbacks
    def _send_random_messages(self, producer, topic, n):
        for _j in range(n):
            resp = yield producer.send_messages(
                topic, msgs=[random_string(10).encode()])

            self.assertFalse(isinstance(resp, Exception))

            if resp:
                self.assertEqual(resp.error, 0)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[
            TopicAndPartition(topic, partition)]
        broker = self.harness.brokers[leader.node_id]
        broker.stop()
        return (broker, time.time())

    @inlineCallbacks
    def _count_messages(self, topic):
        messages = []
        client = KafkaClient(self.harness.bootstrap_hosts,
                             clientId="CountMessages", timeout=500,
                             reactor=self.reactor)

        try:
            yield ensure_topic_creation(client, topic, fully_replicated=False)

            # Need to retry this until we have a leader...
            while True:
                # Ask the client to load the latest metadata. This may avoid a
                # NotLeaderForPartitionError I was seeing upon re-start of the
                # broker.
                yield client.load_metadata_for_topics(topic)
                # if there is an error on the metadata for the topic, raise
                if _check_error(client.metadata_error_for_topic(topic), False) is None:
                    break
            # Ok, should be safe to get the partitions now...
            partitions = client.topic_partitions[topic]

            requests = [FetchRequest(topic, part, 0, 1024 * 1024)
                        for part in partitions]
            resps = []
            while not resps:
                try:
                    log.debug("_count_message: Fetching messages")
                    resps = yield client.send_fetch_request(requests, max_wait_time=400)
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

        log.debug("Got %d messages: %r", len(messages), messages)

        returnValue(len(messages))
