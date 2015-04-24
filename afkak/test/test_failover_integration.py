import os
import logging

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
    random_string, ensure_topic_creation,
    )

log = logging.getLogger(__name__)
logging.basicConfig(level=1, format='%(asctime)s %(levelname)s: %(message)s')


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
        cls.brokers = [
            KafkaFixture.instance(i, *kk_args) for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in cls.brokers]
        # We want a short timeout on message sending for this test, since
        # we are expecting failures when we take down the brokers
        cls.client = KafkaClient(hosts, timeout=1000, clientId=__name__)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    @deferred(timeout=30)
    @inlineCallbacks
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            return

        log.debug("Closing client:%r", cls.client)
        yield cls.client.close()
        # Check for outstanding delayedCalls.
        dcs = cls.reactor.getDelayedCalls()
        if dcs:  # pragma: no cover
            log.debug("Intermitent failure debugging: %s\n\n",
                      ' '.join([str(dc) for dc in dcs]))
        assert(len(dcs) == 0)
        for broker in cls.brokers:
            broker.close()
        cls.zk.close()

    @deferred(timeout=60)
    @kafka_versions("all")
    @inlineCallbacks
    def test_switch_leader(self):
        producer = Producer(self.client)
        topic = self.topic
        try:
            for i in range(1, 3):
                # cause the client to establish connections to all the brokers
                yield self._send_random_messages(producer, topic, 10)
                # kill leader for partition 0
                broker = self._kill_leader(topic, 0)

                yield producer.send_messages(topic, msgs=['part 1'])
                yield producer.send_messages(topic, msgs=['part 2'])

                # send to new leader
                yield self._send_random_messages(producer, topic, 10)

                # restart the kafka broker
                broker.open()

                # count number of messages
                count = yield self._count_messages(topic)
                self.assertEqual(count, 22 * i)
        finally:
            yield producer.stop()

    @inlineCallbacks
    def _send_random_messages(self, producer, topic, n):
        for j in range(n):
            resp = yield producer.send_messages(
                topic, msgs=[random_string(10)])

            self.assertFalse(isinstance(resp, Exception))

            if resp:
                self.assertEquals(resp.error, 0)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[
            TopicAndPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        return broker

    @inlineCallbacks
    def _count_messages(self, topic):
        messages = []
        hosts = '%s:%d,%s:%d' % (self.brokers[0].host, self.brokers[0].port,
                                 self.brokers[1].host, self.brokers[1].port)
        client = KafkaClient(hosts, clientId="CountMessages", timeout=500)

        try:
            yield ensure_topic_creation(client, topic,
                                        reactor=self.reactor)

            # Ask the client to load the latest metadata. This may avoid a
            # NotLeaderForPartitionError I was seeing upon re-start of the
            # broker.
            yield client.load_metadata_for_topics(topic)
            # if there is an error on the metadata for the topic, raise
            check_error(client.metadata_error_for_topic(topic))
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
