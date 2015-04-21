import os
import time
import logging

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.defer import inlineCallbacks, returnValue, setDebugging
from twisted.internet.base import DelayedCall

from afkak import (KafkaClient, Producer)
from afkak.common import (
    TopicAndPartition, check_error, FetchRequest,
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
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        DEBUGGING = True
        setDebugging(DEBUGGING)
        DelayedCall.debug = DEBUGGING

        zk_chroot = random_string(10)
        replicas = 2
        partitions = 2

        # mini zookeeper, 2 kafka brokers
        cls.zk = ZookeeperFixture.instance()
        kk_args = [cls.zk.host, cls.zk.port, zk_chroot, replicas, partitions]
        cls.brokers = [
            KafkaFixture.instance(i, *kk_args) for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in cls.brokers]
        # We want a short timeout on message sending for this test, since
        # we are expecting failures when we take down the brokers
        cls.client = KafkaClient(hosts, timeout=1000)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    @deferred(timeout=30)
    @inlineCallbacks
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        log.debug("Closing client:%r", cls.client)
        yield cls.client.close()
        # Check for outstanding delayedCalls.
        log.debug("Intermitent failure debugging: %s",
                  ' '.join([str(dc) for dc in
                            cls.reactor.getDelayedCalls()]))
        assert(len(cls.reactor.getDelayedCalls()) == 0)
        for broker in cls.brokers:
            broker.close()
        cls.zk.close()

    @deferred(timeout=60)
    @kafka_versions("all")
    @inlineCallbacks
    def test_switch_leader(self):
        log.debug("ZORG: Creating Producer")
        producer = Producer(self.client)
        topic = self.topic
        try:
            for i in range(1, 3):
                log.debug("ZORG: Loop %d", i)
                # cause the client to establish connections to all the brokers
                yield self._send_random_messages(producer, topic, 10)
                log.debug("ZORG: Sent Random Messages")
                # kill leader for partition 0
                broker = self._kill_leader(topic, 0)

                log.debug("Sending 1st set of messages, post broker close")
                yield producer.send_messages(topic, msgs=['part 1'])
                log.debug("Sending 2nd set of messages, post broker close")
                yield producer.send_messages(topic, msgs=['part 2'])
                log.debug("Sent all messages without err")

                # send to new leader
                log.debug("Sending next batch of messages, expecting success")
                yield self._send_random_messages(producer, topic, 10)
                log.debug("Sent next batch of messages")

                # restart the kafka broker
                broker.open()

                # count number of messages
                count = yield self._count_messages(topic)
                self.assertEqual(count, 22 * i)
        finally:
            yield producer.stop()
        # self.assertTrue(False)  # ZORG

    @inlineCallbacks
    def _send_random_messages(self, producer, topic, n):
        for j in range(n):
            resp = yield producer.send_messages(
                topic, msgs=[random_string(10)])
            if resp:
                log.debug("ZORG:TFI: %r", resp)
                self.assertEquals(resp.error, 0)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[
            TopicAndPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        time.sleep(0.001)  # give it some time
        return broker

    @inlineCallbacks
    def _count_messages(self, topic):
        messages = []
        hosts = '%s:%d' % (self.brokers[0].host, self.brokers[0].port)
        client = KafkaClient(hosts, clientId="CountMessages", timeout=20000)

        try:
            yield ensure_topic_creation(client, topic,
                                        reactor=self.reactor)

            # if there is an error on the metadata for the topic, raise
            check_error(client.metadata_error_for_topic(topic))
            # Ok, should be safe to get the partitions now...
            partitions = client.topic_partitions[topic]

            requests = [FetchRequest(topic, part, 0, 1024 * 1024)
                        for part in partitions]
            log.debug("_count_message: Waiting for messages")
            resps = yield client.send_fetch_request(
                requests)
        finally:
            yield client.close()
        for fetch_resp in resps:
            messages.extend(list(fetch_resp.messages))

        log.debug("Got %d messages:%r", len(messages), messages)

        returnValue(len(messages))
