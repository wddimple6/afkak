import os
import time
import logging

log = logging.getLogger("test_failover_integration")

import nose.twistedtools
from twisted.internet.defer import inlineCallbacks, returnValue

from afkak import (KafkaClient, Producer, Consumer)
from afkak.common import (TopicAndPartition, FailedPayloadsError)
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase, random_string,
    )


class TestFailover(KafkaIntegrationTestCase):
    create_client = False

    @classmethod
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        zk_chroot = random_string(10)
        replicas = 2
        partitions = 2

        # mini zookeeper, 2 kafka brokers
        cls.zk = ZookeeperFixture.instance()
        kk_args = [cls.zk.host, cls.zk.port, zk_chroot, replicas, partitions]
        cls.brokers = [
            KafkaFixture.instance(i, *kk_args) for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in cls.brokers]
        cls.client = KafkaClient(hosts, timeout=1)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        nose.twistedtools.threaded_reactor()

    @classmethod
    @nose.twistedtools.deferred(timeout=60)
    @inlineCallbacks
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return
        print "ZORG_tDC_0:", cls.client

        dl = cls.client.close()
        print "ZORG_tDC_0.1:", dl
        yield dl
        print "ZORG_tDC_1:", cls.client, cls.brokers
        for broker in cls.brokers:
            print "ZORG_tDC_2:", broker
            broker.close()
        print "ZORG_tDC_3:", cls.zk
        cls.zk.close()
        print "ZORG_tDC_4:", cls.zk

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=30)
    @inlineCallbacks
    def test_switch_leader(self):
        key, topic, partition = random_string(5), self.topic, 0
        producer = Producer(self.client)

        for i in range(1, 4):
            # cause the client to establish connections to all the brokers
            yield self._send_random_messages(producer, self.topic, 10)
            # kill leader for partition 0
            broker = self._kill_leader(topic, partition)

            # expect failure, reload meta data
            with self.assertRaises(FailedPayloadsError):
                yield producer.send_messages(self.topic, msgs=['part 1'])
                yield producer.send_messages(self.topic, msgs=['part 2'])

            # send to new leader
            yield self._send_random_messages(producer, self.topic, 10)

            broker.open()
            time.sleep(2.0)

            # count number of messages
            count = yield self._count_messages(
                'test_switch_leader group %s' % i, topic)
            self.assertIn(count, range(20 * i, 22 * i + 1))

        yield producer.stop()

    @inlineCallbacks
    def _send_random_messages(self, producer, topic, n):
        for j in range(n):
            resp = yield producer.send_messages(topic,
                                                msgs=[str(j) + '_' +
                                                      random_string(10)])
            if resp:
                self.assertEquals(resp[0].error, 0)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[
            TopicAndPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        time.sleep(0.5)  # give it some time
        return broker

    @inlineCallbacks
    def _count_messages(self, group, topic):
        hosts = '%s:%d' % (self.brokers[0].host, self.brokers[0].port)
        client = KafkaClient(hosts, clientId="CountMessages", timeout=5)
        # Try to get _all_ the messages in the first fetch. Wait for 1.0 secs
        # for up to 128Kbytes of messages
        consumer = Consumer(
            client, group, topic, auto_commit=False,
            fetch_size_bytes=128*1024, fetch_max_wait_time=1000)
        yield consumer.fetch()  # prefetch messages for iteration
        consumer.only_prefetched = True
        all_messages = []
        for d in consumer:
            message = yield d
            all_messages.append(message)
        yield consumer.stop()
        yield client.close()
        returnValue(len(all_messages))
