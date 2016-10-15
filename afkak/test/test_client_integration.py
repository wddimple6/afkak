# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import os
import logging
import time
import random

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.defer import (inlineCallbacks, setDebugging, )
from twisted.internet.base import DelayedCall

from afkak import (KafkaClient,)
from afkak.common import (
    FetchRequest, OffsetFetchRequest, OffsetCommitRequest,
    ProduceRequest, OffsetRequest, ConsumerCoordinatorNotAvailableError,
    NotCoordinatorForConsumerError,
    )
from afkak.kafkacodec import (create_message)
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase, random_string,
    )


DEBUGGING = True
setDebugging(DEBUGGING)
DelayedCall.debug = DEBUGGING

log = logging.getLogger(__name__)
#  logging.basicConfig(level=logging.DEBUG)


class TestAfkakClientIntegration(KafkaIntegrationTestCase):
    create_client = False

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            return

        # Single zookeeper, 3 kafka brokers
        zk_chroot = random_string(10)
        replicas = 3
        partitions = 2

        cls.zk = ZookeeperFixture.instance()
        kk_args = [cls.zk.host, cls.zk.port, zk_chroot, replicas, partitions]
        cls.kafka_brokers = [
            KafkaFixture.instance(i, *kk_args) for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in cls.kafka_brokers]
        cls.client = KafkaClient(hosts, timeout=1500, clientId=__name__)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    @deferred(timeout=450)
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
        for broker in cls.kafka_brokers:
            broker.close()
        cls.zk.close()

    @kafka_versions("all")
    @deferred(timeout=5)
    @inlineCallbacks
    def test_consume_none(self):
        fetch = FetchRequest(self.topic, 0, 0, 1024)

        fetch_resp, = yield self.client.send_fetch_request(
            [fetch], max_wait_time=1000)
        self.assertEqual(fetch_resp.error, 0)
        self.assertEqual(fetch_resp.topic, self.topic)
        self.assertEqual(fetch_resp.partition, 0)

        messages = list(fetch_resp.messages)
        self.assertEqual(len(messages), 0)

    @kafka_versions("all")
    @deferred(timeout=5)
    @inlineCallbacks
    def test_produce_request(self):
        produce = ProduceRequest(
            self.topic, 0,
            [create_message(self.topic + " message %d" % i)
             for i in range(5)])

        produce_resp, = yield self.client.send_produce_request([produce])
        self.assertEqual(produce_resp.error, 0)
        self.assertEqual(produce_resp.topic, self.topic)
        self.assertEqual(produce_resp.partition, 0)
        self.assertEqual(produce_resp.offset, 0)

    ####################
    #   Offset Tests   #
    ####################

    @kafka_versions("0.8.1", "0.8.1.1", "0.8.2.1", "0.8.2.2", "0.9.0.1")
    @deferred(timeout=5)
    @inlineCallbacks
    def test_send_offset_request(self):
        req = OffsetRequest(self.topic, 0, -1, 100)
        (resp,) = yield self.client.send_offset_request([req])
        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.topic, self.topic)
        self.assertEqual(resp.partition, 0)
        self.assertEqual(resp.offsets, (0,))

    @kafka_versions("0.8.2.1", "0.8.2.2", "0.9.0.1")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_commit_fetch_offsets(self):
        """test_commit_fetch_offsets

        RANT: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
        implies that the metadata supplied with the commit will be returned by
        the fetch, but under 0.8.2.1 with a API_version of 0, it's not. Switch
        to using the V1 API and it works.
        """  # noqa
        resp = {}
        c_group = "CG_1"
        metadata = "My_Metadata_{}".format(random_string(10))
        offset = random.randint(0, 1024)
        log.debug("Commiting offset: %d metadata: %s for topic: %s part: 0",
                  offset, metadata, self.topic)
        req = OffsetCommitRequest(self.topic, 0, offset, -1, metadata)
        # We have to retry, since the client doesn't, and Kafka will
        # create the topic on the fly, but the first request will fail
        for attempt in range(20):
            log.debug("test_commit_fetch_offsets: Commit Attempt: %d", attempt)
            try:
                (resp,) = yield self.client.send_offset_commit_request(
                    c_group, [req])
            except ConsumerCoordinatorNotAvailableError:
                log.info(
                    "No Coordinator for Consumer Group: %s Attempt: %d of 20",
                    c_group, attempt)
                time.sleep(0.5)
                continue
            except NotCoordinatorForConsumerError:  # pragma: no cover
                # Kafka seems to have a timing issue: If we ask broker 'A' who
                # the ConsumerCoordinator is for a auto-created, not extant
                # topic, the assigned broker may not realize it's been so
                # designated by the time we find out and make our request.
                log.info(
                    "Coordinator is not coordinator!!: %s Attempt: %d of 20",
                    c_group, attempt)
                time.sleep(0.5)
                continue
            break
        self.assertEqual(getattr(resp, 'error', -1), 0)

        req = OffsetFetchRequest(self.topic, 0)
        (resp,) = yield self.client.send_offset_fetch_request(c_group, [req])
        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.offset, offset)
        # Check we received the proper metadata in the response
        self.assertEqual(resp.metadata, metadata)
        log.debug("test_commit_fetch_offsets: Test Complete.")
