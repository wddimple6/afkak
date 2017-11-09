# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import os
import logging

from nose.twistedtools import threaded_reactor, deferred
from twisted.internet.task import deferLater
from twisted.internet.defer import (inlineCallbacks, setDebugging, returnValue, Deferred)
from twisted.internet.base import DelayedCall
from mock import Mock
from afkak import (KafkaClient, create_message)
from afkak.group import Coordinator, ConsumerGroup
from afkak.common import (
    ProduceRequest
)
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase,
)


DEBUGGING = True
setDebugging(DEBUGGING)
DelayedCall.debug = DEBUGGING

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class TestAfkakGroupIntegration(KafkaIntegrationTestCase):

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            return

        DEBUGGING = True
        setDebugging(DEBUGGING)
        DelayedCall.debug = DEBUGGING

        cls.num_partitions = 6
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(
            0, cls.zk.host, cls.zk.port, partitions=cls.num_partitions)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = threaded_reactor()

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        cls.server.close()
        cls.zk.close()

    @inlineCallbacks
    def send_messages(self, partition, messages):
        messages = [create_message(self.msg(str(msg))) for msg in messages]
        produce = ProduceRequest(self.topic, partition, messages=messages)
        resp, = yield self.client.send_produce_request([produce])

        self.assertEqual(resp.error, 0)

        returnValue([x.value for x in messages])

    def when_called(self, object, method):
        """
            returns a Deferred that will be called back after the
            next time the method gets called
        """
        de = Deferred()
        original = getattr(object, method)

        def side_effect(*args, **kwargs):
            ret = original(*args, **kwargs)
            if not de.called:
                de.callback(None)
            return ret
        setattr(object, method, Mock(side_effect=side_effect))
        return de

    @kafka_versions("0.9.0.1")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_single_coordinator_join(self):
        coord = Coordinator(self.client, self.id(), ["test-topic"])

        de = self.when_called(coord, 'on_join_complete')
        coord.start()
        yield de
        yield coord.stop()

    @kafka_versions("0.9.0.1")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_three_coordinator_join(self):
        self.client2 = KafkaClient(
            '%s:%d' % (self.server.host, self.server.port),
            clientId=self.topic + '2')
        self.addCleanup(self.client2.close)

        self.client3 = KafkaClient(
            '%s:%d' % (self.server.host, self.server.port),
            clientId=self.topic + '3')
        self.addCleanup(self.client3.close)
        coords = [
            Coordinator(
                client, self.id(), topics=["test-topic"],
                retry_backoff_ms=100, heartbeat_interval_ms=100,
                fatal_backoff_ms=1000)
            for client in [self.client, self.client2, self.client3]
        ]
        coords[0].on_join_complete = lambda *args: a_joined.callback(*args)
        coords[0].start()

        a_joined = Deferred()

        # startup the first member of the group
        a_assignment = yield a_joined
        log.warn("first is in")

        self.assertNotEqual(coords[0].generation_id, None)
        self.assertEqual(coords[0].leader_id, coords[0].member_id)
        self.assertEqual(a_assignment, {"test-topic": (0, 1, 2, 3, 4, 5)})
        first_generation_id = coords[0].generation_id

        # now bring someone else into the group
        a_joined, b_joined = Deferred(), Deferred()

        coords[0].on_join_complete = lambda *args: a_joined.callback(*args)
        coords[1].on_join_complete = lambda *args: b_joined.callback(*args)
        log.warn("bringing in second")
        coords[1].start()
        log.warn("waiting for a_joined")
        yield a_joined
        log.warn("waiting for b_joined")
        yield b_joined

        self.assertEqual(coords[0].generation_id, coords[1].generation_id)
        self.assertEqual(coords[0].leader_id, coords[1].leader_id)
        self.assertNotEqual(coords[0].member_id, coords[1].member_id)
        self.assertNotEqual(coords[0].generation_id, first_generation_id)

        # and then bring in a third
        a_joined, b_joined, c_joined = Deferred(), Deferred(), Deferred()

        coords[0].on_join_complete = lambda *args: a_joined.callback(*args)
        coords[1].on_join_complete = lambda *args: b_joined.callback(*args)
        coords[2].on_join_complete = lambda *args: c_joined.callback(*args)
        log.warn("bringing in third")
        coords[2].start()
        log.warn("waiting for a_joined")
        a_assignment = yield a_joined
        log.warn("waiting for b_joined")
        b_assignment = yield b_joined
        log.warn("waiting for c_joined")
        c_assignment = yield c_joined

        self.assertTrue(len(a_assignment["test-topic"]) == 2, a_assignment)
        self.assertTrue(len(b_assignment["test-topic"]) == 2, b_assignment)
        self.assertTrue(len(c_assignment["test-topic"]) == 2, c_assignment)
        self.assertEqual(
            set(
                a_assignment["test-topic"] +
                b_assignment["test-topic"] +
                c_assignment["test-topic"]),
            set(range(6))
        )
        # and remove one
        a_joined, b_joined = Deferred(), Deferred()

        coords[0].on_join_complete = lambda *args: a_joined.callback(*args)
        coords[1].on_join_complete = lambda *args: b_joined.callback(*args)
        log.warn("removing third")
        yield coords[2].stop()

        log.warn("waiting for a_joined")
        yield a_joined
        log.warn("waiting for b_joined")
        yield b_joined

        log.warn("done")
        yield coords[0].stop()
        yield coords[1].stop()

    @kafka_versions("0.9.0.1")
    @deferred(timeout=20)
    @inlineCallbacks
    def test_single_consumergroup_join(self):
        msg_de = Deferred()

        def processor(consumer, records):
            msg_de.callback(records)
        coord = ConsumerGroup(
            self.client, self.id(),
            topics=[self.topic], processor=processor,
            retry_backoff_ms=100, heartbeat_interval_ms=1000,
            fatal_backoff_ms=3000,
        )
        join_de = self.when_called(coord, 'on_join_complete')
        coord.start()
        yield join_de

        self.assertIn(self.topic, coord.consumers)
        self.assertEqual(len(coord.consumers[self.topic]), self.num_partitions)
        self.assertEqual(coord.consumers[self.topic][0].topic, self.topic)
        self.assertEqual(coord.consumers[self.topic][0].partition, 0)

        for part in range(self.num_partitions):
            values = yield self.send_messages(part, [part])
            msgs = yield msg_de
            self.assertEqual(msgs[0].partition, part)
            self.assertEqual(msgs[0].message.value, values[0])
            msg_de = Deferred()
        yield coord.stop()

    @kafka_versions("0.9.0.1")
    @deferred(timeout=40)
    @inlineCallbacks
    def test_two_consumergroup_join(self):
        self.client2 = KafkaClient(
            '%s:%d' % (self.server.host, self.server.port),
            clientId=self.topic + '2')
        self.addCleanup(self.client2.close)

        msg_de = Deferred()

        def processor(consumer, records):
            msg_de.callback(records)

        coord = ConsumerGroup(
            self.client, self.id(),
            topics=[self.topic], processor=processor,
            retry_backoff_ms=100, heartbeat_interval_ms=1000,
            fatal_backoff_ms=3000,
        )
        de = self.when_called(coord, 'on_join_complete')
        coord_start_d = coord.start()
        yield de

        # send some messages and see that they're processed
        for part in range(self.num_partitions):
            values = yield self.send_messages(part, [part])
            msgs = yield msg_de
            self.assertEqual(msgs[0].partition, part)
            self.assertEqual(msgs[0].message.value, values[0])
            msg_de = Deferred()

        coord2 = ConsumerGroup(
            self.client2, self.id(),
            topics=[self.topic], processor=processor,
            retry_backoff_ms=100, heartbeat_interval_ms=1000,
            fatal_backoff_ms=3000,
        )
        de = self.when_called(coord, 'on_join_complete')
        de2 = self.when_called(coord2, 'on_join_complete')
        coord2_start_d = coord2.start()
        yield de
        yield de2
        self.assertIn(self.topic, coord.consumers)
        self.assertIn(self.topic, coord2.consumers)
        self.assertEqual(len(coord.consumers[self.topic]), 3)
        self.assertEqual(len(coord2.consumers[self.topic]), 3)
        self.assertNotEqual(
            coord.consumers[self.topic][0].partition,
            coord2.consumers[self.topic][0].partition)

        # after the cluster has re-formed, send some more messages
        # and check that we get them too (and don't get the old messages again)
        for part in range(self.num_partitions):
            values = yield self.send_messages(part, [part])
            msgs = yield msg_de
            self.assertEqual(msgs[0].partition, part)
            self.assertEqual(msgs[0].message.value, values[0])
            msg_de = Deferred()

        yield coord.stop()
        yield coord2.stop()
        yield coord_start_d
        yield coord2_start_d

    @kafka_versions("0.9.0.1")
    @deferred(timeout=60)
    @inlineCallbacks
    def test_broker_restart(self):
        """
            restart the kafka broker and verify that the group rejoins
        """
        msg_de = Deferred()

        def processor(consumer, records):
            msg_de.callback(records)
        coord = ConsumerGroup(
            self.client, self.id(),
            topics=[self.topic], processor=processor,
            retry_backoff_ms=100, heartbeat_interval_ms=1000,
            fatal_backoff_ms=2000,
        )
        join_de = self.when_called(coord, 'on_join_complete')
        coord.start()

        yield join_de
        self.assertIn(self.topic, coord.consumers)
        self.assertEqual(len(coord.consumers[self.topic]), self.num_partitions)
        self.assertEqual(coord.consumers[self.topic][0].topic, self.topic)
        self.assertEqual(coord.consumers[self.topic][0].partition, 0)

        # restart the broker and see that we re-join and still work
        leave_de = self.when_called(coord, 'on_group_leave')
        prepare_de = self.when_called(coord, 'on_join_prepare')
        join_de = self.when_called(coord, 'on_join_complete')
        self.server.stop()
        yield leave_de
        self.assertEqual(len(coord.consumers), 0)
        self.server.restart()
        yield prepare_de
        yield join_de
        self.assertIn(self.topic, coord.consumers)
        self.assertEqual(len(coord.consumers[self.topic]), self.num_partitions)

        for part in range(self.num_partitions):
            values = yield self.send_messages(part, [part])
            msgs = yield msg_de
            self.assertEqual(msgs[0].partition, part)
            self.assertEqual(msgs[0].message.value, values[0])
            msg_de = Deferred()
        yield coord.stop()

    @kafka_versions("0.9.0.1")
    @deferred(timeout=60)
    @inlineCallbacks
    def test_consumer_rejoin(self):
        """
            trigger a rejoin via consumer commit failure
        """
        self.client2 = KafkaClient(
            '%s:%d' % (self.server.host, self.server.port),
            clientId=self.topic + '2')
        self.addCleanup(self.client2.close)

        msg_de = Deferred()

        def processor(consumer, records):
            msg_de.callback(records)

        coord = ConsumerGroup(
            self.client, self.id(),
            topics=[self.topic], processor=processor,
            session_timeout_ms=6000, retry_backoff_ms=100,
            heartbeat_interval_ms=1000, fatal_backoff_ms=3000,
            consumer_kwargs=dict(auto_commit_every_ms=1000),
        )
        de = self.when_called(coord, 'on_join_complete')
        coord_start_d = coord.start()
        yield de

        # kill the heartbeat timer and start joining the second consumer
        coord._heartbeat_looper.stop()
        coord2 = ConsumerGroup(
            self.client2, self.id(),
            topics=[self.topic], processor=processor,
            session_timeout_ms=6000, retry_backoff_ms=100,
            heartbeat_interval_ms=1000, fatal_backoff_ms=3000,
            consumer_kwargs=dict(auto_commit_every_ms=1000),
        )
        coord2_start_d = coord2.start()

        # send some messages and see that they're processed
        # the commit will eventually fail because we're rebalancing
        for part in range(15):
            yield deferLater(self.reactor, 0.5, lambda: None)
            values = yield self.send_messages(
                part % self.num_partitions, [part])
            msgs = yield msg_de
            msg_de = Deferred()
            if msgs[0].partition != part:
                # once the commit fails, we will see the msg twice
                break
            self.assertEqual(msgs[0].message.value, values[0])

        de = self.when_called(coord, 'on_join_complete')
        de2 = self.when_called(coord2, 'on_join_complete')
        yield de
        yield de2
        self.assertIn(self.topic, coord.consumers)
        self.assertIn(self.topic, coord2.consumers)
        self.assertEqual(len(coord.consumers[self.topic]), 3)
        self.assertEqual(len(coord2.consumers[self.topic]), 3)
        self.assertNotEqual(
            coord.consumers[self.topic][0].partition,
            coord2.consumers[self.topic][0].partition)

        # after the cluster has re-formed, send some more messages
        # and check that we get them too (and don't get the old messages again)
        # due to the failed commit, we may need to reset this one extra time
        if msg_de.called:
            msg_de = Deferred()

        for part in range(6):
            values = yield self.send_messages(part, [part])
            msgs = yield msg_de
            self.assertEqual(msgs[0].partition, part)
            self.assertEqual(msgs[0].message.value, values[0])
            msg_de = Deferred()

        yield coord.stop()
        yield coord2.stop()
        yield coord_start_d
        yield coord2_start_d
