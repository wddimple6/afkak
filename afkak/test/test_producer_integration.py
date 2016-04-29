# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import os
import time
import logging

from nose.twistedtools import threaded_reactor, deferred

import unittest2
from twisted.trial import unittest
from twisted.internet.base import DelayedCall
from twisted.internet.defer import (
    inlineCallbacks, setDebugging,
    )

from afkak import (
    create_message, create_message_set, Producer,
    RoundRobinPartitioner, HashedPartitioner, CODEC_GZIP, CODEC_SNAPPY,
    )
from afkak.common import (ProduceRequest, FetchRequest, SendRequest,
                          PRODUCER_ACK_NOT_REQUIRED,
                          PRODUCER_ACK_ALL_REPLICAS,
                          PRODUCER_ACK_LOCAL_WRITE)

from afkak.codec import has_snappy
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase, make_send_requests, async_delay,
    random_string
    )

log = logging.getLogger(__name__)


class TestAfkakProducerIntegration(
        KafkaIntegrationTestCase, unittest.TestCase):
    topic = 'produce_topic'

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.warning("WARNING: KAFKA_VERSION not found in environment")
            return

        DEBUGGING = True
        setDebugging(DEBUGGING)
        DelayedCall.debug = DEBUGGING

        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

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

    ###########################################################################
    #   client production Tests  - Server setup is 1 replica, 2 partitions    #
    ###########################################################################

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_produce_many_simple(self):
        start_offset = yield self.current_offset(self.topic, 0)

        yield self.assert_produce_request(
            [create_message("Test message %d" % i) for i in range(100)],
            start_offset,
            100,
        )

        yield self.assert_produce_request(
            [create_message("Test message %d" % i) for i in range(100)],
            start_offset+100,
            100,
        )

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_produce_100_keyed(self):
        start_offset = yield self.current_offset(self.topic, 0)

        yield self.assert_produce_request(
            [create_message("Test message %d" % i,
                            key="Key:%d" % i) for i in range(100)],
            start_offset,
            100,
        )
        msgs = ["Test message %d" % i for i in range(100)]
        keys = ["Key:%d" % i for i in range(100)]
        yield self.assert_fetch_offset(0, start_offset, msgs,
                                       expected_keys=keys,
                                       fetch_size=10240)

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_produce_100_keyed_gzipped(self):
        """test_produce_100_keyed_gzipped

        Test that gzipping the batch doesn't affect the partition to which
        the messages are assigned, or the order of the messages in the topics

        """
        start_offset = yield self.current_offset(self.topic, 0)

        msg_set = create_message_set(
                [SendRequest(self.topic, "Key:%d" % i,
                             ["Test msg %d" % i], None)
                 for i in range(100)], CODEC_GZIP)
        yield self.assert_produce_request(
            msg_set,
            start_offset,
            100,
        )
        msgs = ["Test msg %d" % i for i in range(100)]
        keys = ["Key:%d" % i for i in range(100)]
        yield self.assert_fetch_offset(0, start_offset, msgs,
                                       expected_keys=keys,
                                       fetch_size=10240)

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_produce_10k_simple(self):
        start_offset = yield self.current_offset(self.topic, 0)

        yield self.assert_produce_request(
            [create_message("Test message %d" % i) for i in range(10000)],
            start_offset,
            10000,
        )

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_produce_many_gzip(self):
        start_offset = yield self.current_offset(self.topic, 0)

        message1 = create_message_set(
            make_send_requests(
                ["Gzipped 1 %d" % i for i in range(100)]), CODEC_GZIP)[0]
        message2 = create_message_set(
            make_send_requests(
                ["Gzipped 2 %d" % i for i in range(100)]), CODEC_GZIP)[0]

        yield self.assert_produce_request(
            [message1, message2],
            start_offset,
            200,
        )

    @unittest2.skipUnless(has_snappy(), "Snappy not available")
    @kafka_versions("0.8.1", "0.8.1.1", "0.8.2.1", "0.8.2.2", "0.9.0.1")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_produce_many_snappy(self):
        start_offset = yield self.current_offset(self.topic, 0)

        message1 = create_message_set(
            make_send_requests(
                ["Snappy 1 %d" % i for i in range(100)]), CODEC_SNAPPY)[0]
        message2 = create_message_set(
            make_send_requests(
                ["Snappy 2 %d" % i for i in range(100)]), CODEC_SNAPPY)[0]
        yield self.assert_produce_request(
            [message1, message2],
            start_offset,
            200,
        )

    @kafka_versions("0.8.1", "0.8.1.1", "0.8.2.1", "0.8.2.2", "0.9.0.1")
    @deferred(timeout=20)
    @inlineCallbacks
    def test_produce_mixed(self):
        start_offset = yield self.current_offset(self.topic, 0)

        msg_count = 1+100
        messages = [
            create_message("Just a plain message"),
            create_message_set(
                make_send_requests(
                    ["Gzipped %d" % i for i in range(100)]), CODEC_GZIP)[0]
        ]

        # Can't produce snappy messages without snappy...
        if has_snappy():
            msg_count += 100
            messages.append(
                create_message_set(
                    make_send_requests(["Snappy %d" % i for i in range(100)]),
                    CODEC_SNAPPY)[0])

        yield self.assert_produce_request(messages, start_offset, msg_count)
        ptMsgs = ['Just a plain message']
        ptMsgs.extend(["Gzipped %d" % i for i in range(100)])
        if has_snappy():
            ptMsgs.extend(["Snappy %d" % i for i in range(100)])
        yield self.assert_fetch_offset(0, start_offset, ptMsgs,
                                       fetch_size=10240)

    @kafka_versions("all")
    @deferred(timeout=240)
    @inlineCallbacks
    def test_produce_10k_gzipped(self):
        start_offset = yield self.current_offset(self.topic, 0)

        msgs = create_message_set(
            make_send_requests(
                ["Gzipped batch 1, message %d" % i for i in range(5000)]),
            CODEC_GZIP)
        yield self.assert_produce_request(msgs, start_offset, 5000)
        msgs = create_message_set(
            make_send_requests(
                ["Gzipped batch 2, message %d" % i for i in range(5000)]),
            CODEC_GZIP)
        yield self.assert_produce_request(msgs, start_offset+5000, 5000)

    ###################################################################
    #   Producer Tests  - Server setup is 1 replica, 2 partitions     #
    ###################################################################

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_send_messages(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)
        producer = Producer(self.client)

        # Goes to first partition
        resp = yield producer.send_messages(
            self.topic, msgs=[self.msg("one"), self.msg("two")])
        self.assert_produce_response(resp, start_offset0)

        # Goes to the 2nd partition
        resp = yield producer.send_messages(self.topic,
                                            msgs=[self.msg("three")])
        self.assert_produce_response(resp, start_offset1)

        # fetch the messages back and make sure they are as expected
        yield self.assert_fetch_offset(
            0, start_offset0, [self.msg("one"), self.msg("two")])
        yield self.assert_fetch_offset(
            1, start_offset1, [self.msg("three")])
        # Goes back to the first partition because there's only two partitions
        resp = yield producer.send_messages(
            self.topic, msgs=[self.msg("four"), self.msg("five")])
        self.assert_produce_response(resp, start_offset0+2)
        yield self.assert_fetch_offset(
            0, start_offset0, [self.msg("one"),
                               self.msg("two"),
                               self.msg("four"),
                               self.msg("five")])

        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_round_robin_partitioner(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(self.client,
                            partitioner_class=RoundRobinPartitioner)
        resp1 = yield producer.send_messages(
            self.topic, "key1", [self.msg("one")])
        resp2 = yield producer.send_messages(
            self.topic, "key2", [self.msg("two")])
        resp3 = yield producer.send_messages(
            self.topic, "key3", [self.msg("three")])
        resp4 = yield producer.send_messages(
            self.topic, "key4", [self.msg("four")])

        self.assert_produce_response(resp1, start_offset0+0)
        self.assert_produce_response(resp2, start_offset1+0)
        self.assert_produce_response(resp3, start_offset0+1)
        self.assert_produce_response(resp4, start_offset1+1)

        yield self.assert_fetch_offset(
            0, start_offset0, [self.msg("one"), self.msg("three")])
        yield self.assert_fetch_offset(
            1, start_offset1, [self.msg("two"), self.msg("four")])

        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_round_robin_partitioner_random_start(self):
        try:
            RoundRobinPartitioner.set_random_start(True)
            producer = Producer(self.client,
                                partitioner_class=RoundRobinPartitioner)
            # Two partitions, so 1st and 3rd reqs should go to same part, but
            # 2nd should go to different. Other than that, without statistical
            # test, we can't say much... Partitioner tests should ensure that
            # we really aren't always starting on a non-random partition...
            resp1 = yield producer.send_messages(
                self.topic, msgs=[self.msg("one"), self.msg("two")])
            resp2 = yield producer.send_messages(
                self.topic, msgs=[self.msg("three")])
            resp3 = yield producer.send_messages(
                self.topic, msgs=[self.msg("four"), self.msg("five")])

            self.assertEqual(resp1.partition, resp3.partition)
            self.assertNotEqual(resp1.partition, resp2.partition)

            yield producer.stop()
        finally:
            RoundRobinPartitioner.set_random_start(False)

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_hashed_partitioner(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(self.client,
                            partitioner_class=HashedPartitioner)

        resp1 = yield producer.send_messages(
            self.topic, '1', [self.msg("one")])
        resp2 = yield producer.send_messages(
            self.topic, '2', [self.msg("two")])
        resp3 = yield producer.send_messages(
            self.topic, '1', [self.msg("three")])
        resp4 = yield producer.send_messages(
            self.topic, '1', [self.msg("four")])
        resp5 = yield producer.send_messages(
            self.topic, '2', [self.msg("five")])

        self.assert_produce_response(resp2, start_offset0+0)
        self.assert_produce_response(resp5, start_offset0+1)

        self.assert_produce_response(resp1, start_offset1+0)
        self.assert_produce_response(resp3, start_offset1+1)
        self.assert_produce_response(resp4, start_offset1+2)

        yield self.assert_fetch_offset(
            0, start_offset0, [
                self.msg("two"),
                self.msg("five"),
                ])
        yield self.assert_fetch_offset(
            1, start_offset1, [
                self.msg("one"),
                self.msg("three"),
                self.msg("four"),
                ])

        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_batched_gzipped_hashed_partitioner(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)
        offsets = (start_offset0, start_offset1)

        requests = []
        msgs_by_partition = ([], [])
        keys_by_partition = ([], [])

        partitioner = HashedPartitioner(self.topic, [0, 1])
        producer = Producer(
            self.client, codec=CODEC_GZIP, batch_send=True, batch_every_n=100,
            batch_every_t=None, partitioner_class=HashedPartitioner)

        # Send ten groups of messages, each with a different key
        for i in range(10):
            msg_group = []
            key = 'Key: {}'.format(i)
            part = partitioner.partition(key, [0, 1])
            for j in range(10):
                msg = self.msg('Group:{} Msg:{}'.format(i, j))
                msg_group.append(msg)
                msgs_by_partition[part].append(msg)
                keys_by_partition[part].append(key)
            request = producer.send_messages(
                self.topic, key=key, msgs=msg_group)
            requests.append(request)
            yield async_delay(.5)  # Make the NoResult test have teeth...
            if i < 9:
                # This is to ensure we really are batching all the requests
                self.assertNoResult(request)

        # Now ensure we can retrieve the right messages from each partition
        for part in [0, 1]:
            yield self.assert_fetch_offset(
                part, offsets[part], msgs_by_partition[part],
                keys_by_partition[part], fetch_size=20480)

        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=5)
    @inlineCallbacks
    def test_acks_none(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        yield self.current_offset(self.topic, 1)

        producer = Producer(
            self.client, req_acks=PRODUCER_ACK_NOT_REQUIRED)
        resp = yield producer.send_messages(self.topic, msgs=[self.msg("one")])
        self.assertEqual(resp, None)

        yield self.assert_fetch_offset(0, start_offset0, [self.msg("one")])
        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_acks_local_write(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        yield self.current_offset(self.topic, 1)

        producer = Producer(
            self.client, req_acks=PRODUCER_ACK_LOCAL_WRITE)
        resp = yield producer.send_messages(self.topic, msgs=[self.msg("one")])

        self.assert_produce_response(resp, start_offset0)
        yield self.assert_fetch_offset(0, start_offset0, [self.msg("one")])

        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_acks_all_replicas(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(
            self.client,
            req_acks=PRODUCER_ACK_ALL_REPLICAS)

        resp = yield producer.send_messages(self.topic, msgs=[self.msg("one")])
        self.assert_produce_response(resp, start_offset0)
        yield self.assert_fetch_offset(0, start_offset0, [self.msg("one")])
        yield self.assert_fetch_offset(1, start_offset1, [])

        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_batched_by_messages(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(self.client, batch_send=True, batch_every_n=10,
                            batch_every_b=0, batch_every_t=0)
        # Send 4 messages and do a fetch. Fetch should timeout, and send
        # deferred shouldn't have a result yet...
        send1D = producer.send_messages(
            self.topic, msgs=[self.msg("one"), self.msg("two"),
                              self.msg("three"), self.msg("four")])
        # by default the assert_fetch_offset() waits for 0.5 secs on the server
        # side before returning no result. So, these two calls should take 1sec
        yield self.assert_fetch_offset(0, start_offset0, [])
        yield self.assert_fetch_offset(1, start_offset1, [])
        # Messages shouldn't have sent out yet, so we shouldn't have
        # response from server yet on having received/responded to the request
        self.assertNoResult(send1D)
        # Sending 3 more messages won't trigger the send either (7 total)
        send2D = producer.send_messages(
            self.topic, msgs=[self.msg("five"), self.msg("six"),
                              self.msg("seven")])
        # make sure no messages on server...
        yield self.assert_fetch_offset(0, start_offset0, [])
        yield self.assert_fetch_offset(1, start_offset1, [])
        # Still no result on send
        self.assertNoResult(send2D)
        # send final batch which will be on partition 0 again...
        send3D = producer.send_messages(
            self.topic, msgs=[self.msg("eight"), self.msg("nine"),
                              self.msg("ten"), self.msg("eleven")])
        # Now do a fetch, again waiting for up to 0.5 seconds for the response
        # All four messages sent in first batch (to partition 0, given default
        # R-R, start-at-zero partitioner), and 4 in 3rd batch
        yield self.assert_fetch_offset(
            0, start_offset0, [self.msg("one"), self.msg("two"),
                               self.msg("three"), self.msg("four"),
                               self.msg("eight"), self.msg("nine"),
                               self.msg("ten"), self.msg("eleven")],
            fetch_size=2048)
        # Fetch from partition:1 should have all messages in 2nd batch
        yield self.assert_fetch_offset(
            1, start_offset1, [self.msg("five"), self.msg("six"),
                               self.msg("seven")])
        # make sure the deferreds fired with the proper result
        resp1 = self.successResultOf(send1D)
        resp2 = self.successResultOf(send2D)
        resp3 = self.successResultOf(send3D)
        self.assert_produce_response(resp1, start_offset0)
        self.assert_produce_response(resp2, start_offset1)
        self.assert_produce_response(resp3, start_offset0)
        # cleanup
        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_producer_batched_by_bytes(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(self.client, batch_send=True, batch_every_b=4096,
                            batch_every_n=0, batch_every_t=0)
        # Send 4 messages and do a fetch. Fetch should timeout, and send
        # deferred shouldn't have a result yet...
        send1D = producer.send_messages(
            self.topic, msgs=[self.msg("one"), self.msg("two"),
                              self.msg("three"), self.msg("four")])
        # by default the assert_fetch_offset() waits for 0.5 secs on the server
        # side before returning no result. So, these two calls should take 1sec
        yield self.assert_fetch_offset(0, start_offset0, [])
        yield self.assert_fetch_offset(1, start_offset1, [])
        # Messages shouldn't have sent out yet, so we shouldn't have
        # response from server yet on having received/responded to the request
        self.assertNoResult(send1D)
        # Sending 3 more messages should trigger the send, but we don't yield
        # here, so we shouldn't have a response immediately after, and so
        # send2D should still have no result.
        send2D = producer.send_messages(
            self.topic, msgs=[self.msg("five"), self.msg("six"),
                              self.msg("seven")])
        # make sure no messages on server...
        yield self.assert_fetch_offset(0, start_offset0, [])
        yield self.assert_fetch_offset(1, start_offset1, [])
        # Still no result on send
        self.assertNoResult(send2D)
        # send 3rd batch which will be on partition 0 again...
        send3D = producer.send_messages(
            self.topic, msgs=[self.msg("eight"), self.msg("nine"),
                              self.msg("ten"), self.msg("eleven")])
        # make sure no messages on server...
        yield self.assert_fetch_offset(0, start_offset0, [])
        yield self.assert_fetch_offset(1, start_offset1, [])
        # Still no result on send
        self.assertNoResult(send3D)
        # Finally, send a big message to trigger send
        send4D = producer.send_messages(
            self.topic, msgs=[self.msg("1234" * 1024)])
        # Now do a fetch, again waiting for up to 0.5 seconds for the response
        # All four messages sent in first batch (to partition 0, given default
        # R-R, start-at-zero partitioner), and 4 in 3rd batch
        yield self.assert_fetch_offset(
            0, start_offset0, [self.msg("one"), self.msg("two"),
                               self.msg("three"), self.msg("four"),
                               self.msg("eight"), self.msg("nine"),
                               self.msg("ten"), self.msg("eleven")],
            fetch_size=2048)
        # Fetch from partition:1 should have all messages in 2nd batch, as
        # send_messages() treats calls as groups and all/none are sent...
        yield self.assert_fetch_offset(
            1, start_offset1, [self.msg("five"), self.msg("six"),
                               self.msg("seven"), self.msg("1234" * 1024)],
            fetch_size=5*1024)
        # make sure the deferreds fired with the proper result
        resp1 = self.successResultOf(send1D)
        resp2 = self.successResultOf(send2D)
        resp3 = self.successResultOf(send3D)
        resp4 = self.successResultOf(send4D)
        self.assert_produce_response(resp1, start_offset0)
        self.assert_produce_response(resp2, start_offset1)
        self.assert_produce_response(resp3, start_offset0)
        self.assert_produce_response(resp4, start_offset1)
        # cleanup
        yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=20)
    @inlineCallbacks
    def test_producer_batched_by_time(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        # This needs to be big enough that the operations between starting the
        # producer and the time.sleep() call take less time than this... I made
        # it large enough that the test would still pass even with my Macbook's
        # cores all pegged by a load generator.
        batchtime = 5

        try:
            producer = Producer(self.client, batch_send=True,
                                batch_every_n=0, batch_every_t=batchtime)
            startTime = time.time()
            # Send 4 messages and do a fetch
            send1D = producer.send_messages(
                self.topic, msgs=[self.msg("one"), self.msg("two"),
                                  self.msg("three"), self.msg("four")])
            # set assert_fetch_offset() to wait for 0.1 secs on the server-side
            # before returning no result. So, these calls should take 0.2sec
            yield self.assert_fetch_offset(0, start_offset0, [], max_wait=0.1)
            yield self.assert_fetch_offset(1, start_offset1, [], max_wait=0.1)
            # Messages shouldn't have sent out yet, so we shouldn't have
            # response from server yet on having received/responded to request
            self.assertNoResult(send1D)
            # Sending 3 more messages should NOT trigger the send, as less than
            # 1 sec. elapsed by here, so send2D should still have no result.
            send2D = producer.send_messages(
                self.topic, msgs=[self.msg("five"), self.msg("six"),
                                  self.msg("seven")])
            # still no messages...
            yield self.assert_fetch_offset(0, start_offset0, [], max_wait=0.1)
            yield self.assert_fetch_offset(1, start_offset1, [], max_wait=0.1)
            # Still no result on send, and send should NOT have gone out.
            self.assertNoResult(send2D)
            # Wait the timeout out. It'd be nicer to be able to just 'advance'
            # the reactor, but since we need the network so...
            time.sleep(batchtime - (time.time() - startTime) + 0.05)
            # We need to yield to the reactor to have it process the response
            # from the broker. Both send1D and send2D should then have results.
            resp1 = yield send1D
            resp2 = yield send2D
            # ensure the 2 batches went into the proper partitions...
            self.assert_produce_response(resp1, start_offset0)
            self.assert_produce_response(resp2, start_offset1)
            # Should be able to get messages now
            yield self.assert_fetch_offset(
                0, start_offset0, [self.msg("one"), self.msg("two"),
                                   self.msg("three"), self.msg("four")])
            yield self.assert_fetch_offset(
                1, start_offset1, [self.msg("five"), self.msg("six"),
                                   self.msg("seven")])
        except IOError:  # pragma: no cover
            msg = "IOError: Probably time.sleep with negative value due to " \
                "'too slow' system taking more than {0} seconds to do " \
                "something that should take ~0.4 seconds.".format(batchtime)
            log.exception(msg)
            # In case of the IOError, we want to eat the CancelledError
            send1D.addErrback(lambda _: None)  # Eat any uncaught errors
            send2D.addErrback(lambda _: None)  # Eat any uncaught errors
            self.fail(msg)
        finally:
            yield producer.stop()

    @kafka_versions("all")
    @deferred(timeout=15)
    @inlineCallbacks
    def test_write_nonextant_topic(self):
        """test_write_nonextant_topic

        Test we can write to a non-extant topic (which will be auto-created)
        simply by calling producer.send_messages with a long enough timeout.
        """
        test_topics = ["{}-{}-{}".format(
            self.id().split('.')[-1], i, random_string(10)) for i in range(10)]

        producer = Producer(
            self.client, req_acks=PRODUCER_ACK_LOCAL_WRITE)

        for topic in test_topics:
            resp = yield producer.send_messages(topic, msgs=[self.msg(topic)])
            # Make sure the send went ok
            self.assert_produce_response(resp, 0)
            # Make sure we can get the message back
            yield self.assert_fetch_offset(
                0, 0, [self.msg(topic)], topic=topic)

        yield producer.stop()

    # ###########  Utility Functions  ############

    @inlineCallbacks
    def assert_produce_request(self, messages, initial_offset, message_ct):
        produce = ProduceRequest(self.topic, 0, messages=messages)

        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.
        resp, = yield self.client.send_produce_request([produce])
        self.assert_produce_response(resp, initial_offset)

        resp2 = yield self.current_offset(self.topic, 0)

        self.assertEqual(resp2, initial_offset + message_ct)

    def assert_produce_response(self, resp, initial_offset):
        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.offset, initial_offset)

    @inlineCallbacks
    def assert_fetch_offset(self, partition, start_offset,
                            expected_messages, expected_keys=[],
                            max_wait=0.5, fetch_size=1024, topic=None):
        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.
        if topic is None:
            topic = self.topic
        resp, = yield self.client.send_fetch_request(
            [FetchRequest(topic, partition, start_offset, fetch_size)],
            max_wait_time=int(max_wait * 1000))

        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.partition, partition)
        # Convert generator to list
        resp_messages = list(resp.messages)
        messages = [x.message.value for x in resp_messages]
        self.assertEqual(messages, expected_messages)
        if expected_keys:
            keys = [x.message.key for x in resp_messages]
            self.assertEqual(keys, expected_keys)
        self.assertEqual(
            resp.highwaterMark, start_offset+len(expected_messages))
