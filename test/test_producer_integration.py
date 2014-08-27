import os
import time

import nose.twistedtools
from twisted.internet.defer import (
    inlineCallbacks  # , returnValue, setDebugging
)

from afkak import (
    create_message, create_gzip_message, create_snappy_message, Producer,
    RoundRobinPartitioner, HashedPartitioner,
    )
from afkak.common import (ProduceRequest, FetchRequest)
from afkak.codec import has_snappy
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase,
    )

from twisted.trial.unittest import TestCase as TrialTestCase


class TestKafkaProducerIntegration(KafkaIntegrationTestCase, TrialTestCase):
    topic = 'produce_topic'

    @classmethod
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        cls.reactor, cls.thread = nose.twistedtools.threaded_reactor()

    @classmethod
    def tearDownClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server.close()
        cls.zk.close()

    ###########################################################################
    #   client production Tests  - Server setup is 1 replica, 2 partitions    #
    ###########################################################################

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
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
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_produce_10k_simple(self):
        start_offset = yield self.current_offset(self.topic, 0)

        yield self.assert_produce_request(
            [create_message("Test message %d" % i) for i in range(10000)],
            start_offset,
            10000,
        )

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_produce_many_gzip(self):
        start_offset = yield self.current_offset(self.topic, 0)

        message1 = create_gzip_message(
            ["Gzipped 1 %d" % i for i in range(100)])
        message2 = create_gzip_message(
            ["Gzipped 2 %d" % i for i in range(100)])

        yield self.assert_produce_request(
            [message1, message2],
            start_offset,
            200,
        )

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_produce_many_snappy(self):
        # self.skipTest("All snappy integration tests fail with nosnappyjava")
        start_offset = yield self.current_offset(self.topic, 0)

        yield self.assert_produce_request(
            [
                create_snappy_message(["Snappy 1 %d" % i for i in range(100)]),
                create_snappy_message(["Snappy 2 %d" % i for i in range(100)]),
                ],
            start_offset,
            200,
        )

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=10)
    @inlineCallbacks
    def test_produce_mixed(self):
        start_offset = yield self.current_offset(self.topic, 0)

        msg_count = 1+100
        messages = [
            create_message("Just a plain message"),
            create_gzip_message(["Gzipped %d" % i for i in range(100)]),
        ]

        # All snappy integration tests fail with nosnappyjava
        if has_snappy():
            msg_count += 100
            messages.append(
                create_snappy_message(["Snappy %d" % i for i in range(100)]))

        yield self.assert_produce_request(messages, start_offset, msg_count)
        ptMsgs = ['Just a plain message']
        ptMsgs.extend(["Gzipped %d" % i for i in range(100)])
        ptMsgs.extend(["Snappy %d" % i for i in range(100)])
        yield self.assert_fetch_offset(0, start_offset, ptMsgs,
                                       fetch_size=10240)

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_produce_100k_gzipped(self):
        start_offset = yield self.current_offset(self.topic, 0)

        msgs = create_gzip_message(
            ["Gzipped batch 1, message %d" % i for i in range(50000)])
        yield self.assert_produce_request([msgs], start_offset, 50000)
        msgs = create_gzip_message(
            ["Gzipped batch 2, message %d" % i for i in range(50000)])
        yield self.assert_produce_request([msgs], start_offset+50000, 50000)

    ###################################################################
    #   Producer Tests  - Server setup is 1 replica, 2 partitions     #
    ###################################################################

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_producer_simple(self):
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
    @nose.twistedtools.deferred(timeout=5)
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
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_producer_round_robin_partitioner_random_start(self):
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

        self.assertEqual(resp1[0].partition, resp3[0].partition)
        self.assertNotEqual(resp1[0].partition, resp2[0].partition)

        yield producer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_producer_hashed_partitioner(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(self.client,
                            partitioner_class=HashedPartitioner)
        resp1 = yield producer.send_messages(
            self.topic, 1, [self.msg("one")])
        resp2 = yield producer.send_messages(
            self.topic, 2, [self.msg("two")])
        resp3 = yield producer.send_messages(
            self.topic, 3, [self.msg("three")])
        resp4 = yield producer.send_messages(
            self.topic, 3, [self.msg("four")])
        resp5 = yield producer.send_messages(
            self.topic, 4, [self.msg("five")])

        self.assert_produce_response(resp1, start_offset1+0)
        self.assert_produce_response(resp2, start_offset0+0)
        self.assert_produce_response(resp3, start_offset1+1)
        self.assert_produce_response(resp4, start_offset1+2)
        self.assert_produce_response(resp5, start_offset0+1)

        yield self.assert_fetch_offset(
            0, start_offset0, [
                self.msg("two"), self.msg("five")])
        yield self.assert_fetch_offset(
            1, start_offset1, [
                self.msg("one"),
                self.msg("three"), self.msg("four")])

        yield producer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_acks_none(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        yield self.current_offset(self.topic, 1)

        producer = Producer(
            self.client, req_acks=Producer.ACK_NOT_REQUIRED)
        resp = yield producer.send_messages(self.topic, msgs=[self.msg("one")])
        self.assertEquals(len(resp), 0)

        yield self.assert_fetch_offset(0, start_offset0, [self.msg("one")])
        yield producer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_acks_local_write(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        yield self.current_offset(self.topic, 1)

        producer = Producer(
            self.client, req_acks=Producer.ACK_AFTER_LOCAL_WRITE)
        resp = yield producer.send_messages(self.topic, msgs=[self.msg("one")])

        self.assert_produce_response(resp, start_offset0)
        yield self.assert_fetch_offset(0, start_offset0, [self.msg("one")])

        yield producer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
    @inlineCallbacks
    def test_acks_cluster_commit(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        yield self.current_offset(self.topic, 1)

        producer = Producer(
            self.client,
            req_acks=Producer.ACK_AFTER_CLUSTER_COMMIT)

        resp = yield producer.send_messages(self.topic, msgs=[self.msg("one")])
        self.assert_produce_response(resp, start_offset0)
        yield self.assert_fetch_offset(0, start_offset0, [self.msg("one")])

        yield producer.stop()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=5)
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
        # Fetch from partition:1 should have all messages in 2nd batch, as
        # send_messages() treats calls as groups and all/none are sent...
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
    @nose.twistedtools.deferred(timeout=5)
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
    @nose.twistedtools.deferred(timeout=10)
    @inlineCallbacks
    def test_producer_batched_by_time(self):
        start_offset0 = yield self.current_offset(self.topic, 0)
        start_offset1 = yield self.current_offset(self.topic, 1)

        producer = Producer(self.client, batch_send=True,
                            batch_every_n=0, batch_every_t=2.5)
        # Send 4 messages and do a fetch
        send1D = producer.send_messages(
            self.topic, msgs=[self.msg("one"), self.msg("two"),
                              self.msg("three"), self.msg("four")])
        # set assert_fetch_offset() to wait for 0.5 secs on the server-side
        # before returning no result. So, these two calls should take 1sec
        yield self.assert_fetch_offset(0, start_offset0, [], max_wait=0.5)
        yield self.assert_fetch_offset(1, start_offset1, [], max_wait=0.5)
        # Messages shouldn't have sent out yet, so we shouldn't have
        # response from server yet on having received/responded to the request
        self.assertNoResult(send1D)
        # Sending 3 more messages should NOT trigger the send, as only approx.
        # 1 sec. elapsed by here, so send2D should still have no result.
        send2D = producer.send_messages(
            self.topic, msgs=[self.msg("five"), self.msg("six"),
                              self.msg("seven")])
        # still no messages...
        yield self.assert_fetch_offset(0, start_offset0, [], max_wait=0.5)
        yield self.assert_fetch_offset(1, start_offset1, [], max_wait=0.5)
        # Still no result on send, and send should NOT have gone out.
        self.assertNoResult(send2D)
        # Wait the timeout out. It'd be nicer to be able to just 'advance' the
        # reactor, but since we need the network we need a 'real' reactor so...
        time.sleep(1)
        # We need to yield to the reactor to have it process the tcp response
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
        yield producer.stop()

    ############  Utility Functions  ############

    @inlineCallbacks
    def assert_produce_request(self, messages, initial_offset, message_ct):
        produce = ProduceRequest(self.topic, 0, messages=messages)

        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.
        resp = yield self.client.send_produce_request([produce])
        self.assert_produce_response(resp, initial_offset)

        resp2 = yield self.current_offset(self.topic, 0)
        self.assertEqual(resp2, initial_offset + message_ct)

    def assert_produce_response(self, resp, initial_offset):
#        print "ZORG_tpi_3: Resp:", resp, "init_offset:", initial_offset
        self.assertEqual(len(resp), 1)
        self.assertEqual(resp[0].error, 0)
        self.assertEqual(resp[0].offset, initial_offset)

    @inlineCallbacks
    def assert_fetch_offset(self, partition, start_offset,
                            expected_messages, max_wait=0.5, fetch_size=1024):
        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.

        resp, = yield self.client.send_fetch_request(
            [FetchRequest(self.topic, partition, start_offset, fetch_size)],
            max_wait_time=max_wait)

        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.partition, partition)
        messages = [x.message.value for x in resp.messages]
#        print "ZORG_tpi_0: start_offset:", start_offset, \
#            "Messages:\n\t", messages, "\nExpected:\n\t", \
#            expected_messages
        self.assertEqual(messages, expected_messages)
        self.assertEquals(
            resp.highwaterMark, start_offset+len(expected_messages))
