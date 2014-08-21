import os

import nose.twistedtools
from twisted.internet.defer import inlineCallbacks

from afkak.common import (
    FetchRequest, OffsetFetchRequest, OffsetCommitRequest,
    ProduceRequest, OffsetRequest,
    )
from afkak.kafkacodec import (create_message)
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import (
    kafka_versions, KafkaIntegrationTestCase,
    )


class TestKafkaClientIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        nose.twistedtools.threaded_reactor()

    @classmethod
    def tearDownClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server.close()
        cls.zk.close()

        # Shutdown the twisted reactor
        nose.twistedtools.stop_reactor()

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=100)
    @inlineCallbacks
    def test_consume_none(self):
        fetch = FetchRequest(self.topic, 0, 0, 1024)

        fetch_resp, = yield self.client.send_fetch_request([fetch])
        self.assertEquals(fetch_resp.error, 0)
        self.assertEquals(fetch_resp.topic, self.topic)
        self.assertEquals(fetch_resp.partition, 0)

        messages = list(fetch_resp.messages)
        self.assertEquals(len(messages), 0)

    @kafka_versions("all")
    @nose.twistedtools.deferred(timeout=100)
    @inlineCallbacks
    def test_produce_request(self):
        produce = ProduceRequest(
            self.topic, 0,
            [create_message(self.topic + " message %d" % i)
             for i in range(5)])

        produce_resp, = yield self.client.send_produce_request([produce])
        self.assertEquals(produce_resp.error, 0)
        self.assertEquals(produce_resp.topic, self.topic)
        self.assertEquals(produce_resp.partition, 0)
        self.assertEquals(produce_resp.offset, 0)

    ####################
    #   Offset Tests   #
    ####################

    @kafka_versions("0.8.1")
    @nose.twistedtools.deferred(timeout=100)
    @inlineCallbacks
    def test_send_offset_request(self):
        req = OffsetRequest(self.topic, 0, -1, 100)
        (resp,) = yield self.client.send_offset_request([req])
        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.topic, self.topic)
        self.assertEquals(resp.partition, 0)
        self.assertEquals(resp.offsets, (0,))

    @kafka_versions("0.8.1")
    @nose.twistedtools.deferred(timeout=100)
    @inlineCallbacks
    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequest(self.topic, 0, 42, "metadata")
        (resp,) = yield self.client.send_offset_commit_request("group", [req])
        self.assertEquals(resp.error, 0)

        req = OffsetFetchRequest(self.topic, 0)
        (resp,) = yield self.client.send_offset_fetch_request("group", [req])
        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.offset, 42)
        self.assertEquals(resp.metadata, "")  # Metadata isn't stored for now
