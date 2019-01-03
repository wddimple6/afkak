# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2017, 2018, 2019 Ciena Corporation
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

"""
Test code for KafkaClient class.
"""

from __future__ import absolute_import, division

import logging
import struct
from copy import copy
from functools import partial
from itertools import cycle

from mock import ANY, MagicMock, Mock, patch
from twisted.internet.defer import Deferred, succeed
from twisted.internet.error import ConnectError, ConnectionLost, UserError
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial import unittest

from .. import KafkaClient
from ..brokerclient import _KafkaBrokerClient
from ..client import _normalize_hosts
from ..client import log as client_log
from ..common import (
    BrokerMetadata, ConsumerCoordinatorNotAvailableError, FailedPayloadsError,
    FetchRequest, FetchResponse, KafkaUnavailableError,
    LeaderNotAvailableError, LeaderUnavailableError,
    NotCoordinatorForConsumerError, NotLeaderForPartitionError,
    OffsetAndMessage, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, OffsetResponse,
    PartitionMetadata, PartitionUnavailableError, ProduceRequest,
    ProduceResponse, RequestTimedOutError, TopicAndPartition, TopicMetadata,
    UnknownError, UnknownTopicOrPartitionError,
)
from ..kafkacodec import KafkaCodec, create_message
from .endpoints import BlackholeEndpoint, Connections, FailureEndpoint
from .logtools import capture_logging
from .testutil import first

log = logging.getLogger(__name__)


def createMetadataResp():
    from .test_kafkacodec import create_encoded_metadata_response
    node_brokers = {
        0: BrokerMetadata(0, "brokers1.afkak.example.com", 1000),
        1: BrokerMetadata(1, "brokers1.afkak.example.com", 1001),
        3: BrokerMetadata(3, "brokers2.afkak.example.com", 1000),
    }

    topic_partitions = {
        "topic1": TopicMetadata(
            'topic1', 0, {
                0: PartitionMetadata("topic1", 0, 0, 1, (0, 2), (2,)),
                1: PartitionMetadata("topic1", 1, 1, 3, (0, 1), (0, 1)),
            },
        ),
        "topic2": TopicMetadata(
            'topic2', 0, {
                0: PartitionMetadata("topic2", 0, 0, 0, (), ()),
            },
        ),
        "topic3": TopicMetadata('topic3', 5, {}),
    }
    return create_encoded_metadata_response(node_brokers, topic_partitions)


def brkrAndReqsForTopicAndPartition(client, topic, part=0):
    """
    Helper function to dig out the outstanding request so we can
    "send" the response to it. (fire the deferred)
    """
    broker = client.topics_to_brokers[TopicAndPartition(topic, part)]
    brokerClient = client._get_brokerclient(broker.node_id)
    return (brokerClient, brokerClient.requests)


class TestKafkaClient(unittest.TestCase):
    testMetaData = createMetadataResp()

    def shortDescription(self):
        """
        Show the ID of the test when nose displays its name, rather than
        a snippet of the docstring.
        """
        return self.id()

    def client_with_metadata(self, brokers, topics={}):
        """
        :param brokers: Broker metadata to load
        :type brokers: List[BrokerMetadata]

        :param topics:
            Map of topic names to partition count. Partition numbers from 0..n
            will be assigned in the generated metadata. The replica and ISR
            lists for each partition will be empty.
        :type topics: Dict[str, int]
        """
        from .test_kafkacodec import create_encoded_metadata_response

        broker_map = {bm.node_id: bm for bm in brokers}
        broker_id_seq = cycle(broker_map.keys())

        topic_map = {}
        for topic, partition_count in topics.items():
            topic_map[topic] = TopicMetadata(topic, 0, {
                p: PartitionMetadata(
                    topic=topic,
                    partition=p,
                    partition_error_code=0,  # no error
                    leader=next(broker_id_seq),
                    replicas=(),
                    isr=(),
                )
                for p in range(partition_count)
            })

        reactor = Clock()
        connections = Connections()
        client = KafkaClient(hosts='bootstrap:9092', reactor=reactor, endpoint_factory=connections)

        d = client.load_metadata_for_topics()
        conn = connections.accept('bootstrap')
        connections.flush()
        request = self.successResultOf(conn.server.expectRequest(KafkaCodec.METADATA_KEY, 0, ANY))
        request.respond(create_encoded_metadata_response(broker_map, topic_map)[4:])
        connections.flush()
        self.assertTrue(self.successResultOf(d))

        return reactor, connections, client

    def getLeaderWrapper(self, c, *args, **kwArgs):
        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            d = c._get_leader_for_partition(*args)

        if 'errs' not in kwArgs:
            return self.successResultOf(d)
        return self.failureResultOf(d, kwArgs['errs'])

    def test_repr(self):
        c = KafkaClient('1.kafka.internal, 2.kafka.internal:9910', clientId='MyClient')
        self.assertEqual((
            "<KafkaClient clientId=MyClient"
            " hosts=1.kafka.internal:9092 2.kafka.internal:9910"
            " timeout=10.0>",
        ), repr(c))

    def test_client_bad_timeout(self):
        with self.assertRaises(TypeError):
            KafkaClient('kafka.example.com', clientId='MyClient', timeout="100ms")

    def test_update_cluster_hosts(self):
        c = KafkaClient(hosts='www.example.com')
        c.update_cluster_hosts('meep.org')
        self.assertEqual(c._bootstrap_hosts, [('meep.org', 9092)])

    def test_send_broker_unaware_request_bootstrap_fail(self):
        """
        Broker unaware requests fail with `KafkaUnavailableError` when boostrap
        fails.

        This scenario makes two connection attempts in random order, one for
        each configured bootstrap host. Both fail.
        """
        client = KafkaClient(
            hosts=['kafka01:9092', 'kafka02:9092'],
            reactor=MemoryReactorClock(),
            # Every connection attempt will immediately fail due to this
            # endpoint, including attempts to bootstrap.
            endpoint_factory=FailureEndpoint,
        )

        d = client.load_metadata_for_topics('sometopic')

        self.failureResultOf(d).trap(KafkaUnavailableError)

    def test_send_broker_unaware_request_brokers_fail(self):
        """
        Broker unaware requests fail with `KafkaUnavailableError` when none of
        the clients can answer requests and the fallback bootstrap also fails.

        In this scenario five connection attempts occur:

        1. A bootstrap request to get cluster metadata. This succeeds and
           returns topic metadata which references two brokers (we will call
           these "topic brokers").
        2. A request to a topic broker. We fail this by timeout.
        3. A request to the other topic broker. We fail this by timeout.
        4. A fallback bootstrap request to a bootstrap broker. We fail this.
        5. A fallback bootstrap request to the other bootstrap broker. We fail
           this.

        At this point all brokers have been tried so the request fails with
        a `KafkaUnavailableError`.
        """
        from .test_kafkacodec import create_encoded_metadata_response

        reactor = MemoryReactorClock()
        conns = Connections()
        client = KafkaClient(
            hosts='bootstrap',
            timeout=10 * 1000,  # 10 seconds in ms
            reactor=reactor,
            endpoint_factory=conns,
            # We want to hit the request timeout to test failover behavior, so
            # this retry policy returns a delay much longer than the timeout.
            retry_policy=lambda failures: 100.0,
        )
        topic_brokers = [
            BrokerMetadata(1, 'broker1', 9092),
            BrokerMetadata(2, 'broker2', 9092),
        ]

        # Bootstrap the client.
        bootstrap_d = client.load_metadata_for_topics('bootstrap_topic')
        self.assertNoResult(bootstrap_d)
        conn = conns.accept('bootstrap')
        conn.pump.flush()
        request = self.successResultOf(conn.server.requests.get())
        request.respond(create_encoded_metadata_response(
            {b.node_id: b for b in topic_brokers},
            {},  # No topic metadata.
        )[4:])
        conn.pump.flush()
        self.assertTrue(self.successResultOf(bootstrap_d))
        self.assertEqual({1, 2}, client._brokers.keys())

        # Now hit all the failure paths.
        client.update_cluster_hosts('bootstrap1,bootstrap2')
        d = client.load_metadata_for_topics('sometopic')
        self.assertNoResult(d)

        conns.fail('broker?', ConnectError())
        # The broker client will wait and retry until the request times out
        # (probably should change this for unaware requests) so advance the
        # clock up to the timeout.
        reactor.pump([1.0] * 10)
        self.assertNoResult(d)
        conns.accept('broker?').client.connectionLost(Failure(ConnectionLost()))
        reactor.pump([1.0] * 10)  # Again with the timeout.
        self.assertNoResult(d)

        # Now it falls back to the bootstrap hosts.
        conns.accept('bootstrap?').client.connectionLost(Failure(ConnectionLost()))
        self.assertNoResult(d)
        conns.fail('bootstrap?', ConnectError())

        self.failureResultOf(d).check(KafkaUnavailableError)

        self.successResultOf(client.close())

    def test_make_request_to_broker_alerts_when_blocked(self):
        """
        A blocked reactor will cause an error to be logged.
        """
        reactor, connections, client = self.client_with_metadata(
            brokers=[BrokerMetadata(0, 'kafka', 9092)],
            topics={"topic": 1},
        )

        with capture_logging(client_log, logging.WARNING) as records:
            d = client.send_produce_request(
                [ProduceRequest("topic", 0, [create_message(b"a")])],
            )

            reactor.advance(client.timeout + 1)  # fire the timeout warning errback

        [record] = [r for r in records if r.msg == 'Reactor was starved for %r seconds']
        self.assertEqual(record.args, (client.timeout + 1,))

        self.failureResultOf(d)

    def test_load_metadata_for_topics(self):
        """
        A broker unaware request succeeds if at least one bootstrap host is
        available.
        """
        from .test_kafkacodec import create_encoded_metadata_response

        reactor = Clock()
        conns = Connections()
        client = KafkaClient(
            hosts='kafka1,kafka2,kafka3',
            reactor=reactor,
            endpoint_factory=conns,
        )

        d = client.load_metadata_for_topics('topic1', 'topic2', 'topic3', 'topic4')

        conns.fail('kafka?', ConnectError())
        conns.accept('kafka?').client.connectionLost(Failure(ConnectionLost()))
        conn = conns.accept('kafka?')
        conn.pump.flush()
        request = self.successResultOf(conn.server.expectRequest(
            api_key=KafkaCodec.METADATA_KEY,
            api_version=0,
            correlation_id=1,
        ))

        self.assertEqual(request.rest, (
            b'\x00\x00\x00\x04'
            b'\x00\x06topic1\x00\x06topic2\x00\x06topic3\x00\x06topic4'
        ))
        response_with_api_prefix = create_encoded_metadata_response(
            {
                1: BrokerMetadata(1, 'kafka1', 9092),
                2: BrokerMetadata(2, 'kafka2', 9092),
                3: BrokerMetadata(3, 'kafka3', 9092),
            },
            {
                "topic1": TopicMetadata(
                    'topic1', 0, {
                        0: PartitionMetadata("topic1", 0, 0, 1, (0, 2), (2,)),
                        1: PartitionMetadata("topic1", 1, 1, 3, (0, 1), (0, 1)),
                    },
                ),
                "topic2": TopicMetadata(
                    'topic2', 0, {
                        # A partition with no replicas or ISRs.
                        0: PartitionMetadata("topic2", 0, 0, 2, (), ()),
                    },
                ),
                "topic3": TopicMetadata('topic3', LeaderNotAvailableError.errno, {}),
                # This topic has partitions which lack a leader.
                "topic4": TopicMetadata('topic4', UnknownError.errno, {
                    0: PartitionMetadata('topic4', 0, 0, -1, [], []),
                    1: PartitionMetadata('topic4', 1, 0, -1, [], []),
                }),
            },
        )
        request.respond(response_with_api_prefix[4:])  # Strip off prefix.
        conn.pump.flush()
        self.assertTrue(self.successResultOf(d))

        self.assertEqual(client._brokers, {
            1: BrokerMetadata(1, 'kafka1', 9092),
            2: BrokerMetadata(2, 'kafka2', 9092),
            3: BrokerMetadata(3, 'kafka3', 9092),
        })
        self.assertEqual(client.topics_to_brokers, {
            TopicAndPartition('topic1', 0): BrokerMetadata(1, 'kafka1', 9092),
            TopicAndPartition('topic1', 1): BrokerMetadata(3, 'kafka3', 9092),
            TopicAndPartition('topic2', 0): BrokerMetadata(2, 'kafka2', 9092),
            TopicAndPartition('topic4', 0): None,
            TopicAndPartition('topic4', 1): None,
        })
        # TODO more assertions about datastructures

    def test_get_leader_for_partitions_reloads_metadata(self):
        """
        test_get_leader_for_partitions_reloads_metadata
        Get leader for partitions reload metadata if it is not available
        """
        # Create a client to test
        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
        # Setup the client with the metadata we want it to have
        brokers = [
            BrokerMetadata(node_id=0, host='broker_1', port=4567),
            BrokerMetadata(node_id=1, host='broker_2', port=5678),
        ]
        client.topic_partitions = {}
        client.topics_to_brokers = {}
        # topic metadata is loaded but empty
        self.assertDictEqual({}, client.topics_to_brokers)

        T1 = 'topic_no_partitions'

        def fake_lmdft(self, *topics):
            client.topics_to_brokers = {
                TopicAndPartition(topic=T1, partition=0): brokers[0],
            }
            return succeed(None)

        with patch.object(KafkaClient, 'load_metadata_for_topics',
                          side_effect=fake_lmdft) as lmdft:
            # Call _get_leader_for_partition and ensure it
            # calls load_metadata_for_topics
            leader = client._get_leader_for_partition('topic_no_partitions', 0)
            self.assertEqual(brokers[0], self.successResultOf(leader))
            lmdft.assert_called_once_with('topic_no_partitions')

    @patch('afkak.client.KafkaCodec')
    def test_get_leader_for_unassigned_partitions(self, kCodec):
        """
        test_get_leader_for_unassigned_partitions
        Get leader raises if no partitions is defined for a topic
        """

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {
            'topic_no_partitions': TopicMetadata('topic_no_partitions', 0, {}),
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)

        self.assertDictEqual({}, client.topics_to_brokers)

        key = TopicAndPartition('topic_no_partitions', 0)
        eFail = PartitionUnavailableError(
            "{} not available".format(str(key)))

        fail1 = self.getLeaderWrapper(client, 'topic_no_partitions', 0,
                                      errs=PartitionUnavailableError)
        # Make sure the error msg is correct
        self.assertEqual(type(eFail), fail1.type)
        self.assertEqual(eFail.args, fail1.value.args)

    @patch('afkak.client.KafkaCodec')
    def test_get_leader_returns_none_when_noleader(self, kCodec):
        """
        test_get_leader_returns_none_when_noleader
        Confirm that _get_leader_for_partition() returns None when
        the partiion has no leader.
        Test by creating a client, patch afkak.client.KafkaCodec to return
        our special metadata, patch _send_broker_unaware_request to avoid
        making a connection, and then confirm that None is returned.
        """

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {}
        topics['topic_noleader'] = TopicMetadata('topic_noleader', 0, {
            0: PartitionMetadata('topic_noleader', 0, 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, 0, -1, [], []),
        })
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567', 'broker_2:5678'],
                             timeout=None)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client._get_leader_for_partition('topic_noleader', 0)
        self.assertEqual({
            TopicAndPartition('topic_noleader', 0): None,
            TopicAndPartition('topic_noleader', 1): None,
        }, client.topics_to_brokers)

        self.assertIsNone(self.getLeaderWrapper(client, 'topic_noleader', 0))
        self.assertIsNone(self.getLeaderWrapper(client, 'topic_noleader', 1))

        topics['topic_noleader'] = TopicMetadata('topic_noleader', 0, {
            0: PartitionMetadata('topic_noleader', 0, 0, 0, [0, 1], [0, 1]),
            1: PartitionMetadata('topic_noleader', 1, 0, 1, [1, 0], [1, 0]),
        })
        kCodec.decode_metadata_response.return_value = (brokers, topics)
        self.assertEqual(brokers[0], self.getLeaderWrapper(client, 'topic_noleader', 0))
        self.assertEqual(brokers[1], self.getLeaderWrapper(client, 'topic_noleader', 1))

    @patch('afkak.client.KafkaCodec')
    def test_send_produce_request_raises_when_noleader(self, kCodec):
        """
        test_send_produce_request_raises_when_noleader
        Send producer request raises LeaderUnavailableError if
        leader is not available
        """

        brokers = {
            0: BrokerMetadata(0, b'broker_1', 4567),
            1: BrokerMetadata(1, b'broker_2', 5678),
        }

        topics = {
            'topic_noleader': TopicMetadata('topic_noleader', 0, {
                0: PartitionMetadata('topic_noleader', 0, 0, -1, [], []),
                1: PartitionMetadata('topic_noleader', 1, 0, -1, [], []),
            }),
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)

            # create a list of requests (really just one)
            requests = [
                ProduceRequest("topic_noleader", 0,
                               [create_message(b"a"), create_message(b"b")])]
            # Attempt to send it, and ensure the returned deferred fails
            # properly
            fail1 = client.send_produce_request(requests)
            self.successResultOf(
                self.failUnlessFailure(fail1, LeaderUnavailableError))

    def test_get_brokerclient(self):
        """
        _get_brokerclient generates _KafkaBrokerClient objects based on cached
        broker metadata.
        """
        client = KafkaClient(
            hosts=['broker_1:4567', 'broker_2', 'broker_3:45678'],
            timeout=None,
            endpoint_factory=FailureEndpoint,  # Should not be used.
        )
        # Set the metadat cache as if bootstrap has completed:
        client._brokers = {
            1: BrokerMetadata(1, 'broker_1', 4567),
            2: BrokerMetadata(2, 'broker_2', 9092),
            3: BrokerMetadata(3, 'broker_3', 45678),
        }

        b1 = client._get_brokerclient(1)
        self.assertEqual('broker_1', b1.host)
        self.assertEqual(4567, b1.port)

        # The same broker client is returned each time.
        b2 = client._get_brokerclient(2)
        self.assertIs(b2, client._get_brokerclient(2))

        # Get another one...
        b3 = client._get_brokerclient(3)
        self.assertEqual(BrokerMetadata(3, 'broker_3', 45678), b3.brokerMetadata)

        self.assertIs(b1, client._get_brokerclient(1))

        self.assertRaises(KeyError, client._get_brokerclient, 4)

    @patch('afkak.client._KafkaBrokerClient')
    def test_update_brokers(self, broker):
        """
        test_update_brokers
        Test that we create/close brokers as they come and go in the metadata
        """
        # Create 6 Mocks to act as brokerclients. The first three we manually
        # assign, the next 3 will be returned as side-effects of calls to the
        # constructor for _KafkaBrokerClient as setup with the patch and below
        brokermocks = [Mock(**{'close.return_value': Deferred()}) for i in range(6)]
        broker.side_effect = brokermocks[3:]

        client = KafkaClient(
            hosts=['broker_1:4567', 'broker_2', 'broker_3:45678'])
        client.clients = {('broker_1', 4567): brokermocks[0],
                          ('broker_2', 9092): brokermocks[1],
                          ('broker_3', 45678): brokermocks[2]}

        # Keep track of the before load_metadata_for_topics clients dict
        beforeClients = client.clients.copy()

        # Replace brokers with 'brokersX.afkak.example.com:100X' from way above
        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client.load_metadata_for_topics()

        # Make sure the old brokers got 'close()'ed
        # But we don't callback() all the close deferreds yet
        for brkr in beforeClients.values():
            brkr.close.assert_called_once_with()

        # Complete the close of 2 of the 3
        for brkr in list(beforeClients.values())[:2]:
            # Use the deferred as the result for the callback strictly
            # for tracking purposes in debugging...
            brkr.close.return_value.callback(id(brkr.close.return_value))

        # Now remove one of the current clients
        new_clients = list(client.clients.keys())
        removed = client.clients[new_clients.pop()]
        client._update_brokers(new_clients, remove=True)
        # Removed should have been 'close()'d
        removed.close.assert_called_once_with()

        # Callback remaining 'beforeClient' and 'removed' to clear close_dlist
        # Use errback() for one to make sure the client correctly eats that err
        removed.close.return_value.callback(id(removed.close.return_value))

        # Should not have been cleared due to overlapping closing sequence
        self.assertNotEqual(client.close_dlist, None)

        # Callback the final outstanding close deferred
        # XXX This test appears to rely on dict enumeration order being stable.
        list(beforeClients.values())[2].close.return_value.callback(
            Failure(ConnectionLost()))
        # Now it should be cleared, as all outstanding closes have completed
        self.assertEqual(client.close_dlist, None)

        # At this point, there are two remaining brokers, we'll remove one
        # via update, and then close the client to test the handling of a
        # nested deferredlist in client.close()
        final_clients = list(client.clients.keys())
        removed = client.clients[final_clients.pop()]
        client._update_brokers(final_clients, remove=True)
        # Removed should have been 'close()'d
        removed.close.assert_called_once_with()

        # close the client and make sure the last brokers are closed
        last_client = list(client.clients.values())[0]
        d = client.close()
        # The last client should have been closed
        last_client.close.assert_called_once_with()
        # Use the id of the deferred as the callback for debug/tracking
        res = id(last_client.close.return_value)
        last_client.close.return_value.callback(res)

        # The 'removed' brokerclient's close deferred result won't make
        # it through the deferredList callback handler, so we just use True
        removed.close.return_value.callback(True)

        # Make sure the client.close callback got called
        self.assertEqual(None, self.successResultOf(d))

    def test_send_broker_aware_request(self):
        """
        test_send_broker_aware_request
        Test that send_broker_aware_request returns the proper responses
        when given the correct data
        """
        T1 = "Topic1"
        T2 = "Topic2"

        client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want it to have
        brokers = [
            BrokerMetadata(node_id=1, host='kafka01', port=9092),
            BrokerMetadata(node_id=2, host='kafka02', port=9092),
        ]
        topic_partitions = {
            T1: [0],
            T2: [0],
        }
        client.topic_partitions = copy(topic_partitions)
        topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=0): brokers[0],
            TopicAndPartition(topic=T2, partition=0): brokers[1],
        }
        client.topics_to_brokers = copy(topics_to_brokers)

        # Setup the payloads, encoder & decoder funcs
        payloads = [
            ProduceRequest(
                T1, 0, [create_message(T1.encode() + b" message %d" % i)
                        for i in range(10)]),
            ProduceRequest(
                T2, 0, [create_message(T2.encode() + b" message %d" % i)
                        for i in range(5)]),
        ]

        encoder = partial(
            KafkaCodec.encode_produce_request,
            acks=1, timeout=1000)
        decoder = KafkaCodec.decode_produce_response

        # patch the _KafkaBrokerClient so it doesn't really connect
        with patch.object(_KafkaBrokerClient, '_connect'):
            respD = client._send_broker_aware_request(
                payloads, encoder, decoder)
        # Shouldn't have a result yet. If we do, there was an error
        self.assertNoResult(respD)

        # Dummy up some responses, one for each broker.
        resp0 = struct.pack('>ih%dsiihq' % (len(T1)),
                            1, len(T1), T1.encode(), 1, 0, 0, 10)
        resp1 = struct.pack('>ih%dsiihq' % (len(T2)),
                            1, len(T2), T2.encode(), 1, 0, 0, 20)

        # "send" the results
        for topic, resp in ((T1, resp0), (T2, resp1)):
            brkr, reqs = brkrAndReqsForTopicAndPartition(client, topic)
            for req in reqs.values():
                brkr.handleResponse(struct.pack('>i', req.id) + resp)

        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results,
                         [ProduceResponse(T1, 0, 0, 10),
                          ProduceResponse(T2, 0, 0, 20)])

        # Now try again, but with one request failing...
        with patch.object(_KafkaBrokerClient, '_connect'):
            respD = client._send_broker_aware_request(
                payloads, encoder, decoder)

        # dummy responses
        resp0 = struct.pack('>ih%dsiihq' % (len(T1)),
                            1, len(T1), T1.encode(), 1, 0, 0, 10)
        resp1 = struct.pack('>ih%dsiihq' % (len(T2)),
                            1, len(T2), T2.encode(), 1, 0, 7, 20)
        # 'send' the response for T1 request
        brkr, reqs = brkrAndReqsForTopicAndPartition(client, T1)
        for req in reqs.values():
            brkr.handleResponse(struct.pack('>i', req.id) + resp0)

        # cancel the request for T2 request (timeout eailure)
        brkr, reqs = brkrAndReqsForTopicAndPartition(client, T2)
        for req in reqs.values():
            brkr.cancelRequest(req.id)

        # check the result. Should be Failure(FailedPayloadsError)
        results = self.failureResultOf(respD, FailedPayloadsError)
        # And the exceptions args should hold the payload of the
        # failed request
        self.assertEqual(results.value.args[1][0][0], payloads[1])

        # above failure reset the client's metadata, so we need to re-install
        client.topic_partitions = copy(topic_partitions)
        client.topics_to_brokers = copy(topics_to_brokers)

        # And finally, without expecting a response...
        for brkr in client.clients.values():
            brkr.proto = MagicMock()
        respD = client._send_broker_aware_request(payloads, encoder, None)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results, [])

    def test_reset_topic_metadata(self):
        """
        Test that reset_topic_metadata makes the proper changes
        to the client's metadata
        """
        client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want start with
        Ts = ["Topic1", "Topic2", "Topic3", "Topic4"]
        brokers = [
            BrokerMetadata(node_id=1, host='kafka01', port=9092),
            BrokerMetadata(node_id=2, host='kafka02', port=9092),
        ]
        client.topic_partitions = {
            Ts[0]: [0],
            Ts[1]: [0],
            Ts[2]: [0, 1, 2, 3],
            Ts[3]: [],
        }
        client.topic_errors = {
            Ts[0]: 0,
            Ts[1]: 0,
            Ts[2]: 0,
            Ts[3]: 5,
        }
        client.topics_to_brokers = {
            TopicAndPartition(topic=Ts[0], partition=0): brokers[0],
            TopicAndPartition(topic=Ts[1], partition=0): brokers[1],
            TopicAndPartition(topic=Ts[2], partition=0): brokers[0],
            TopicAndPartition(topic=Ts[2], partition=1): brokers[1],
            TopicAndPartition(topic=Ts[2], partition=2): brokers[0],
            TopicAndPartition(topic=Ts[2], partition=3): brokers[1],
        }

        # make copies...
        tParts = copy(client.topic_partitions)
        topicsToBrokers = copy(client.topics_to_brokers)

        # Check if we can get the error code. This should maybe have a separate
        # test, but the setup is a pain, and it's all done here...
        self.assertEqual(client.metadata_error_for_topic(Ts[3]), 5)
        client.reset_topic_metadata(Ts[3])
        self.assertEqual(client.metadata_error_for_topic(Ts[3]),
                         UnknownTopicOrPartitionError.errno)

        # Reset the client's metadata for Topic2
        client.reset_topic_metadata(Ts[1])

        # No change to brokers...
        # topics_to_brokers gets every (topic,partition) tuple removed
        # for that topic
        del topicsToBrokers[TopicAndPartition(topic=Ts[1], partition=0)]
        # topic_paritions gets topic deleted
        del tParts[Ts[1]]
        del tParts[Ts[3]]
        # Check correspondence
        self.assertEqual(topicsToBrokers, client.topics_to_brokers)
        self.assertEqual(tParts, client.topic_partitions)

        # Resetting an unknown topic has no effect
        client.reset_topic_metadata("bogus")
        # Check correspondence with unchanged copies
        self.assertEqual(topicsToBrokers, client.topics_to_brokers)
        self.assertEqual(tParts, client.topic_partitions)

    def test_has_metadata_for_topic(self):
        client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want start with
        Ts = [u"Topic1", u"Topic2", u"Topic3"]
        brokers = [
            BrokerMetadata(node_id=1, host='kafka01', port=9092),
            BrokerMetadata(node_id=2, host='kafka02', port=9092),
        ]
        client.topic_partitions = {
            Ts[0]: [0],
            Ts[1]: [0],
            Ts[2]: [0, 1, 2, 3],
        }
        client.topics_to_brokers = {
            TopicAndPartition(topic=Ts[0], partition=0): brokers[0],
            TopicAndPartition(topic=Ts[1], partition=0): brokers[1],
            TopicAndPartition(topic=Ts[2], partition=0): brokers[0],
            TopicAndPartition(topic=Ts[2], partition=1): brokers[1],
            TopicAndPartition(topic=Ts[2], partition=2): brokers[0],
            TopicAndPartition(topic=Ts[2], partition=3): brokers[1],
        }

        for topic in Ts:
            self.assertTrue(client.has_metadata_for_topic(topic))
        self.assertFalse(client.has_metadata_for_topic("Unknown"))

    def test_reset_all_metadata(self):
        brokers = [
            BrokerMetadata(node_id=1, host='kafka01', port=9092),
            BrokerMetadata(node_id=2, host='kafka02', port=9092),
        ]
        reactor, connections, client = self.client_with_metadata(
            brokers, topics={"Topic1": 1, "Topic2": 1, "Topic3": 4},
        )

        # FIXME: Patch this in the client with the metadata we want start with
        client.consumer_group_to_brokers = {
            u'ConsumerGroup1': brokers[1],
        }

        self.assertTrue(client.has_metadata_for_topic('Topic1'))
        self.assertTrue(client.has_metadata_for_topic('Topic2'))
        self.assertTrue(client.has_metadata_for_topic('Topic3'))
        self.assertFalse(client.has_metadata_for_topic("Unknown"))

        client.reset_all_metadata()

        self.assertEqual(client.topics_to_brokers, {})
        self.assertEqual(client.topic_partitions, {})
        self.assertEqual(client.consumer_group_to_brokers, {})

    def test_client_close(self):
        """
        close() terminates any outstanding broker connections, ignoring any
        errors.
        """
        reactor, connections, client = self.client_with_metadata(brokers=[
            BrokerMetadata(1, 'kafka0', 9092),
            BrokerMetadata(2, 'kafka2', 9092),
        ])

        request_d = client.load_metadata_for_topics()
        conn1 = connections.accept('kafka*')
        reactor.advance(client.timeout)  # Time out first attempt, try second broker.
        self.assertNoResult(request_d)
        conn2 = connections.accept('kafka*')
        conn2.client.transport.disconnectReason = UserError()

        close_d = client.close()
        self.assertNoResult(close_d)  # Waiting for connection shutdown.

        connections.flush()  # Complete connection shutdown.
        self.assertIs(None, self.successResultOf(close_d))
        self.assertTrue(conn1.server.transport.disconnected)
        self.assertTrue(conn2.server.transport.disconnected)
        # FIXME Afkak #71: The bootstrap retry loop leaves delayed calls (for timeouts)
        # in the reactor.
        self.assertEqual([], reactor.getDelayedCalls())

    def test_client_close_no_clients(self):
        client = KafkaClient(
            hosts='kafka',
            reactor=Clock(),
            endpoint_factory=BlackholeEndpoint,
        )
        self.successResultOf(client.close())
        self.assertEqual([], client.reactor.getDelayedCalls())

    def test_client_close_poisons(self):
        """
        Once close() has been called the client cannot be used to make further
        requests.
        """
        reactor, connections, client = self.client_with_metadata(brokers=[
            BrokerMetadata(1, 'kakfa1', 9092),
        ])

        self.successResultOf(client.close())

        self.failureResultOf(client.load_metadata_for_topics())
        # FIXME Afkak #71: Reject new requests with a clean exception. We
        # currently get a KafkaUnavailableError that wraps an AttributeError
        # due to self.clients being None.
        # self.failureResultOf(client.load_metadata_for_topics(), ClientError)

    def test_client_close_during_metadata_load(self):
        reactor = Clock()
        client = KafkaClient(
            reactor=reactor,
            hosts='kafka',
            # Every connection attempt hangs forever.
            endpoint_factory=BlackholeEndpoint,
        )
        load_d = client.load_metadata_for_topics()

        self.assertIs(None, self.successResultOf(client.close()))
        self.assertEqual([], reactor.getDelayedCalls())

        # XXX: This API doesn't make sense. This deferred should fail with an
        # exception like ClientClosed, not succeed as if metadata has loaded.
        self.assertIs(None, self.successResultOf(load_d))

    def test_load_consumer_metadata_for_group(self):
        """
        Test that a subsequent load request for the same group before the
        the previous one has completed does not make a new request, and that
        a request for a different group does make a new request. Also, check
        That once the request completes for 'Group1', that subsequent requests
        will make a new request.
        """
        G1 = u"ConsumerGroup1"
        G2 = u"ConsumerGroup2"
        response = b"".join([
            struct.pack('>i', 4),           # Correlation ID
            struct.pack('>h', 0),           # Error Code
            struct.pack('>i', 0),           # Coordinator id
            struct.pack('>h', len(b"host1")), b"host1",  # The Coordinator host
            struct.pack('>i', 9092),          # The Coordinator port
        ])
        request_ds = [Deferred(), Deferred(), Deferred()]

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=request_ds):
            client = KafkaClient(hosts='host1:9092')
            load1_d = client.load_consumer_metadata_for_group(G1)
            load2_d = client.load_consumer_metadata_for_group(G2)
            load3_d = client.load_consumer_metadata_for_group(G1)
            # Request 1 & 3 should return the same deferred.
            # Request 2 should be distinct
            self.assertEqual(request_ds[0], load1_d)
            self.assertEqual(load1_d, load3_d)
            self.assertEqual(request_ds[1], load2_d)
            # Now 'send' a response to the first/3rd requests
            request_ds[0].callback(response)
            # And check the client's consumer metadata got properly updated
            self.assertEqual({
                u'ConsumerGroup1': BrokerMetadata(node_id=0, host='host1', port=9092),
            }, client.consumer_group_to_brokers)

            # After response, new request for same group gets new deferred
            load4_d = client.load_consumer_metadata_for_group(G1)
            self.assertNotEqual(load1_d, load4_d)
            self.assertEqual(request_ds[2], load4_d)

            # Clean up outstanding requests by sending same response
            request_ds[1].callback(response)
            request_ds[2].callback(response)
            for req_d in request_ds:
                self.assertTrue(self.successResultOf(req_d))
            client.close()

    def test_load_consumer_metadata_for_group_failure(self):
        """test_load_consumer_metadata_for_group_failure

        Test that a failure to retrieve the metadata for a group properly
        raises a ConsumerCoordinatorNotAvailableError exception
        """
        G1 = u"ConsumerGroup1"
        G2 = u"ConsumerGroup2"
        response = b"".join([
            struct.pack('>i', 6),           # Correlation ID
            struct.pack('>h', 15),          # Error Code
            struct.pack('>i', -1),          # Coordinator id
            struct.pack('>h', len(b"")), b"",    # The Coordinator host
            struct.pack('>i', -1),          # The Coordinator port
        ])

        d1 = Deferred()
        d2 = Deferred()
        request_ds = [d1, d2]

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=request_ds):
            client = KafkaClient(hosts='host1:9092')
            load1_d = client.load_consumer_metadata_for_group(G1)
            load2_d = client.load_consumer_metadata_for_group(G2)
            # Check we got the right ones back
            self.assertEqual(request_ds[0], load1_d)
            self.assertEqual(request_ds[1], load2_d)

            # Now 'send' an error response via errBack() to the first request
            request_ds[0].errback(KafkaUnavailableError('No Kafka Available'))
            # And callback the 2nd with a response with an error code
            request_ds[1].callback(response)
            for req_d in request_ds:
                self.assertTrue(self.failureResultOf(
                    req_d, ConsumerCoordinatorNotAvailableError))
            client.close()

    def test_send_produce_request(self):
        """
        send_produce_request routes payloads to the correct Kafka broker. This
        test covers several scenarios:

        1. Regular produce. The deferred fires once the broker responds.
        2. Produce with acks=0. No response is expected, so the deferred fires
           once the request has been written to the OS socket buffer.
        3. A regular produce that results in a NotLeaderForPartiton error. This
           has the side effect of clearing the topic's cached metadata.
        4. Another regular produce. Due to missing metadata a metadata request
           goes out first, then the actual requests.

        This should really be several separate tests.
        """
        from .test_kafkacodec import create_encoded_metadata_response

        T1 = u"Topic1"
        T2 = u"Topic2"

        connections = Connections()
        client = KafkaClient(hosts='kafka31:9092,kafka32:9092',
                             reactor=Clock(),
                             endpoint_factory=connections)

        # Setup the client with the metadata we want it to have
        brokers = [
            BrokerMetadata(node_id=1, host='kafka31', port=9092),
            BrokerMetadata(node_id=2, host='kafka32', port=9092),
        ]
        client._brokers.update({bm.node_id: bm for bm in brokers})
        client.topic_partitions = {
            T1: [0],
            T2: [0],
        }
        client.topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=0): brokers[0],
            TopicAndPartition(topic=T2, partition=0): brokers[1],
        }

        # Setup the payloads
        payloads = [
            ProduceRequest(
                topic=T1,
                partition=0,
                messages=[create_message("{} message {}".format(T1, i).encode('ascii'))
                          for i in range(10)],
            ),
            ProduceRequest(
                topic=T2,
                partition=0,
                messages=[create_message("{} message {}".format(T2, i).encode('ascii'))
                          for i in range(5)],
            ),
        ]

        respD = client.send_produce_request(payloads)

        # Accept the connection attemtps triggered by the request.
        conn31 = connections.accept('kafka31')
        conn32 = connections.accept('kafka32')

        # Dummy up some responses, one from each broker
        resp0 = struct.pack('>ih%dsiihq' % (len(T1)), 1, len(T1), T1.encode(), 1, 0, 0, 10)
        resp1 = struct.pack('>ih%dsiihq' % (len(T2)), 1, len(T2), T2.encode(), 1, 0, 0, 20)

        connections.flush()
        self.successResultOf(conn31.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY)).respond(resp0)
        self.successResultOf(conn32.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY)).respond(resp1)
        connections.flush()

        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results, [ProduceResponse(T1, 0, 0, 10), ProduceResponse(T2, 0, 0, 20)])

        respD = client.send_produce_request(payloads, acks=0)
        connections.flush()

        # These requests succeed as soon as they are written to a socket because of acks=0.
        self.assertEqual([], list(self.successResultOf(respD)))

        # Read the requests on the server side.
        self.successResultOf(conn31.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY))
        self.successResultOf(conn32.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY))

        # And again, this time with an error coming back...
        respD = client.send_produce_request(payloads)
        connections.flush()

        # Dummy up some responses, one from each broker
        resp0 = struct.pack('>ih%dsiihq' % len(T1), 1, len(T1), T1.encode(), 1, 0, 0, 10)
        resp1 = struct.pack('>ih%dsiihq' % len(T2), 1, len(T2), T2.encode(), 1, 0,
                            NotLeaderForPartitionError.errno, 20)
        self.successResultOf(conn31.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY)).respond(resp0)
        self.successResultOf(conn32.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY)).respond(resp1)
        connections.flush()

        self.failureResultOf(respD, NotLeaderForPartitionError)
        # *Only* the error'd topic's metadata was reset.
        self.assertTrue(client.has_metadata_for_topic(T1))
        self.assertFalse(client.has_metadata_for_topic(T2))

        # And again, this time with an error coming back...but ignored,
        # and a callback to pre-process the response
        def preprocCB(response):
            return response

        respD = client.send_produce_request(payloads, fail_on_error=False, callback=preprocCB)
        connections.flush()

        # Handle the metadata request. It may have gone to either broker.
        request = self.successResultOf(first([
            conn31.server.requests.get(),
            conn32.server.requests.get(),
        ]))
        request.respond(create_encoded_metadata_response(
            {bm.node_id: bm for bm in brokers},
            {T2: TopicMetadata(T2, 0, {0: PartitionMetadata(T2, 0, 0, 2, (1,), (1,))})},
        )[4:])
        connections.flush()

        # Metadata has been updated:
        self.assertTrue(client.has_metadata_for_topic(T2))

        # Now the ProduceRequest requests will be sent:
        connections.flush()

        # Dummy up some responses, one from each broker
        resp0 = struct.pack('>ih%dsiihq' % len(T1), 1, len(T1), T1.encode(), 1, 0, 0, 10)
        resp1 = struct.pack('>ih%dsiihq' % len(T2), 1, len(T2), T2.encode(), 1, 0,
                            NotLeaderForPartitionError.errno, 20)
        self.successResultOf(conn31.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY)).respond(resp0)
        self.successResultOf(conn32.server.expectRequest(KafkaCodec.PRODUCE_KEY, 0, ANY)).respond(resp1)
        connections.flush()

        results = list(self.successResultOf(respD))
        self.assertEqual(results, [ProduceResponse(T1, 0, 0, 10), ProduceResponse(T2, 0, 6, 20)])

    def test_send_produce_request_timeout(self):
        """
        send_produce_request fails with a FailedPayloadsError when payloads are
        sent, but fail. That failure wraps the details of the underlying
        failures.
        """
        reactor, connections, client = self.client_with_metadata(
            brokers=[BrokerMetadata(0, 'kafka', 9092)],
            topics={"topic": 1},
        )
        send_payload = ProduceRequest("topic", 0, [create_message(b"a")])

        d = client.send_produce_request([send_payload])
        reactor.advance(client.timeout)  # Time out the request.

        f = self.failureResultOf(d, FailedPayloadsError)
        self.assertEqual([], f.value.responses)
        [[failed_payload, failure]] = f.value.failed_payloads
        self.assertIs(send_payload, failed_payload)
        failure.trap(RequestTimedOutError)

    def test_send_fetch_request(self):
        """
        Test send_fetch_request
        """
        T1 = "Topic41"
        T2 = "Topic42"
        mocked_brokers = {
            1: MagicMock(),
            2: MagicMock(),
        }
        # inject broker side effects
        ds = [
            [Deferred(), Deferred(), Deferred(), Deferred()],
            [Deferred(), Deferred(), Deferred(), Deferred()],
        ]
        mocked_brokers[1].makeRequest.side_effect = ds[0]
        mocked_brokers[1].makeRequest.side_effect = ds[1]

        def mock_get_brkr(node_id):
            return mocked_brokers[node_id]

        client = KafkaClient(hosts='kafka41:9092,kafka42:9092')

        # Setup the client with the metadata we want it to have
        brokers = [
            BrokerMetadata(node_id=1, host='kafka41', port=9092),
            BrokerMetadata(node_id=2, host='kafka42', port=9092),
        ]
        client.topic_partitions = {
            T1: [0],
            T2: [0],
        }
        client.topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=0): brokers[0],
            TopicAndPartition(topic=T2, partition=0): brokers[1],
        }

        # Setup the payloads
        payloads = [FetchRequest(T1, 0, 0, 1024), FetchRequest(T2, 0, 0, 1024)]

        # patch the client so we control the brokerclients
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_fetch_request(payloads)

        # Dummy up some responses, the same from both brokers for simplicity
        msgs = [create_message(m) for m in
                [b"message1", b"hi", b"boo", b"foo", b"so fun!"]]
        ms1 = KafkaCodec._encode_message_set([msgs[0], msgs[1]], offset=0)
        ms2 = KafkaCodec._encode_message_set([msgs[2]], offset=0)
        ms3 = KafkaCodec._encode_message_set([msgs[3], msgs[4]], offset=48)

        # Note this includes a response for a topic/partition we didn't request
        # and further, that response has an error. However,
        # '_send_broker_aware_request' won't return it to us since it doesn't
        # match our request, so we don't error out.
        encoded = struct.pack('>iih%dsiihqi%dsihqi%dsh%dsiihqi%ds' %
                              (len(T1), len(ms1), len(ms2), len(T2), len(ms3)),
                              2345, 2,  # Correlation ID, Num Topics
                              len(T1), T1.encode(), 2, 0, 0, 10, len(ms1), ms1, 1,
                              1, 20, len(ms2), ms2,  # Topic41, 2 partitions,
                              # part0, no-err, high-water-mark-offset, msg1
                              # part1, err=1, high-water-mark-offset, msg1
                              len(T2), T2.encode(), 1, 0, 0, 30, len(ms3), ms3)
        # 'send' the responses
        ds[0][0].callback(encoded)
        ds[1][0].callback(encoded)
        # check the results
        results = list(self.successResultOf(respD))

        def expand_messages(response):
            return FetchResponse(response.topic, response.partition,
                                 response.error, response.highwaterMark,
                                 list(response.messages))
        expanded_responses = [expand_messages(r) for r in results]
        expect = [FetchResponse(T1, 0, 0, 10, [OffsetAndMessage(0, msgs[0]),
                                               OffsetAndMessage(1, msgs[1])]),
                  FetchResponse(T2, 0, 0, 30, [OffsetAndMessage(48, msgs[3]),
                                               OffsetAndMessage(49, msgs[4])])]
        self.assertEqual(expect, expanded_responses)

        # Again, with a callback
        def preprocCB(response):
            return response
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_fetch_request(payloads, callback=preprocCB)
        # 'send' the responses
        ds[0][1].callback(encoded)
        ds[1][1].callback(encoded)
        # check the results
        results = list(self.successResultOf(respD))
        expanded_responses = [expand_messages(r) for r in results]
        expect = [FetchResponse(T1, 0, 0, 10, [OffsetAndMessage(0, msgs[0]),
                                               OffsetAndMessage(1, msgs[1])]),
                  FetchResponse(T2, 0, 0, 30, [OffsetAndMessage(48, msgs[3]),
                                               OffsetAndMessage(49, msgs[4])])]
        self.assertEqual(expect, expanded_responses)

    def test_send_fetch_request_bad_timeout(self):
        client = KafkaClient(hosts='kafka41:9092,kafka42:9092')
        payload = [FetchRequest(u'T1', 0, 0, 1024)]
        timeout = client.timeout * 1000  # client.timeout=secs, max_wait=msecs
        d = client.send_fetch_request(payload, max_wait_time=timeout)
        self.failureResultOf(d, ValueError)

    def test_send_offset_request(self):
        """
        Test send_offset_request
        """
        T1 = "Topic51"
        T2 = "Topic52"

        connections = Connections()
        client = KafkaClient(hosts='kafka51:9092,kafka52:9092',
                             reactor=Clock(),
                             endpoint_factory=connections)

        # Setup the client with the metadata we want it to have
        brokers = [
            BrokerMetadata(node_id=1, host='kafka51', port=9092),
            BrokerMetadata(node_id=2, host='kafka52', port=9092),
        ]
        client._brokers.update({bm.node_id: bm for bm in brokers})
        client.topic_partitions = {
            T1: [0],
            T2: [0],
        }
        client.topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=0): brokers[0],
            TopicAndPartition(topic=T2, partition=0): brokers[1],
        }

        # Setup the payloads
        payloads = [
            OffsetRequest(T1, 0, -1, 20),  # ask for up to 20
            OffsetRequest(T2, 0, -2, 1),  # -2==earliest, only get 1
        ]

        respD = client.send_offset_request(payloads)

        # Dummy up some responses, one from each broker
        resp1 = b"".join([
            # struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topics
            struct.pack(">h", len(T1)), T1.encode(),   # First topic
            struct.pack(">i", 1),             # 1 partition

            struct.pack(">i", 0),             # Partition 0
            struct.pack(">h", 0),             # No error
            struct.pack(">i", 3),             # 3 offsets
            struct.pack(">q", 96),            # Offset 96
            struct.pack(">q", 98),            # Offset 98
            struct.pack(">q", 99),            # Offset 99
        ])
        resp2 = b"".join([
            # struct.pack(">i", 68),            # Correlation ID
            struct.pack(">i", 1),             # One topic
            struct.pack(">h", len(T2)), T2.encode(),   # First topic
            struct.pack(">i", 1),             # 1 partition

            struct.pack(">i", 0),             # Partition 0
            struct.pack(">h", 0),             # No error
            struct.pack(">i", 1),             # 1 offset
            struct.pack(">q", 4096),          # Offset 4096
        ])

        # send the responses
        conn51 = connections.accept('kafka51')
        conn52 = connections.accept('kafka52')
        connections.flush()
        self.successResultOf(conn51.server.expectRequest(KafkaCodec.OFFSET_KEY, 0, ANY)).respond(resp1)
        self.successResultOf(conn52.server.expectRequest(KafkaCodec.OFFSET_KEY, 0, ANY)).respond(resp2)
        connections.flush()

        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(set(results), set([
            OffsetResponse(topic=T1, partition=0, error=0, offsets=(96, 98, 99)),
            OffsetResponse(topic=T2, partition=0, error=0, offsets=(4096,)),
        ]))

        # Again, with a callback
        def preprocCB(response):
            return response

        respD = client.send_offset_request(payloads, callback=preprocCB)

        connections.flush()
        self.successResultOf(conn51.server.expectRequest(KafkaCodec.OFFSET_KEY, 0, ANY)).respond(resp1)
        self.successResultOf(conn52.server.expectRequest(KafkaCodec.OFFSET_KEY, 0, ANY)).respond(resp2)
        connections.flush()

        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(set(results), set([
            OffsetResponse(topic=T1, partition=0, error=0, offsets=(96, 98, 99)),
            OffsetResponse(topic=T2, partition=0, error=0, offsets=(4096,)),
        ]))

    def test_send_offset_fetch_request(self):
        T1 = "Topic71"
        T2 = "Topic72"
        G1 = "ConsumerGroup1"
        G2 = "ConsumerGroup2"
        mock_load_cmfg_calls = {G1: 0, G2: 0}
        mocked_brokers = {
            1: MagicMock(),
            2: MagicMock(),
        }

        # inject broker side effects
        ds = [
            [Deferred(), Deferred(), Deferred(), Deferred()],
            [Deferred(), Deferred(), Deferred(), Deferred()],
        ]

        mocked_brokers[1].makeRequest.side_effect = ds[0]
        mocked_brokers[2].makeRequest.side_effect = ds[1]

        brokers = [
            BrokerMetadata(node_id=1, host='kafka71', port=9092),
            BrokerMetadata(node_id=2, host='kafka72', port=9092),
        ]
        broker_for_group = {
            G1: brokers[0],
            G2: brokers[1],
        }

        def mock_get_brkr(node_id):
            return mocked_brokers[node_id]

        def mock_load_cmfg(group):
            mock_load_cmfg_calls[group] += 1
            client.consumer_group_to_brokers[group] = broker_for_group[group]
            return True

        client = KafkaClient(hosts='kafka71:9092,kafka72:9092')
        client.load_consumer_metadata_for_group = mock_load_cmfg

        # Setup the payloads
        payloads = [OffsetFetchRequest(T1, 71),
                    OffsetFetchRequest(T2, 72),
                    ]

        # Dummy the response
        resp = b"".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 2),             # Two topics
            struct.pack(">h", len(T1)), T1.encode(),   # First topic
            struct.pack(">i", 1),             # 1 partition
            struct.pack(">i", 71),            # Partition 71
            struct.pack(">q", 49),            # Offset 49
            struct.pack(">h", len(b"Metadata1")), b"Metadata1",  # Metadata
            struct.pack(">h", 0),             # No error
            struct.pack(">h", len(T2)), T2.encode(),   # Second topic
            struct.pack(">i", 1),             # 1 partition
            struct.pack(">i", 72),            # Partition 72
            struct.pack(">q", 27),            # Offset 27
            struct.pack(">h", len(b"Metadata2")), b"Metadata2",  # Metadata
            struct.pack(">h", 0),             # No error
        ])

        # patch the client so we control the brokerclients
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_offset_fetch_request(G1, payloads)

        # That first lookup for the group should result in one call to
        # load_consumer_metadata_for_group
        self.assertEqual(mock_load_cmfg_calls[G1], 1)
        # 'send' the response
        ds[0][0].callback(resp)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(set(results), set([
            OffsetFetchResponse(topic=T1, partition=71, offset=49,
                                metadata=b'Metadata1', error=0),
            OffsetFetchResponse(topic=T2, partition=72, offset=27,
                                metadata=b'Metadata2', error=0),
        ]))

        # Again, with a callback (for full coverage)
        preproc_call_count = [0]

        def preprocCB(response):
            preproc_call_count[0] += 1
            return response

        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_offset_fetch_request(
                G1, payloads, callback=preprocCB)

        # Check that another call is not made for the same group
        self.assertEqual(mock_load_cmfg_calls[G1], 1)
        # 'send' the responses
        ds[0][1].callback(resp)
        # check the results
        results = list(self.successResultOf(respD))
        # Test that the preprocessor callback was called (2 responses)
        self.assertEqual(preproc_call_count[0], 2)

        self.assertEqual(set(results), set([
            OffsetFetchResponse(topic=T1, partition=71, offset=49,
                                metadata=b'Metadata1', error=0),
            OffsetFetchResponse(topic=T2, partition=72, offset=27,
                                metadata=b'Metadata2', error=0),
        ]))
        client.close()

    def test_send_offset_fetch_request_failure(self):
        """
        Test that when a request involving a consumer metadata broker fails
        that we reset the cached broker for the consumer group.
        """
        T1 = "Topic71"
        G1 = "ConsumerGroup1"
        G2 = "ConsumerGroup2"
        mock_load_cmfg_calls = {G1: 0, G2: 0}
        mocked_brokers = {
            1: MagicMock(),
            2: MagicMock(),
        }

        # inject broker side effects
        ds = [
            [Deferred(), Deferred(), Deferred(), Deferred()],
            [Deferred(), Deferred(), Deferred(), Deferred()],
        ]

        mocked_brokers[1].makeRequest.side_effect = ds[0]
        mocked_brokers[2].makeRequest.side_effect = ds[1]

        brokers = [
            BrokerMetadata(node_id=1, host='kafka71', port=9092),
            BrokerMetadata(node_id=2, host='kafka72', port=9092),
        ]
        broker_for_group = {
            G1: brokers[0],
            G2: brokers[1],
        }

        def mock_get_brkr(node_id):
            return mocked_brokers[node_id]

        def mock_load_cmfg(group):
            mock_load_cmfg_calls[group] += 1
            client.consumer_group_to_brokers[group] = broker_for_group[group]
            return True

        client = KafkaClient(hosts='kafka71:9092,kafka72:9092')
        client.load_consumer_metadata_for_group = mock_load_cmfg

        # Setup the payload
        payloads = [OffsetFetchRequest(T1, 78)]

        # Dummy the response
        resp = b"".join([
            struct.pack(">i", 42),  # Correlation ID
            struct.pack(">i", 1),   # 1 topic
            struct.pack(">h", len(T1)), T1.encode(),  # Topic
            struct.pack(">i", 1),   # 1 partition
            struct.pack(">i", 78),  # Partition 78
            struct.pack(">q", -1),  # Offset -1
            struct.pack(">h", 0),   # Metadata
            struct.pack(">h", 16),  # NotCoordinatorForConsumerError
        ])

        # patch the client so we control the brokerclients
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_offset_fetch_request(G1, payloads)

        # That first lookup for the group should result in one call to
        # load_consumer_metadata_for_group
        self.assertEqual(mock_load_cmfg_calls[G1], 1)
        # And the cache of the broker for the consumer group should be set
        self.assertEqual(client.consumer_group_to_brokers[G1],
                         broker_for_group[G1])
        # 'send' the response
        ds[0][0].callback(resp)
        # check the results
        self.assertTrue(
            self.failureResultOf(respD, NotCoordinatorForConsumerError))
        self.assertNotIn(G1, client.consumer_group_to_brokers)
        # cleanup
        client.close()

    def test_send_offset_commit_request(self):
        """test_send_offset_commit_request"""
        T1 = "Topic61"
        T2 = "Topic62"
        G1 = "ConsumerGroup1"
        G2 = "ConsumerGroup2"
        mock_load_cmfg_calls = {G1: 0, G2: 0}

        mocked_brokers = {
            1: MagicMock(),
            2: MagicMock(),
        }
        # inject broker side effects
        ds = [
            [Deferred(), Deferred(), Deferred(), Deferred()],
            [Deferred(), Deferred(), Deferred(), Deferred()],
        ]

        mocked_brokers[1].makeRequest.side_effect = ds[0]
        mocked_brokers[2].makeRequest.side_effect = ds[1]

        brokers = [
            BrokerMetadata(node_id=1, host='kafka61', port=9092),
            BrokerMetadata(node_id=2, host='kafka62', port=9092),
        ]
        broker_for_group = {
            G1: brokers[0],
            G2: brokers[1],
        }

        def mock_get_brkr(node_id):
            return mocked_brokers[node_id]

        def mock_load_cmfg(group):
            mock_load_cmfg_calls[group] += 1
            client.consumer_group_to_brokers[group] = broker_for_group[group]
            return True

        client = KafkaClient(hosts='kafka61:9092,kafka62:9092')
        client.load_consumer_metadata_for_group = mock_load_cmfg

        # Setup the client with the metadata we want it to have
        client.topic_partitions = {
            T1: [61],
            T2: [62],
        }
        client.topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=61): brokers[0],
            TopicAndPartition(topic=T2, partition=62): brokers[1],
        }

        # Setup the payloads
        payloads = [
            OffsetCommitRequest(T1, 61, 81, -1, b"metadata1"),
            OffsetCommitRequest(T2, 62, 91, -1, b"metadata2"),
        ]

        # patch the client so we control the brokerclients
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_offset_commit_request(G2, payloads)

        # Dummy up a response for the commit
        resp1 = b"".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 2),             # Two topics
            struct.pack(">h", len(T1)), T1.encode(),   # First topic
            struct.pack(">i", 1),             # 1 partition
            struct.pack(">i", 61),            # Partition 61
            struct.pack(">h", 0),             # No error
            struct.pack(">h", len(T2)), T2.encode(),   # Second topic
            struct.pack(">i", 1),             # 1 partition
            struct.pack(">i", 62),            # Partition 62
            struct.pack(">h", 0),             # No error
        ])

        # 'send' the responses
        ds[1][0].callback(resp1)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(set(results), set([
            OffsetCommitResponse(topic=T1, partition=61, error=0),
            OffsetCommitResponse(topic=T2, partition=62, error=0),
        ]))

        client.close()

    def test_send_offset_commit_request_failure(self):
        """
        Test that when the kafka broker is unavailable, that the proper
        ConsumerCoordinatorNotAvailableError is raised"""

        T1 = "Topic61"
        G1 = "ConsumerGroup1"

        def mock_gcfg(group):
            return None

        client = KafkaClient(hosts='kafka61:9092')
        client._get_coordinator_for_group = mock_gcfg

        # Setup the client with the metadata we want it to have
        client.topic_partitions = {
            T1: [61],
        }

        # Setup the payloads
        payloads = [
            OffsetCommitRequest(T1, 61, 81, -1, b"metadata1"),
        ]

        respD1 = client.send_offset_commit_request(G1, [])
        self.assertTrue(
            self.failureResultOf(respD1, ValueError))
        respD2 = client.send_offset_commit_request(G1, payloads)
        self.assertTrue(
            self.failureResultOf(respD2, ConsumerCoordinatorNotAvailableError))
        client.close()

    def test_client_disconnect_on_timeout_false(self):
        """
        The connection to a broker is not terminated due to request timeout
        unless `disconnect_on_timeout` is set.
        """
        d = Deferred()
        d2 = Deferred()
        ds = [d, d2]
        m = Mock()
        mocked_brokers = {('kafka_dot', 9092): m}
        # inject broker side effects
        m.makeRequest.side_effect = ds
        m.cancelRequest.side_effect = lambda rId, reason: ds.pop(0).errback(reason)
        m.configure_mock(host='kafka_dot', port=9092)

        reactor = MemoryReactorClock()
        client = KafkaClient(
            hosts='kafka_dot:9092',
            reactor=reactor,
            disconnect_on_timeout=False,
            endpoint_factory=BlackholeEndpoint,
        )

        # Alter the client's brokerclient dict to use our mocked broker
        client.clients = mocked_brokers
        client._collect_hosts_d = None

        # Kick off a request to the client to trigger a request to our Mock
        respD = client._send_broker_unaware_request(1, b'fake request')

        # Cause that request to timeout
        reactor.advance(client.timeout * 0.91)  # fire the reactor-blocked
        reactor.advance(client.timeout)  # fire the timeout errback

        # Check that the request was timed out
        self.successResultOf(
            self.failUnlessFailure(respD, KafkaUnavailableError))

        # Make sure we didn't try to disconnect
        m.disconnect.assert_not_called()

    def test_client_disconnect_on_timeout_true(self):
        """
        When a client request to a broker times out the connection to that
        broker is terminated.
        """
        d = Deferred()
        d2 = Deferred()
        ds = [d, d2]
        m = Mock()
        mocked_brokers = {('kafka_dot', 9092): m}
        # inject broker side effects
        m.makeRequest.side_effect = ds
        m.cancelRequest.side_effect = lambda rId, reason: ds.pop(0).errback(reason)
        m.configure_mock(host='kafka_dot', port=9092)

        reactor = MemoryReactorClock()
        client = KafkaClient(hosts='kafka_dot:9092', reactor=reactor, disconnect_on_timeout=True)

        # Alter the client's brokerclient dict to use our mocked broker
        client.clients = mocked_brokers
        client._collect_hosts_d = None

        # Kick off a request to the client to trigger a request to our Mock
        respD = client._send_broker_unaware_request(1, b'fake request')

        # Cause that request to timeout
        reactor.advance(client.timeout * 0.91)  # fire the reactor-blocked
        reactor.advance(client.timeout)  # fire the timeout errback

        # Check that the request was timed out
        self.successResultOf(
            self.failUnlessFailure(respD, KafkaUnavailableError))

        # Make sure we didn't try to disconnect
        m.disconnect.assert_called_with()

    def test_client_topic_fully_replicated(self):
        """test_client_topic_fully_replicated"

        Test the happy path of client.topic_fully_replicated()
        """
        client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want start with
        client.topic_partitions = {
            'Topic0': [],  # No partitions.
            'Topic2': [0],  # Not in partition_meta below
            'Topic3': [0, 1, 2, 3],  # "Good" partition
        }
        client.partition_meta = {
            TopicAndPartition(topic='Topic3', partition=0):
                PartitionMetadata('Topic3', 0, 0, 1, [1, 2], [1, 2]),
            TopicAndPartition(topic='Topic3', partition=1):
                PartitionMetadata('Topic3', 1, 0, 2, [2, 1], [1, 2]),
            TopicAndPartition(topic='Topic3', partition=2):
                PartitionMetadata('Topic3', 2, 0, 1, [1, 2], [1, 2]),
            TopicAndPartition(topic='Topic3', partition=3):
                PartitionMetadata('Topic3', 3, 0, 2, [2, 1], [1, 2]),
        }

        # Topics which don't have partitions aren't 'fully replicated'
        self.assertFalse(client.topic_fully_replicated('Topic0'))
        # Topics which we don't have any metadata for aren't 'fully replicated'
        self.assertFalse(client.topic_fully_replicated('Topic1'))
        # Topics which are only partially represented aren't 'fully replicated'
        self.assertFalse(client.topic_fully_replicated('Topic2'))

        # Topic with all the partitions having equal replicas & ISRs ARE
        self.assertTrue(client.topic_fully_replicated('Topic3'))


class NormalizeHostsTests(unittest.TestCase):
    def test_bare_host_string(self):
        """
        The default port is inferred when none is given.
        """
        self.assertEqual([('kafka', 9092)], _normalize_hosts('kafka'))
        self.assertEqual([('127.0.0.1', 9092)], _normalize_hosts(u'127.0.0.1'))
        self.assertEqual([('kafka.internal', 9092)], _normalize_hosts('kafka.internal'))
        self.assertEqual([('kafka-1.internal', 9092), ('kafka-2.internal', 9092)],
                         _normalize_hosts(b'kafka-1.internal, kafka-2.internal'))

    def test_string_with_ports(self):
        """
        The string may specify ports.
        """
        self.assertEqual([('kafka', 1234)], _normalize_hosts(u'kafka:1234 '))
        self.assertEqual([('kafka', 1234), ('kafka', 2345)], _normalize_hosts(u'kafka:1234 ,kafka:2345'))
        self.assertEqual([('1.2.3.4', 5555)], _normalize_hosts(b' 1.2.3.4:5555 '))
