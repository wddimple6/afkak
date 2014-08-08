"""
Test code for KafkaClient(object) class.
"""

from __future__ import division, absolute_import

import pickle
from functools import partial
from copy import copy

from twisted.internet import defer
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase
from twisted.internet.defer import (
    inlineCallbacks, Deferred, returnValue, DeferredList, maybeDeferred,
    succeed, fail,
    )
from twisted.internet.error import ConnectionRefusedError

import os
import random
import struct
import logging

logging.basicConfig(filename='_trial_temp/test_client.log')

from mock import MagicMock, patch

from kafkatwisted import KafkaClient
from kafkatwisted.common import (
    ProduceRequest, ProduceResponse, BrokerMetadata, PartitionMetadata,
    RequestTimedOutError, TopicAndPartition, KafkaUnavailableError,
    DefaultKafkaPort, LeaderUnavailableError, PartitionUnavailableError,
    FailedPayloadsError,
)
from kafkatwisted.protocol import KafkaProtocol
from kafkatwisted.kafkacodec import (create_message, KafkaCodec)
from kafkatwisted.client import collect_hosts
from twisted.python.failure import Failure


def createMetadataResp():
    from .test_kafkacodec import TestKafkaCodec
    codec = TestKafkaCodec()
    node_brokers = {
        0: BrokerMetadata(0, "brokers1.kafkatwisted.rdio.com", 1000),
        1: BrokerMetadata(1, "brokers1.kafkatwisted.rdio.com", 1001),
        3: BrokerMetadata(3, "brokers2.kafkatwisted.rdio.com", 1000)
        }

    topic_partitions = {
        "topic1": {
            0: PartitionMetadata("topic1", 0, 1, (0, 2), (2,)),
            1: PartitionMetadata("topic1", 1, 3, (0, 1), (0, 1))
            },
        "topic2": {
            0: PartitionMetadata("topic2", 0, 0, (), ())
            }
        }
    topic_errors = {"topic1": 0, "topic2": 1}
    partition_errors = {
        ("topic1", 0): 0,
        ("topic1", 1): 1,
        ("topic2", 0): 0
        }
    encoded = codec._create_encoded_metadata_response(
        node_brokers, topic_partitions, topic_errors, partition_errors)
    return encoded

class TestKafkaClient(TestCase):
    testMetaData = createMetadataResp()

    def getLeaderWrapper(self, c, *args, **kwArgs):
        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            d = c._get_leader_for_partition(*args)

        if 'errs' not in kwArgs:
            return self.successResultOf(d)
        return self.failureResultOf(d, kwArgs['errs'])

    def test_repr(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            c = KafkaClient('kafka.example.com',
                            clientId='MyClient')
        self.assertEqual('<KafkaClient clientId=MyClient>',
                         c.__repr__())

    def test_init_with_list(self):
        # we patch out load_metadata_for_topics so that when it's called
        # from the KafkaClient constructor, it doesn't actually try to
        # establish a connection...
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts=['kafka01:9092', 'kafka02:9092', 'kafka03:9092'])

        self.assertItemsEqual(
            [('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)],
            client.hosts)

    def test_init_with_csv(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka01:9092,kafka02,kafka03:9092')

        self.assertItemsEqual(
            [('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)],
            client.hosts)

    def test_init_with_unicode_csv(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts=u'kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertItemsEqual(
            [('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)],
            client.hosts)

    def test_send_broker_unaware_request_fail(self):
        'Tests that call fails when all hosts are unavailable'

        mocked_brokers = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock()
        }

        # inject side effects (makeRequest returns deferreds that are
        # pre-failed with a timeout...
        mocked_brokers[('kafka01', 9092)].makeRequest.side_effect = \
            lambda a, b: defer.fail(RequestTimedOutError("kafka01 went away (unittest)"))
        mocked_brokers[('kafka02', 9092)].makeRequest.side_effect = \
            lambda a, b: defer.fail(RequestTimedOutError("WTF?Kafka02 went away (unittest)"))

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch load_metadata_for_topics to do nothing...
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_brokerclient',
                              side_effect=mock_get_brkr):
                client = KafkaClient(hosts=['kafka01:9092', 'kafka02:9092'])

                # Get the deferred (should be already failed)
                fail = client._send_broker_unaware_request(
                    1, 'fake request')
                # check it
                self.failUnlessFailure(fail, KafkaUnavailableError)
                result = self.successResultOf(fail)

                # Check that the proper calls were made
                for key, brkr in mocked_brokers.iteritems():
                    brkr.makeRequest.assert_called_with(1, 'fake request')

    def test_send_broker_unaware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_brokers = {
            ('kafka21', 9092): MagicMock(),
            ('kafka22', 9092): MagicMock(),
            ('kafka23', 9092): MagicMock()
        }
        # inject broker side effects
        mocked_brokers[('kafka21', 9092)].makeRequest.side_effect = \
            RequestTimedOutError("kafka01 went away (unittest)")
        mocked_brokers[('kafka22', 9092)].makeRequest.return_value = \
            'valid response'
        mocked_brokers[('kafka23', 9092)].makeRequest.side_effect = \
            RequestTimedOutError("kafka03 went away (unittest)")

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_brokerclient',
                              side_effect=mock_get_brkr):
                client = KafkaClient(hosts='kafka21:9092,kafka22:9092')

                resp = self.successResultOf(
                    client._send_broker_unaware_request(1, 'fake request'))

                self.assertEqual('valid response', resp)
                mocked_brokers[('kafka22', 9092)].makeRequest.assert_called_with(
                    1, 'fake request')


    @patch('kafkatwisted.client.KafkaCodec')
    def test_load_metadata_for_topics(self, kCodec):
        "Load metadata for all topics"

        brokers = {}
        brokers[0] = BrokerMetadata(1, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(2, 'broker_2', 5678)

        topics = {}
        topics['topic_1'] = {
            0: PartitionMetadata('topic_1', 0, 1, [1, 2], [1, 2])
        }
        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, -1, [], [])
        }
        topics['topic_no_partitions'] = {}
        topics['topic_3'] = {
            0: PartitionMetadata('topic_3', 0, 0, [0, 1], [0, 1]),
            1: PartitionMetadata('topic_3', 1, 1, [1, 0], [1, 0]),
            2: PartitionMetadata('topic_3', 2, 0, [0, 1], [0, 1])
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        # client starts load of metadata at init
        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)

        self.assertDictEqual({
            TopicAndPartition('topic_1', 0): brokers[1],
            TopicAndPartition('topic_noleader', 0): None,
            TopicAndPartition('topic_noleader', 1): None,
            TopicAndPartition('topic_3', 0): brokers[0],
            TopicAndPartition('topic_3', 1): brokers[1],
            TopicAndPartition('topic_3', 2): brokers[0]},
            client.topics_to_brokers)

        # now test that the failure case is handled properly
        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: fail(
                KafkaUnavailableError("test_load_metadata_for_topics"))):
            d = client.load_metadata_for_topics()
        self.failUnlessFailure(d, KafkaUnavailableError)

    @patch('kafkatwisted.client.KafkaCodec')
    def test_get_leader_for_partitions_reloads_metadata(self, kCodec):
        "Get leader for partitions reload metadata if it is not available"

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {'topic_no_partitions': {}}
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed('_')):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)

            # topic metadata is loaded but empty
            self.assertDictEqual({}, client.topics_to_brokers)

            # Change what metadata will be 'returned'
            topics['topic_no_partitions'] = {
                0: PartitionMetadata('topic_no_partitions', 0, 0, [0, 1], [0, 1])
            }
            kCodec.decode_metadata_response.return_value = (brokers, topics)

            # calling _get_leader_for_partition (from any broker aware request)
            # will try loading metadata again for the same topic
            leader = self.getLeaderWrapper(client, 'topic_no_partitions', 0)

            self.assertEqual(brokers[0], leader)
            self.assertDictEqual(
                { TopicAndPartition('topic_no_partitions', 0): brokers[0]},
                client.topics_to_brokers)

    @patch('kafkatwisted.client.KafkaCodec')
    def test_get_leader_for_unassigned_partitions(self, kCodec):
        "Get leader raises if no partitions is defined for a topic"

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {'topic_no_partitions': {}}
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)

            self.assertDictEqual({}, client.topics_to_brokers)

            key = TopicAndPartition('topic_no_partitions', 0)
            eFail = PartitionUnavailableError("{} not available".format(str(key)))

            fail = self.getLeaderWrapper(client, 'topic_no_partitions', 0,
                                         errs=PartitionUnavailableError)
            # Make sure the error msg is correct
            self.assertEqual(type(eFail), fail.type)
            self.assertEqual(eFail.args, fail.value.args)


    @patch('kafkatwisted.client.KafkaCodec')
    def test_get_leader_returns_none_when_noleader(self, kCodec):
        "Getting leader for partitions returns None when the partiion has no leader"

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {}
        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, -1, [], [])
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
            self.assertDictEqual(
                {
                    TopicAndPartition('topic_noleader', 0): None,
                    TopicAndPartition('topic_noleader', 1): None
                    },
                client.topics_to_brokers)

            self.assertIsNone(
                self.getLeaderWrapper(client, 'topic_noleader', 0))
            self.assertIsNone(
                self.getLeaderWrapper(client, 'topic_noleader', 1))

            topics['topic_noleader'] = {
                0: PartitionMetadata('topic_noleader', 0, 0, [0, 1], [0, 1]),
                1: PartitionMetadata('topic_noleader', 1, 1, [1, 0], [1, 0])
            }
            kCodec.decode_metadata_response.return_value = (brokers, topics)
            self.assertEqual(
                brokers[0], self.getLeaderWrapper(client, 'topic_noleader', 0))
            self.assertEqual(
                brokers[1], self.getLeaderWrapper(client, 'topic_noleader', 1))

    @patch('kafkatwisted.client.KafkaCodec')
    @inlineCallbacks
    def test_send_produce_request_raises_when_noleader(self, kCodec):
        """
        Send producer request raises LeaderUnavailableError if
        leader is not available
        """

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {}
        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, -1, [], [])
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
            client = KafkaClient(hosts=['broker_1:4567'], timeout=None)

            # create a list of requests (really just one)
            requests = [ProduceRequest(
                    "topic_noleader", 0,
                    [create_message("a"), create_message("b")])]
            # Attempt to send it, and ensure the returned deferred fails
            # properly
            fail = client.send_produce_request(requests)
            f2 = self.failUnlessFailure(fail, LeaderUnavailableError)

        result = yield fail


    """
    Test the collect_hosts function in client.py
    """
    def test_collect_hosts__happy_path(self):
        hosts = "localhost:1234,localhost"
        results = collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_collect_hosts__string_list(self):
        hosts = [
            'localhost:1234',
            'localhost',
        ]

        results = collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_collect_hosts__with_spaces(self):
        hosts = "localhost:1234, localhost"
        results = collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))


    @patch('kafkatwisted.client.KafkaBrokerClient')
    def test_get_brokerclient(self, broker):
        """
        Test _get_brokerclient()
        """
        brokermocks = [MagicMock(), MagicMock(), MagicMock()]
        broker.side_effect = brokermocks
        # make sure the KafkaClient constructor doesn't create the brokers...
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts=['broker_1:4567', 'broker_2', 'broker_3:45678'],
                timeout=None)

        # client shouldn't have any brokerclients yet (because the load
        # on __init__ is mocked out
        self.assertFalse(client.clients)
        # See that a new brokerclient was created
        client._get_brokerclient('broker_2', DefaultKafkaPort)
        self.assertEqual(len(broker.call_args_list), 1)
        self.assertEqual(client.clients[('broker_2', DefaultKafkaPort)],
                         brokermocks[0])
        brokermocks[0].connect.assert_called_once_with()
        # Get the same host/port again, and make sure the same one is returned
        client._get_brokerclient('broker_2', DefaultKafkaPort)
        self.assertEqual(len(broker.call_args_list), 1)
        self.assertEqual(client.clients[('broker_2', DefaultKafkaPort)],
                         brokermocks[0])
        brokermocks[0].connect.assert_called_once_with()
        # Get another one...
        client._get_brokerclient('broker_3', 45678)
        self.assertEqual(len(broker.call_args_list), 2)
        self.assertEqual(client.clients[('broker_3', 45678)],
                         brokermocks[1])
        brokermocks[1].connect.assert_called_once_with()
        # Get the first one again, and make sure the same one is returned
        client._get_brokerclient('broker_2', DefaultKafkaPort)
        self.assertEqual(len(broker.call_args_list), 2)
        self.assertEqual(client.clients[('broker_2', DefaultKafkaPort)],
                         brokermocks[0])
        brokermocks[0].connect.assert_called_once_with()
        # Get the last one
        client._get_brokerclient('broker_1', 4567)
        self.assertEqual(len(broker.call_args_list), 3)
        self.assertEqual(client.clients[('broker_1', 4567)],
                         brokermocks[2])
        brokermocks[2].connect.assert_called_once_with()


    @patch('kafkatwisted.client.KafkaBrokerClient')
    def test_handleConnFailed(self, broker):
        """
        Test _handleConnFailed()
        Make sure that the client resets its metadata when it can't
        connect to a broker.
        """
        brokermocks = [MagicMock(), MagicMock(), MagicMock()]
        broker.side_effect = brokermocks
        d = Deferred()
        brokermocks[0].connect.side_effect = [d]
        # make sure the KafkaClient constructor doesn't create the brokers...
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts=['broker_1:4567', 'broker_2', 'broker_3:45678'],
                timeout=None)

            client._get_brokerclient('broker_2', DefaultKafkaPort)
            with patch.object(KafkaClient, 'reset_all_metadata') as mock_method:
                f = Failure(ConnectionRefusedError())
                d.errback(f)
                mock_method.assert_called_once_with()

    @patch('kafkatwisted.client.KafkaBrokerClient')
    def test_updateBrokerState(self, broker):
        """
        Test _updateBrokerState()
        Make sure that the client logs when a broker changes state
        """
        # make sure the KafkaClient constructor doesn't create the brokers...
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts=['broker_1:4567', 'broker_2', 'broker_3:45678'],
                timeout=None)

        import kafkatwisted.client as kclient
        kclient.log = MagicMock()
        e = ConnectionRefusedError()
        bk = ('broker_1', 4567)
        bkr = "aBroker"
        client._updateBrokerState(bk, bkr, False, e)
        errStr = 'Broker:{} state changed:Disconnected for reason:'.format(bkr)
        errStr += str(e)
        kclient.log.debug.assert_called_once_with(errStr)

    def test_send_broker_aware_request(self):
        """
        Test that send_broker_aware_request returns the proper responses
        when given the correct data
        """
        T1 = "Topic1"
        T2 = "Topic2"
        mocked_brokers = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock(),
        }
        # inject broker side effects
        ds = [ [Deferred(), Deferred(), Deferred(), ],
               [Deferred(), Deferred(), Deferred(), ],]
        mocked_brokers[('kafka01', 9092)].makeRequest.side_effect = ds[0]
        mocked_brokers[('kafka02', 9092)].makeRequest.side_effect = ds[1]

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want it to have
        client.brokers = {
            0: BrokerMetadata(nodeId=1, host='kafka01', port=9092),
            1: BrokerMetadata(nodeId=2, host='kafka02', port=9092),
            }
        client.topic_partitions = {
            T1: [0],
            T2: [0],
            }
        client.topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=0): client.brokers[0],
            TopicAndPartition(topic=T2, partition=0): client.brokers[1],
            }

        # Setup the payloads, encoder & decoder funcs
        payloads = [
            ProduceRequest(T1, 0,
                           [ create_message(T1 + " message %d" % i)
                             for i in range(10) ]
                           ),
            ProduceRequest(T2, 0,
                           [ create_message(T2 + " message %d" % i)
                               for i in range(5) ]
                           ),
            ]

        encoder = partial(
            KafkaCodec.encode_produce_request,
            acks=1,
            timeout=1000)
        decoder = KafkaCodec.decode_produce_response

        # patch the client so we control the brokerclients
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client._send_broker_aware_request(
                payloads, encoder, decoder)

        # Dummy up some responses, one from each broker
        corlID = 9876
        resp0 = struct.pack('>iih%dsiihq' % (len(T1)),
                            corlID, 1, len(T1), T1, 1, 0, 0, 10L)
        resp1 = struct.pack('>iih%dsiihq' % (len(T2)),
                            corlID + 1, 1, len(T2), T2, 1, 0, 0, 20L)
        # 'send' the responses
        ds[0][0].callback(resp0)
        ds[1][0].callback(resp1)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results,
                         [ProduceResponse(T1, 0, 0, 10L),
                          ProduceResponse(T2, 0, 0, 20L)])

        # Now try again, but with one request failing...
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client._send_broker_aware_request(
                payloads, encoder, decoder)
        # dummy responses
        corlID = 1234
        resp0 = struct.pack('>iih%dsiihq' % (len(T1)),
                            corlID, 1, len(T1), T1, 1, 0, 0, 10L)
        # 'send' the responses
        ds[0][1].callback(resp0)
        ds[1][1].errback(RequestTimedOutError(
                    "Request:{} timed out".format(corlID)))
        # check the result. Should be Failure(FailedPayloadsError)
        results = self.failureResultOf(respD, FailedPayloadsError)
        # And the exceptions args should hold the payload of the
        # failed request
        self.assertEqual(results.value.args[0][0], payloads[1])

        # And finally, without expecting a response...
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client._send_broker_aware_request(
                payloads, encoder, None)
        # 'send' the responses
        ds[0][2].callback(None)
        ds[1][2].callback(None)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results, [])

    def test_reset_topic_metadata(self):
        """
        Test that reset_topic_metadata makes the proper changes
        to the client's metadata
        """
        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want start with
        Ts = [ "Topic1", "Topic2", "Topic3" ]
        client.brokers = {
            0: BrokerMetadata(nodeId=1, host='kafka01', port=9092),
            1: BrokerMetadata(nodeId=2, host='kafka02', port=9092),
            }
        client.topic_partitions = {
            Ts[0]: [0],
            Ts[1]: [0],
            Ts[2]: [0, 1, 2, 3],
            }
        client.topics_to_brokers = {
            TopicAndPartition(topic=Ts[0], partition=0): client.brokers[0],
            TopicAndPartition(topic=Ts[1], partition=0): client.brokers[1],
            TopicAndPartition(topic=Ts[2], partition=0): client.brokers[0],
            TopicAndPartition(topic=Ts[2], partition=1): client.brokers[1],
            TopicAndPartition(topic=Ts[2], partition=2): client.brokers[0],
            TopicAndPartition(topic=Ts[2], partition=3): client.brokers[1],
            }

        # make copies...
        brokers = copy(client.brokers)
        tParts = copy(client.topic_partitions)
        topicsToBrokers = copy(client.topics_to_brokers)

        # Reset the client's metadata
        client.reset_topic_metadata(Ts[1])

        # No change to brokers...
        # topics_to_brokers gets every (topic,partition) tuple removed
        # for that topic
        del topicsToBrokers[TopicAndPartition(topic=Ts[1], partition=0)]
        # topic_paritions gets topic deleted
        del tParts[Ts[1]]
        # Check correspondence
        self.assertEqual(brokers, client.brokers)
        self.assertEqual(topicsToBrokers, client.topics_to_brokers)
        self.assertEqual(tParts, client.topic_partitions)

        # Resetting an unknown topic has no effect
        client.reset_topic_metadata("bogus")
        # Check correspondence with unchanged copies
        self.assertEqual(brokers, client.brokers)
        self.assertEqual(topicsToBrokers, client.topics_to_brokers)
        self.assertEqual(tParts, client.topic_partitions)

    def test_has_metadata_for_topic(self):
        """
        Test that has_metatdata_for_topic works
        """
        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # Setup the client with the metadata we want start with
        Ts = [ "Topic1", "Topic2", "Topic3" ]
        client.brokers = {
            0: BrokerMetadata(nodeId=1, host='kafka01', port=9092),
            1: BrokerMetadata(nodeId=2, host='kafka02', port=9092),
            }
        client.topic_partitions = {
            Ts[0]: [0],
            Ts[1]: [0],
            Ts[2]: [0, 1, 2, 3],
            }
        client.topics_to_brokers = {
            TopicAndPartition(topic=Ts[0], partition=0): client.brokers[0],
            TopicAndPartition(topic=Ts[1], partition=0): client.brokers[1],
            TopicAndPartition(topic=Ts[2], partition=0): client.brokers[0],
            TopicAndPartition(topic=Ts[2], partition=1): client.brokers[1],
            TopicAndPartition(topic=Ts[2], partition=2): client.brokers[0],
            TopicAndPartition(topic=Ts[2], partition=3): client.brokers[1],
            }

        for topic in Ts:
            self.assertTrue(client.has_metadata_for_topic(topic))
        self.assertFalse(client.has_metadata_for_topic("Unknown"))

    def test_close(self):
        mocked_brokers = {
            ('kafka91', 9092): MagicMock(),
            ('kafka92', 9092): MagicMock(),
            ('kafka93', 9092): MagicMock()
        }

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

        # patch in our fake brokers
        client.clients = mocked_brokers
        client.close()

        # Check that each fake broker had its disconnect() called
        for broker in mocked_brokers.values():
            broker.disconnect.assert_called_once_with()


    def test_send_produce_request(self):
        """
        Test send_produce_request
        """
        T1 = "Topic1"
        T2 = "Topic2"
        mocked_brokers = {
            ('kafka31', 9092): MagicMock(),
            ('kafka32', 9092): MagicMock(),
        }
        # inject broker side effects
        ds = [ [Deferred(), Deferred(), Deferred(), Deferred(), Deferred(), ],
               [Deferred(), Deferred(), Deferred(), Deferred(), Deferred(), ],]
        mocked_brokers[('kafka31', 9092)].makeRequest.side_effect = ds[0]
        mocked_brokers[('kafka32', 9092)].makeRequest.side_effect = ds[1]

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka31:9092,kafka32:9092')

        # Setup the client with the metadata we want it to have
        client.brokers = {
            0: BrokerMetadata(nodeId=1, host='kafka31', port=9092),
            1: BrokerMetadata(nodeId=2, host='kafka32', port=9092),
            }
        client.topic_partitions = {
            T1: [0],
            T2: [0],
            }
        client.topics_to_brokers = {
            TopicAndPartition(topic=T1, partition=0): client.brokers[0],
            TopicAndPartition(topic=T2, partition=0): client.brokers[1],
            }

        # Setup the payloads
        payloads = [ ProduceRequest(T1, 0, [ create_message(
                        T1 + " message %d" % i) for i in range(10) ]),
                     ProduceRequest(T2, 0, [ create_message(
                        T2 + " message %d" % i) for i in range(5) ]),
                     ]

        # patch the client so we control the brokerclients
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_produce_request(payloads)

        # Dummy up some responses, one from each broker
        corlID = 9876
        resp0 = struct.pack('>iih%dsiihq' % (len(T1)),
                            corlID, 1, len(T1), T1, 1, 0, 0, 10L)
        resp1 = struct.pack('>iih%dsiihq' % (len(T2)),
                            corlID + 1, 1, len(T2), T2, 1, 0, 0, 20L)
        # 'send' the responses
        ds[0][0].callback(resp0)
        ds[1][0].callback(resp1)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results,
                         [ProduceResponse(T1, 0, 0, 10L),
                          ProduceResponse(T2, 0, 0, 20L)])

        # And again, with acks=0
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_produce_request(payloads, acks=0)
        ds[0][1].callback(None)
        #print "ZORG:test_send_produce_request:2", respD
        ds[1][1].callback(None)
        results = list(self.successResultOf(respD))
        self.assertEqual(results, [])

        # And again, this time with an error coming back...
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_produce_request(payloads, fail_on_error=False)

        # Dummy up some responses, one from each broker
        corlID = 13579
        resp0 = struct.pack('>iih%dsiihq' % (len(T1)),
                            corlID, 1, len(T1), T1, 1, 0, 0, 10L)
        resp1 = struct.pack('>iih%dsiihq' % (len(T2)),
                            corlID + 1, 1, len(T2), T2, 1, 0,
                            6, 20L)  # NotLeaderForPartition=6
        # 'send' the responses
        ds[0][2].callback(resp0)
        ds[1][2].callback(resp1)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results,
                         [ProduceResponse(T1, 0, 0, 10L),
                          ProduceResponse(T2, 0, 6, 20L)])

        # And again, this time with an error coming back...but ignored
        with patch.object(KafkaClient, '_get_brokerclient',
                          side_effect=mock_get_brkr):
            respD = client.send_produce_request(payloads, fail_on_error=False)

        # Dummy up some responses, one from each broker
        corlID = 13579
        resp0 = struct.pack('>iih%dsiihq' % (len(T1)),
                            corlID, 1, len(T1), T1, 1, 0, 0, 10L)
        resp1 = struct.pack('>iih%dsiihq' % (len(T2)),
                            corlID + 1, 1, len(T2), T2, 1, 0,
                            6, 20L)  # NotLeaderForPartition=6
        # 'send' the responses
        ds[0][3].callback(resp0)
        ds[1][3].callback(resp1)
        # check the results
        results = list(self.successResultOf(respD))
        self.assertEqual(results,
                         [ProduceResponse(T1, 0, 0, 10L),
                          ProduceResponse(T2, 0, 6, 20L)])

