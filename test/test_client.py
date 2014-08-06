"""
Test code for KafkaClient(object) class.
"""

from __future__ import division, absolute_import

import pickle

from twisted.internet import defer
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase
from twisted.internet.defer import (inlineCallbacks, Deferred, returnValue,
                                    DeferredList, maybeDeferred, succeed,
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
    ProduceRequest, BrokerMetadata, PartitionMetadata, RequestTimedOutError,
    TopicAndPartition, KafkaUnavailableError, DefaultKafkaPort,
    LeaderUnavailableError, PartitionUnavailableError,
)
from kafkatwisted.protocol import KafkaProtocol
from kafkatwisted.kafkacodec import (create_message, KafkaCodec)
from kafkatwisted.client import collect_hosts
from twisted.python.failure import Failure


def getLeaderWrapper(client, *args):
    # This gets complicated, since _get_leader_for_partition() will first
    # yield a deferred from an attempt to re-load the metadata, we then
    # need to callback() the client.dMetaDataLoad deferred, which will in
    # turn trigger the callback on the deferred 'd'.
    # We addCallback(getResult) to 'd' which has access to 'result'
    # in this scope and sets it to the result returned by
    # the 2nd call into _get_leader_for_partition() [which is done by
    # the @inlineCallbacks framework]

    # This construct let's us access the result of the deferred
    # returned by the @inlineCallbacks wrapped function(s)
    # we're calling
    result = [0]
    def getResult(r):
        result[0] = r
    def gotErr(e):
        # return the error for comparison by caller
        result[0] = e

    d = client._get_leader_for_partition(*args)
    result = [d]
    d.addCallback(getResult)
    d.addErrback(gotErr)
    if client.dMetaDataLoad is not None and not client.dMetaDataLoad.called:
        client.dMetaDataLoad.callback('anything')
    return result[0]

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
    def test_repr(self):
        with patch.object(KafkaClient, '_send_broker_unaware_request',
                          side_effect=lambda a, b: succeed(self.testMetaData)):
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

    @inlineCallbacks
    def test_send_broker_unaware_request_fail(self):
        'Tests that call fails when all hosts are unavailable'

        mocked_brokers = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock()
        }

        # inject side effects (makeRequest returns deferreds that are
        # pre-failed with a timeout...
        mocked_brokers[('kafka01', 9092)].makeRequest.side_effect = \
            defer.fail(RequestTimedOutError("kafka01 went away (unittest)"))
        mocked_brokers[('kafka02', 9092)].makeRequest.side_effect = \
            defer.fail(RequestTimedOutError("Kafka02 went away (unittest)"))

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
                yield fail

                # Check that the proper calls were made
                for key, brkr in mocked_brokers.iteritems():
                    brkr.makeRequest.assert_called_with(1, 'fake request')

    @inlineCallbacks
    def test_send_broker_unaware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_brokers = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock(),
            ('kafka03', 9092): MagicMock()
        }
        # inject KafkaConnection side effects
        mocked_brokers[('kafka01', 9092)].makeRequest.side_effect = RuntimeError("kafka01 went away (unittest)")
        mocked_brokers[('kafka02', 9092)].makeRequest.return_value = 'valid response'
        mocked_brokers[('kafka03', 9092)].makeRequest.side_effect = RuntimeError("kafka03 went away (unittest)")

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_brokerclient',
                              side_effect=mock_get_brkr):
                client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

                resp = yield client._send_broker_unaware_request(1, 'fake request')

                self.assertEqual('valid response', resp)
                mocked_brokers[('kafka02', 9092)].makeRequest.assert_called_with(
                    1, 'fake request')

    @patch('kafkatwisted.client.KafkaCodec')
    @inlineCallbacks
    def test_send_broker_aware_request(self, kCodec):
        'Tests that call works when the host is available'

        mocked_brokers = {
            ('broker_0', 4567): MagicMock(),
            ('broker_1', 5678): MagicMock(),
        }
        mocked_brokers[('broker_0', 4567)].makeRequest.return_value = ''
        mocked_brokers[('broker_1', 5678)].makeRequest.return_value = 'valid response'
        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_0', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_1', 5678)

        topics = {}
        topics['topic_withleader'] = {
            0: PartitionMetadata('topic_withleader', 0, 0, [], []),
            1: PartitionMetadata('topic_withleader', 1, 1, [], [])
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
        # this will trigger the call to decode the metadata, which we
        # mocked above
        client.dMetaDataLoad.callback('anything')

        # create a list of requests
        requests = [
            ProduceRequest("topic_withleader", 0,
                           [create_message("a"), create_message("b")]),
            ProduceRequest("topic_withleader", 1,
                           [create_message("c"), create_message("d")]),
            ]
        encoder = partial(KafkaCodec.encode_produce_request,
                          acks=1, timeout=1000)
        decoder = KafkaCodec.decode_produce_response

        # send it
        result = yield client._send_broker_aware_request(
            requests, encoder, decoder)
        # Check that the mocked brokers got the right calls


    @inlineCallbacks
    def test_send_broker_aware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_brokers = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock(),
            ('kafka03', 9092): MagicMock()
        }
        # inject KafkaConnection side effects
        mocked_brokers[('kafka01', 9092)].makeRequest.side_effect = RuntimeError("kafka01 went away (unittest)")
        mocked_brokers[('kafka02', 9092)].makeRequest.return_value = 'valid response'
        mocked_brokers[('kafka03', 9092)].makeRequest.side_effect = RuntimeError("kafka03 went away (unittest)")

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_brokerclient',
                              side_effect=mock_get_brkr):
                client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

                resp = yield client._send_broker_unaware_request(1, 'fake request')

                self.assertEqual('valid response', resp)
                mocked_brokers[('kafka02', 9092)].makeRequest.assert_called_with(
                    1, 'fake request')

    @patch('kafkatwisted.client.KafkaCodec')
    def test_load_metadata(self, kCodec):
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
            leader = getLeaderWrapper(client, 'topic_no_partitions', 0)

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

            fail = getLeaderWrapper(client, 'topic_no_partitions', 0)
            # getLeaderWrapper() returns the Failure object in the errback
            # case, so we need to tweek the comparison a bit...
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

            self.assertIsNone(getLeaderWrapper(client, 'topic_noleader', 0))
            self.assertIsNone(getLeaderWrapper(client, 'topic_noleader', 1))

            topics['topic_noleader'] = {
                0: PartitionMetadata('topic_noleader', 0, 0, [0, 1], [0, 1]),
                1: PartitionMetadata('topic_noleader', 1, 1, [1, 0], [1, 0])
            }
            kCodec.decode_metadata_response.return_value = (brokers, topics)
            self.assertEqual(
                brokers[0], getLeaderWrapper(client, 'topic_noleader', 0))
            self.assertEqual(
                brokers[1], getLeaderWrapper(client, 'topic_noleader', 1))

    @patch('kafkatwisted.client.KafkaCodec')
    @inlineCallbacks
    def test_send_produce_request_raises_when_noleader(self, kCodec):
        "Send producer request raises LeaderUnavailableError if leader is not available"

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
            if client.dMetaDataLoad is not None:
                client.dMetaDataLoad.callback('anything')

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
        brokermocks[0].connect.side_effect = [d, d, d]
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
                errors = self.flushLoggedErrors(ConnectionRefusedError)
                self.assertEqual(len(errors), 1)
