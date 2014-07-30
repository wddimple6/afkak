"""
Test code for KafkaClient(object) class.
"""

from __future__ import division, absolute_import

import pickle

from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredList, maybeDeferred, succeed

import os
import random
import struct
import logging

logging.basicConfig(filename='_trial_temp/test_client.log')

from mock import MagicMock, patch

from kafkatwisted import KafkaClient
from kafkatwisted.common import (
    ProduceRequest, BrokerMetadata, PartitionMetadata, RequestTimedOutError,
    TopicAndPartition, KafkaUnavailableError,
    LeaderUnavailableError, PartitionUnavailableError,
)
from kafkatwisted.protocol import KafkaProtocol
from kafkatwisted.kafkacodec import (create_message, KafkaCodec)
from kafkatwisted.client import collect_hosts
from twisted.python.failure import Failure


class TestKafkaClient(TestCase):
    def test_init_with_list(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts=['kafka01:9092', 'kafka02:9092', 'kafka03:9092'])

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

        # inject KafkaConnection side effects
        mocked_brokers[('kafka01', 9092)].makeRequest.side_effect = \
            RequestTimedOutError("kafka01 went away (unittest)")
        mocked_brokers[('kafka02', 9092)].makeRequest.side_effect = \
            RequestTimedOutError("Kafka02 went away (unittest)")

        def mock_get_brkr(host, port):
            return mocked_brokers[(host, port)]

        # patch load_metadata_for_topics to do nothing...
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_brokerclient',
                              side_effect=mock_get_brkr):
                client = KafkaClient(hosts=['kafka01:9092', 'kafka02:9092'])

                self.assertRaises(
                    KafkaUnavailableError, client._send_broker_unaware_request,
                    1, 'fake request',
                )

                for key, brkr in mocked_brokers.iteritems():
                    brkr.makeRequest.assert_called_with(1, 'fake request')

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
            return succeed(mocked_brokers[(host, port)])

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
        client = KafkaClient(hosts=['broker_1:4567'])
        # this will trigger the call to decode the metadata, which we
        # mocked above
        client.dMetaDataLoad.callback('anything')

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

        kCodec.recv.return_value = 'response'  # anything but None

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {'topic_no_partitions': {}}
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        # topic metadata is loaded but empty
        self.assertDictEqual({}, client.topics_to_brokers)

        topics['topic_no_partitions'] = {
            0: PartitionMetadata('topic_no_partitions', 0, 0, [0, 1], [0, 1])
        }
        protocol.decode_metadata_response.return_value = (brokers, topics)

        # calling _get_leader_for_partition (from any broker aware request)
        # will try loading metadata again for the same topic
        leader = client._get_leader_for_partition('topic_no_partitions', 0)

        self.assertEqual(brokers[0], leader)
        self.assertDictEqual({
            TopicAndPartition('topic_no_partitions', 0): brokers[0]},
            client.topics_to_brokers)

    @patch('kafkatwisted.client.KafkaCodec')
    def test_get_leader_for_unassigned_partitions(self, protocol, conn):
        "Get leader raises if no partitions is defined for a topic"

        conn.recv.return_value = 'response'  # anything but None

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {'topic_no_partitions': {}}
        protocol.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        self.assertDictEqual({}, client.topics_to_brokers)

        with self.assertRaises(PartitionUnavailableError):
            client._get_leader_for_partition('topic_no_partitions', 0)

    @patch('kafkatwisted.client.KafkaCodec')
    def test_get_leader_returns_none_when_noleader(self, protocol, conn):
        "Getting leader for partitions returns None when the partiion has no leader"

        conn.recv.return_value = 'response'  # anything but None

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {}
        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, -1, [], [])
        }
        protocol.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])
        self.assertDictEqual(
            {
                TopicAndPartition('topic_noleader', 0): None,
                TopicAndPartition('topic_noleader', 1): None
            },
            client.topics_to_brokers)
        self.assertIsNone(client._get_leader_for_partition('topic_noleader', 0))
        self.assertIsNone(client._get_leader_for_partition('topic_noleader', 1))

        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, 0, [0, 1], [0, 1]),
            1: PartitionMetadata('topic_noleader', 1, 1, [1, 0], [1, 0])
        }
        protocol.decode_metadata_response.return_value = (brokers, topics)
        self.assertEqual(brokers[0], client._get_leader_for_partition('topic_noleader', 0))
        self.assertEqual(brokers[1], client._get_leader_for_partition('topic_noleader', 1))

    @patch('kafkatwisted.client.KafkaCodec')
    def test_send_produce_request_raises_when_noleader(self, protocol, conn):
        "Send producer request raises LeaderUnavailableError if leader is not available"

        conn.recv.return_value = 'response'  # anything but None

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {}
        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, -1, [], [])
        }
        protocol.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        requests = [ProduceRequest(
            "topic_noleader", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(LeaderUnavailableError):
            client.send_produce_request(requests)

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

