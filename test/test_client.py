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
        print "ZORG: getResult1", r
        result[0] = r
    def gotErr(e):
        # return the error for comparison by caller
        result[0] = e

    print "ZORG: getLeaderWrapper1", result
    d = client._get_leader_for_partition(*args)
    result = [d]
    print "ZORG: getLeaderWrapper2", result
    d.addCallback(getResult)
    d.addErrback(gotErr)
    print "ZORG: getLeaderWrapper3", result
    if client.dMetaDataLoad is not None:
        print "ZORG: getLeaderWrapper3.1", result
        client.dMetaDataLoad.callback('anything')
    print "ZORG: getLeaderWrapper4", result
    return result[0]

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
        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
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

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {'topic_no_partitions': {}}
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
        # this will trigger the call to decode the metadata, which we
        # mocked above
        client.dMetaDataLoad.callback('anything')

        # topic metadata is loaded but empty
        self.assertDictEqual({}, client.topics_to_brokers)

        topics['topic_no_partitions'] = {
            0: PartitionMetadata('topic_no_partitions', 0, 0, [0, 1], [0, 1])
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        # calling _get_leader_for_partition (from any broker aware request)
        # will try loading metadata again for the same topic
        leader = getLeaderWrapper(client, 'topic_no_partitions', 0)

        self.assertEqual(brokers[0], leader)
        self.assertDictEqual({
            TopicAndPartition('topic_no_partitions', 0): brokers[0]},
            client.topics_to_brokers)

    @patch('kafkatwisted.client.KafkaCodec')
    def test_get_leader_for_unassigned_partitions(self, kCodec):
        "Get leader raises if no partitions is defined for a topic"

        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {'topic_no_partitions': {}}
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
        # this will trigger the call to decode the metadata, which we
        # mocked above
        client.dMetaDataLoad.callback('anything')

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

        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
        # this will trigger the call to decode the metadata, which we
        # mocked above
        client.dMetaDataLoad.callback('anything1')
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

        self.timeout = 5
        brokers = {}
        brokers[0] = BrokerMetadata(0, 'broker_1', 4567)
        brokers[1] = BrokerMetadata(1, 'broker_2', 5678)

        topics = {}
        topics['topic_noleader'] = {
            0: PartitionMetadata('topic_noleader', 0, -1, [], []),
            1: PartitionMetadata('topic_noleader', 1, -1, [], [])
        }
        kCodec.decode_metadata_response.return_value = (brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'], timeout=None)
        # this will trigger the call to decode the metadata, which we
        # mocked above
        client.dMetaDataLoad.callback('anything')

        # create a list of requests (really just one)
        requests = [ProduceRequest(
            "topic_noleader", 0,
            [create_message("a"), create_message("b")])]
        # Attempt to send it, and ensure the returned deferred fails
        # properly
        print "ZORG: test_send_produce_request_raises_when_noleader0"
        fail = client.send_produce_request(requests)
        print "ZORG: test_send_produce_request_raises_when_noleader1", fail
        f2 = self.failUnlessFailure(fail, LeaderUnavailableError)
        print "ZORG: test_send_produce_request_raises_when_noleader2", f2
        if client.dMetaDataLoad is not None:
            print "ZORG: test_send_produce_request_raises_when_noleader2.1"
            client.dMetaDataLoad.callback('anything')

        print "ZORG: test_send_produce_request_raises_when_noleader3", fail
        result = yield fail
        print "ZORG: test_send_produce_request_raises_when_noleader4", result


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

