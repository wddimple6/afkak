# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

"""
Test code for KafkaCodec(object) class.
"""

from __future__ import division, absolute_import

from unittest2 import TestCase, SkipTest

from contextlib import contextmanager
import struct

import mock
from mock import sentinel

from afkak.common import (
    OffsetRequest, OffsetCommitRequest, OffsetFetchRequest,
    OffsetResponse, OffsetCommitResponse, OffsetFetchResponse,
    ProduceRequest, FetchRequest, Message, ChecksumError,
    ConsumerFetchSizeTooSmall, ProduceResponse, FetchResponse,
    OffsetAndMessage, BrokerMetadata, PartitionMetadata, TopicMetadata,
    ProtocolError, UnsupportedCodecError, InvalidMessageError,
    ConsumerMetadataResponse,
)
from afkak.codec import (
    has_snappy, gzip_decode, snappy_decode
)
import afkak.kafkacodec
from afkak.kafkacodec import (
    ATTRIBUTE_CODEC_MASK, CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY,
    create_message, create_gzip_message, create_snappy_message,
    create_message_set, KafkaCodec
)
from .testutil import make_send_requests


def create_encoded_metadata_response(broker_data, topic_data):
    encoded = struct.pack('>ii', 3, len(broker_data))
    for node_id, broker in broker_data.iteritems():
        encoded += struct.pack('>ih%dsi' % len(broker.host), node_id,
                               len(broker.host), broker.host, broker.port)

    encoded += struct.pack('>i', len(topic_data))
    for topic, topic_metadata in topic_data.iteritems():
        _, topic_err, partitions = topic_metadata
        encoded += struct.pack('>hh%dsi' % len(topic), topic_err,
                               len(topic), topic, len(partitions))
        for partition, metadata in partitions.iteritems():
            encoded += struct.pack('>hiii',
                                   metadata.partition_error_code,
                                   partition, metadata.leader,
                                   len(metadata.replicas))
            if len(metadata.replicas) > 0:
                encoded += struct.pack('>%di' % len(metadata.replicas),
                                       *metadata.replicas)

            encoded += struct.pack('>i', len(metadata.isr))
            if len(metadata.isr) > 0:
                encoded += struct.pack('>%di' % len(metadata.isr),
                                       *metadata.isr)

    return encoded


class TestKafkaCodec(TestCase):
    def test_create_message(self):
        payload = "test"
        key = "key"
        msg = create_message(payload, key)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, 0)
        self.assertEqual(msg.key, key)
        self.assertEqual(msg.value, payload)

    def test_create_gzip(self):
        message_list = [create_message("v1", None),
                        create_message("v2", key='42')]
        msg = create_gzip_message(message_list)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, ATTRIBUTE_CODEC_MASK & CODEC_GZIP)
        self.assertEqual(msg.key, None)
        # Need to decode to check since gzipped payload is non-deterministic
        decoded = gzip_decode(msg.value)
        expect = "".join([
            struct.pack(">q", 0),           # MsgSet offset
            struct.pack(">i", 16),          # MsgSet size
            struct.pack(">i", 1285512130),  # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", -1),          # -1 indicates a null key
            struct.pack(">i", 2),           # Msg length (bytes)
            "v1",                           # Message contents

            struct.pack(">q", 0),           # MsgSet offset
            struct.pack(">i", 18),          # MsgSet size
            struct.pack(">i", 1929437987),  # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i2s", 2, '42'),   # Key is 2 bytes long, 42
            struct.pack(">i", 2),           # Msg length (bytes)
            "v2",                           # Message contents
        ])

        self.assertEqual(decoded, expect)

    def test_create_snappy(self):
        if not has_snappy():
            raise SkipTest("Snappy not available")  # pragma: no cover
        message_list = [create_message("v3", key='84'),
                        create_message("v4", None)]
        msg = create_snappy_message(message_list)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, ATTRIBUTE_CODEC_MASK & CODEC_SNAPPY)
        self.assertEqual(msg.key, None)
        decoded = snappy_decode(msg.value)
        expect = "".join([
            struct.pack(">q", 0),           # MsgSet offset
            struct.pack(">i", 18),          # MsgSet size
            struct.pack(">i", 813233088),   # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i2s", 2, '84'),   # Key is 2 bytes long, '84'
            struct.pack(">i", 2),           # Msg length (bytes)
            "v3",                           # Message contents

            struct.pack(">q", 0),           # MsgSet offset
            struct.pack(">i", 16),          # MsgSet size
            struct.pack(">i", 1022734157),  # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", -1),          # -1 indicates a null key
            struct.pack(">i", 2),           # Msg length (bytes)
            "v4",                           # Message contents
        ])

        self.assertEqual(decoded, expect)

    def test_encode_message_header(self):
        expect = "".join([
            struct.pack(">h", 10),              # API Key
            struct.pack(">h", 0),               # API Version
            struct.pack(">i", 4),               # Correlation Id
            struct.pack(">h", len("client3")),  # Length of clientId
            "client3",                          # ClientId
        ])

        encoded = KafkaCodec._encode_message_header("client3", 4, 10)
        self.assertEqual(encoded, expect)

    def test_encode_message(self):
        message = create_message("test", "key")
        encoded = KafkaCodec._encode_message(message)
        expect = "".join([
            struct.pack(">i", -1427009701),  # CRC
            struct.pack(">bb", 0, 0),        # Magic, flags
            struct.pack(">i", 3),            # Length of key
            "key",                           # key
            struct.pack(">i", 4),            # Length of value
            "test",                          # value
        ])

        self.assertEqual(encoded, expect)

    def test_decode_message(self):
        encoded = "".join([
            struct.pack(">i", -1427009701),  # CRC
            struct.pack(">bb", 0, 0),        # Magic, flags
            struct.pack(">i", 3),            # Length of key
            "key",                           # key
            struct.pack(">i", 4),            # Length of value
            "test",                          # value
        ])

        offset = 10
        (returned_offset, decoded_message) = list(
            KafkaCodec._decode_message(encoded, offset))[0]

        self.assertEqual(returned_offset, offset)
        self.assertEqual(decoded_message, create_message("test", "key"))

    def test_encode_message_failure(self):
        self.assertRaises(ProtocolError,
                          KafkaCodec._encode_message,
                          Message(1, 0, "key", "test"))

    def test_encode_message_set(self):
        message_set = [
            create_message("v1", "k1"),
            create_message("v2", "k2")
        ]

        encoded = KafkaCodec._encode_message_set(message_set)
        expect = "".join([
            struct.pack(">q", 0),           # MsgSet Offset
            struct.pack(">i", 18),          # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", 2),           # Length of key
            "k1",                           # Key
            struct.pack(">i", 2),           # Length of value
            "v1",                           # Value

            struct.pack(">q", 0),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            "k2",                          # Key
            struct.pack(">i", 2),          # Length of value
            "v2",                          # Value
        ])

        self.assertEqual(encoded, expect)

    def test_decode_message_set(self):
        encoded = "".join([
            struct.pack(">q", 0),           # MsgSet Offset
            struct.pack(">i", 18),          # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", 2),           # Length of key
            "k1",                           # Key
            struct.pack(">i", 2),           # Length of value
            "v1",                           # Value

            struct.pack(">q", 1),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            "k2",                          # Key
            struct.pack(">i", 2),          # Length of value
            "v2",                          # Value
        ])

        msgs = list(KafkaCodec._decode_message_set_iter(encoded))
        self.assertEqual(len(msgs), 2)
        msg1, msg2 = msgs

        returned_offset1, decoded_message1 = msg1
        returned_offset2, decoded_message2 = msg2

        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1", "k1"))

        self.assertEqual(returned_offset2, 1)
        self.assertEqual(decoded_message2, create_message("v2", "k2"))

    def test_decode_message_gzip(self):
        gzip_encoded = ('\xc0\x11\xb2\xf0\x00\x01\xff\xff\xff\xff\x00\x00\x000'
                        '\x1f\x8b\x08\x00\xa1\xc1\xc5R\x02\xffc`\x80\x03\x01'
                        '\x9f\xf9\xd1\x87\x18\x18\xfe\x03\x01\x90\xc7Tf\xc8'
                        '\x80$wu\x1aW\x05\x92\x9c\x11\x00z\xc0h\x888\x00\x00'
                        '\x00')
        offset = 11
        messages = list(KafkaCodec._decode_message(gzip_encoded, offset))

        self.assertEqual(len(messages), 2)
        msg1, msg2 = messages

        returned_offset1, decoded_message1 = msg1
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1"))

        returned_offset2, decoded_message2 = msg2
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message("v2"))

    def test_decode_message_snappy(self):
        if not has_snappy():
            raise SkipTest("Snappy not available")  # pragma: no cover
        snappy_encoded = ('\xec\x80\xa1\x95\x00\x02\xff\xff\xff\xff\x00\x00'
                          '\x00,8\x00\x00\x19\x01@\x10L\x9f[\xc2\x00\x00\xff'
                          '\xff\xff\xff\x00\x00\x00\x02v1\x19\x1bD\x00\x10\xd5'
                          '\x96\nx\x00\x00\xff\xff\xff\xff\x00\x00\x00\x02v2')
        offset = 11
        messages = list(KafkaCodec._decode_message(snappy_encoded, offset))
        self.assertEqual(len(messages), 2)

        msg1, msg2 = messages

        returned_offset1, decoded_message1 = msg1
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1"))

        returned_offset2, decoded_message2 = msg2
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message("v2"))

    def test_decode_message_checksum_error(self):
        invalid_encoded_message = "This is not a valid encoded message"
        iter = KafkaCodec._decode_message(invalid_encoded_message, 0)
        self.assertRaises(ChecksumError, list, iter)

    # NOTE: The error handling in _decode_message_set_iter() is questionable.
    # If it's modified, the next two tests might need to be fixed.
    def test_decode_message_set_fetch_size_too_small(self):
        self.assertRaises(ConsumerFetchSizeTooSmall,
                          list, KafkaCodec._decode_message_set_iter('a'))

    def test_decode_message_set_stop_iteration(self):
        encoded = "".join([
            struct.pack(">q", 0),           # MsgSet Offset
            struct.pack(">i", 18),          # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", 2),           # Length of key
            "k1",                           # Key
            struct.pack(">i", 2),           # Length of value
            "v1",                           # Value

            struct.pack(">q", 1),           # MsgSet Offset
            struct.pack(">i", 18),          # Msg Size
            struct.pack(">i", -16383415),   # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", 2),           # Length of key
            "k2",                           # Key
            struct.pack(">i", 2),           # Length of value
            "v2",                           # Value
            "@1$%(Y!",                      # Random padding
        ])

        msgs = list(KafkaCodec._decode_message_set_iter(encoded))
        self.assertEqual(len(msgs), 2)
        msg1, msg2 = msgs

        returned_offset1, decoded_message1 = msg1
        returned_offset2, decoded_message2 = msg2

        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1", "k1"))

        self.assertEqual(returned_offset2, 1)
        self.assertEqual(decoded_message2, create_message("v2", "k2"))

    def test_get_response_correlation_id(self):
        t1 = "topic1"
        t2 = "topic2"
        corrID = 30372
        encoded = struct.pack('>iih%dsiihqihqh%dsiihq' % (len(t1), len(t2)),
                              corrID, 2, len(t1), t1, 2, 0, 0, 10L, 1, 1, 20L,
                              len(t2), t2, 1, 0, 0, 30L)
        self.assertEqual(
            corrID, KafkaCodec.get_response_correlation_id(encoded))

    def test_encode_produce_request(self):
        requests = [
            ProduceRequest("topic1", 0, [
                create_message("a"),
                create_message("b")
            ]),
            ProduceRequest("topic2", 1, [
                create_message("c")
            ])
        ]

        msg_a_binary = KafkaCodec._encode_message(create_message("a"))
        msg_b_binary = KafkaCodec._encode_message(create_message("b"))
        msg_c_binary = KafkaCodec._encode_message(create_message("c"))

        header = "".join([
            struct.pack('>h', 0),        # Msg Header, Message type = Produce
            struct.pack('>h', 0),        # Msg Header, API version
            struct.pack('>i', 2),        # Msg Header, Correlation ID
            struct.pack('>h7s', 7, "client1"),  # Msg Header, The client ID
            struct.pack('>h', 2),        # Num acks required
            struct.pack('>i', 100),      # Request Timeout
            struct.pack('>i', 2),        # The number of requests
        ])

        total_len = len(msg_a_binary) + len(msg_b_binary)
        topic1 = "".join([
            struct.pack('>h6s', 6, 'topic1'),          # The topic1
            struct.pack('>i', 1),                      # One message set
            struct.pack('>i', 0),                      # Partition 0
            struct.pack('>i', total_len + 24),         # Size of message set
            struct.pack('>q', 0),                      # No offset specified
            struct.pack('>i', len(msg_a_binary)),      # Length of message
            msg_a_binary,                              # Actual message
            struct.pack('>q', 0),                      # No offset specified
            struct.pack('>i', len(msg_b_binary)),      # Length of message
            msg_b_binary,                              # Actual message
        ])

        topic2 = "".join([
            struct.pack('>h6s', 6, 'topic2'),           # The topic1
            struct.pack('>i', 1),                       # One message set
            struct.pack('>i', 1),                       # Partition 1
            struct.pack('>i', len(msg_c_binary) + 12),  # Size of message set
            struct.pack('>q', 0),                       # No offset specified
            struct.pack('>i', len(msg_c_binary)),       # Length of message
            msg_c_binary,                               # Actual message
        ])

        expected1 = "".join([header, topic1, topic2])
        expected2 = "".join([header, topic2, topic1])

        encoded = KafkaCodec.encode_produce_request(
            "client1", 2, requests, 2, 100)
        self.assertIn(encoded, [expected1, expected2])

    def test_decode_produce_response(self):
        t1 = "topic1"
        t2 = "topic2"
        encoded = struct.pack('>iih%dsiihqihqh%dsiihq' % (len(t1), len(t2)),
                              2, 2, len(t1), t1, 2, 0, 0, 10L, 1, 1, 20L,
                              len(t2), t2, 1, 0, 0, 30L)
        responses = list(KafkaCodec.decode_produce_response(encoded))
        self.assertEqual(responses,
                         [ProduceResponse(t1, 0, 0, 10L),
                          ProduceResponse(t1, 1, 1, 20L),
                          ProduceResponse(t2, 0, 0, 30L)])

    def test_encode_fetch_request(self):
        requests = [
            FetchRequest("topic1", 0, 10, 1024),
            FetchRequest("topic2", 1, 20, 100),
        ]

        header = "".join([
            struct.pack('>h', 1),       # Msg Header, Message type = Fetch
            struct.pack('>h', 0),               # Msg Header, API version
            struct.pack('>i', 3),               # Msg Header, Correlation ID
            struct.pack('>h7s', 7, "client1"),  # Msg Header, The client ID
            struct.pack('>i', -1),              # Replica Id
            struct.pack('>i', 2),               # Max wait time
            struct.pack('>i', 100),             # Min bytes
            struct.pack('>i', 2),               # Num requests
        ])

        topic1 = "".join([
            struct.pack('>h6s', 6, 'topic1'),  # Topic
            struct.pack('>i', 1),              # Num Payloads
            struct.pack('>i', 0),              # Partition 0
            struct.pack('>q', 10),             # Offset
            struct.pack('>i', 1024),           # Max Bytes
        ])

        topic2 = "".join([
            struct.pack('>h6s', 6, 'topic2'),  # Topic
            struct.pack('>i', 1),              # Num Payloads
            struct.pack('>i', 1),              # Partition 0
            struct.pack('>q', 20),             # Offset
            struct.pack('>i', 100),            # Max Bytes
        ])

        expected1 = "".join([header, topic1, topic2])
        expected2 = "".join([header, topic2, topic1])

        encoded = KafkaCodec.encode_fetch_request(
            "client1", 3, requests, 2, 100)
        self.assertIn(encoded, [expected1, expected2])

    def test_decode_fetch_response(self):
        t1 = "topic1"
        t2 = "topic2"
        msgs = map(create_message, ["message1", "hi", "boo", "foo", "so fun!"])
        ms1 = KafkaCodec._encode_message_set([msgs[0], msgs[1]])
        ms2 = KafkaCodec._encode_message_set([msgs[2]])
        ms3 = KafkaCodec._encode_message_set([msgs[3], msgs[4]])

        encoded = struct.pack('>iih%dsiihqi%dsihqi%dsh%dsiihqi%ds' %
                              (len(t1), len(ms1), len(ms2), len(t2), len(ms3)),
                              4, 2, len(t1), t1, 2, 0, 0, 10, len(ms1), ms1, 1,
                              1, 20, len(ms2), ms2, len(t2), t2, 1, 0, 0, 30,
                              len(ms3), ms3)

        responses = list(KafkaCodec.decode_fetch_response(encoded))

        def expand_messages(response):
            return FetchResponse(response.topic, response.partition,
                                 response.error, response.highwaterMark,
                                 list(response.messages))

        expanded_responses = map(expand_messages, responses)
        expect = [FetchResponse(t1, 0, 0, 10, [OffsetAndMessage(0, msgs[0]),
                                               OffsetAndMessage(0, msgs[1])]),
                  FetchResponse(t1, 1, 1, 20, [OffsetAndMessage(0, msgs[2])]),
                  FetchResponse(t2, 0, 0, 30, [OffsetAndMessage(0, msgs[3]),
                                               OffsetAndMessage(0, msgs[4])])]
        self.assertEqual(expanded_responses, expect)

    def test_encode_metadata_request_no_topics(self):
        expected = "".join([
            struct.pack('>h', 3),           # API key metadata fetch
            struct.pack('>h', 0),           # API version
            struct.pack('>i', 4),           # Correlation ID
            struct.pack('>h3s', 3, "cid"),  # The client ID
            struct.pack('>i', 0),           # No topics, give all the data!
        ])

        encoded = KafkaCodec.encode_metadata_request("cid", 4)

        self.assertEqual(encoded, expected)

    def test_encode_metadata_request_with_topics(self):
        expected = "".join([
            struct.pack('>h', 3),           # API key metadata fetch
            struct.pack('>h', 0),           # API version
            struct.pack('>i', 4),           # Correlation ID
            struct.pack('>h3s', 3, "cid"),  # The client ID
            struct.pack('>i', 2),           # Number of topics in the request
            struct.pack('>h2s', 2, "t1"),   # Topic "t1"
            struct.pack('>h2s', 2, "t2"),   # Topic "t2"
        ])

        encoded = KafkaCodec.encode_metadata_request("cid", 4, ["t1", "t2"])

        self.assertEqual(encoded, expected)

    def test_decode_metadata_response(self):
        node_brokers = {
            0: BrokerMetadata(0, "brokers1.afkak.rdio.com", 1000),
            1: BrokerMetadata(1, "brokers1.afkak.rdio.com", 1001),
            3: BrokerMetadata(3, "brokers2.afkak.rdio.com", 1000)
        }

        topic_partitions = {
            "topic1": TopicMetadata(
                'topic1', 0, {
                    0: PartitionMetadata("topic1", 0, 0, 1, (0, 2), (2,)),
                    1: PartitionMetadata("topic1", 1, 1, 3, (0, 1), (0, 1))
                    }),
            "topic2": TopicMetadata(
                'topic2', 1, {
                    0: PartitionMetadata("topic2", 0, 0, 0, (), ())
                    })
            }
        encoded = create_encoded_metadata_response(
            node_brokers, topic_partitions)
        decoded = KafkaCodec.decode_metadata_response(encoded)
        self.assertEqual(decoded, (node_brokers, topic_partitions))

    def test_decode_metadata_response_failures(self):
        data = "".join([
            struct.pack(">ii", 0xCAFE, 0xBABE),  # correlation ID & numbrokers
            struct.pack('>i', 1234),             # Node Id
            struct.pack('>h8s', 3, "hostname"),  # host
            struct.pack('>i', 1025),             # port
        ])
        self.assertRaises(InvalidMessageError,
                          KafkaCodec.decode_metadata_response, data)

    def test_encode_offset_request(self):
        expected = "".join([
            struct.pack('>h', 2),           # Message type = offset fetch
            struct.pack('>h', 0),           # API version
            struct.pack('>i', 4),           # Correlation ID
            struct.pack('>h3s', 3, "cid"),  # The client ID
            struct.pack('>i', -1),          # Replica Id
            struct.pack('>i', 0),           # No topic/partitions
        ])

        encoded = KafkaCodec.encode_offset_request("cid", 4)

        self.assertEqual(encoded, expected)

    def test_encode_offset_request__no_payload(self):
        expected = "".join([
            struct.pack('>h', 2),             # Message type = offset fetch
            struct.pack('>h', 0),             # API version
            struct.pack('>i', 4),             # Correlation ID
            struct.pack('>h3s', 3, "cid"),    # The client ID
            struct.pack('>i', -1),            # Replica Id
            struct.pack('>i', 1),             # Num topics
            struct.pack(">h6s", 6, "topic1"),  # Topic for the request
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 3),             # Partition 3
            struct.pack(">q", -1),            # No time offset
            struct.pack(">i", 1),             # One offset requested

            struct.pack(">i", 4),             # Partition 3
            struct.pack(">q", -1),            # No time offset
            struct.pack(">i", 1),             # One offset requested
        ])

        encoded = KafkaCodec.encode_offset_request("cid", 4, [
            OffsetRequest('topic1', 3, -1, 1),
            OffsetRequest('topic1', 4, -1, 1),
        ])

        self.assertEqual(encoded, expected)

    def test_decode_offset_response(self):
        encoded = "".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topics
            struct.pack(">h6s", 6, "topic1"),  # First topic
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 2),             # Partition 2
            struct.pack(">h", 0),             # No error
            struct.pack(">i", 1),             # One offset
            struct.pack(">q", 4),             # Offset 4

            struct.pack(">i", 4),             # Partition 4
            struct.pack(">h", 0),             # No error
            struct.pack(">i", 1),             # One offset
            struct.pack(">q", 8),             # Offset 8
        ])

        results = KafkaCodec.decode_offset_response(encoded)
        self.assertEqual(set(results), set([
            OffsetResponse(topic='topic1', partition=2, error=0, offsets=(4,)),
            OffsetResponse(topic='topic1', partition=4, error=0, offsets=(8,)),
        ]))

    def test_encode_offset_commit_request(self):
        header = "".join([
            struct.pack('>h', 8),                # Message type = offset commit
            struct.pack('>h', 1),                # API version
            struct.pack('>i', 42),               # Correlation ID
            struct.pack('>h9s', 9, "client_id"),  # The client ID
            struct.pack('>h8s', 8, "group_id"),  # The group to commit for
            struct.pack('>i', 996),              # Group generation ID
            struct.pack('>h11s', 11, 'consumer_id'),  # Consumer ID
            struct.pack('>i', 2),                # Num topics
        ])

        topic1 = "".join([
            struct.pack(">h6s", 6, "topic1"),    # Topic for the request
            struct.pack(">i", 2),                # Two partitions
            struct.pack(">i", 0),                # Partition 0
            struct.pack(">q", 123),              # Offset 123
            struct.pack(">q", 1437585816816),    # Timestamp in ms > epoch
            struct.pack(">h", -1),               # Null metadata
            struct.pack(">i", 1),                # Partition 1
            struct.pack(">q", 234),              # Offset 234
            struct.pack(">q", 1436981054199),    # Timestamp in ms > epoch
            struct.pack(">h11s", 11, 'My_Metadata'),  # Null metadata
        ])

        topic2 = "".join([
            struct.pack(">h6s", 6, "topic2"),    # Topic for the request
            struct.pack(">i", 1),                # One partition
            struct.pack(">i", 2),                # Partition 2
            struct.pack(">q", 345),              # Offset 345
            struct.pack(">q", -1),               # Timestamp 'invalid-time'
            struct.pack(">h", -1),               # Null metadata
        ])

        # A dict is used, so we can't predict the order of the topics...
        expected1 = "".join([header, topic1, topic2])
        expected2 = "".join([header, topic2, topic1])

        encoded = KafkaCodec.encode_offset_commit_request(
            "client_id", 42, "group_id", 996, 'consumer_id', [
                OffsetCommitRequest("topic1", 0, 123, 1437585816816, None),
                OffsetCommitRequest("topic1", 1, 234, 1436981054199,
                                    'My_Metadata'),
                OffsetCommitRequest("topic2", 2, 345, -1, None),
                ])

        self.assertIn(encoded, [expected1, expected2])

    def test_decode_offset_commit_response(self):
        encoded = "".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topic
            struct.pack(">h6s", 6, "topic1"),  # First topic
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 2),             # Partition 2
            struct.pack(">h", 0),             # No error

            struct.pack(">i", 4),             # Partition 4
            struct.pack(">h", 0),             # No error
        ])

        results = KafkaCodec.decode_offset_commit_response(encoded)
        self.assertEqual(set(results), set([
            OffsetCommitResponse(topic='topic1', partition=2, error=0),
            OffsetCommitResponse(topic='topic1', partition=4, error=0),
        ]))

    def test_encode_offset_fetch_request(self):
        header = "".join([
            struct.pack('>h', 9),                # Message type = offset fetch
            struct.pack('>h', 1),                # API version
            struct.pack('>i', 42),               # Correlation ID
            struct.pack('>h9s', 9, "client_id"),  # The client ID
            struct.pack('>h8s', 8, "group_id"),  # The group to commit for
            struct.pack('>i', 2),                # Num topics
        ])

        topic1 = "".join([
            struct.pack(">h6s", 6, "topic1"),    # Topic for the request
            struct.pack(">i", 2),                # Two partitions
            struct.pack(">i", 0),                # Partition 0
            struct.pack(">i", 1),                # Partition 1
        ])

        topic2 = "".join([
            struct.pack(">h6s", 6, "topic2"),    # Topic for the request
            struct.pack(">i", 1),                # One partitions
            struct.pack(">i", 2),                # Partition 2
        ])

        expected1 = "".join([header, topic1, topic2])
        expected2 = "".join([header, topic2, topic1])

        encoded = KafkaCodec.encode_offset_fetch_request(
            "client_id", 42, "group_id", [
                OffsetFetchRequest("topic1", 0),
                OffsetFetchRequest("topic1", 1),
                OffsetFetchRequest("topic2", 2),
                ])

        self.assertIn(encoded, [expected1, expected2])

    def test_decode_offset_fetch_response(self):
        encoded = "".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topics
            struct.pack(">h6s", 6, "topic1"),  # First topic
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 2),             # Partition 2
            struct.pack(">q", 4),             # Offset 4
            struct.pack(">h4s", 4, "meta"),   # Metadata
            struct.pack(">h", 0),             # No error

            struct.pack(">i", 4),             # Partition 4
            struct.pack(">q", 8),             # Offset 8
            struct.pack(">h4s", 4, "meta"),   # Metadata
            struct.pack(">h", 0),             # No error
        ])

        results = KafkaCodec.decode_offset_fetch_response(encoded)
        self.assertEqual(set(results), set([
            OffsetFetchResponse(topic='topic1', partition=2, offset=4,
                                error=0, metadata="meta"),
            OffsetFetchResponse(topic='topic1', partition=4, offset=8,
                                error=0, metadata="meta"),
        ]))

    @contextmanager
    def mock_create_message_fns(self):
        p1 = mock.patch.object(afkak.kafkacodec, "create_message",
                               return_value=sentinel.message)
        p2 = mock.patch.object(afkak.kafkacodec, "create_gzip_message",
                               return_value=sentinel.gzip_message)
        p3 = mock.patch.object(afkak.kafkacodec, "create_snappy_message",
                               return_value=sentinel.snappy_message)
        with p1, p2, p3:
            yield

    def test_create_message_set(self):
        messages = [1, 2, 3]
        reqs = make_send_requests(messages)

        # Default codec is CODEC_NONE. Expect list of regular messages.
        expect = [sentinel.message] * len(messages)
        with self.mock_create_message_fns():
            message_set = create_message_set(reqs)
        self.assertEqual(message_set, expect)

        # CODEC_NONE: Expect list of regular messages.
        expect = [sentinel.message] * len(messages)
        with self.mock_create_message_fns():
            message_set = create_message_set(reqs, CODEC_NONE)
        self.assertEqual(message_set, expect)

        # CODEC_GZIP: Expect list of one gzip-encoded message.
        expect = [sentinel.gzip_message]
        with self.mock_create_message_fns():
            message_set = create_message_set(reqs, CODEC_GZIP)
        self.assertEqual(message_set, expect)

        # CODEC_SNAPPY: Expect list of one snappy-encoded message.
        expect = [sentinel.snappy_message]
        with self.mock_create_message_fns():
            message_set = create_message_set(reqs, CODEC_SNAPPY)
        self.assertEqual(message_set, expect)

        # Unknown codec should raise UnsupportedCodecError.
        self.assertRaises(UnsupportedCodecError,
                          create_message_set, reqs, -1)

    def test_encode_consumer_metadata_request(self):
        expected = "".join([
            struct.pack('>h', 10),          # API key ConsumerMetadataRequest
            struct.pack('>h', 0),           # API version
            struct.pack('>i', 9),           # Correlation ID
            struct.pack('>h3s', 3, "cID"),  # The client ID
            struct.pack('>h6s', 6, "group1"),   # Consumer group 'group1'
        ])

        encoded = KafkaCodec.encode_consumermetadata_request(
            "cID", 9, "group1")

        self.assertEqual(encoded, expected)

    def test_decode_consumermetadata_response(self):
        expected = ConsumerMetadataResponse(0, 5, "theHost", 4242)
        encoded = "".join([
            struct.pack('>i', 9),  # Correlation ID
            struct.pack('>h', 0),  # Error Code
            struct.pack('>i', 5),  # Coordinator ID
            struct.pack('>h7s', 7, "theHost"),  # Coordinator Host
            struct.pack('>i', 4242),  # Coordinator port
        ])
        decoded = KafkaCodec.decode_consumermetadata_response(encoded)
        self.assertEqual(decoded, expected)
