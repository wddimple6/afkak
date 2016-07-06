# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

from __future__ import absolute_import

import logging
import struct
import zlib

from .codec import (
    gzip_encode, gzip_decode, snappy_encode, snappy_decode
)
from .common import (
    BrokerMetadata, PartitionMetadata, Message, OffsetAndMessage,
    ProduceResponse, FetchResponse, OffsetResponse, TopicMetadata,
    OffsetCommitResponse, OffsetFetchResponse, ProtocolError,
    BufferUnderflowError, ChecksumError, ConsumerFetchSizeTooSmall,
    UnsupportedCodecError, InvalidMessageError, ConsumerMetadataResponse,
)
from .util import (
    read_short_string, read_int_string, relative_unpack,
    write_short_string, write_int_string, group_by_topic_and_partition
)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

ATTRIBUTE_CODEC_MASK = 0x03
CODEC_NONE = 0x00
CODEC_GZIP = 0x01
CODEC_SNAPPY = 0x02
ALL_CODECS = (CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY)
MAX_BROKERS = 1024

# Default number of msecs the lead-broker will wait for replics to
# ack produce requests before failing the request
DEFAULT_REPLICAS_ACK_TIMEOUT_MSECS = 1000


class KafkaCodec(object):
    """
    Class to encapsulate all of the protocol encoding/decoding.
    This class does not have any state associated with it, it is purely
    for organization.
    """
    PRODUCE_KEY = 0
    FETCH_KEY = 1
    OFFSET_KEY = 2
    METADATA_KEY = 3
    # Non-user facing control APIs: 4-7
    OFFSET_COMMIT_KEY = 8
    OFFSET_FETCH_KEY = 9
    CONSUMER_METADATA_KEY = 10

    ###################
    #   Private API   #
    ###################

    @classmethod
    def _encode_message_header(cls, client_id, correlation_id, request_key,
                               api_version=0):
        """
        Encode the common request envelope
        """
        return struct.pack('>hhih%ds' % len(client_id),
                           request_key,          # ApiKey
                           api_version,          # ApiVersion
                           correlation_id,       # CorrelationId
                           len(client_id),       # ClientId size
                           client_id)            # ClientId

    @classmethod
    def _encode_message_set(cls, messages, offset=None):
        """
        Encode a MessageSet. Unlike other arrays in the protocol,
        MessageSets are not length-prefixed.  Format::

            MessageSet => [Offset MessageSize Message]
              Offset => int64
              MessageSize => int32
        """
        message_set = ""
        incr = 1
        if offset is None:
            incr = 0
            offset = 0
        for message in messages:
            encoded_message = KafkaCodec._encode_message(message)
            message_set += struct.pack(
                '>qi%ds' % len(encoded_message), offset, len(encoded_message),
                encoded_message)
            offset += incr
        return message_set

    @classmethod
    def _encode_message(cls, message):
        """
        Encode a single message.

        The magic number of a message is a format version number.  The only
        supported magic number right now is zero.  Format::

            Message => Crc MagicByte Attributes Key Value
              Crc => int32
              MagicByte => int8
              Attributes => int8
              Key => bytes
              Value => bytes
        """
        if message.magic == 0:
            msg = struct.pack('>BB', message.magic, message.attributes)
            msg += write_int_string(message.key)
            msg += write_int_string(message.value)
            crc = zlib.crc32(msg)
            msg = struct.pack('>i%ds' % len(msg), crc, msg)
        else:
            raise ProtocolError("Unexpected magic number: %d" % message.magic)
        return msg

    @classmethod
    def _decode_message_set_iter(cls, data):
        """
        Iteratively decode a MessageSet

        Reads repeated elements of (offset, message), calling decode_message
        to decode a single message. Since compressed messages contain futher
        MessageSets, these two methods have been decoupled so that they may
        recurse easily.
        """
        cur = 0
        read_message = False
        while cur < len(data):
            try:
                ((offset, ), cur) = relative_unpack('>q', data, cur)
                (msg, cur) = read_int_string(data, cur)
                msgIter = KafkaCodec._decode_message(msg, offset)
                for (offset, message) in msgIter:
                    read_message = True
                    yield OffsetAndMessage(offset, message)
            except BufferUnderflowError:
                # NOTE: Not sure this is correct error handling:
                # Is it possible to get a BUE if the message set is somewhere
                # in the middle of the fetch response? If so, we probably have
                # an issue that's not fetch size too small.
                # Aren't we ignoring errors if we fail to unpack data by
                # raising StopIteration()?
                # If _decode_message() raises a ChecksumError, couldn't that
                # also be due to the fetch size being too small?
                if read_message is False:
                    # If we get a partial read of a message, but haven't
                    # yielded anything there's a problem
                    raise ConsumerFetchSizeTooSmall()
                else:
                    raise StopIteration()

    @classmethod
    def _decode_message(cls, data, offset):
        """
        Decode a single Message

        The only caller of this method is decode_message_set_iter.
        They are decoupled to support nested messages (compressed MessageSets).
        The offset is actually read from decode_message_set_iter (it is part
        of the MessageSet payload).
        """
        ((crc, magic, att), cur) = relative_unpack('>iBB', data, 0)
        if crc != zlib.crc32(data[4:]):
            raise ChecksumError("Message checksum failed")

        (key, cur) = read_int_string(data, cur)
        (value, cur) = read_int_string(data, cur)

        codec = att & ATTRIBUTE_CODEC_MASK

        if codec == CODEC_NONE:
            yield (offset, Message(magic, att, key, value))

        elif codec == CODEC_GZIP:
            gz = gzip_decode(value)
            for (offset, msg) in KafkaCodec._decode_message_set_iter(gz):
                yield (offset, msg)

        elif codec == CODEC_SNAPPY:
            snp = snappy_decode(value)
            for (offset, msg) in KafkaCodec._decode_message_set_iter(snp):
                yield (offset, msg)

    ##################
    #   Public API   #
    ##################

    @classmethod
    def get_response_correlation_id(cls, data):
        """
        return just the correlationId part of the response

        :param bytes data: bytes to decode
        """
        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        return correlation_id

    @classmethod
    def encode_produce_request(cls, client_id, correlation_id,
                               payloads=None, acks=1,
                               timeout=DEFAULT_REPLICAS_ACK_TIMEOUT_MSECS):
        """
        Encode some ProduceRequest structs

        :param bytes client_id:
        :param int correlation_id:
        :param list payloads: list of ProduceRequest
        :param int acks:

            How "acky" you want the request to be:

            0: immediate response
            1: written to disk by the leader
            2+: waits for this many number of replicas to sync
            -1: waits for all replicas to be in sync

        :param int timeout:
            Maximum time the server will wait for acks from replicas.  This is
            _not_ a socket timeout.
        """
        payloads = [] if payloads is None else payloads
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = cls._encode_message_header(client_id, correlation_id,
                                             KafkaCodec.PRODUCE_KEY)

        message += struct.pack('>hii', acks, timeout, len(grouped_payloads))

        for topic, topic_payloads in grouped_payloads.items():
            message += struct.pack('>h%dsi' % len(topic),
                                   len(topic), topic, len(topic_payloads))

            for partition, payload in topic_payloads.items():
                msg_set = KafkaCodec._encode_message_set(payload.messages)
                message += struct.pack('>ii%ds' % len(msg_set), partition,
                                       len(msg_set), msg_set)

        return struct.pack('>%ds' % len(message), message)

    @classmethod
    def decode_produce_response(cls, data):
        """
        Decode bytes to a ProduceResponse

        :param bytes data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

        for i in range(num_topics):
            ((strlen,), cur) = relative_unpack('>h', data, cur)
            topic = data[cur:cur + strlen]
            cur += strlen
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(num_partitions):
                ((partition, error, offset), cur) = relative_unpack('>ihq',
                                                                    data, cur)

                yield ProduceResponse(topic, partition, error, offset)

    @classmethod
    def encode_fetch_request(cls, client_id, correlation_id, payloads=None,
                             max_wait_time=100, min_bytes=4096):
        """
        Encodes some FetchRequest structs

        :param bytes client_id:
        :param int correlation_id:
        :param list payloads: list of :class:`FetchRequest`
        :param int max_wait_time: how long to block waiting on min_bytes of data
        :param int min_bytes:
            the minimum number of bytes to accumulate before returning the
            response
        """
        payloads = [] if payloads is None else payloads
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = cls._encode_message_header(client_id, correlation_id,
                                             KafkaCodec.FETCH_KEY)

        assert isinstance(max_wait_time, int)

        # -1 is the replica id
        message += struct.pack('>iiii', -1, max_wait_time, min_bytes,
                               len(grouped_payloads))

        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))
            for partition, payload in topic_payloads.items():
                message += struct.pack('>iqi', partition, payload.offset,
                                       payload.max_bytes)

        return struct.pack('>%ds' % len(message), message)

    @classmethod
    def decode_fetch_response(cls, data):
        """
        Decode bytes to a FetchResponse

        :param bytes data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for i in range(num_partitions):
                ((partition, error, highwater_mark_offset), cur) = \
                    relative_unpack('>ihq', data, cur)

                (message_set, cur) = read_int_string(data, cur)

                yield FetchResponse(
                    topic, partition, error,
                    highwater_mark_offset,
                    KafkaCodec._decode_message_set_iter(message_set))

    @classmethod
    def encode_offset_request(cls, client_id, correlation_id, payloads=None):
        payloads = [] if payloads is None else payloads
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = cls._encode_message_header(client_id, correlation_id,
                                             KafkaCodec.OFFSET_KEY)

        # -1 is the replica id
        message += struct.pack('>ii', -1, len(grouped_payloads))

        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))

            for partition, payload in topic_payloads.items():
                message += struct.pack('>iqi', partition, payload.time,
                                       payload.max_offsets)

        return struct.pack('>%ds' % len(message), message)

    @classmethod
    def decode_offset_response(cls, data):
        """
        Decode bytes to an :class:`OffsetResponse`

        :param bytes data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for i in range(num_partitions):
                ((partition, error, num_offsets,), cur) = \
                    relative_unpack('>ihi', data, cur)

                offsets = []
                for j in range(num_offsets):
                    ((offset,), cur) = relative_unpack('>q', data, cur)
                    offsets.append(offset)

                yield OffsetResponse(topic, partition, error, tuple(offsets))

    @classmethod
    def encode_metadata_request(cls, client_id, correlation_id, topics=None):
        """
        Encode a MetadataRequest

        :param bytes client_id: string
        :param int correlation_id: int
        :param list topics: list of bytes
        """
        topics = [] if topics is None else topics
        message = cls._encode_message_header(client_id, correlation_id,
                                             KafkaCodec.METADATA_KEY)

        message += struct.pack('>i', len(topics))

        for topic in topics:
            message += struct.pack('>h%ds' % len(topic), len(topic), topic)

        return message

    @classmethod
    def decode_metadata_response(cls, data):
        """
        Decode bytes to a MetadataResponse

        :param bytes data: bytes to decode
        """
        ((correlation_id, numbrokers), cur) = relative_unpack('>ii', data, 0)

        # In testing, I saw this routine swap my machine to death when
        # passed bad data. So, some checks are in order...
        if numbrokers > MAX_BROKERS:
            raise InvalidMessageError(
                "Brokers:{} exceeds max:{}".format(numbrokers, MAX_BROKERS))

        # Broker info
        brokers = {}
        for i in range(numbrokers):
            ((nodeId, ), cur) = relative_unpack('>i', data, cur)
            (host, cur) = read_short_string(data, cur)
            ((port,), cur) = relative_unpack('>i', data, cur)
            brokers[nodeId] = BrokerMetadata(nodeId, host, port)

        # Topic info
        ((num_topics,), cur) = relative_unpack('>i', data, cur)
        topic_metadata = {}

        for i in range(num_topics):
            ((topic_error,), cur) = relative_unpack('>h', data, cur)
            (topic_name, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            partition_metadata = {}

            for j in range(num_partitions):
                ((partition_error_code, partition, leader, numReplicas),
                 cur) = relative_unpack('>hiii', data, cur)

                (replicas, cur) = relative_unpack(
                    '>%di' % numReplicas, data, cur)

                ((num_isr,), cur) = relative_unpack('>i', data, cur)
                (isr, cur) = relative_unpack('>%di' % num_isr, data, cur)

                partition_metadata[partition] = \
                    PartitionMetadata(
                        topic_name, partition, partition_error_code, leader,
                        replicas, isr)

            topic_metadata[topic_name] = TopicMetadata(
                topic_name, topic_error, partition_metadata)

        return brokers, topic_metadata

    @classmethod
    def encode_consumermetadata_request(cls, client_id, correlation_id,
                                        consumer_group):
        """
        Encode a ConsumerMetadataRequest

        :param bytes client_id: string
        :param int correlation_id: int
        :param bytes consumer_group: string
        """
        message = cls._encode_message_header(client_id, correlation_id,
                                             KafkaCodec.CONSUMER_METADATA_KEY)

        message += struct.pack('>h%ds' % len(consumer_group),
                               len(consumer_group), consumer_group)

        return message

    @classmethod
    def decode_consumermetadata_response(cls, data):
        """
        Decode bytes to a ConsumerMetadataResponse

        :param bytes data: bytes to decode
        """
        (correlation_id, error_code, node_id), cur = \
            relative_unpack('>ihi', data, 0)
        host, cur = read_short_string(data, cur)
        (port,), cur = relative_unpack('>i', data, cur)

        return ConsumerMetadataResponse(
            error_code, node_id, host, port)

    @classmethod
    def encode_offset_commit_request(cls, client_id, correlation_id,
                                     group, group_generation_id, consumer_id,
                                     payloads):
        """
        Encode some OffsetCommitRequest structs (v1)

        :param bytes client_id: string
        :param int correlation_id: int
        :param bytes group: the consumer group to which you are committing offsets
        :param int group_generation_id: int32, generation ID of the group
        :param bytes consumer_id: string, Identifier for the consumer
        :param list payloads: list of :class:`OffsetCommitRequest`
        """
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = cls._encode_message_header(
            client_id, correlation_id, KafkaCodec.OFFSET_COMMIT_KEY,
            api_version=1)

        message += write_short_string(group)
        message += struct.pack('>i', group_generation_id)
        message += write_short_string(consumer_id)
        message += struct.pack('>i', len(grouped_payloads))

        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))

            for partition, payload in topic_payloads.items():
                message += struct.pack('>iqq', partition, payload.offset,
                                       payload.timestamp)
                message += write_short_string(payload.metadata)

        return struct.pack('>%ds' % len(message), message)

    @classmethod
    def decode_offset_commit_response(cls, data):
        """
        Decode bytes to an OffsetCommitResponse

        :param bytes data: bytes to decode
        """
        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        ((num_topics,), cur) = relative_unpack('>i', data, cur)

        for i in xrange(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for i in xrange(num_partitions):
                ((partition, error), cur) = relative_unpack('>ih', data, cur)
                yield OffsetCommitResponse(topic, partition, error)

    @classmethod
    def encode_offset_fetch_request(cls, client_id, correlation_id,
                                    group, payloads):
        """
        Encode some OffsetFetchRequest structs

        :param bytes client_id: string
        :param int correlation_id: int
        :param bytes group: string, the consumer group you are fetching offsets for
        :param list payloads: list of :class:`OffsetFetchRequest`
        """
        grouped_payloads = group_by_topic_and_partition(payloads)
        message = cls._encode_message_header(
            client_id, correlation_id, KafkaCodec.OFFSET_FETCH_KEY,
            api_version=1)

        message += write_short_string(group)
        message += struct.pack('>i', len(grouped_payloads))

        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))

            for partition, payload in topic_payloads.items():
                message += struct.pack('>i', partition)

        return struct.pack('>%ds' % len(message), message)

    @classmethod
    def decode_offset_fetch_response(cls, data):
        """
        Decode bytes to an OffsetFetchResponse

        :param bytes data: bytes to decode
        """

        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        ((num_topics,), cur) = relative_unpack('>i', data, cur)

        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for i in range(num_partitions):
                ((partition, offset), cur) = relative_unpack('>iq', data, cur)
                (metadata, cur) = read_short_string(data, cur)
                ((error,), cur) = relative_unpack('>h', data, cur)

                yield OffsetFetchResponse(topic, partition, offset,
                                          metadata, error)


def create_message(payload, key=None):
    """
    Construct a :class:`Message`

    :param bytes payload: the payload to send to Kafka
    :param bytes key: bytes, a key used for partition routing (optional)
    """
    return Message(0, 0, key, payload)


def create_gzip_message(message_set):
    """
    Construct a gzip-compressed message containing multiple messages

    The given messages will be encoded, compressed, and sent as a single atomic
    message to Kafka.

    :param list message_set: a list of :class:`Message` instances
    """
    encoded_message_set = KafkaCodec._encode_message_set(message_set)

    gzipped = gzip_encode(encoded_message_set)
    codec = ATTRIBUTE_CODEC_MASK & CODEC_GZIP

    return Message(0, 0x00 | codec, None, gzipped)


def create_snappy_message(message_set):
    """
    Construct a Snappy-compressed message containing multiple messages

    The given messages will be encoded, compressed, and sent as a single atomic
    message to Kafka.

    :param list message_set: a list of :class:`Message` instances
    """
    encoded_message_set = KafkaCodec._encode_message_set(message_set)

    snapped = snappy_encode(encoded_message_set)
    codec = ATTRIBUTE_CODEC_MASK & CODEC_SNAPPY

    return Message(0, 0x00 | codec, None, snapped)


def create_message_set(requests, codec=CODEC_NONE):
    """
    Create a message set from a list of requests.

    Each request can have a list of messages and its own key.  If codec is
    :data:`CODEC_NONE`, return a list of raw Kafka messages. Otherwise, return
    a list containing a single codec-encoded message.

    :param codec:
        The encoding for the message set, one of the constants:

          * :const:`CODEC_NONE`
          * :const:`CODEC_GZIP`
          * :const:`CODEC_SNAPPY`

    :raises: :exc:`UnsupportedCodecError` for an unsupported codec
    """
    msglist = []
    for req in requests:
        msglist.extend([create_message(m, key=req.key) for m in req.messages])

    if codec == CODEC_NONE:
        return msglist
    elif codec == CODEC_GZIP:
        return [create_gzip_message(msglist)]
    elif codec == CODEC_SNAPPY:
        return [create_snappy_message(msglist)]
    else:
        raise UnsupportedCodecError("Codec 0x%02x unsupported" % codec)
