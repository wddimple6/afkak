# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.
# Copyright 2017 Ciena Corporation.

import collections
import struct

from .common import BufferUnderflowError


def _coerce_topic(topic):
    """
    Ensure that the topic name is a byte string. If a text string is
    provided, it is encoded as ASCII (this is okay because the valid character
    set of a topic name is ``[a-zA-Z0-9._-]``).

    :param topic: :class:`bytes` or :class:`str` instance
    :raises ValueError: when the topic name exceeds 249 bytes
    :raises TypeError: when the topic is not :class:`bytes` or :class:`str`
    """
    if isinstance(topic, type(u'')):
        topic = topic.encode('ascii')
    if not isinstance(topic, bytes):
        raise TypeError('topic={!r} should be bytes'.format(topic))
    if len(topic) > 249:
        raise ValueError('topic={!r} name is too long: {} > 249'.format(
            topic, len(topic)))
    return topic


def _coerce_consumer_group(consumer_group):
    """
    Ensure that the consumer group is a byte string. If a text string is
    provided, it is encoded as UTF-8 bytes.

    :param consumer_group: :class:`bytes` or :class:`str` instance
    :raises TypeError: when `consumer_group` is not :class:`bytes`
        or :class:`str`
    """
    if isinstance(consumer_group, type(u'')):
        consumer_group = consumer_group.encode('ascii')
    if not isinstance(consumer_group, bytes):
        raise TypeError('consumer_group={!r} should be bytes'.format(
            consumer_group))
    return consumer_group


def _coerce_client_id(client_id):
    """
    Ensure the provided client ID is a byte string. If a text string is
    provided, it is encoded as UTF-8 bytes.

    :param client_id: :class:`bytes` or :class:`str` instance
    """
    if isinstance(client_id, type(u'')):
        client_id = client_id.encode('utf-8')
    if not isinstance(client_id, bytes):
        raise TypeError('{!r} is not a valid consumer group (must be'
                        ' str or bytes)'.format(client_id))
    return client_id


def write_int_string(s):
    if s is None:
        return struct.pack('>i', -1)
    return struct.pack('>i', len(s)) + s


def write_short_string(s):
    if s is None:
        return struct.pack('>h', -1)
    elif len(s) > 32767:
        raise struct.error(len(s))
    else:
        return struct.pack('>h', len(s)) + s


def read_short_string(data, cur):
    if len(data) < cur + 2:
        raise BufferUnderflowError("Not enough data left")

    (strlen,) = struct.unpack('>h', data[cur:cur + 2])
    if strlen == -1:
        return None, cur + 2

    cur += 2
    if len(data) < cur + strlen:
        raise BufferUnderflowError("Not enough data left")

    out = data[cur:cur + strlen]
    return out, cur + strlen


def read_int_string(data, cur):
    if len(data) < cur + 4:
        raise BufferUnderflowError(
            "Not enough data left to read string len (%d < %d)" %
            (len(data), cur + 4))

    (strlen,) = struct.unpack('>i', data[cur:cur + 4])
    if strlen == -1:
        return None, cur + 4

    cur += 4
    if len(data) < cur + strlen:
        raise BufferUnderflowError("Not enough data left")

    out = data[cur:cur + strlen]
    return out, cur + strlen


def relative_unpack(fmt, data, cur):
    size = struct.calcsize(fmt)
    if len(data) < cur + size:
        raise BufferUnderflowError("Not enough data left")

    out = struct.unpack(fmt, data[cur:cur + size])
    return out, cur + size


def group_by_topic_and_partition(tuples):
    out = collections.defaultdict(dict)
    for t in tuples:
        out[t.topic][t.partition] = t
    return out
