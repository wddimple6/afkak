# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import logging
import warnings

from itertools import cycle
from random import randint

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


try:
    import murmur

    def murmur2_hash_c(bytes, seed=0x9747b28c):
        """murmur2_hash_c

        Use the murmur c-extension's string_hash routine
        """
        return murmur.string_hash(str(bytes), seed)

    murmur2_hash = murmur2_hash_c

except ImportError:  # pragma: no cover
    warnings.warn("Import of murmur failed, using pure python", ImportWarning)
    murmur2_hash = None


def pure_murmur2(bytes, seed=0x9747b28c):
    """Pure-python Murmur2 implementation.

    Based on java client, see org.apache.kafka.common.utils.Utils.murmur2
    https://github.com/apache/kafka/blob/0.8.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L244  # noqa
    Args:
        bytes: bytearray - Raises TypeError otherwise

    Returns: MurmurHash2 of bytes bytearray
    Raises: TypeError if bytes arg is not of type bytearray

    """

    # Ensure bytes arg is a bytearray
    if not isinstance(bytes, bytearray):
        raise TypeError("Type: %r of 'bytes' arg must be 'bytearray'".format(
                        type(bytes)))

    length = len(bytes)
    # 'm' and 'r' are mixing constants generated offline.
    # They're not really 'magic', they just happen to work well.
    m = 0x5bd1e995
    r = 24
    M32 = 0xffffffffL

    # Initialize the hash to a random value
    h = seed ^ length
    length4 = length / 4

    for i in range(length4):
        i4 = i * 4
        k = ((bytes[i4 + 0] & 0xff) + ((bytes[i4 + 1] & 0xff) << 8) +
             ((bytes[i4 + 2] & 0xff) << 16) + ((bytes[i4 + 3] & 0xff) << 24))
        k &= M32
        k *= m
        k &= M32
        k ^= (k % 0x100000000) >> r  # k ^= k >>> r
        k &= M32
        k *= m
        k &= M32

        h *= m
        h &= M32
        h ^= k
        h &= M32

    # Handle the last few bytes of the input array
    extra_bytes = length % 4
    if extra_bytes == 3:
        h ^= (bytes[(length & ~3) + 2] & 0xff) << 16
        h &= M32

    if extra_bytes >= 2:
        h ^= (bytes[(length & ~3) + 1] & 0xff) << 8
        h &= M32

    if extra_bytes >= 1:
        h ^= (bytes[length & ~3] & 0xff)
        h &= M32
        h *= m
        h &= M32

    h ^= (h % 0x100000000) >> 13  # h >>> 13;
    h &= M32
    h *= m
    h &= M32
    h ^= (h % 0x100000000) >> 15  # h >>> 15;
    h &= M32

    return h

if murmur2_hash is None:  # pragma: no cover
    murmur2_hash = pure_murmur2


class Partitioner(object):
    """
    Base class for a partitioner
    """
    def __init__(self, topic, partitions):
        """
        Initialize the partitioner

        partitions - A list of available partitions (during startup)
        """
        self.topic = topic
        self.partitions = partitions

    def partition(self, key, partitions):
        """
        Takes a key (string) and partitions (list) as argument and returns
        a partition to be used for the message

        partitions - The list of partitions is passed in every call. This
                     may look like an overhead, but it will be useful
                     (in future) when we handle cases like rebalancing
        """
        raise NotImplementedError('partition function has to be implemented')


class RoundRobinPartitioner(Partitioner):
    """
    Implements a round robin partitioner which sends data to partitions
    in a round robin fashion. Also supports starting each new partitioner
    at a random offset into the cycle of partitions
    """
    randomStart = False

    @classmethod
    def set_random_start(cls, randomStart):
        cls.randomStart = randomStart

    def __init__(self, topic, partitions):
        super(RoundRobinPartitioner, self).__init__(topic, partitions)
        self._set_partitions(partitions)

    def __repr__(self):
        return '<RoundRobinPartitioner {}:{}>'.format(self.randomStart,
                                                      self.partitions)

    def _set_partitions(self, partitions):
        self.partitions = sorted(partitions)
        self.iterpart = cycle(partitions)
        if self.randomStart:
            for _ in xrange(randint(0, len(partitions)-1)):
                self.iterpart.next()

    def partition(self, key, partitions):
        # Refresh the partition list if necessary
        if self.partitions != partitions:
            self._set_partitions(partitions)
        return self.iterpart.next()


class HashedPartitioner(Partitioner):
    """
    Implements a partitioner which selects the target partition based on
    the hash of the key
    """
    def partition(self, key, partitions):
        if key is None:
            key = bytearray('')
        elif isinstance(key, basestring):
            key = bytearray(key, 'UTF-8')
        elif not isinstance(key, bytearray):
            key = bytearray(str(key), 'UTF-8')
        return partitions[(murmur2_hash(key) & 0x7FFFFFFF) % len(partitions)]
