"""
Test code for Partitioner(object), RoundRobinPartitioner(object),
and HashedPartitionerPartitioner(object) classes.
"""

from __future__ import division, absolute_import

from collections import defaultdict
from numpy import std

from twisted.trial.unittest import TestCase
from .testutil import random_string

from afkak.partitioner import (Partitioner, RoundRobinPartitioner,
                               HashedPartitioner)

class TestPartitioner(TestCase):
    def test_constructor(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = Partitioner(parts)
        self.assertEqual(parts, p.partitions)

    def test_partition(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = Partitioner(parts)

        self.assertRaises(NotImplementedError, p.partition, "key", parts)


class TestRoundRobinPartitioner(TestCase):
    def test_constructor(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = RoundRobinPartitioner(parts)
        self.assertEqual(parts, p.partitions)

    def test_partition(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = RoundRobinPartitioner(parts)

        for part in parts:
            self.assertEqual(part, p.partition("key", parts))

        for part in parts:
            self.assertEqual(part, p.partition("key", parts))

        # Make sure updates work
        parts = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        for part in parts:
            self.assertEqual(part, p.partition("key", parts))

        for part in parts:
            self.assertEqual(part, p.partition("key", parts))


class TestHashedPartitioner(TestCase):
    def test_partition(self):
        parts = [1, 2, 3, 4, 5]
        p = HashedPartitioner(parts)

        # Make sure we have decent distribution
        keycount = 5000
        key_list = []
        part_keycount = defaultdict(lambda: 0)
        key_to_part = {}
        for i in xrange(keycount):
            key = random_string(16)
            key_list.append(key)
            part = p.partition(key, parts)
            part_keycount[part] += 1
            key_to_part[key] = part

        self.assertLess(std(part_keycount.values()), keycount/100)

        # Ensure we get the same partition for the same key
        for key in key_to_part.keys():
            part = p.partition(key, parts)
            self.assertEqual(part, key_to_part[key])
