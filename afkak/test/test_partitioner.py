"""
Test code for Partitioner(object), RoundRobinPartitioner(object),
and HashedPartitionerPartitioner(object) classes.
"""

from __future__ import division, absolute_import

from collections import defaultdict

from math import sqrt

from unittest2 import TestCase
from .testutil import random_string

from afkak.partitioner import (Partitioner, RoundRobinPartitioner,
                               HashedPartitioner)


# Re-implement std so we don't need numpy just for a couple of tests,
# since it wouldn't build in the pypy tox environment
def std(data):
    if not len(data):
        return float('nan')
    mean = sum([float(x) for x in data]) / len(data)
    var = sum([(mean - x)**2 for x in data]) / len(data)
    return sqrt(var)


class TestPartitioner(TestCase):
    def test_constructor(self):
        topic = "TheTestTopic"
        parts = [1, 2, 3, 4, 5, 6]
        p = Partitioner(topic, parts)
        self.assertEqual(topic, p.topic)
        self.assertEqual(parts, p.partitions)

    def test_partition(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = Partitioner(None, parts)

        self.assertRaises(NotImplementedError, p.partition, "key", parts)


class TestRoundRobinPartitioner(TestCase):
    def test_constructor(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = RoundRobinPartitioner(None, parts)
        self.assertEqual(parts, p.partitions)

    def test_repr(self):
        parts = [1, 3, 5, 7]
        p = RoundRobinPartitioner(None, parts)
        self.assertEqual(p.__repr__(),
                         '<RoundRobinPartitioner False:[1, 3, 5, 7]>')

    def test_partition(self):
        parts = [1, 2, 3, 4, 5, 6]
        p = RoundRobinPartitioner(None, parts)

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

    def test_set_random_start(self):
        RoundRobinPartitioner.set_random_start(True)
        self.assertTrue(RoundRobinPartitioner.randomStart)
        parts = [1, 2, 3, 4, 5, 6, 7, 8, 19, 20, 30]
        # Try a number of times and check the distribution of the start
        firstParts = defaultdict(lambda: 0)
        trycount = 10000
        for i in xrange(trycount):
            p1 = RoundRobinPartitioner(None, parts)
            firstParts[p1.partition(None, parts)] += 1

        self.assertLess(std(firstParts.values()), trycount/100)

        RoundRobinPartitioner.set_random_start(False)
        self.assertFalse(RoundRobinPartitioner.randomStart)
        p2 = RoundRobinPartitioner(None, parts)
        self.assertFalse(p2.randomStart)
        for part in parts:
            self.assertEqual(part, p2.partition(None, parts))


class TestHashedPartitioner(TestCase):
    def test_partition(self):
        T1 = "TestTopic1"
        parts = [1, 2, 3, 4, 5]
        p = HashedPartitioner(T1, parts)

        # Make sure we have decent distribution
        keycount = 10000
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
