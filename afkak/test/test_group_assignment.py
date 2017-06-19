from unittest2 import TestCase
from mock import Mock
from afkak.group_assignment import round_robin_assignment


class TestGroupAssignment(TestCase):
    def test_roundrobin_normal(self):
        assignments = round_robin_assignment(
            client=Mock(topic_partitions={"topic1": [0, 1, 2, 3, 4]}),
            member_metadata={
                "m1": Mock(subscriptions=["topic1"]),
                "m2": Mock(subscriptions=["topic1"]),
                "m3": Mock(subscriptions=["topic1"]),
            }
        )
        self.assertEqual(assignments, {
            "m1": {"topic1": [0, 3]},
            "m2": {"topic1": [1, 4]},
            "m3": {"topic1": [2]},
        })

    def test_roundrobin_no_topic(self):
        assignments = round_robin_assignment(
            client=Mock(topic_partitions={}),
            member_metadata={
                "m1": Mock(subscriptions=["topic1"]),
                "m2": Mock(subscriptions=["topic1"]),
            }
        )
        self.assertEqual(assignments, {})

    def test_roundrobin_leftover(self):
        assignments = round_robin_assignment(
            client=Mock(topic_partitions={"topic1": [0]}),
            member_metadata={
                "m1": Mock(subscriptions=["topic1"]),
                "m2": Mock(subscriptions=["topic1"]),
            }
        )
        self.assertEqual(assignments, {
            "m1": {"topic1": [0]},
        })

    def test_roundrobin_two_topic(self):
        assignments = round_robin_assignment(
            client=Mock(topic_partitions={"topic1": [0], "topic2": [0, 1]}),
            member_metadata={
                "m1": Mock(subscriptions=["topic1"]),
                "m2": Mock(subscriptions=["topic2"]),
            }
        )
        self.assertEqual(assignments, {
            "m1": {"topic1": [0]},
            "m2": {"topic2": [0, 1]},
        })
