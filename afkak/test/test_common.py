# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2018 Ciena Corporation

"""
Test code for the afkak.common module.
"""

from __future__ import division, absolute_import

import unittest

from afkak import common
from afkak.common import (
    ProduceResponse, FetchResponse, OffsetResponse, OffsetCommitResponse,
    OffsetFetchResponse, LeaderNotAvailableError, kafka_errors, check_error,
    UnknownTopicOrPartitionError, MessageSizeTooLargeError,
    OffsetOutOfRangeError, OffsetMetadataTooLargeError,
    NotCoordinatorForConsumerError, OffsetsLoadInProgressError,
    ConsumerCoordinatorNotAvailableError, ConsumerMetadataResponse,
)


class TestAfkakCommon(unittest.TestCase):
    def test_error_codes(self):
        # The kafka_errors mapping includes all subclasses of BrokerError by
        # errno attribute.
        count = 0
        expected = {}
        for name in dir(common):
            value = getattr(common, name)
            if not isinstance(value, type):
                continue
            if value is common.BrokerResponseError:
                continue
            if issubclass(value, common.BrokerResponseError):
                expected[value.errno] = value
                count += 1

        self.assertEqual(expected, common.kafka_errors)
        self.assertEqual(count, len(common.kafka_errors), "errno values are reused")

    def test_check_error(self):
        for code, e in kafka_errors.items():
            self.assertRaises(e, check_error, code)
        responses = [
            (ProduceResponse("topic1", 5, 3, 9), UnknownTopicOrPartitionError),
            (FetchResponse("topic2", 3, 10, 8, []), MessageSizeTooLargeError),
            (OffsetResponse("topic3", 8, 1, []), OffsetOutOfRangeError),
            (OffsetCommitResponse("topic4", 10, 12),
             OffsetMetadataTooLargeError),
            (OffsetFetchResponse("topic5", 33, 12, "", 5),
             LeaderNotAvailableError),
            (ConsumerMetadataResponse(15, -1, "", -1),
             ConsumerCoordinatorNotAvailableError),
            (OffsetFetchResponse("topic6", 23, -1, "", 14),
             OffsetsLoadInProgressError),
            (OffsetCommitResponse("topic7", 24, 16),
             NotCoordinatorForConsumerError),
            ]

        for resp, e in responses:
            self.assertRaises(e, check_error, resp)

    def test_check_error_no_raise(self):
        for code, e in kafka_errors.items():
            self.assertRaises(e, check_error, code)
        responses = [
            (ProduceResponse("topic1", 5, 3, 9), UnknownTopicOrPartitionError),
            (FetchResponse("topic2", 3, 10, 8, []), MessageSizeTooLargeError),
            (OffsetResponse("topic3", 8, 1, []), OffsetOutOfRangeError),
            (OffsetCommitResponse("topic4", 10, 12),
             OffsetMetadataTooLargeError),
            (OffsetFetchResponse("topic5", 33, 12, "", 5),
             LeaderNotAvailableError),
            ]

        for resp, e in responses:
            self.assertTrue(isinstance(check_error(resp, False), e))
