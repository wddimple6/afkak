# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2018 Ciena Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Test the afkak.common module.
"""

from __future__ import absolute_import, division

import unittest

from afkak import common
from afkak.common import (BrokerResponseError, ConsumerMetadataResponse,
                          CoordinatorLoadInProgress, CoordinatorNotAvailable,
                          FetchResponse, LeaderNotAvailableError,
                          MessageSizeTooLargeError, NotCoordinator,
                          OffsetCommitResponse, OffsetFetchResponse,
                          OffsetMetadataTooLargeError, OffsetOutOfRangeError,
                          OffsetResponse, ProduceResponse,
                          RetriableBrokerResponseError,
                          UnknownTopicOrPartitionError, _check_error)


class TestAfkakCommon(unittest.TestCase):
    maxDiff = None

    def test_error_codes(self):
        """
        The `afkak.common.BrokerResponseError.errnos` mapping includes all
        concrete subclasses of `BrokerResponseError` by errno attribute.
        """
        count = 0
        expected = {}
        for name in dir(common):
            value = getattr(common, name)
            if not isinstance(value, type):
                continue
            if value is BrokerResponseError:
                continue
            if value is RetriableBrokerResponseError:
                continue
            if value.__name__ != name:
                continue  # Skip backwards-compatibility aliases.
            if issubclass(value, BrokerResponseError):
                expected[value.errno] = value
                count += 1

        self.assertEqual(expected, BrokerResponseError.errnos)
        self.assertEqual(count, len(BrokerResponseError.errnos), "errno values are reused")

    def test_check_error(self):
        for code, e in BrokerResponseError.errnos.items():
            self.assertRaises(e, _check_error, code)
        responses = [
            (ProduceResponse("topic1", 5, 3, 9), UnknownTopicOrPartitionError),
            (FetchResponse("topic2", 3, 10, 8, []), MessageSizeTooLargeError),
            (OffsetResponse("topic3", 8, 1, []), OffsetOutOfRangeError),
            (OffsetCommitResponse("topic4", 10, 12), OffsetMetadataTooLargeError),
            (OffsetFetchResponse("topic5", 33, 12, "", 5), LeaderNotAvailableError),
            (ConsumerMetadataResponse(15, -1, "", -1), CoordinatorNotAvailable),
            (OffsetFetchResponse("topic6", 23, -1, "", 14), CoordinatorLoadInProgress),
            (OffsetCommitResponse("topic7", 24, 16), NotCoordinator),
        ]

        for resp, e in responses:
            self.assertRaises(e, _check_error, resp)

    def test_check_error_no_raise(self):
        for code, e in BrokerResponseError.errnos.items():
            self.assertRaises(e, _check_error, code)
        responses = [
            (ProduceResponse("topic1", 5, 3, 9), UnknownTopicOrPartitionError),
            (FetchResponse("topic2", 3, 10, 8, []), MessageSizeTooLargeError),
            (OffsetResponse("topic3", 8, 1, []), OffsetOutOfRangeError),
            (OffsetCommitResponse("topic4", 10, 12), OffsetMetadataTooLargeError),
            (OffsetFetchResponse("topic5", 33, 12, "", 5), LeaderNotAvailableError),
        ]

        for resp, e in responses:
            self.assertTrue(isinstance(_check_error(resp, False), e))

    def test_raise_unknown_errno(self):
        """
        `BrokerResponseError` is raised when there is no subclass of it defined
        for a given errno. This exception indicates that the request cannot be
        retried, as we have no information on it.
        """
        unknown_errno = max(BrokerResponseError.errnos.keys()) + 1

        with self.assertRaises(BrokerResponseError) as cm:
            _check_error(unknown_errno)

        self.assertIs(BrokerResponseError, type(cm.exception))  # Not a subclass.
        self.assertFalse(cm.exception.retriable)
