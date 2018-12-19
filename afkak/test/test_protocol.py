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

import logging
import unittest

import mock
from twisted.internet.address import IPv4Address
from twisted.internet.error import ConnectionDone
from twisted.python.failure import Failure
from twisted.test.proto_helpers import StringTransportWithDisconnection

from .._protocol import KafkaProtocol, log
from .logtools import capture_logging


class TheFactory(object):
    """
    `TheFactory` implements the bits of `_KafkaBrokerClient` that
    `_KafkaProtocol` interacts with.
    """
    log = logging.getLogger(__name__).getChild('TheFactory')

    def handleResponse(self, string):
        """Called for each response."""

    def _connectionLost(self, reason):
        """Called when the connection goes down."""


class KafkaProtocolTests(unittest.TestCase):
    def test_stringReceived(self):
        kp = KafkaProtocol()
        kp.factory = factory_spy = mock.Mock(wraps=TheFactory())

        kp.stringReceived(b"testing")

        factory_spy.handleResponse.assert_called_once_with(b"testing")

    def test_connectionLost_cleanly(self):
        kp = KafkaProtocol()
        kp.factory = factory_spy = mock.Mock(wraps=TheFactory())
        reason = Failure(ConnectionDone())

        kp.connectionLost(reason)

        factory_spy._connectionLost.assert_called_once_with(reason)
        self.assertIsNone(kp.factory)

    def test_lengthLimitExceeded(self):
        """
        An error is logged and the connection dropped when an oversized message
        is received.
        """
        too_long = KafkaProtocol.MAX_LENGTH + 1
        peer = IPv4Address('TCP', '1.2.3.4', 1234)
        kp = KafkaProtocol()
        kp.factory = factory_spy = mock.Mock(wraps=TheFactory())
        kp.transport = StringTransportWithDisconnection(peerAddress=peer)
        kp.transport.protocol = kp

        with capture_logging(log) as records:
            kp.lengthLimitExceeded(too_long)

        self.assertEqual(1, len(factory_spy._connectionLost.mock_calls))

        [record] = records
        record.getMessage()  # Formats okay.
        self.assertEqual((
            'Broker at %s sent a %d byte message, exceeding the size limit of %d. '
            'Terminating connection.'
        ), record.msg)
        self.assertEqual((peer, too_long, kp.MAX_LENGTH), record.args)
