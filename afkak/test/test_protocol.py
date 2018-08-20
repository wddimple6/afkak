# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import unittest

import afkak.protocol
from afkak.protocol import KafkaProtocol
from twisted.internet.error import ConnectionLost
from twisted.python.failure import Failure

from mock import MagicMock


class TestProtocol(unittest.TestCase):
    def test_stringReceived(self):
        kp = KafkaProtocol()
        kp.factory = MagicMock()
        kp.stringReceived("testing")
        kp.factory.handleResponse.assert_called_once_with("testing")

    def test_connectionLost_cleanly(self):
        kp = KafkaProtocol()
        logsave = afkak.protocol.log
        try:
            afkak.protocol.log = MagicMock()
            kp.factory = MagicMock()
            kp.connectionLost()
            self.assertIsNone(kp.factory)
            afkak.protocol.log.debug.assert_called_once_with(
                "Connection to Kafka Broker closed: %r Closing: %r",
                None, False)
        finally:
            afkak.protocol.log = logsave

    def test_connectionLost_uncleanly(self):
        kp = KafkaProtocol()
        logsave = afkak.protocol.log
        try:
            afkak.protocol.log = MagicMock()
            kp.factory = MagicMock()
            failure = Failure(ConnectionLost())
            kp.connectionLost(failure)
            self.assertIsNone(kp.factory)
            afkak.protocol.log.warning.assert_called_once_with(
                'Lost Connection to Kafka Broker: %r',
                failure)
        finally:
            afkak.protocol.log = logsave

    def test_lengthLimitExceeded(self):
        kp = KafkaProtocol()
        logsave = afkak.protocol.log
        try:
            afkak.protocol.log = MagicMock()
            kp.transport = MagicMock()
            kp_peer = kp.transport.getPeer()
            kp.lengthLimitExceeded(kp.MAX_LENGTH + 1)
            kp.transport.loseConnection.assert_called_once_with()
            afkak.protocol.log.error.assert_called_once_with(
                'Remote Peer (%r) sent a %d byte message. Max allowed: %d.  '
                'Terminating connection.', kp_peer, kp.MAX_LENGTH + 1,
                kp.MAX_LENGTH)
        finally:
            afkak.protocol.log = logsave
