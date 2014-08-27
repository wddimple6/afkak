import unittest2

import afkak.protocol
from afkak.protocol import KafkaProtocol
from twisted.internet.error import ConnectionDone

from mock import MagicMock


class TestProtocol(unittest2.TestCase):
    def test_stringReceived(self):
        kp = KafkaProtocol()
        kp.factory = MagicMock()
        kp.stringReceived("testing")
        kp.factory.handleResponse.assert_called_once_with("testing")

    def test_connectionLost(self):
        kp = KafkaProtocol()
        afkak.protocol.log = MagicMock()
        kp.factory = MagicMock()
        kp.connectionLost()
        self.assertIsNone(kp.factory)
        afkak.protocol.log.warning.assert_called_once_with(
            'Lost Connection to Kafka Broker:%r',
            ConnectionDone)

    def test_lengthLimitExceeded(self):
        kp = KafkaProtocol()
        afkak.protocol.log = MagicMock()
        kp.transport = MagicMock()
        kp.lengthLimitExceeded(kp.MAX_LENGTH + 1)
        kp.transport.loseConnection.assert_called_once_with()
        afkak.protocol.log.error.assert_called_once_with(
            "KafkaProtocol Max Length:%d exceeded:%d",
            kp.MAX_LENGTH, kp.MAX_LENGTH + 1)
