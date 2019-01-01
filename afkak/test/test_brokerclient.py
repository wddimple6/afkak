# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2017, 2018 Ciena Corporation

"""
Test the _KafkaBrokerClient class.
"""

from __future__ import absolute_import, division

import logging

from twisted.internet.error import ConnectionRefusedError
from twisted.internet.task import Clock
# from twisted.python.failure import Failure
from twisted.trial.unittest import SynchronousTestCase

from ..brokerclient import _KafkaBrokerClient
from ..common import (
    BrokerMetadata, ClientError, DuplicateRequestError,
)
from ..common import CancelledError as AfkakCancelledError
from ..kafkacodec import KafkaCodec
from .endpoints import Connections

log = logging.getLogger(__name__)


# The bytes of a trivial Kakfa request, a metadata request v0 for all topics
# with correlationId=1.
METADATA_REQUEST_1 = (
    b'\x00\x03'          # apiKey=METADATA
    b'\x00\x00'          # apiVersion=0
    b'\x00\x00\x00\x01'  # correlationId=1
    b'\xff\xff'          # clientId=null
    b'\x00\x00\x00\x00'  # 0 topics (meaning all topics)
)

# The bytes of a trivial Kakfa request, a metadata request v0 for all topics
# with correlationId=1.
METADATA_REQUEST_2 = (
    b'\x00\x03'          # apiKey=METADATA
    b'\x00\x00'          # apiVersion=0
    b'\x00\x00\x00\x02'  # correlationId=2
    b'\xff\xff'          # clientId=null
    b'\x00\x00\x00\x00'  # 0 topics (meaning all topics)
)

# The bytes of a trivial response to METADATA_REQUEST_1, an response with empty
# broker and topic lists. This is somewhat nonsensical (of course there must be
# brokers!) but _KafkaBrokerClient doesn't care.
METADATA_RESPONSE = (
    b'\x00\x00\x00\x00'  # Number of brokers (nonsense).
    b'\x00\x00\x00\x00'  # Number of topics.
)



class BrokerClientTests(SynchronousTestCase):
    """Test `_KafkaBrokerClient`

    :ivar brokerClient: The `_KafkaBrokerClient` under test. It is configured
        with ``BrokerMetadata(node_id=1, host='host', port=1234)``.

    :ivar reactor: `Clock` used by the broker client for scheduling.

    :ivar connections: `afkak.test.endpoints.Connections` installed as the
        broker client's endpoint.

    :ivar float retryDelay: Seconds of delay between each connection attempt
        according to the broker client's retry policy. The default is 1.0. This
        may be mutated.
    """
    retryDelay = 1.0

    def setUp(self):
        self.reactor = Clock()
        self.connections = Connections()
        self.brokerClient = _KafkaBrokerClient(
            reactor=self.reactor,
            endpointFactory=self.connections,
            clientId='myclient',
            brokerMetadata=BrokerMetadata(
                node_id=1,
                host='host',
                port=1234,
            ),
            retryPolicy=lambda attempt: self.retryDelay,
        )

    def test_repr_init(self):
        """
        On initialization, the repr of the broker client shows the client ID
        and broker metadata.
        """
        self.assertEqual(
            "_KafkaBrokerClient<clientId=myclient node_id=1 host:1234 unconnected>",
            repr(self.brokerClient),
        )

    def test_repr_connected(self):
        """
        Once connected, the repr shows the state as connected.
        """
        self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        self.connections.accept('*')

        self.assertEqual(
            "_KafkaBrokerClient<clientId=myclient node_id=1 host:1234 connected>",
            repr(self.brokerClient),
        )

    def test_host_port(self):
        """
        The `host` and `port` properties reflect the broker metadata.
        """
        self.assertEqual('host', self.brokerClient.host)
        self.assertEqual(1234, self.brokerClient.port)

    def test_updateMetadata_retry(self):
        """
        Updating the broker metadata of the client changes the destination of
        the next connection attempt. Any outstanding connections remain until
        then.
        """
        d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        self.assertNoResult(d)
        self.assertEqual([('host', 1234)], self.connections.calls)

        self.brokerClient.updateMetadata(BrokerMetadata(node_id=1, host='other', port=2345))
        self.assertEqual('other', self.brokerClient.host)
        self.assertEqual(2345, self.brokerClient.port)
        # A connection to the new host was *not* triggered.
        self.assertEqual([('host', 1234)], self.connections.calls)

        # Fail the pending connection:
        self.connections.fail('host', ConnectionRefusedError("Nope."))
        # Trigger retry attempt, which happens after a delay:
        self.reactor.advance(self.retryDelay)

        # The retry attempt was made to the new host.
        self.assertEqual([('host', 1234), ('other', 2345)], self.connections.calls)

    def test_updateMetadata_invalid(self):
        """
        As a sanity check, a metadata update is rejected unless the node ID
        matches.
        """
        brokerMetadata = BrokerMetadata(node_id=2, host='other', port='2345')

        with self.assertRaises(ValueError):
            self.brokerClient.updateMetadata(brokerMetadata)

    def test_close_quiescent(self):
        """
        A quiescent broker client closes immediately.
        """
        self.assertIs(None, self.successResultOf(self.brokerClient.close()))

    def test_close_connecting(self):
        """
        When the broker client is closed during a connection attempt the
        connection attempt is cancelled.
        """
        request_d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)

        close_d = self.brokerClient.close()

        self.assertIs(None, self.successResultOf(close_d))
        f = self.failureResultOf(request_d, AfkakCancelledError)
        self.assertEqual(
            "Broker client for node_id=1 host:1234 closed",
            str(f.value),
        )

    def test_close_connected(self):
        """
        close() drops any open connection to the Kafka broker.
        """
        request_d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        conn = self.connections.accept('*')

        close_d = self.brokerClient.close()
        conn.pump.flush()  # Propagate connection loss.

        self.assertIs(None, self.successResultOf(close_d))
        self.failureResultOf(request_d, AfkakCancelledError)

    def test_disconnect_quiescent(self):
        """
        disconnect() has no effect when no connection is open.
        """
        self.brokerClient.disconnect()

    def test_disconnect_connected(self):
        """
        disconnect() drops any ongoing connection. A pending request triggers
        a reconnection attempt.
        """
        request_d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        conn1 = self.connections.accept('*')

        self.brokerClient.disconnect()
        conn1.pump.flush()
        self.assertTrue(conn1.server.transport.disconnected)  # Connection was dropped.
        self.assertNoResult(request_d)

        conn2 = self.connections.accept('*')
        self.assertNoResult(request_d)

    def test_disconnect_no_requests(self):
        """
        disconnect() drops any ongoing connection. No reconnection is attempted
        when no requests are pending.
        """
        # Make a request to trigger a connection attempt, then cancel it so
        # that there aren't any pending requests.
        request_d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        conn1 = self.connections.accept('*')
        request_d.cancel()
        self.failureResultOf(request_d)

        self.brokerClient.disconnect()
        conn1.pump.flush()
        self.assertTrue(conn1.server.transport.disconnected)  # Connection was dropped.

        self.reactor.advance(self.retryDelay)  # Not necessary, but let's be sure.
        self.assertEqual([('host', 1234)], self.connections.calls)  # No further connection attempts.

    def test_makeRequest_closed(self):
        """
        makeRequest() fails with ClientError once the broker client has been
        closed.
        """
        self.successResultOf(self.brokerClient.close())

        d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)

        self.failureResultOf(d, ClientError)

    def test_makeRequest_duplicate(self):
        """
        makeRequest() raises DuplicateRequestError when presented with an
        obviously duplicate correlation ID.
        """
        d = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)

        with self.assertRaises(DuplicateRequestError):
            self.brokerClient.makeRequest(1, METADATA_REQUEST_1)

    def test_makeRequest_connecting(self):
        """
        makeRequest() waits for an ongoing connection attempt.
        """
        d1 = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        d2 = self.brokerClient.makeRequest(2, METADATA_REQUEST_2)

        conn = self.connections.accept('*')
        conn.pump.flush()

        self.successResultOf(conn.server.expectRequest(KafkaCodec.METADATA_KEY, 0, 1)).respond(METADATA_RESPONSE)
        self.successResultOf(conn.server.expectRequest(KafkaCodec.METADATA_KEY, 0, 2)).respond(METADATA_RESPONSE)
        conn.pump.flush()

        self.assertEqual(b'\x00\x00\x00\x01' + METADATA_RESPONSE, self.successResultOf(d1))
        self.assertEqual(b'\x00\x00\x00\x02' + METADATA_RESPONSE, self.successResultOf(d2))

    def test_makeRequest_connected(self):
        """
        makeRequest() reuses an extant connection.
        """
        d1 = self.brokerClient.makeRequest(1, METADATA_REQUEST_1)
        conn = self.connections.accept('*')
        conn.pump.flush()
        self.successResultOf(conn.server.expectRequest(KafkaCodec.METADATA_KEY, 0, 1)).respond(METADATA_RESPONSE)
        conn.pump.flush()
        self.successResultOf(d1)

        d2 = self.brokerClient.makeRequest(2, METADATA_REQUEST_2)
        conn.pump.flush()
        self.successResultOf(conn.server.expectRequest(KafkaCodec.METADATA_KEY, 0, 2)).respond(METADATA_RESPONSE)
        conn.pump.flush()
        self.successResultOf(d1)
