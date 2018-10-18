# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2017, 2018 Ciena Corporation

"""
Test code for KafkaBrokerClient(ReconnectingClientFactory) class.
"""

from __future__ import division, absolute_import

import struct
import logging

from mock import Mock, patch

from twisted.internet.address import IPv4Address
from twisted.internet.defer import Deferred
from twisted.internet.error import (
    ConnectionRefusedError, ConnectionDone, UserError, NotConnectingError)
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import StringTooLongError
from twisted.python.failure import Failure
from twisted.test import proto_helpers
from twisted.test.proto_helpers import MemoryReactorClock, _FakeConnector
from twisted.trial import unittest

import afkak.brokerclient as brokerclient
from afkak.brokerclient import _KafkaBrokerClient as KafkaBrokerClient
from afkak.kafkacodec import KafkaCodec, create_message
from afkak.common import (ClientError, DuplicateRequestError, CancelledError)


log = logging.getLogger(__name__)
destAddr = IPv4Address('TCP', '0.0.0.0', 1234)


class FactoryAwareFakeConnector(_FakeConnector):
    connectCalled = False
    factory = None
    transport = None
    state = "disconnected"

    def __init__(self, address):
        """
        MemoryReactorClock doesn't call 'connect' on the connector
        it creates, so, we just do it here.
        """
        super(FactoryAwareFakeConnector, self).__init__(address)
        self.connect()

    def stopConnecting(self):
        """
        Behave as though an ongoing connection attempt has now
        failed, and notify the factory of this.
        """
        if self.state != "connecting":
            raise NotConnectingError("we're not trying to connect")

        self.state = "disconnected"
        self.factory.clientConnectionFailed(self, UserError)

    def connect(self):
        """
        Record any connection attempts
        """
        self.connectCalled = True
        self.state = "connecting"

    def connectionFailed(self, reason):
        """
        Behave as though an ongoing connection attempt has now
        failed, and notify the factory of this.
        """
        self.state = "disconnected"
        self.factory.clientConnectionFailed(self, reason)


# Override the _FakeConnector used by MemoryReactorClock to ours which
# has a factory and will let the factory know the connection failed...
# NOTE: since the MemoryReactorClock doesn't properly setup the factory
# attribute on the fake connectors, you have to manually do that in tests
proto_helpers._FakeConnector = FactoryAwareFakeConnector


class KafkaBrokerClientTestCase(unittest.TestCase):
    """
    Tests for L{KafkaBrokerClient}.
    """

    def test_stopTryingWhenConnected(self):
        """
        If a L{KafkaBrokerClient} has C{stopTrying} called while it is
        connected, it does not subsequently attempt to reconnect if the
        connection is later lost.
        """
        reactor = MemoryReactorClock()

        class NoConnectConnector(object):
            def stopConnecting(self):
                raise ClientError("Shouldn't be called, "
                                  "we're connected.")  # pragma: no cover

            def connect(self):
                raise ClientError(
                    "Shouldn't be reconnecting.")  # pragma: no cover

        c = KafkaBrokerClient(reactor, 'broker', 9092, 'clientId')
        c.protocol = Protocol
        # Let's pretend we've connected:
        c.buildProtocol(None)
        # Now we stop trying, then disconnect:
        c.stopTrying()
        c.clientConnectionLost(NoConnectConnector(), Failure(ConnectionDone()))
        self.assertFalse(c.continueTrying)

    def test_parametrizedClock(self):
        """
        The clock used by L{KafkaBrokerClient} can be parametrized, so
        that one can cleanly test reconnections.
        """
        reactor = MemoryReactorClock()
        factory = KafkaBrokerClient(reactor, 'broker', 9092, 'clientId')

        # XXX This ignores internal invariants, not a great test...
        factory.clientConnectionLost(FactoryAwareFakeConnector(None),
                                     Failure(ConnectionDone()))
        self.assertEqual(len(reactor.getDelayedCalls()), 2)

    def test_subscriber(self):
        """
        Any subscriber callback is called on each connection state transition.
        """
        reactor = MemoryReactorClock()
        calls = []

        def subscriber(brokerclient, connected, reason):
            """
            Record the arguments passed to the callback.
            """
            calls.append((brokerclient, connected, reason))

        c = KafkaBrokerClient(reactor, 'slc', 9092, 'clientId',
                              subscriber=subscriber)

        # FIXME: This test should pretend to connect at the reactor level, not
        # call a private method.
        c._notify(True)
        self.assertEqual([(c, True, None)], calls)

        del calls[:]
        c._notify(False)
        self.assertEqual([(c, False, None)], calls)

        del calls[:]
        c._notify(True)
        reason = Failure(Exception())
        c._notify(False, reason)
        self.assertEqual([
            (c, True, None),
            (c, False, reason),
        ], calls)

    def test_repr(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'kafka.example.com', 9092,
                              clientId='MyClient')
        self.assertEqual((
            "<KafkaBrokerClient kafka.example.com:9092 "
            "clientId='MyClient' unconnected>"
        ), repr(c))

    def test_connect(self):
        reactor = MemoryReactorClock()
        reactor.running = True
        c = KafkaBrokerClient(reactor, 'kafka.example.com', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Build the protocol, like a real connector would
        addr = IPv4Address('TCP', '1.2.3.4', 9092)
        c.buildProtocol(addr)

        self.assertEqual((
            "<KafkaBrokerClient kafka.example.com:9092 "
            "clientId='clientId' connected>"
        ), repr(c))

    def test_connected(self):
        reactor = MemoryReactorClock()
        reactor.running = True
        c = KafkaBrokerClient(reactor, 'test_connect', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        self.assertFalse(c.connected())
        # Build the protocol, like a real connector would
        c.buildProtocol(None)
        self.assertTrue(c.connected())
        reactor.advance(1.0)  # Trigger the DelayedCall to _notify

    def test_connectTwice(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_connectTwice', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        self.assertRaises(ClientError, c._connect)

    def test_connectNotify(self):
        from afkak.protocol import KafkaProtocol
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_connectNotify', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        proto = c.buildProtocol(None)
        self.assertIsInstance(proto, KafkaProtocol)
        reactor.advance(1.0)
        self.assertEqual([], reactor.getDelayedCalls())

    # Patch KafkaBrokerClient's superclass with a Mock() so we can make
    # sure KafkaBrokerClient is properly calling it's clientConnectionFailed()
    # to reconnect
    @patch(
        'afkak.brokerclient.ReconnectingClientFactory.clientConnectionFailed')
    def test_connectFailNotify(self, ccf):
        """
        Check that if the connection fails to come up that the brokerclient
        errback's the deferred returned from the '_connect' call.
        """
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_connectFailNotify', 9092, 'clientId')
        # attempt connection
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        conn = c.connector
        # Claim the connection failed
        e = ConnectionRefusedError()
        conn.connectionFailed(e)
        # Check that the brokerclient called super to reconnect
        ccf.assert_called_once_with(c, conn, e)

    def test_close(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_close', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        c.connector.state = 'connected'  # set the connector to connected state
        dd = c.close()
        self.assertIsInstance(dd, Deferred)
        self.assertNoResult(dd)
        f = Failure(ConnectionDone('test_close'))
        c.clientConnectionLost(c.connector, f)
        self.assertNoResult(dd)
        # Advance the clock so the notify() call fires
        reactor.advance(0.1)
        r = self.successResultOf(dd)
        self.assertIs(r, None)

    def test_disconnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_close', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        conn = c.connector
        conn.factory = c  # MemoryReactor doesn't make this connection.
        conn.state = 'connected'  # set the connector to connected state
        self.assertIs(conn._disconnected, False)
        c.disconnect()
        self.assertIs(conn._disconnected, True)

    def test_close_disconnected(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_close', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        c.connector.state = 'disconnected'  # set connector's state for test
        dd = c.close()
        self.assertIsInstance(dd, Deferred)
        r = self.successResultOf(dd)
        self.assertIs(r, None)

    def test_reconnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_reconnect', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        dd = c.close()
        self.assertIsInstance(dd, Deferred)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.

    def test_delay_reset(self):
        """
        Test that reconnect delay is handled correctly:
        1) That initializer values are respected
        2) That delay maximum is respected
        3) That delay is reset to initial delay on successful connection
        """
        init_delay = last_delay = 0.025
        max_delay = 14
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_delay_reset', 9092, 'clientId',
                              initDelay=init_delay, maxDelay=max_delay)
        c.jitter = 0  # Eliminate randomness for test
        # Ensure KBC was initialized correctly
        self.assertEqual(c.retries, 0)
        self.assertEqual(c.delay, init_delay)
        self.assertEqual(c.maxDelay, max_delay)
        self.assertTrue(c.continueTrying)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        self.assertTrue(c.connector.connectCalled)
        # Reset it so we can track future calls
        c.connector.connectCalled = False
        # Build the protocol, like a real connector would on successful connect
        c.buildProtocol(None)
        # Fake server connection close
        f = Failure(ConnectionDone('test_delay_reset'))
        c.clientConnectionLost(c.connector, f)
        # Now loop failing connection attempts until we get to the max
        while c.delay < max_delay:
            # Assert a reconnect wasn't immediately attempted
            self.assertFalse(c.connector.connectCalled)
            # Assert the new delay was calculated correctly
            self.assertEqual(last_delay * c.factor, c.delay)
            last_delay = c.delay
            # advance the reactor, but not enough to connect
            reactor.advance(0.1 * c.delay)
            # Still no connection
            self.assertFalse(c.connector.connectCalled)
            # Should see a connection attempt after this
            reactor.advance(c.delay)
            self.assertTrue(c.connector.connectCalled)
            c.connector.connectCalled = False  # Reset again
            # Claim the connection failed
            e = ConnectionRefusedError()
            c.connector.connectionFailed(e)
        # Assert the delay was calculated correctly
        self.assertEqual(max_delay, c.delay)
        self.assertFalse(c.connector.connectCalled)
        # "run" the reactor, but not enough to connect
        reactor.advance(0.1 * c.delay)
        # Still no connection
        self.assertFalse(c.connector.connectCalled)
        # Should see a connection attempt after this
        reactor.advance(c.delay)
        self.assertTrue(c.connector.connectCalled)
        # Build the protocol, like a real connector would on successful connect
        c.buildProtocol(None)
        # Assert that the delay and retry count were reset
        self.assertEqual(init_delay, c.delay)
        self.assertEqual(c.retries, 0)

    def test_closeNotConnected(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_closeNotConnected', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        d = c.close()
        self.assertIsInstance(d, Deferred)

    def test_close_no_connect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_closeNotConnected', 9092, 'clientId')
        d = c.close()
        self.assertIsInstance(d, Deferred)

    def test_closeNotify(self):
        from twisted.internet.error import ConnectionDone
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_closeNotify', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        c.buildProtocol(None)
        reactor.advance(1.0)
        self.assertEqual([], reactor.getDelayedCalls())
        c.continueTrying = False
        c.close()
        c.clientConnectionLost(c.connector, Failure(ConnectionDone()))
        reactor.advance(1.0)
        self.assertEqual([], reactor.getDelayedCalls())

    def test_closeNotifyDuringConnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_closeNotify', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        reactor.advance(1.0)
        self.assertEqual([], reactor.getDelayedCalls())
        c.close()
        c.clientConnectionFailed(c.connector, Failure(UserError()))
        reactor.advance(1.0)
        self.assertEqual([], reactor.getDelayedCalls())

    def test_makeRequest(self):
        id1 = 54321
        id2 = 76543
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'testmakeRequest', 9092, 'clientId')
        request = KafkaCodec.encode_fetch_request(b'testmakeRequest', id1)
        d = c.makeRequest(id1, request)
        eb1 = Mock()
        self.assertIsInstance(d, Deferred)
        d.addErrback(eb1)
        # Make sure the request shows unsent
        self.assertFalse(c.requests[id1].sent)
        # Make sure a connection was attempted
        self.assertTrue(c.connector)
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = Mock()
        # Advance the clock so sendQueued() will be called
        reactor.advance(1.0)
        # The proto should have be asked to sendString the request
        c.proto.sendString.assert_called_once_with(request)

        # now call with 'expectReply=False'
        c.proto = Mock()
        request = KafkaCodec.encode_fetch_request(b'testmakeRequest2', id2)
        d2 = c.makeRequest(id2, request, expectResponse=False)
        self.assertIsInstance(d2, Deferred)
        c.proto.sendString.assert_called_once_with(request)

        # Now close the KafkaBrokerClient
        c.close()
        fail1 = eb1.call_args[0][0]  # The actual failure sent to errback
        self.assertTrue(fail1.check(CancelledError))

    def test_makeRequest_fails(self):
        id1 = 15432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'testmakeRequest', 9092, 'clientId')
        request = KafkaCodec.encode_fetch_request(b'testmakeRequest', id1)
        d = c.makeRequest(id1, request)
        eb1 = Mock()
        self.assertIsInstance(d, Deferred)
        d.addErrback(eb1)
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = Mock()
        c.proto.sendString.side_effect = StringTooLongError(
            "Tried to send too many bytes")
        # Advance the clock so sendQueued() will be called
        reactor.advance(1.0)
        # The proto should have be asked to sendString the request
        c.proto.sendString.assert_called_once_with(request)

        # Now close the KafkaBrokerClient
        c.close()

    def test_makeRequest_after_close(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_closeNotConnected', 9092, 'clientId')
        d = c.close()
        self.assertIsInstance(d, Deferred)
        d2 = c.makeRequest(1, b'fake request')
        self.successResultOf(
            self.failUnlessFailure(d2, ClientError))

    def test_cancelRequest(self):
        errBackCalled = [False]

        def _handleCancelErrback(reason):
            log.debug("_handleCancelErrback: %r", reason)
            reason.trap(CancelledError)
            errBackCalled[0] = True

        id1 = 65432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_connect', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Fake a protocol
        c.proto = Mock()
        request = KafkaCodec.encode_fetch_request(b'testcancelRequest', id1)
        d = c.makeRequest(id1, request)
        self.assertIsInstance(d, Deferred)
        d.addErrback(_handleCancelErrback)
        c.proto.sendString.assert_called_once_with(request)
        # Now try to cancel the request
        d.cancel()
        self.assertTrue(errBackCalled[0])

    def test_cancelRequestNoReply(self):
        id2 = 87654
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'test_connect', 9092, 'clientId')
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Fake a protocol
        c.proto = Mock()
        # now call with 'expectReply=False'
        c.proto = Mock()
        request = KafkaCodec.encode_fetch_request(b'testcancelRequest2', id2)
        d2 = c.makeRequest(id2, request, expectResponse=False)
        self.assertIsInstance(d2, Deferred)
        c.proto.sendString.assert_called_once_with(request)
        # This one we cancel by ID. It should fail due to the
        # expectResponse=False, since we don't keep the requestID after send
        self.assertRaises(KeyError, c.cancelRequest, id2)

    def test_makeUnconnectedRequest(self):
        """
        Ensure that sending a request when not connected will attempt to bring
        up a connection if one isn't already in the process of being brought up
        """
        id1 = 65432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'testmakeUnconnectedRequest', 9092, 'clientId')
        request = KafkaCodec.encode_fetch_request(
            b'testmakeUnconnectedRequest', id1)
        d = c.makeRequest(id1, request)
        self.assertIsInstance(d, Deferred)
        # Make sure the request shows unsent
        self.assertFalse(c.requests[id1].sent)
        # Make sure a connection was attempted
        self.assertTrue(c.connector)
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = Mock()
        reactor.advance(1.0)
        # Now, we should have seen the 'sendString' called
        c.proto.sendString.assert_called_once_with(request)

    def test_requestsRetried(self):
        id1 = 65432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'testrequestsRetried', 9092, 'clientId')
        request = KafkaCodec.encode_fetch_request(
            b'testrequestsRetried', id1)
        c.makeRequest(id1, request)
        # Make sure the request shows unsent
        self.assertFalse(c.requests[id1].sent)
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = Mock()
        reactor.advance(0.1)
        # Now, we should have seen the 'sendString' called
        c.proto.sendString.assert_called_once_with(request)
        # And the request should be 'sent'
        self.assertTrue(c.requests[id1].sent)
        # Before the reply 'comes back' drop the connection
        from twisted.internet.main import CONNECTION_LOST
        c.clientConnectionLost(c.connector, Failure(CONNECTION_LOST))
        # Make sure the proto was reset
        self.assertIs(c.proto, None)
        # Advance the clock again
        reactor.advance(0.1)
        # Make sure the request shows unsent
        self.assertFalse(c.requests[id1].sent)
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = Mock()
        reactor.advance(0.1)
        # Now, we should have seen the 'sendString' called
        c.proto.sendString.assert_called_once_with(request)
        # And the request should be 'sent'
        self.assertTrue(c.requests[id1].sent)

    def test_handleResponse(self):
        def make_fetch_response(id):
            t1 = b"topic1"
            t2 = b"topic2"
            msgs = [create_message(m) for m in
                    [b"message1", b"hi", b"boo", b"foo", b"so fun!"]]
            ms1 = KafkaCodec._encode_message_set([msgs[0], msgs[1]])
            ms2 = KafkaCodec._encode_message_set([msgs[2]])
            ms3 = KafkaCodec._encode_message_set([msgs[3], msgs[4]])

            packFmt = '>iih{}siihqi{}sihqi{}sh{}siihqi{}s'.format(
                len(t1), len(ms1), len(ms2), len(t2), len(ms3))
            return struct.pack(
                packFmt, id, 2, len(t1), t1, 2, 0, 0, 10, len(ms1), ms1, 1,
                1, 20, len(ms2), ms2, len(t2), t2, 1, 0, 0, 30, len(ms3), ms3)

        reactor = MemoryReactorClock()
        c = KafkaBrokerClient(reactor, 'testhandleResponse', 9092, 'clientId')
        logsave = brokerclient.log
        try:
            brokerclient.log = Mock()
            badId = 98765
            response = make_fetch_response(badId)
            # First test that a response without first sending a request
            # generates a log message
            c.handleResponse(response)
            brokerclient.log.warning.assert_called_once_with(
                'Unexpected response:%r, %r', badId, response)

            # Now try a request/response pair and ensure the deferred is called
            goodId = 12345
            c.proto = Mock()
            request = KafkaCodec.encode_fetch_request(
                b'testhandleResponse2', goodId)
            d = c.makeRequest(goodId, request)
            self.assertIsInstance(d, Deferred)
            c.proto.sendString.assert_called_once_with(request)
            response = make_fetch_response(goodId)
            self.assertFalse(d.called)
            # Ensure trying to make another request with the same ID before
            # we've got a response raises an exception
            c.proto.sendString = Mock(
                side_effect=Exception(
                    'brokerclient sending duplicate request'))
            self.assertRaises(
                DuplicateRequestError, c.makeRequest, goodId, request)
            c.handleResponse(response)
            self.assertTrue(d.called)

            # Now that we've got the response, try again with the goodId
            c.proto = Mock()  # Need a new mock...
            d = c.makeRequest(goodId, request)
            self.assertIsInstance(d, Deferred)
            c.proto.sendString.assert_called_once_with(request)
            response = make_fetch_response(goodId)
            self.assertFalse(d.called)
            c.handleResponse(response)
            self.assertTrue(d.called)

            # Now try with a real request, but with expectResponse=False
            # We still get a deferred back, because the request can be
            # cancelled before it's sent, and in that case, we errback()
            # the deferred
            d2 = c.makeRequest(goodId, request, expectResponse=False)
            self.assertIsInstance(d2, Deferred)
            # Send the (unexpected) response, and check for the log message
            c.handleResponse(response)
            brokerclient.log.warning.assert_called_with(
                'Unexpected response:%r, %r', goodId, response)
        finally:
            brokerclient.log = logsave

    def test_Request(self):
        """
        test_Request
        Make sure we have complete converage on the _Request object
        """
        from afkak.brokerclient import _Request

        tReq = _Request(5, b"data", True)
        self.assertEqual(tReq.__repr__(), '_Request:5:True')
