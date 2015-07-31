# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

"""
Test code for KafkaBrokerClient(ReconnectingClientFactory) class.
"""

from __future__ import division, absolute_import

import struct
import logging

from mock import Mock, patch

from twisted.internet.address import IPv4Address
from twisted.internet.base import DelayedCall
from twisted.internet.defer import Deferred, setDebugging
from twisted.internet.error import (
    ConnectionRefusedError, ConnectionDone, UserError, NotConnectingError)
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import StringTooLongError
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.test import proto_helpers
from twisted.test.proto_helpers import MemoryReactorClock, _FakeConnector
from twisted.trial import unittest

import afkak.brokerclient as brokerclient
from afkak.brokerclient import KafkaBrokerClient
from afkak.kafkacodec import KafkaCodec, create_message
from afkak.common import (ClientError, DuplicateRequestError, CancelledError)

DEBUGGING = True
setDebugging(DEBUGGING)
DelayedCall.debug = DEBUGGING

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
        test_stopTryingWhenConnected
        If a L{KafkaBrokerClient} has C{stopTrying} called while it is
        connected, it does not subsequently attempt to reconnect if the
        connection is later lost.
        """
        class NoConnectConnector(object):
            def stopConnecting(self):
                raise ClientError("Shouldn't be called, "
                                  "we're connected.")  # pragma: no cover

            def connect(self):
                raise ClientError(
                    "Shouldn't be reconnecting.")  # pragma: no cover

        c = KafkaBrokerClient('broker')
        c.protocol = Protocol
        # Let's pretend we've connected:
        c.buildProtocol(None)
        # Now we stop trying, then disconnect:
        c.stopTrying()
        c.clientConnectionLost(NoConnectConnector(), Failure(ConnectionDone()))
        self.assertFalse(c.continueTrying)

    def test_parametrizedClock(self):
        """
        test_parametrizedClock
        The clock used by L{KafkaBrokerClient} can be parametrized, so
        that one can cleanly test reconnections.
        """
        clock = Clock()
        factory = KafkaBrokerClient('broker', reactor=clock)

        factory.clientConnectionLost(FactoryAwareFakeConnector(None),
                                     Failure(ConnectionDone()))
        self.assertEqual(len(clock.calls), 2)

    def test_subscribersList(self):
        """
        test_subscribersList
        Test that a brokerclient's connSubscribers instance
        variable is set by the subscribers parameter on the
        constructor
        """
        sublist = [Mock(), Mock(), Mock()]
        c = KafkaBrokerClient('broker', subscribers=sublist)
        self.assertEqual(sublist, c.connSubscribers)

        c4 = Mock()
        c5 = Mock()
        c.addSubscriber(c4)
        c.addSubscriber(c5)
        addedList = sublist
        addedList.extend([c4, c5])
        self.assertEqual(addedList, c.connSubscribers)

        rmdList = addedList
        rmdList.remove(sublist[2])
        rmdList.remove(c4)
        c.delSubscriber(sublist[2])
        c.delSubscriber(c4)
        self.assertEqual(rmdList, c.connSubscribers)

    def test_subscribersListCalls(self):
        """
        test_subscribersListCalls
        Test that a brokerclient's connSubscribers callbacks
        are called in the proper order, and that all the deferreds
        of a previous call are resolved before the next round of calls
        is done.
        """
        reactor = MemoryReactorClock()
        callList = []

        def c1(c, conn, reason):
            s = 'c1:{0}'.format(conn)
            if reason is not None:
                s += ':' + reason
            callList.append(s)

        def c2(c, conn, reason):
            def c2_cb(_, c, conn, reason):
                callList.append('c2_cb:{0}'.format(conn))

            d = Deferred()
            d.addCallback(c2_cb, c, conn, reason)
            reactor.callLater(1.0, d.callback, None)
            s = 'c2:{0}'.format(conn)
            if reason is not None:
                s += ':' + reason
            callList.append(s)
            return d

        def c3(c, conn, reason):
            s = 'c3:{0}'.format(conn)
            if reason is not None:
                s += ':' + reason
            callList.append(s)

        def c4(c, conn, reason):
            callList.append('c4:{0}'.format(conn))

        def c5(c, conn, reason):
            callList.append('c5:{0}'.format(conn))

        sublist = [c1, c2, c3]
        c = KafkaBrokerClient('slc', subscribers=sublist,
                              reactor=reactor)

        # Trigger the call to the 3 subscribers
        c._notify(True)
        self.assertEqual(callList, ['c1:True', 'c2:True', 'c3:True'])
        callList = []
        c._notify(False)
        # Nothing should be called yet, because the c2_cb
        # callback hasn't been called yet...
        self.assertEqual(callList, [])

        # advance the clock to trigger the callback to c2_cb
        reactor.advance(1.0)
        self.assertEqual(callList, ['c2_cb:True', 'c1:False',
                                    'c2:False', 'c3:False'])
        callList = []
        reactor.advance(1.0)
        self.assertEqual(callList, ['c2_cb:False'])
        callList = []

        # Trigger the call to the subscribers
        c._notify(True, reason='TheReason')
        c.addSubscriber(c4)
        self.assertEqual(callList, ['c1:True:TheReason', 'c2:True:TheReason',
                                    'c3:True:TheReason'])
        callList = []
        c._notify(False)
        self.assertEqual(callList, [])
        # Add a subscriber after the notify call, but before the advance
        # and ensure that the new subscriber isn't notified for the event
        # which occurred before it was added
        c.addSubscriber(c5)
        # advance the clock to trigger the callback to c2_cb
        reactor.advance(1.0)
        self.assertEqual(callList, ['c2_cb:True', 'c1:False',
                                    'c2:False', 'c3:False', 'c4:False'])
        callList = []

        c.delSubscriber(c2)
        # advance the clock to trigger the callback to c2_cb
        reactor.advance(1.0)
        # We should still get the c2_cb:False here...
        self.assertEqual(callList, ['c2_cb:False'])
        callList = []

        c.delSubscriber(c4)
        # Trigger the call to the subscribers
        c._notify(True)
        reactor.advance(1.0)
        self.assertEqual(callList, ['c1:True', 'c3:True', 'c5:True'])
        callList = []
        c._notify(False)
        reactor.advance(1.0)
        self.assertEqual(callList, ['c1:False', 'c3:False', 'c5:False'])
        callList = []

    def test_repr(self):
        c = KafkaBrokerClient('kafka.example.com',
                              clientId='MyClient')
        self.assertEqual(
            '<KafkaBrokerClient kafka.example.com:9092:MyClient',
            c.__repr__())

    def test_connect(self):
        reactor = MemoryReactorClock()
        reactor.running = True
        c = KafkaBrokerClient('test_connect', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Let's pretend we've connected, which will schedule the firing
        c.buildProtocol(None)
        reactor.advance(1.0)

    def test_connectTwice(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_connectTwice', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        self.assertRaises(ClientError, c._connect)

    def test_connectNotify(self):
        from afkak.protocol import KafkaProtocol
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_connectNotify', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        proto = c.buildProtocol(None)
        self.assertIsInstance(proto, KafkaProtocol)
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())

    # Patch KafkaBrokerClient's superclass with a Mock() so we can make
    # sure KafkaBrokerClient is properly calling it's clientConnectionFailed()
    # to reconnect
    @patch(
        'afkak.brokerclient.ReconnectingClientFactory.clientConnectionFailed')
    def test_connectFailNotify(self, ccf):
        """
        test_connectFailNotify
        Check that if the connection fails to come up that the brokerclient
        errback's the deferred returned from the '_connect' call.
        """
        c = KafkaBrokerClient('test_connectFailNotify',
                              reactor=MemoryReactorClock())
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
        c = KafkaBrokerClient('test_close', reactor=reactor)
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

    def test_close_disconnected(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_close', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        c.connector.state = 'disconnected'  # set connector's state for test
        dd = c.close()
        self.assertIsInstance(dd, Deferred)
        r = self.successResultOf(dd)
        self.assertIs(r, None)

    def test_reconnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_reconnect', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        dd = c.close()
        self.assertIsInstance(dd, Deferred)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.

    def test_closeNotConnected(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_closeNotConnected', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        d = c.close()
        self.assertIsInstance(d, Deferred)

    def test_close_no_connect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_closeNotConnected', reactor=reactor)
        d = c.close()
        self.assertIsInstance(d, Deferred)

    def test_closeNotify(self):
        from twisted.internet.error import ConnectionDone
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_closeNotify', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        c.buildProtocol(None)
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())
        c.continueTrying = False
        c.close()
        c.clientConnectionLost(c.connector, Failure(ConnectionDone()))
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())

    def test_closeNotifyDuringConnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('test_closeNotify', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())
        c.close()
        c.clientConnectionFailed(c.connector, Failure(UserError()))
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())

    def test_makeRequest(self):
        id1 = 54321
        id2 = 76543
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testmakeRequest', reactor=reactor)
        request = KafkaCodec.encode_fetch_request('testmakeRequest', id1)
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
        request = KafkaCodec.encode_fetch_request('testmakeRequest2', id2)
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
        c = KafkaBrokerClient('testmakeRequest', reactor=reactor)
        request = KafkaCodec.encode_fetch_request('testmakeRequest', id1)
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
        c = KafkaBrokerClient('test_closeNotConnected', reactor=reactor)
        d = c.close()
        self.assertIsInstance(d, Deferred)
        d2 = c.makeRequest(1, 'fake request')
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
        c = KafkaBrokerClient('test_connect', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Fake a protocol
        c.proto = Mock()
        request = KafkaCodec.encode_fetch_request('testcancelRequest', id1)
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
        c = KafkaBrokerClient('test_connect', reactor=reactor)
        c._connect()  # Force a connection attempt
        c.connector.factory = c  # MemoryReactor doesn't make this connection.
        # Fake a protocol
        c.proto = Mock()
        # now call with 'expectReply=False'
        c.proto = Mock()
        request = KafkaCodec.encode_fetch_request('testcancelRequest2', id2)
        d2 = c.makeRequest(id2, request, expectResponse=False)
        self.assertIsInstance(d2, Deferred)
        c.proto.sendString.assert_called_once_with(request)
        # This one we cancel by ID. It should fail due to the
        # expectResponse=False, since we don't keep the requestID after send
        self.assertRaises(KeyError, c.cancelRequest, id2)

    def test_makeUnconnectedRequest(self):
        """ test_makeUnconnectedRequest
        Ensure that sending a request when not connected will attempt to bring
        up a connection if one isn't already in the process of being brought up
        """
        id1 = 65432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testmakeUnconnectedRequest',
                              reactor=reactor)
        request = KafkaCodec.encode_fetch_request(
            'testmakeUnconnectedRequest', id1)
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
        c = KafkaBrokerClient('testrequestsRetried',
                              reactor=reactor)
        request = KafkaCodec.encode_fetch_request(
            'testrequestsRetried', id1)
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
            t1 = "topic1"
            t2 = "topic2"
            msgs = map(
                create_message, ["message1", "hi", "boo", "foo", "so fun!"])
            ms1 = KafkaCodec._encode_message_set([msgs[0], msgs[1]])
            ms2 = KafkaCodec._encode_message_set([msgs[2]])
            ms3 = KafkaCodec._encode_message_set([msgs[3], msgs[4]])

            packFmt = '>iih{}siihqi{}sihqi{}sh{}siihqi{}s'.format(
                len(t1), len(ms1), len(ms2), len(t2), len(ms3))
            return struct.pack(
                packFmt, id, 2, len(t1), t1, 2, 0, 0, 10, len(ms1), ms1, 1,
                1, 20, len(ms2), ms2, len(t2), t2, 1, 0, 0, 30, len(ms3), ms3)

        c = KafkaBrokerClient('testhandleResponse')
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
                'testhandleResponse2', goodId)
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

        tReq = _Request(5, "data", True)
        self.assertEqual(tReq.__repr__(), '_Request:5:True')
