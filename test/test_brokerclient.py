
"""
Test code for KafkaBrokerClient(ReconnectingClientFactory) class.
"""

from __future__ import division, absolute_import

import pickle
import struct

from mock import MagicMock, call, patch

from twisted.internet.defer import Deferred
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase

import kafkatwisted.brokerclient as brokerclient
from kafkatwisted.brokerclient import KafkaBrokerClient
from kafkatwisted.kafkacodec import KafkaCodec, create_message
from kafkatwisted.common import (
    ClientError, DuplicateRequestError, RequestTimedOutError
)

from pprint import PrettyPrinter
pp = PrettyPrinter(indent=2, width=1024)
pf = pp.pformat


class FakeConnector(object):
    """
    A fake connector class, to be used to mock connections failed or lost.
    """

    state = "disconnected"

    def stopConnecting(self):
        pass

    def connect(self):
        self.state = "connecting"

    def disconnect(self):
        self.state = "disconnecting"

class FactoryAwareFakeConnector(FakeConnector):
    connectCalled = False
    factory = None

    def stopConnecting(self):
        """
        Behave as though an ongoing connection attempt has now
        failed, and notify the factory of this.
        """
        self.factory.clientConnectionFailed(self, None)

    def connect(self):
        """
        Record any connection attempts
        """
        self.connectCalled = True

    def connectionFailed(self, reason):
        """
        Record our state as disconnected and notify the factory
        """
        self.state = "disconnected"
        self.factory.clientConnectionFailed(self, reason)


class KafkaBrokerClientTestCase(TestCase):
    """
    Tests for L{KafkaBrokerClient}.
    """

    def test_stopTryingWhenConnected(self):
        """
        If a L{KafkaBrokerClient} has C{stopTrying} called while it is
        connected, it does not subsequently attempt to reconnect if the
        connection is later lost.
        """
        class NoConnectConnector(object):
            def stopConnecting(self):
                raise KafkaError("Shouldn't be called, we're connected.")

            def connect(self):
                raise KafkaError("Shouldn't be reconnecting.")

        c = KafkaBrokerClient('broker')
        c.protocol = Protocol
        # Let's pretend we've connected:
        c.buildProtocol(None)
        # Now we stop trying, then disconnect:
        c.stopTrying()
        c.clientConnectionLost(NoConnectConnector(), None)
        self.assertFalse(c.continueTrying)

    def test_stopTryingDoesNotReconnect(self):
        """
        Calling stopTrying on a L{KafkaBrokerClient} doesn't attempt a
        retry on any active connector.
        """
        f = KafkaBrokerClient('broker')
        f.clock = Clock()

        # simulate an active connection - stopConnecting on this connector
        # should be triggered when we call stopTrying
        f.connector = FactoryAwareFakeConnector()
        f.connector.factory = f
        f.stopTrying()

        # make sure we never attempted to retry
        self.assertFalse(f.connector.connectCalled)

        # Since brokerclient uses callLater() to call it's notify()
        # method, we need to remove that first...
        for call in f.clock.getDelayedCalls():
            if call.func == f.notify:
                f.clock.calls.remove(call)
        self.assertFalse(f.clock.getDelayedCalls())

    def test_serializeUnused(self):
        """
        A L{KafkaBrokerClient} which hasn't been used for anything
        can be pickled and unpickled and end up with the same state.
        """
        original = KafkaBrokerClient('broker')
        reconstituted = pickle.loads(pickle.dumps(original))
        self.assertEqual(original.__dict__, reconstituted.__dict__)

    def test_serializeWithClock(self):
        """
        The clock attribute of L{KafkaBrokerClient} is not serialized,
        and the restored value sets it to the default value, the reactor.
        """
        clock = Clock()
        original = KafkaBrokerClient('broker')
        original.clock = clock
        reconstituted = pickle.loads(pickle.dumps(original))
        self.assertIs(reconstituted.clock, None)

    def test_deserializationResetsParameters(self):
        """
        A L{KafkaBrokerClient} which is unpickled does not have an
        L{IConnector} and has its reconnecting timing parameters reset to their
        initial values.
        """
        factory = KafkaBrokerClient('broker')
        factory.clientConnectionFailed(FakeConnector(), None)
        self.addCleanup(factory.stopTrying)

        serialized = pickle.dumps(factory)
        unserialized = pickle.loads(serialized)
        self.assertIs(unserialized.connector, None)
        self.assertIs(unserialized._callID, None)
        self.assertEqual(unserialized.retries, 0)
        self.assertEqual(unserialized.delay, factory.initialDelay)
        self.assertEqual(unserialized.continueTrying, True)

    def test_parametrizedClock(self):
        """
        The clock used by L{KafkaBrokerClient} can be parametrized, so
        that one can cleanly test reconnections.
        """
        clock = Clock()
        factory = KafkaBrokerClient('broker')
        factory.clock = clock

        factory.clientConnectionLost(FakeConnector(), None)
        self.assertEqual(len(clock.calls), 2)

    def test_subscribersList(self):
        """
        Test that a brokerclient's connSubscribers instance
        variable is set by the subscribers parameter on the
        constructor
        """
        def c1():
            pass

        def c2():
            pass

        def c3():
            pass

        def c4():
            pass

        def c5():
            pass
        sublist = [c1, c2, c3]
        c = KafkaBrokerClient('broker', subscribers=sublist)
        self.assertEqual(sublist, c.connSubscribers)

        c.addSubscriber(c4)
        c.addSubscriber(c5)
        addedList = sublist
        addedList.extend([c4, c5])
        self.assertEqual(addedList, c.connSubscribers)

        rmdList = addedList
        rmdList.remove(c3)
        rmdList.remove(c4)
        c.delSubscriber(c3)
        c.delSubscriber(c4)
        self.assertEqual(rmdList, c.connSubscribers)

    def test_subscribersListCalls(self):
        """
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
        c.notify(True)
        self.assertEqual(callList, ['c1:True', 'c2:True', 'c3:True'])
        callList = []
        c.notify(False)
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
        c.notify(True, reason='TheReason')
        c.addSubscriber(c4)
        self.assertEqual(callList, ['c1:True:TheReason', 'c2:True:TheReason',
                                    'c3:True:TheReason'])
        callList = []
        c.notify(False)
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
        c.notify(True)
        reactor.advance(1.0)
        self.assertEqual(callList, ['c1:True', 'c3:True', 'c5:True'])
        callList = []
        c.notify(False)
        reactor.advance(1.0)
        self.assertEqual(callList, ['c1:False', 'c3:False', 'c5:False'])
        callList = []

    def test_repr(self):
        c = KafkaBrokerClient('kafka.example.com',
                              clientId='MyClient')
        self.assertEqual('<KafkaBrokerClient kafka.example.com:MyClient:None',
                         c.__repr__())

    def test_connect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testconnect', reactor=reactor)
        d = c.connect()
        self.assertIsInstance(d, Deferred)
        # The deferred shouldn't have fired yet.
        self.assertFalse(d.called)
        # Let's pretend we've connected, which will schedule the firing
        c.buildProtocol(None)
        # This should trigger the d.callback
        reactor.advance(1.0)
        # The deferred should have fired.
        self.assertTrue(d.called)

    def test_connectTwice(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testconnect2', reactor=reactor)
        c.connector = FakeConnector()
        c.connect()
        self.assertRaises(ClientError, c.connect)

    def test_connectNotify(self):
        from kafkatwisted.protocol import KafkaProtocol
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testconnectNotify', reactor=reactor)
        c.connector = FakeConnector()
        d = c.connect()
        proto = c.buildProtocol(None)
        self.assertIsInstance(proto, KafkaProtocol)
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())
        self.assertTrue(d.called)

    @patch('kafkatwisted.brokerclient.ReconnectingClientFactory')
    def test_connectFailNotify(self, rcFactory):
        """
        Check that if the connection fails to come up that the brokerclient
        calls the errback's the deferred returned from the 'connect' call.
        """
        # Testing reactor/clock
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testconnectFailNotify', reactor=reactor)
        brokerclient.ReconnectingClientFactory = rcFactory

        # Stub out the connector with something that won't actually connect
        c.connector = FactoryAwareFakeConnector()
        c.connector.factory = c
        # attempt connection
        d = c.connect()
        eb1 = MagicMock()
        d.addErrback(eb1)
        # Claim the connection failed
        e = ConnectionRefusedError()
        c.connector.connectionFailed(e)
        # Check that the brokerclient called super to reconnect
        rcFactory.clientConnectionFailed.assert_called_once_with(
            c, c.connector, e)
        # Check that the deferred fired with the same error
        self.assertTrue(d.called)
        fail1 = eb1.call_args[0][0]  # The actual failure sent to errback
        self.assertEqual(e, fail1.value)

    def test_disconnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testdisconnect', reactor=reactor)
        cd = c.connect()
        self.assertIsInstance(cd, Deferred)
        dd = c.disconnect()
        self.assertIsInstance(dd, Deferred)

    def test_reconnect(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testdisconnect', reactor=reactor)
        c.connector = FakeConnector()
        cd = c.connect()
        self.assertIsInstance(cd, Deferred)
        dd = c.disconnect()
        self.assertIsInstance(dd, Deferred)
        c.connector.state = 'disconnected'
        cd2 = c.connect()
        self.assertIsInstance(cd2, Deferred)

    def test_disconnectNotConnected(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testdisconnectNotConnected', reactor=reactor)
        self.assertRaises(ClientError, c.disconnect)

    def test_disconnectNotify(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testdisconnectNotify', reactor=reactor)
        c.connector = FakeConnector()
        c.connect()
        c.buildProtocol(None)
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())
        c.continueTrying = False
        c.disconnect()
        c.clientConnectionLost(c.connector, 'testdisconnectNotify')
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())

    def test_makeRequest(self):
        id1 = 54321
        id2 = 76543
        c = KafkaBrokerClient('testmakeRequest')
        c.proto = MagicMock()
        request = KafkaCodec.encode_fetch_request('testmakeRequest', id1)
        d = c.makeRequest(id1, request)
        self.assertIsInstance(d, Deferred)
        c.proto.sendString.assert_called_once_with(request)

        # now call with 'expectReply=False'
        c.proto = MagicMock()
        request = KafkaCodec.encode_fetch_request('testmakeRequest2', id2)
        d2 = c.makeRequest(id2, request, expectResponse=False)
        self.assertIsInstance(d2, Deferred)
        c.proto.sendString.assert_called_once_with(request)

        # cancel the request so the reactor isn't unclean
        c.cancelRequest(id1)

    def test_requestTimeout(self):
        id1 = 87654
        id2 = 45654
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('requesttimeout', timeout=5.0, reactor=reactor)
        c.proto = MagicMock()
        request = KafkaCodec.encode_fetch_request('requestTimeout', id1)
        d = c.makeRequest(id1, request)
        self.assertIsInstance(d, Deferred)
        c.proto.sendString.assert_called_once_with(request)

        # now call with 'timeout=1'
        c.proto = MagicMock()
        request = KafkaCodec.encode_fetch_request('testmakeRequest2', id2)
        d2 = c.makeRequest(id2, request, timeout=1.0)
        self.assertIsInstance(d2, Deferred)
        c.proto.sendString.assert_called_once_with(request)

        # Add handlers for the errback() so we don't throw exceptions
        eb1 = MagicMock()
        eb2 = MagicMock()
        d.addErrback(eb1)
        d2.addErrback(eb2)
        # The expected failures passed to the errback() call. Note, you can't
        # compare these directly, because two different exception instances won't
        # compare the same, even if they are created with the same args
        eFail1 = Failure(RequestTimedOutError('Request:{} timed out'.format(id1)))
        eFail2 = Failure(RequestTimedOutError('Request:{} timed out'.format(id2)))

        # advance the clock...
        reactor.advance(1.1)
        # Make sure the 2nd request timed out, but not the first
        self.assertFalse(d.called)
        self.assertTrue(d2.called)
        # can't use 'assert_called_with' because the exception instances won't
        # compare equal. Instead, get the failure from the mock's call_args, and
        # then look at parts of it...
        fail2 = eb2.call_args[0][0]  # The actual failure sent to errback
        self.assertEqual(eFail2.type, fail2.type)
        self.assertEqual(eFail2.value.args, fail2.value.args)
        # advance the clock...
        reactor.advance(4.0)
        # Now the first...
        self.assertTrue(d.called)
        fail1 = eb1.call_args[0][0]  # The actual failure sent to errback
        self.assertEqual(eFail1.type, fail1.type)
        self.assertEqual(eFail1.value.args, fail1.value.args)


    def test_makeUnconnectedRequest(self):
        id1 = 65432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testmakeUnconnectedRequest',
                              timeout=5.0, reactor=reactor)
        c.connector = FakeConnector()
        request = KafkaCodec.encode_fetch_request(
            'testmakeUnconnectedRequest', id1)
        d = c.makeRequest(id1, request)
        self.assertIsInstance(d, Deferred)
        # Make sure the request shows unsent
        self.assertFalse(c.requests[id1].sent)
        # Initiate the connection
        c.connect()
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = MagicMock()
        reactor.advance(1.0)
        # Now, we should have seen the 'sendString' called
        c.proto.sendString.assert_called_once_with(request)
        # cancel the request so the reactor isn't unclean (timeout callback)
        c.cancelRequest(id1)

    def test_requestsRetried(self):
        id1 = 65432
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testrequestsRetried',
                              timeout=5.0, reactor=reactor)
        c.connector = FakeConnector()
        request = KafkaCodec.encode_fetch_request(
            'testrequestsRetried', id1)
        d = c.makeRequest(id1, request)
        # Make sure the request shows unsent
        self.assertFalse(c.requests[id1].sent)
        # Initiate the connection
        c.connect()
        # Bring up the "connection"...
        c.buildProtocol(None)
        # Replace the created proto with a mock
        c.proto = MagicMock()
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
        c.proto = MagicMock()
        reactor.advance(0.1)
        # Now, we should have seen the 'sendString' called
        c.proto.sendString.assert_called_once_with(request)
        # And the request should be 'sent'
        self.assertTrue(c.requests[id1].sent)
        # cancel the request so the reactor isn't unclean (timeout callback)
        c.cancelRequest(id1)

    def test_handleResponse(self):
        def make_fetch_response(id):
            t1 = "topic1"
            t2 = "topic2"
            msgs = map(create_message, ["message1", "hi", "boo", "foo", "so fun!"])
            ms1 = KafkaCodec._encode_message_set([msgs[0], msgs[1]])
            ms2 = KafkaCodec._encode_message_set([msgs[2]])
            ms3 = KafkaCodec._encode_message_set([msgs[3], msgs[4]])

            packFmt = '>iih{}siihqi{}sihqi{}sh{}siihqi{}s'.format(
                len(t1), len(ms1), len(ms2), len(t2), len(ms3))
            return struct.pack(
                packFmt, id, 2, len(t1), t1, 2, 0, 0, 10, len(ms1), ms1, 1,
                1, 20, len(ms2), ms2, len(t2), t2, 1, 0, 0, 30, len(ms3), ms3)

        c = KafkaBrokerClient('testhandleResponse')
        brokerclient.log = MagicMock()
        badId = 98765
        response = make_fetch_response(badId)
        # First test that a response without first sending a request
        # generates a log message
        c.handleResponse(response)
        brokerclient.log.warning.assert_called_once_with(
            'Unexpected response:', badId, response)

        # Now try a request/response pair and ensure the deferred is called
        goodId = 12345
        c.proto = MagicMock()
        request = KafkaCodec.encode_fetch_request('testhandleResponse2', goodId)
        d = c.makeRequest(goodId, request)
        self.assertIsInstance(d, Deferred)
        c.proto.sendString.assert_called_once_with(request)
        response = make_fetch_response(goodId)
        self.assertFalse(d.called)
        # Ensure trying to make another request with the same ID before
        # we've got a response raises an exception
        c.proto.sendString = MagicMock(side_effect=Exception(
                'brokerclient sending duplicate request'))
        self.assertRaises(DuplicateRequestError, c.makeRequest, goodId, request)
        c.handleResponse(response)
        self.assertTrue(d.called)

        # Now that we've got the response, try again with the goodId
        c.proto = MagicMock()  # Need a new mock...
        d = c.makeRequest(goodId, request)
        self.assertIsInstance(d, Deferred)
        c.proto.sendString.assert_called_once_with(request)
        response = make_fetch_response(goodId)
        self.assertFalse(d.called)
        c.handleResponse(response)
        self.assertTrue(d.called)

        # Now try with a real request, but with expectResponse=False
        # We still get a deferred back, because the request can timeout
        # before it's ever sent, and in that case, we errback() the
        # deferred
        d2 = c.makeRequest(goodId, request, expectResponse=False)
        self.assertIsInstance(d2, Deferred)
        # Send the (unexpected) response, and check for the log message
        c.handleResponse(response)
        brokerclient.log.warning.assert_called_with(
            'Unexpected response:', goodId, response)


