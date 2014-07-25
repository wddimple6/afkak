
"""
Test code for KafkaBrokerClient(ReconnectingClientFactory) class.
"""

from __future__ import division, absolute_import

import pickle
import struct

from mock import MagicMock


from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase

import kafkatwisted.brokerclient as brokerclient
from kafkatwisted.brokerclient import KafkaBrokerClient
from kafkatwisted.kafkacodec import KafkaCodec, create_message
from kafkatwisted.common import ClientError, DuplicateRequestError

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
                raise RuntimeError("Shouldn't be called, we're connected.")

            def connect(self):
                raise RuntimeError("Shouldn't be reconnecting.")

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
        class FactoryAwareFakeConnector(FakeConnector):
            attemptedRetry = False

            def stopConnecting(self):
                """
                Behave as though an ongoing connection attempt has now
                failed, and notify the factory of this.
                """
                f.clientConnectionFailed(self, None)

            def connect(self):
                """
                Record an attempt to reconnect, since this is what we
                are trying to avoid.
                """
                self.attemptedRetry = True

        f = KafkaBrokerClient('broker')
        f.clock = Clock()

        # simulate an active connection - stopConnecting on this connector
        # should be triggered when we call stopTrying
        f.connector = FactoryAwareFakeConnector()
        f.stopTrying()

        # make sure we never attempted to retry
        self.assertFalse(f.connector.attemptedRetry)

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

        def c1(c, conn):
            callList.append('c1:{0}'.format(conn))

        def c2(c, conn):
            def c2_cb(_, c, conn):
                callList.append('c2_cb:{0}'.format(conn))

            d = Deferred()
            d.addCallback(c2_cb, c, conn)
            reactor.callLater(1.0, d.callback, None)
            callList.append('c2:{0}'.format(conn))
            return d

        def c3(c, conn):
            callList.append('c3:{0}'.format(conn))

        def c4(c, conn):
            callList.append('c4:{0}'.format(conn))

        def c5(c, conn):
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
        c.notify(True)
        c.addSubscriber(c4)
        self.assertEqual(callList, ['c1:True', 'c2:True', 'c3:True'])
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
        self.assertEqual('<KafkaBrokerClient kafka.example.com:MyClient:30',
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
        c.connect()
        proto = c.buildProtocol(None)
        self.assertIsInstance(proto, KafkaProtocol)
        reactor.advance(1.0)
        self.assertFalse(c.clock.getDelayedCalls())

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
        n = c.makeRequest(id2, request, expectResponse=False)
        self.assertIs(n, None)
        c.proto.sendString.assert_called_once_with(request)

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
        n = c.makeRequest(goodId, request, expectResponse=False)
        self.assertIs(n, None)
        # Send the (unexpected) response, and check for the log message
        c.handleResponse(response)
        brokerclient.log.warning.assert_called_with(
            'Unexpected response:', goodId, response)


