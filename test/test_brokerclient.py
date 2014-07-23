
"""
Test code for KafkaBrokerClient(ReconnectingClientFactory) class.
"""

from __future__ import division, absolute_import

import pickle

from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial.unittest import TestCase

from kafkatwisted.brokerclient import KafkaBrokerClient

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
        self.assertIdentical(reconstituted.clock, None)

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
        self.assertEqual(unserialized.connector, None)
        self.assertEqual(unserialized._callID, None)
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
        self.assertRaises(RuntimeError, c.connect)

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

    def test_disconnectNotConnected(self):
        reactor = MemoryReactorClock()
        c = KafkaBrokerClient('testdisconnectNotConnected', reactor=reactor)
        self.assertRaises(RuntimeError, c.disconnect)

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
