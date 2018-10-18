# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2017, 2018 Ciena Corporation

"""KafkaBrokerClient and private _Request classes.

Low level network client for the Apache Kafka Message Broker.
"""

from __future__ import absolute_import

import logging
from collections import OrderedDict
from functools import partial

from twisted.internet.defer import Deferred, fail, succeed
from twisted.internet.error import ConnectionDone, UserError
from twisted.internet.protocol import ReconnectingClientFactory

from .common import CancelledError, ClientError, DuplicateRequestError
from .kafkacodec import KafkaCodec
from .protocol import KafkaProtocol

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

MAX_RECONNECT_DELAY_SECONDS = 15
INIT_DELAY_SECONDS = 0.1


class _Request(object):

    """Private class to encapsulate requests we are processing."""

    sent = False  # Have we written this request to our protocol?

    def __init__(self, requestId, data, expectResponse, canceller=None):
        self.id = requestId
        self.data = data
        self.expect = expectResponse
        self.canceller = canceller
        self.d = Deferred(canceller=canceller)
        self._repr = '_Request:{}:{}'.format(self.id, self.expect)

    def __repr__(self):
        return self._repr


class _KafkaBrokerClient(ReconnectingClientFactory):

    """The low-level client which handles transport to a single Kafka broker.

    The KafkaBrokerClient object is responsible for maintaining a connection to
    a single Kafka broker, reconnecting as needed, over which is sends requests
    and receives responses.  Callers can register as 'subscribers' which will
    cause them to be notified of changes in the state of the connection to the
    Kafka broker. Callers make requests with :py:method:`makeRequest`

    """

    # What class protocol instances do we produce?
    protocol = KafkaProtocol

    # Reduce log spam from twisted
    noisy = False

    def __init__(self, reactor, host, port, clientId,
                 subscriber=None,
                 maxDelay=MAX_RECONNECT_DELAY_SECONDS,
                 maxRetries=None,
                 initDelay=INIT_DELAY_SECONDS):
        """Create a _KafkaBrokerClient for a given host/port.

        Create a new object to manage the connection to a single Kafka broker.
        KafkaBrokerClient will reconnect as needed to keep the connection to
        the broker up and ready to service requests. Requests are retried when
        the connection fails before the client receives the response. Requests
        can be cancelled at any time.

        Args:
            reactor: Twisted reactor to use when making connections or
                scheduling delayed calls. Used primarily for testing.
            host (str): hostname or IP address of a Kafka broker
            port (int): port number of Kafka broker on `host`
            clientId (str): Identifying string for log messages. NOTE: not the
                ClientId in the RequestMessage PDUs going over the wire.
            subscriber (callback): Connection state change callback. It is
                called with this instance, a boolean indicating whether there
                is a connection, and a reason which indicates why a non-clean
                disconnection happened.
            maxDelay (seconds): The maximum amount of time between reconnect
                attempts when a connection has failed.
            maxRetries: The maximum number of times a reconnect attempt will be
                made.
            initDelay: Initial delay, multiplied by 'factor', when reconnecting
                after the connection is lost. Defaults to 0.1 seconds.
        """
        self.clock = reactor  # ReconnectingClientFactory uses self.clock.
        self.host = host
        self.port = port
        self.clientId = clientId

        # No connector until we try to connect
        self.connector = None
        # If the caller set maxRetries, we will retry that many
        # times to reconnect, otherwise we retry forever
        self.maxRetries = maxRetries
        # Set initial delay on reconnect attempt
        self.initialDelay = self.delay = initDelay
        # Set max delay between reconnect attempts
        self.maxDelay = maxDelay

        # The protocol object for the current connection
        self.proto = None
        # ordered dict of _Requests, keyed by requestId
        self.requests = OrderedDict()
        # deferred which fires when the close() completes
        self._dDown = None
        self._subscriber = subscriber

    def __repr__(self):
        """return a string representing this KafkaBrokerClient."""
        return '<KafkaBrokerClient {}:{} clientId={!r} {}>'.format(
            self.host, self.port, self.clientId,
            'connected' if self.connected() else 'unconnected',
        )

    def makeRequest(self, requestId, request, expectResponse=True):
        """
        Send a request to our broker via our self.proto KafkaProtocol object.

        Return a deferred which will fire when the reply matching the requestId
        comes back from the server, or, if expectResponse is False, then
        return None instead.
        If we are not currently connected, then we buffer the request to send
        when the connection comes back up.
        """
        if requestId in self.requests:
            # Id is duplicate to 'in-flight' request. Reject it, as we
            # won't be able to properly deliver the response(s)
            # Note that this won't protect against a client calling us
            # twice with the same ID, but first with expectResponse=False
            # But that's pathological, and the only defense is to track
            # all requestIds sent regardless of whether we expect to see
            # a response, which is effectively a memory leak...
            raise DuplicateRequestError(
                'Reuse of requestId:{}'.format(requestId))

        # If we've been told to shutdown (close() called) then fail request
        if self._dDown:
            return fail(ClientError('makeRequest() called after close()'))

        # Ok, we are going to save/send it, create a _Request object to track
        canceller = partial(
            self.cancelRequest, requestId,
            CancelledError("Request:{} was cancelled".format(requestId)))
        tReq = _Request(requestId, request, expectResponse, canceller)

        # add it to our requests dict
        self.requests[requestId] = tReq

        # Add an errback to the tReq.d to remove it from our requests dict
        # if something goes wrong...
        tReq.d.addErrback(self._handleRequestFailure, requestId)

        # Do we have a connection over which to send the request?
        if self.proto:
            # Send the request
            self._sendRequest(tReq)
        # Have we not even started trying to connect yet? Do so now
        elif not self.connector:
            self._connect()
        return tReq.d

    def disconnect(self):
        """Disconnect from the Kafka broker by closing the socket.
        Does not cancel requests, so they will be retried."""
        if self.connector:
            self.connector.disconnect()

    def close(self):
        """Close the brokerclient's connection, cancel any pending requests."""
        log.debug('%r: close proto:%r connector:%r', self,
                  self.proto, self.connector)
        # Give our proto a 'heads up'...
        if self.proto is not None:
            self.proto.closing = True
        # Don't try to reconnect, and if we have an outstanding 'callLater',
        # cancel it. Also, if we are in the middle of connecting, stop
        # and call our clientConnectionFailed method with UserError
        self.stopTrying()
        # Ok, stopTrying() call above took care of the 'connecting' state,
        # now handle 'connected' state
        connector, self.connector = self.connector, None
        if connector and connector.state != "disconnected":
            # Create a deferred to return
            self._dDown = Deferred()
            connector.disconnect()
        else:
            # Fake a cleanly closing connection
            self._dDown = succeed(None)
        # Cancel any requests
        for tReq in list(self.requests.values()):  # must copy, may del
            tReq.d.cancel()
        return self._dDown

    def connected(self):
        """Return whether brokerclient is currently connected to a broker"""
        return self.proto is not None

    def buildProtocol(self, addr):
        """Create a KafkaProtocol object, store it in self.proto, return it."""
        # Reset our reconnect delay parameters
        self.resetDelay()
        # Schedule notification of subscribers
        self.clock.callLater(0, self._notify, True)
        # Build the protocol
        self.proto = ReconnectingClientFactory.buildProtocol(self, addr)
        # point it at us for notifications of arrival of messages
        self.proto.factory = self
        log.debug('%r: buildProtocol:%r addr:%r', self, self.proto, addr)
        return self.proto

    def clientConnectionLost(self, connector, reason):
        """Handle notification from the lower layers of connection loss.

        If we are shutting down, and twisted sends us the expected type of
        error, eat the error. Otherwise, log it and pass it along.
        Also, schedule notification of our subscribers at the next pass
        through the reactor.
        """
        if self._dDown and reason.check(ConnectionDone):
            # We initiated the close, this is an expected close/lost
            log.debug('%r: Connection Closed:%r:%r', self, connector, reason)
            notifyReason = None  # Not a failure
        else:
            log.debug('%r: clientConnectionLost:%r:%r', self, connector,
                      reason)
            notifyReason = reason

        # Reset our proto so we don't try to send to a down connection
        self.proto = None
        # Schedule notification of subscribers
        self.clock.callLater(0, self._notify, False, notifyReason)
        # Call our superclass's method to handle reconnecting
        ReconnectingClientFactory.clientConnectionLost(
            self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        """Handle notification from the lower layers of connection failure.

        If we are shutting down, and twisted sends us the expected type of
        error, eat the error. Otherwise, log it and pass it along.
        Also, schedule notification of our subscribers at the next pass
        through the reactor.
        """
        if self._dDown and reason.check(UserError):
            # We initiated the close, this is an expected connectionFailed,
            # given we were trying to connect when close() was called
            log.debug('%r: clientConnectionFailed:%r:%r', self, connector,
                      reason)
            notifyReason = None  # Not a failure
        else:
            log.error('%r: clientConnectionFailed:%r:%r', self, connector,
                      reason)
            notifyReason = reason

        # Reset our proto so we don't try to send to a down connection
        # Needed?  I'm not sure we should even _have_ a proto at this point...
        self.proto = None

        # Schedule notification of subscribers
        self.clock.callLater(0, self._notify, False, notifyReason)
        # Call our superclass's method to handle reconnecting
        return ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason)

    def handleResponse(self, response):
        """Handle the response string received by KafkaProtocol.

        Ok, we've received the response from the broker. Find the requestId
        in the message, lookup & fire the deferred with the response.
        """
        requestId = KafkaCodec.get_response_correlation_id(response)
        # Protect against responses coming back we didn't expect
        tReq = self.requests.pop(requestId, None)
        if tReq is None:
            # This could happen if we've sent it, are waiting on the response
            # when it's cancelled, causing us to remove it from self.requests
            log.warning('Unexpected response:%r, %r', requestId, response)
        else:
            tReq.d.callback(response)

    # # Private Methods # #

    def _sendRequest(self, tReq):
        """Send a single request over our protocol to the Kafka broker."""
        try:
            tReq.sent = True
            self.proto.sendString(tReq.data)
        except Exception as e:
            log.exception(
                '%r: request id: %d send failed:', self, tReq.id)
            del self.requests[tReq.id]
            tReq.d.errback(e)
        else:
            if not tReq.expect:
                # Once we've sent a request for which we don't expect a reply,
                # we're done, remove it from requests, and fire the deferred
                # with 'None', since there is no reply to be expected
                del self.requests[tReq.id]
                tReq.d.callback(None)

    def _sendQueued(self):
        """Connection just came up, send the unsent requests."""
        for tReq in list(self.requests.values()):  # must copy, may del
            if not tReq.sent:
                self._sendRequest(tReq)

    def cancelRequest(self, requestId, reason=CancelledError(), _=None):
        """Cancel a request: remove it from requests, & errback the deferred.

        NOTE: Attempts to cancel a request which is no longer tracked
          (expectResponse == False and already sent, or response already
          received) will raise KeyError
        """
        tReq = self.requests.pop(requestId)
        tReq.d.errback(reason)

    def _handlePending(self, reason):
        """Connection went down: handle in-flight & unsent as configured.

        Note: for now, we just 'requeue' all the in-flight by setting their
          'sent' variable to False and let '_sendQueued()' handle resending
          when the connection comes back.
          In the future, we may want to extend this so we can errback()
          to our client's any in-flight (and possibly queued) so they can deal
          with it at the application level.
        """
        for tReq in self.requests.values():
            tReq.sent = False
        return reason

    def _handleRequestFailure(self, failure, requestId):
        """Remove a failed request from our bookkeeping dict.

        Not an error if already removed (canceller removes).
        """
        self.requests.pop(requestId, None)
        return failure

    def _connect(self):
        """Initiate a connection to the Kafka Broker."""
        log.debug('%r: _connect', self)
        # We can't connect, we're not disconnected!
        if self.connector:
            raise ClientError('_connect called but not disconnected')
        # Start the initial connection and save the returned connector
        self.connector = self.clock.connectTCP(self.host, self.port, self)
        log.debug('%r: _connect got connector: %r', self, self.connector)

    def _notify(self, connected, reason=None):
        """Notify the caller of :py:method:`close` of completion.

        Also notify any subscribers of the state change of the connection.
        """
        if connected:
            self._sendQueued()
        else:
            if self._dDown and not self._dDown.called:
                self._dDown.callback(reason)
            # If the connection just went down, we need to handle any
            # outstanding requests.
            self._handlePending(reason)

        if self._subscriber is not None:
            self._subscriber(self, connected, reason)
