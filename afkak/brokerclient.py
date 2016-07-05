# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

"""KafkaBrokerClient and private _Request classes.

Low level network client for the Apache Kafka Message Broker.
"""

from __future__ import absolute_import

import logging
from collections import OrderedDict
from functools import partial

from twisted.internet.error import (ConnectionDone, UserError)
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import (
    Deferred, DeferredList, maybeDeferred, fail, succeed,
)

from .protocol import KafkaProtocol
from .kafkacodec import KafkaCodec
from .common import (
    ClientError, DuplicateRequestError, DefaultKafkaPort,
    CancelledError,
)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

MAX_RECONNECT_DELAY_SECONDS = 15
CLIENT_ID = "a.kbc"


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


class KafkaBrokerClient(ReconnectingClientFactory):

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

    def __init__(self, host, port=DefaultKafkaPort,
                 clientId=CLIENT_ID, subscribers=None,
                 maxDelay=MAX_RECONNECT_DELAY_SECONDS, maxRetries=None,
                 reactor=None):
        """Create a KafkaBrokerClient for a given host/port.

        Create a new object to manage the connection to a single Kafka broker.
        KafkaBrokerClient will reconnect as needed to keep the connection to
        the broker up and ready to service requests. Requests are retried when
        the connection fails before the client receives the response. Requests
        can be cancelled at any time.

        Args:
            host (str): hostname or IP address of Kafka broker
            port (int): port number of Kafka broker on `host`. Defaulted: 9092
            clientId (str): Identifying string for log messages. NOTE: not the
                ClientId in the RequestMessage PDUs going over the wire.
            subscribers (list of callbacks): Initial list of callbacks to be
                called when the connection changes state.
            maxDelay (seconds): The maximum amount of time between reconnect
                attempts when a connection has failed.
            maxRetries: The maximum number of times a reconnect attempt will be
                made.
            reactor: the twisted reactor to use when making connections or
                scheduling iDelayedCall calls. Used primarily for testing.
        """
        # Set the broker host & port
        self.host = host
        self.port = port
        # No connector until we try to connect
        self.connector = None
        # Set our clientId
        self.clientId = clientId
        # If the caller set maxRetries, we will retry that many
        # times to reconnect, otherwise we retry forever
        self.maxRetries = maxRetries
        # Set max delay between reconnect attempts
        self.maxDelay = maxDelay
        # clock/reactor for testing...
        self.clock = reactor

        # The protocol object for the current connection
        self.proto = None
        # ordered dict of _Requests, keyed by requestId
        self.requests = OrderedDict()
        # deferred which fires when the close() completes
        self.dDown = None
        # Deferred list for any on-going notification
        self.notifydList = None
        # list of subscribers to our connection status:
        # when the connection goes up or down, we call the callback
        # with ourself and True/False for Connection-Up/Down
        if subscribers is None:
            self.connSubscribers = []
        else:
            self.connSubscribers = subscribers

    def __repr__(self):
        """return a string representing this KafkaBrokerClient."""
        return ('<KafkaBrokerClient {0}:{1}:{2}'
                .format(self.host, self.port, self.clientId))

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
        if self.dDown:
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

    def addSubscriber(self, cb):
        """Add a callback to be called when the connection changes state."""
        self.connSubscribers.append(cb)

    def delSubscriber(self, cb):
        """Remove a previously added 'subscriber' callback."""
        if cb in self.connSubscribers:
            self.connSubscribers.remove(cb)

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
            self.dDown = Deferred()
            connector.disconnect()
        else:
            # Fake a cleanly closing connection
            self.dDown = succeed(None)
        # Cancel any requests
        for tReq in self.requests.values():  # can't use itervalues() may del()
            tReq.d.cancel()
        return self.dDown

    def buildProtocol(self, addr):
        """Create a KafkaProtocol object, store it in self.proto, return it."""
        # Schedule notification of subscribers
        self._get_clock().callLater(0, self._notify, True)
        # Build the protocol
        self.proto = ReconnectingClientFactory.buildProtocol(self, addr)
        # point it at us for notifications of arrival of messages
        self.proto.factory = self
        return self.proto

    def clientConnectionLost(self, connector, reason):
        """Handle notification from the lower layers of connection loss.

        If we are shutting down, and twisted sends us the expected type of
        error, eat the error. Otherwise, log it and pass it along.
        Also, schedule notification of our subscribers at the next pass
        through the reactor.
        """
        if self.dDown and reason.check(ConnectionDone):
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
        self._get_clock().callLater(0, self._notify, False, notifyReason)
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
        if self.dDown and reason.check(UserError):
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
        self._get_clock().callLater(0, self._notify, False, notifyReason)
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
        for tReq in self.requests.values():  # can't use itervalues() may del()
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
        for tReq in self.requests.itervalues():
            tReq.sent = False
        return reason

    def _handleRequestFailure(self, failure, requestId):
        """Remove a failed request from our bookkeeping dict.

        Not an error if already removed (canceller removes).
        """
        self.requests.pop(requestId, None)
        return failure

    def _get_clock(self):
        """Reactor to use for connecting, callLater, etc [for testing]."""
        if self.clock is None:
            from twisted.internet import reactor
            self.clock = reactor
        return self.clock

    def _connect(self):
        """Initiate a connection to the Kafka Broker."""
        log.debug('%r: _connect', self)
        # We can't connect, we're not disconnected!
        if self.connector:
            raise ClientError('_connect called but not disconnected')
        # Needed to enable retries after a disconnect
        self.resetDelay()
        self.connector = self._get_clock().connectTCP(
            self.host, self.port, self)
        log.debug('%r: _connect got connector: %r', self, self.connector)

    def _notify(self, connected, reason=None, subs=None):
        """Notify the caller of :py:method:`close` of completion.

        Also notify any subscribers of the state change of the connection.
        """
        if connected:
            self._sendQueued()
        else:
            if self.dDown and not self.dDown.called:
                self.dDown.callback(reason)
            # If the connection just went down, we need to handle any
            # outstanding requests.
            self._handlePending(reason)

        # Notify if requested. We call all of the callbacks, but don't
        # wait for any returned deferreds to fire here. Instead we add
        # them to a deferredList which we check for and wait on before
        # calling any callbacks for subsequent events.
        # This should keep any state-changes done by these callbacks in the
        # proper order. Note however that the ordering of the individual
        # callbacks in each (connect/disconnect) list isn't guaranteed, and
        # they can all be progressing in parallel if they yield or otherwise
        # deal with deferreds
        if self.notifydList:
            # We already have a notify list in progress, so just call back here
            # when the deferred list fires, with the _current_ list of subs
            subs = list(self.connSubscribers)
            self.notifydList.addCallback(
                lambda _: self._notify(connected, reason=reason, subs=subs))
            return

        # Ok, no notifications currently in progress. Notify all the
        # subscribers, keep track of any deferreds, so we can make sure all
        # the subs have had a chance to completely process this event before
        # we send them any new ones.
        dList = []
        if subs is None:
            subs = list(self.connSubscribers)
        for cb in subs:
            dList.append(maybeDeferred(cb, self, connected, reason))
        self.notifydList = DeferredList(dList)

        def clearNotifydList(_):
            """Reset the notifydList once we've notified our subscribers."""
            self.notifydList = None

        self.notifydList.addCallback(clearNotifydList)
