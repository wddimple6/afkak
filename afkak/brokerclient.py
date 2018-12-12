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

from twisted.internet.defer import Deferred, fail, inlineCallbacks, succeed
from twisted.internet.error import ConnectionDone
from twisted.internet.protocol import ClientFactory
from twisted.internet.task import deferLater

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
        self._repr = '_Request<{}{}>'.format(
            self.id,
            '' if self.expect else ' no response expected',
        )

    def __repr__(self):
        return self._repr


class _KafkaBrokerClient(ClientFactory):
    """
    The low-level client which handles transport to a single Kafka broker.

    The KafkaBrokerClient object is responsible for maintaining a connection to
    a single Kafka broker, reconnecting as needed, over which is sends requests
    and receives responses.  Callers can register as 'subscribers' which will
    cause them to be notified of changes in the state of the connection to the
    Kafka broker. Callers make requests with :py:method:`makeRequest`

    """
    protocol = KafkaProtocol

    # Reduce log spam from twisted
    noisy = False

    def __init__(self, reactor, endpointFactory, brokerMetadata, clientId,
                 retryPolicy):
        """
        Create a client for a specific broker

        The broker client connects to the broker as needed to handle requests.
        Requests are retried when the connection fails before the client
        receives the response. Requests can be cancelled at any time.

        Args:
            reactor: Twisted reactor to use for connecting and when scheduling
                delayed calls.
            endpointFactory: Callable which accepts (reactor, host, port) as
                arguments and returns an IStreamClientEndpoint to use to
                connect to the broker.
            brokerMetadata (BrokerMetadata):
                Broker node ID, host, and port. This may be updated later by
                calling updateMetadata().
            clientId (str): Identifying string for log messages. NOTE: not the
                ClientId in the RequestMessage PDUs going over the wire.
        """
        self._reactor = reactor  # ReconnectingClientFactory uses self.clock.
        self._endpointFactory = endpointFactory
        self.brokerMetadata = brokerMetadata
        self.clientId = clientId
        self._retryPolicy = retryPolicy

        # No connector until we try to connect
        self.connector = None
        # The protocol object for the current connection
        self.proto = None
        # ordered dict of _Requests, keyed by requestId
        self.requests = OrderedDict()
        # deferred which fires when the close() completes
        self._dDown = None

    def __repr__(self):
        """return a string representing this KafkaBrokerClient."""
        return '<_KafkaBrokerClient node_id={} clientId={!r} {}:{} {}>'.format(
            self.brokerMetadata.node_id, self.clientId,
            self.host, self.port,
            'connected' if self.connected() else 'unconnected',
            # TODO: Add transport.getPeer() when connected
        )

    # Compatibilty stubs (these are referenced in a number of places for
    # logging).
    host = property(lambda self: self.brokerMetadata.host)
    port = property(lambda self: self.brokerMetadata.port)

    def updateMetadata(self, new):
        """
        Update the metadata stored for this broker.

        Future connections made to the broker will use the host and port
        defined in the new metadata. Any existing connection is not dropped,
        however.

        :param new:
            :clas:`afkak.common.BrokerMetadata` with the same node ID as the
            current metadata.
        """
        assert self.brokerMetadata.node_id == new.node_id
        self.brokerMetadata = new

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
            self.connector = self._connect().addCallback(self._connected)
        return tReq.d

    def disconnect(self):
        """
        Disconnect from the Kafka broker.

        This is used to implement disconnection on timeout as a workaround for
        Kafka connections occasionally getting stuck on the server side under
        load. Requests are not cancelled, so they will be retried.
        """
        if self.proto:
            log.debug('%r Disconnecting from %r', self, self.proto.transport.getPeer())
            self.proto.transport.loseConnection()

    def close(self):
        """
        Permanently dispose of the broker client.

        This terminates any outstanding connection and cancels any pending
        requests.
        """
        log.debug('%r: close proto=%r connector=%r', self, self.proto, self.connector)
        self._dDown = Deferred()

        if self.proto is not None:
            self.proto.transport.loseConnection()
        else:
            if self.connector:
                # Create a deferred to return
                self.connector.cancel()
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

    def _connectionLost(self, reason):
        """Called when the protocol connection is lost

        If we are shutting down, and twisted sends us the expected type of
        error, eat the error. Otherwise, log it and pass it along.
        Also, schedule notification of our subscribers at the next pass
        through the reactor.

        :param reason:
            Failure that indicates the reason for disconnection.
        """
        log.debug('%r: Connection closed: %r', self, reason)
        if self._dDown and reason.check(ConnectionDone):
            # We initiated the close, this is an expected close/lost
            notifyReason = None  # Not a failure
        else:
            notifyReason = reason

        # Reset our proto so we don't try to send to a down connection
        self.proto = None

        # Mark any in-flight requests as unsent.
        for tReq in self.requests.values():
            tReq.sent = False

        if self._dDown:
            self._dDown.callback(notifyReason)
        elif self.requests:
            self.connector = self._connect().addCallback(self._connected)

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
            log.exception('%r: Failed to send request %r', self, tReq)
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

    def _handleRequestFailure(self, failure, requestId):
        """Remove a failed request from our bookkeeping dict.

        Not an error if already removed (canceller removes).
        """
        self.requests.pop(requestId, None)
        return failure

    @inlineCallbacks
    def _connect(self):
        """Initiate a connection to the Kafka Broker.

        This routine will repeatedly try to connect to the broker (with
        exponential backoff) until it succeeds.
        """
        failures = 0
        while True:
            endpoint = self._endpointFactory(self._reactor, self.host, self.port)
            log.debug('%r: connecting with %s', self, endpoint)
            try:
                self.proto = yield endpoint.connect(self)
            except Exception as e:
                failures += 1
                delay = self._retryPolicy(failures)
                if self._dDown:
                    break
                log.debug('%r: failure %d to connect %s -> %s; retry in %.2f seconds.',
                          self, failures, endpoint, e, delay)
                yield deferLater(self._reactor, delay, lambda: None)
                if self._dDown:
                    break
            else:
                self.connector = None
                return

    def _connected(self, _result):
        assert self.proto
        self.connector = None
        self._sendQueued()
