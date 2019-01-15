# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2017, 2018, 2019 Ciena Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""KafkaBrokerClient and private _Request classes.

Low level network client for the Apache Kafka Message Broker.
"""

from __future__ import absolute_import

import logging
from collections import OrderedDict
from functools import partial

from six.moves import reprlib
from twisted.internet.defer import Deferred, fail, maybeDeferred
from twisted.internet.protocol import ClientFactory
from twisted.internet.task import deferLater
from twisted.python.failure import Failure

from ._protocol import KafkaProtocol
from .common import CancelledError, ClientError, DuplicateRequestError
from .kafkacodec import KafkaCodec

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
        self.node_id = brokerMetadata.node_id
        self.host = brokerMetadata.host
        self.port = brokerMetadata.port
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
        return '_KafkaBrokerClient<clientId={} node_id={} {}:{} {}>'.format(
            self.clientId,
            self.node_id,
            self.host,
            self.port,
            'connected' if self.connected() else 'unconnected',
            # TODO: Add transport.getPeer() when connected
        )

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
        if self.node_id != new.node_id:
            raise ValueError("Broker metadata {!r} doesn't match node_id={}".format(new, self.node_id))

        self.node_id = new.node_id
        self.host = new.host
        self.port = new.port

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
            CancelledError(message="Request correlationId={} was cancelled".format(requestId)))
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
        """Permanently dispose of the broker client.

        This terminates any outstanding connection and cancels any pending
        requests.
        """
        log.debug('%r: close() proto=%r connector=%r', self, self.proto, self.connector)
        assert self._dDown is None
        self._dDown = Deferred()

        if self.proto is not None:
            self.proto.transport.loseConnection()
        elif self.connector is not None:
            def connectingFailed(reason):
                """
                Handle the failure resulting from cancellation.

                :reason: a `Failure`, most likely a cancellation error (but
                    that's not guaranteed).
                :returns: `None` to handle the failure
                """
                log.debug('%r: connection attempt has been cancelled: %r', self, reason)
                self._dDown.callback(None)

            self.connector.addErrback(connectingFailed)
            self.connector.cancel()
        else:
            # Fake a cleanly closing connection
            self._dDown.callback(None)

        try:
            raise CancelledError(message="Broker client for node_id={} {}:{} was closed".format(
                self.node_id, self.host, self.port))
        except Exception:
            reason = Failure()
        # Cancel any requests
        for correlation_id in list(self.requests.keys()):  # must copy, may del
            self.cancelRequest(correlation_id, reason)
        return self._dDown

    def connected(self):
        """Are we connected to a Kafka broker?"""
        return self.proto is not None

    def _connectionLost(self, reason):
        """Called when the protocol connection is lost

        - Log the disconnection.
        - Mark any outstanding requests as unsent so they will be sent when
          a new connection is made.
        - If closing the broker client, mark completion of that process.

        :param reason:
            Failure that indicates the reason for disconnection.
        """
        log.info('%r: Connection closed: %r', self, reason)

        # Reset our proto so we don't try to send to a down connection
        self.proto = None

        # Mark any in-flight requests as unsent.
        for tReq in self.requests.values():
            tReq.sent = False

        if self._dDown:
            self._dDown.callback(None)
        elif self.requests:
            self._connect()

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
            log.warning('Unexpected response with correlationId=%d: %r',
                        requestId, reprlib.repr(response))
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

    # FIXME: This should not be a public API. The Deferred returned by
    # makeRequest already has a cancel() method.
    # XXX: Wat is the fourth argument I don't even?!
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

    def _connect(self):
        """Connect to the Kafka Broker

        This routine will repeatedly try to connect to the broker (with backoff
        according to the retry policy) until it succeeds.
        """
        def tryConnect():
            self.connector = d = maybeDeferred(connect)
            d.addCallback(cbConnect)
            d.addErrback(ebConnect)

        def connect():
            endpoint = self._endpointFactory(self._reactor, self.host, self.port)
            log.debug('%r: connecting with %s', self, endpoint)
            return endpoint.connect(self)

        def cbConnect(proto):
            log.debug('%r: connected to %r', self, proto.transport.getPeer())
            self._failures = 0
            self.connector = None
            self.proto = proto
            if self._dDown:
                proto.transport.loseConnection()
            else:
                self._sendQueued()

        def ebConnect(fail):
            if self._dDown:
                log.debug('%r: breaking connect loop due to %r after close()', self, fail)
                return fail
            self._failures += 1
            delay = self._retryPolicy(self._failures)
            log.debug('%r: failure %d to connect -> %s; retry in %.2f seconds.',
                      self, self._failures, fail.value, delay)

            self.connector = d = deferLater(self._reactor, delay, lambda: None)
            d.addCallback(cbDelayed)

        def cbDelayed(result):
            tryConnect()

        self._failures = 0
        tryConnect()
