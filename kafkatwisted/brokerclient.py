# -*- coding: utf-8 -*-
# Copyright (C) 2014 Cyan, Inc.
#
# PROPRIETARY NOTICE
# This Software consists of confidential information.  Trade secret law and
# copyright law protect this Software.  The above notice of copyright on this
# Software does not indicate any actual or intended publication of such
# Software.

from __future__ import absolute_import

import logging
from collections import deque

from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import (
    Deferred, DeferredList, maybeDeferred, inlineCallbacks,
)

from .protocol import KafkaProtocol
from .kafkacodec import KafkaCodec
from .common import ClientError, DuplicateRequestError

log = logging.getLogger("kafkaclient")

DEFAULT_KAFKA_TIMEOUT_SECONDS = 30
MAX_RECONNECT_DELAY_SECONDS = 30
DEFAULT_KAFKA_PORT = 9092
CLIENT_ID = "kafka-twisted"

class KafkaBrokerClient(ReconnectingClientFactory):

    # What class protocol instances do we produce?
    protocol = KafkaProtocol

    def __init__(self, host, port=DEFAULT_KAFKA_PORT,
                 clientId=CLIENT_ID, subscribers=None,
                 reactor=None, maxRetries=None,
                 timeout=DEFAULT_KAFKA_TIMEOUT_SECONDS,
                 maxDelay=MAX_RECONNECT_DELAY_SECONDS):

        # Set the broker host & port
        self.host = host
        self.port = port
        # Set our clientId
        self.clientId = clientId
        # clock/reactor for testing...
        if reactor is not None:
            self.clock = reactor
        # If the caller set maxRetries, we will retry that many
        # times to reconnect, otherwise we retry forever
        self.maxRetries = maxRetries
        # Set max delay between reconnect attempts
        self.maxDelay = maxDelay
        # Set our kafka timeout (not network related!)
        self.timeout = timeout
        # The protocol object for the current connection
        self.proto = None
        # dict of requests we've sent and for which we are awaiting requests
        self.requests = {}
        # deferreds which fires when the connect()/disconnect() completes
        self.dUp = None
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
        return ('<KafkaBrokerClient {0}:{1}:{2}'
                .format(self.host, self.clientId, self.timeout))

    def addSubscriber(self, cb):
        self.connSubscribers.append(cb)

    def delSubscriber(self, cb):
        if cb in self.connSubscribers:
            self.connSubscribers.remove(cb)

    def connect(self):
        # We can't connect, we're not disconnected!
        if self.connector and self.connector.state != 'disconnected':
            raise ClientError('connect called but not disconnected')
        # Needed to enable retries after a disconnect
        self.resetDelay()
        if not self.connector:
            self.connector = self._getClock().connectTCP(self.host, self.port, self)
        else:
            self.connector.connect()
        self.dUp = Deferred()
        return self.dUp

    def disconnect(self):
        # Are we connected?
        if not self.connector:
            raise ClientError('disconnect called but not connected')
        # Keep us from trying to reconnect when the connection closes
        self.stopTrying()
        self.connector.disconnect()
        self.dDown = Deferred()
        return self.dDown

    def buildProtocol(self, addr):
        """
        create & return a KafkaProtocol object, saving it away based
        in self.protos
        """
        log.debug('buildProtocol(addr=%r)', addr)

        # Schedule notification of subscribers
        self._getClock().callLater(0, self.notify, True)
        # Build & return the protocol
        self.proto = ReconnectingClientFactory.buildProtocol(self, addr)
        self.proto.factory = self
        return self.proto

    def clientConnectionLost(self, connector, reason):
        """
        Handle notification from the lower layers that the connection
        was closed/dropped
        """
        # Schedule notification of subscribers
        self._getClock().callLater(0, self.notify, False)
        # Call our superclass's method to handle reconnecting
        return ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        """
        Handle notification from the lower layers that the connection failed
        """
        # Schedule notification of subscribers
        self._getClock().callLater(0, self.notify, False)
        # Call our superclass's method to handle reconnecting
        return ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def notify(self, connected, subs=None):
        # fire the proper deferred, if there is one
        if connected:
            if self.dUp and not self.dUp.called:
                dUp, self.dUp = self.dUp, None
                dUp.callback(self)
        else:
            if self.dDown and not self.dDown.called:
                dDown, self.dDown = self.dDown, None
                dDown.callback(self)

        # Notify the user if requested. We call all of the callbacks, but don't
        # wait for any deferreds to fire here. Instead we add them to a deferred
        # list which we check for and wait on before calling any onDisconnectCBs
        # (should the connection subsequently disconnect).
        # This should keep any state-changes done by these callbacks in the
        # proper order. Note however that the ordering of the individual
        # callbacks in each (connect/disconnect) list isn't guaranteed, and they
        # can all be progressing in parallel if they yield or otherwise deal
        # with deferreds

        if self.notifydList:
            # We already have a notify list in progress, so just call back here
            # when the deferred list fires, with the current list of subs
            subs=list(self.connSubscribers)
            self.notifydList.addCallback(
                lambda _: self.notify(connected,
                                      subs=subs))
            return

        dList = []
        if subs is None:
            subs = self.connSubscribers
        for cb in subs:
            dList.append(maybeDeferred(cb, self, connected))
        self.notifydList = DeferredList(dList)

        # Add clearing of self.onConnectDList to the deferredList so that once
        # it fires, it is reset to None
        def clearNotifydList(_):
            self.notifydList = None

        self.notifydList.addCallback(clearNotifydList)

    def makeRequest(self, requestId, request, expectResponse=True):
        """
        Send a request to our broker via our self.proto KafkaProtocol object
        Return a deferred which will fire when the reply matching the requestId
        comes back from the server, or, if expectResponse is False, then
        return None instead.
        If we are not currently connected, then we buffer the request to send
        when the connection comes back up.  Clients are responsible for
        implementing any timeouts
        """
        if requestId in self.requests:
            # Id is duplicate to 'in-flight' request. Reject it, as we
            # won't be able to properly deliver the response(s)
            # Note that this won't protect against a client calling us
            # twice with the same ID, but first with expectResponse=False
            # But that's pathological, and the only defense is to track
            # all requestIds sent regardless of whether we expect to see
            # a response, which is effectively a memory leak...
            raise DuplicateRequestError('Reuse of requestId:{}'.format(requestId))
        self.proto.sendString(request)
        if expectResponse:
            self.requests[requestId] = Deferred()
            return self.requests[requestId]

    def handleResponse(self, response):
        """
        Ok, we've received the response from the broker. Find the requestId
        in the message, lookup & fire the deferred
        """
        requestId = KafkaCodec.get_response_correlation_id(response)
        # Protect against responses coming back we didn't expect
        d = self.requests.get(requestId)
        if d is None:
            log.warning('Unexpected response:', requestId, response)
        else:
            # We don't expect another reply
            del self.requests[requestId]
            d.callback(response)

    def _getClock(self):
        # Reactor to use for connecting, callLater, etc [test]
        if self.clock is None:
            from twisted.internet import reactor
            self.clock = reactor
        return self.clock
