# -*- coding: utf-8 -*-
# Copyright (C) 2006-2013 Cyan, Inc.
#
# PROPRIETARY NOTICE
# This Software consists of confidential information.  Trade secret law and
# copyright law protect this Software.  The above notice of copyright on this
# Software does not indicate any actual or intended publication of such
# Software.

from __future__ import absolute_import

from collections import deque

from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredList, maybeDeferred

log = logging.getLogger("kafka")

DEFAULT_KAFKA_TIMEOUT_SECONDS = 30
MAX_RECONNECT_DELAY_SECONDS = 30
DEFAULT_KAFKA_PORT = 9092
CLIENT_ID = "kafka-twisted"

class KafkaBrokerClient(ReconnectingClientFactory):

    # What class protocol instances do we produce?
    protocol = KafkaProtocol

    def __init__(self, client_id=CLIENT_ID, subscribers=[],
                 timeout=DEFAULT_KAFKA_TIMEOUT_SECONDS,
                 maxDelay=MAX_RECONNECT_DELAY_SECONDS,
                 maxRetries=None):

        # Set our client id
        self.client_id = client_id
        # If the caller set maxRetries, we will retry that many times to
        # reconnect, otherwise we retry forever
        self.maxRetries = maxRetries
        # Set max delay between reconnect attempts
        self.maxDelay = maxDelay
        # Set our kafka timeout (not network related!)
        self.timeout = timeout
        # The protocol object for the current connection
        self.proto = None
        # The current connector object
        self.connector = None
        # queue of requests to send when the connection is up
        self.requestQueue = deque()
        # deferreds which fires when the connect()/disconnect() completes
        self.dUp = None
        self.dDown = None
        # list of subscribers to our connection status:
        # when the connection goes up or down, we call the callback
        # with ourself and True/False for Connection-Up/Down
        self.connSubscribers = subscribers

    def __repr__(self):
        return '<KafkaBrokerClient {0}:{1}'.format(self.host, self.port)

    def addSubscriber(self, cb):
        self.connSubscribers.append(cb)

    def delSubscriber(self, cb):
        if cb in self.connSubscribers:
            self.connSubscribers.remove(cb)

    def connect(self):
        if self.proto.connected:
            raise RuntimeError('connect called but already connected')
        # Needed to enable retries after a disconnect
        self.resetDelay()
        self.connector = reactor.connectTCP(self.host, self.port, self)
        self.dUp = Deferred()
        return self.dUp

    def disconnect(self):
        if not self.connector:
            raise RuntimeError('disconnect called but not connected')
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
        self.proto = ReconnectingClientFactory.buildProtocol(self, addr)

        # Let our subscribers know we're up
        reactor.callLater(0, self.notify, True)

        return self.proto

    def notify(self, connected):
        # fire the deferred
        if not self.d.called:
            self.dUp.callback(self)

        # Notify the user if requested. We call all of the callbacks, but don't
        # wait for any deferreds to fire here. Instead we add them to a deferred
        # list which we check for and wait on before calling any onDisconnectCBs
        # (should the connection subsequently disconnect).
        # This should keep any state-changes done by these callbacks in the
        # proper order. Note however that the ordering of the individual
        # callbacks in each (connect/disconnect) list isn't guaranteed, and they
        # can all be progressing in parallel if they yield or otherwise deal
        # with deferreds
        dList = []
        for cb in self.onConnectCbs:
            dList.append(maybeDeferred(cb, self))
        self.onConnectDList = DeferredList(dList)

        # Add clearing of self.onConnectDList to the deferredList so that once
        # it fires, it is reset to None
        def clearOnConnectDList(_):
            self.onConnectDList = None

        d.addCallback(clearOnConnectDList)


