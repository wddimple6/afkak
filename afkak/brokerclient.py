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
from collections import OrderedDict
from functools import partial

from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import (
    Deferred, DeferredList, maybeDeferred,
)

from .protocol import KafkaProtocol
from .kafkacodec import KafkaCodec
from .common import (
    ClientError, DuplicateRequestError, DefaultKafkaPort,
    RequestTimedOutError,
)


log = logging.getLogger("afkak.brokerclient")

MAX_RECONNECT_DELAY_SECONDS = 15
CLIENT_ID = "afkak.kafkabrokerclient"


class _Request(object):
    """
    Object to encapsulate the data about the requests we are processing
    """
    from twisted.internet import reactor
    _reactor = reactor
    sent = False  # Have we written this request to our protocol?
    cancelled = False  # Has this request been cancelled?
    timeOut = None  # Time to wait 'till timeout
    timedOut = False  # Has this request timed out?
    timeoutCB = None  # Function to call if/when we timeout
    timeoutCall = None  # IDelayedCall used to handle the timeout

    def __init__(self, requestId, data, expectResponse,
                 timeout=None, timeoutCB=None):
        self.id = requestId
        self.data = data
        self.expect = expectResponse
        self.d = Deferred()
        if timeout is not None and timeoutCB is not None:
            self.timeout = timeout
            self.timeoutCB = timeoutCB
            self.timeoutCall = self._reactor.callLater(
                timeout, self.handleTimeout)

    def handleTimeout(self):
        self.timedOut = True
        self.timeoutCall = None
        self.timeoutCB()

    def cancelTimeout(self):
        if self.timeoutCall:
            tCall, self.timeoutCall = self.timeoutCall, None
            tCall.cancel()


class KafkaBrokerClient(ReconnectingClientFactory):

    # What class protocol instances do we produce?
    protocol = KafkaProtocol

    def __init__(self, host, port=DefaultKafkaPort,
                 clientId=CLIENT_ID, subscribers=None,
                 reactor=None, maxRetries=None,
                 timeout=None,
                 maxDelay=MAX_RECONNECT_DELAY_SECONDS):

        # Set the broker host & port
        self.host = host
        self.port = port
        # Set our clientId
        self.clientId = clientId
        # clock/reactor for testing...
        if reactor is not None:
            _Request._reactor = reactor
            self.clock = reactor
        # If the caller set maxRetries, we will retry that many
        # times to reconnect, otherwise we retry forever
        self.maxRetries = maxRetries
        # Set max delay between reconnect attempts
        self.maxDelay = maxDelay
        # How long do we wait for responses to requests
        self.timeout = timeout
        # The protocol object for the current connection
        self.proto = None
        # ordered dict of _Requests, keyed by requestId
        self.requests = OrderedDict()
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
            self.connector = self._getClock().connectTCP(
                self.host, self.port, self)
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
        in self.proto
        """
        # Schedule notification of subscribers
        self._getClock().callLater(0, self.notify, True)
        # Build the protocol
        self.proto = ReconnectingClientFactory.buildProtocol(self, addr)
        # point it at us for notifications of arrival of messages
        self.proto.factory = self
        return self.proto

    def clientConnectionLost(self, connector, reason):
        """
        Handle notification from the lower layers that the connection
        was closed/dropped
        """
        # Reset our proto so we don't try to send to a down connection
        self.proto = None
        # Schedule notification of subscribers
        self._getClock().callLater(0, self.notify, False, reason)
        # Call our superclass's method to handle reconnecting
        ReconnectingClientFactory.clientConnectionLost(
            self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        """
        Handle notification from the lower layers that the connection failed
        """
        # Reset our proto so we don't try to send to a down connection
        # Needed?  I'm not sure we should even _have_ a proto at this point...
        self.proto = None

        # errback() the deferred returned from the connect() call
        if self.dUp and not self.dUp.called:
            dUp, self.dUp = self.dUp, None
            dUp.errback(reason)

        # Schedule notification of subscribers
        self._getClock().callLater(0, self.notify, False, reason)
        # Call our superclass's method to handle reconnecting
        return ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason)

    def notify(self, connected, reason=None, subs=None):
        # fire the proper deferred, if there is one
        if connected:
            if self.dUp and not self.dUp.called:
                dUp, self.dUp = self.dUp, None
                dUp.callback(reason)
            self.sendQueued()
        else:
            if self.dDown and not self.dDown.called:
                dDown, self.dDown = self.dDown, None
                dDown.callback(reason)
            # If the connection just went down, we need to handle any
            # outstanding requests.
            self.handlePending(reason)

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
                lambda _: self.notify(connected, reason=reason, subs=subs))
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

        # Add clearing of self.onConnectDList to the deferredList so that once
        # it fires, it is reset to None
        def clearNotifydList(_):
            self.notifydList = None

        self.notifydList.addCallback(clearNotifydList)

    def makeRequest(self, requestId, request, expectResponse=True,
                    timeout=None):
        """
        Send a request to our broker via our self.proto KafkaProtocol object
        Return a deferred which will fire when the reply matching the requestId
        comes back from the server, or, if expectResponse is False, then
        return None instead.
        If we are not currently connected, then we buffer the request to send
        when the connection comes back up.
        If the timeout expires before the request could be sent, or a response
        is received, the request is failed with a RequestTimedOutError.
        If timeout is None, the KafkaBrokerClient's timeout is used
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

        # Use brokerclient-default timeout if not set
        if timeout is None:
            timeout = self.timeout

        # Ok, we are going to save/send it, create a _Request object to track
        tReq = _Request(
            requestId, request, expectResponse, timeout,
            partial(
                self.cancelRequest, requestId, RequestTimedOutError(
                    "Request:{} timed out".format(requestId))
                ),
        )
        # add it to our requests dict
        self.requests[requestId] = tReq

        # Do we have a connection over which to send the request?
        if self.proto:
            # Send the request
            self.sendRequest(tReq)
        return tReq.d

    def sendRequest(self, tReq):
        """
        Send a single request
        """
        self.proto.sendString(tReq.data)
        tReq.sent = True
        if not tReq.expect:
            # Once we've sent a request for which we don't expect a reply,
            # we're done, remove it from requests, cancel the timeout and
            # fire the deferred with 'None', since there is no reply
            del self.requests[tReq.id]
            tReq.cancelTimeout()
            tReq.d.callback(None)

    def sendQueued(self):
        """
        Connection just came up, send the unsent requests
        """
        for tReq in self.requests.itervalues():
            if not tReq.sent:
                self.sendRequest(tReq)

    def cancelRequest(self, requestId, reason=None):
        """
        Cancel a request. Removes it from requests, errbacks the deferred
        """
        tReq = self.requests.pop(requestId)
        tReq.cancelled = True
        # We don't want the timeout timer going off...
        tReq.cancelTimeout()
        # If there's no 'reason', it's not an err-type cancellation, we
        # avoid the errback() call...
        if reason is not None:
            tReq.d.errback(reason)

    def handleResponse(self, response):
        """
        Ok, we've received the response from the broker. Find the requestId
        in the message, lookup & fire the deferred
        """
        requestId = KafkaCodec.get_response_correlation_id(response)
        # Protect against responses coming back we didn't expect
        tReq = self.requests.pop(requestId, None)
        if tReq is None:
            # This could happen if we've sent it, are waiting on the response
            # when it's cancelled, causing us to remove it from self.requests
            log.warning('Unexpected response:', requestId, response)
        else:
            tReq.cancelTimeout()
            tReq.d.callback(response)

    def handlePending(self, reason):
        """
        Connection went down, handle in-flight & unsent as configured
        Note: for now, we just 'requeue' all the in-flight by setting their
          'sent' variable to False and let 'sendQueued()' handle resending
          when the connection comes back.
          In the future, we may want to extend this so we can errback()
          to our client's any in-flight (and possibly queued) so they can deal
          with it at the application level.
        """
        for tReq in self.requests.itervalues():
            tReq.sent = False

    def _getClock(self):
        # Reactor to use for connecting, callLater, etc [test]
        if self.clock is None:
            from twisted.internet import reactor
            self.clock = reactor
        return self.clock
