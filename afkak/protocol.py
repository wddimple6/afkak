# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

from __future__ import absolute_import

import logging

from twisted.internet.error import ConnectionDone
from twisted.protocols.basic import Int32StringReceiver

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class KafkaProtocol(Int32StringReceiver):
    """
    Very thin wrapper around the Int32StringReceiver
    Simply knows to call its factory.handleResponse()
    method with the string received by stringReceived() and
    to cleanup the factory reference when the connection is lost
    """
    factory = None
    closing = False  # set by factory so we know to expect connectionLost
    MAX_LENGTH = 2 ** 31 - 1  # Max a signed Int32 can represent

    def stringReceived(self, string):
        self.factory.handleResponse(string)

    def connectionLost(self, reason=None):
        # If we are closing, or if the connection was cleanly closed (as
        # Kafka brokers will do after 10 minutes of idle connection) we log
        # only at debug level. Other connection close reasons when not
        # shutting down will cause a warning log.
        if self.closing or reason is None or reason.check(ConnectionDone):
            log.debug("Connection to Kafka Broker closed: %r Closing: %r",
                      reason, self.closing)
        else:
            log.warning("Lost Connection to Kafka Broker: %r", reason)

        self.factory = None

    def lengthLimitExceeded(self, length):
        log.error("Remote Peer (%r) sent a %d byte message. Max allowed: %d.  "
                  "Terminating connection.", self.transport.getPeer(), length,
                  self.MAX_LENGTH)
        self.transport.loseConnection()
