# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2018 Ciena Corporation

from __future__ import absolute_import

import logging

from twisted.internet.protocol import connectionDone
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
    MAX_LENGTH = 2 ** 31 - 1  # Max a signed Int32 can represent

    def stringReceived(self, string):
        self.factory.handleResponse(string)

    def connectionLost(self, reason=connectionDone):
        self.factory._connectionLost(reason)
        self.factory = None

    def lengthLimitExceeded(self, length):
        log.error("Broker at %s sent a %d byte message, exceeding the size limit of %d. "
                  "Terminating connection.", self.transport.getPeer(), length,
                  self.MAX_LENGTH)
        self.transport.loseConnection()
