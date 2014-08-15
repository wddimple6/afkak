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

from twisted.internet.error import ConnectionDone
from twisted.protocols.basic import Int32StringReceiver

log = logging.getLogger("afkak.protocol")


class KafkaProtocol(Int32StringReceiver):
    """
    Very thin wrapper around the Int32StringReceiver
    Simply knows to call its factory.handleResponse()
    method with the string received by stringReceived() and
    to cleanup the factory reference when the connection is lost
    """
    factory = None

    def stringReceived(self, string):
        self.factory.handleResponse(string)

    def connectionLost(self, reason=ConnectionDone):
        log.error("Lost Connection to Kafka Broker:%r", reason)
        self.factory = None
