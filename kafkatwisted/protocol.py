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

from twisted.protocols.basic import Int32StringReceiver

log = logging.getLogger("kafkaclient")

class KafkaProtocol(Int32StringReceiver):

    def stringReceived(self, string):
        self.factory.handleResponse(string)

