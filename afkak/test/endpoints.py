# -*- coding: utf-8 -*-
# Copyright 2018 Ciena Corporation
"""
Endpoints for testing
"""
from __future__ import absolute_import, division

from functools import partial

import attr
from twisted.internet import defer
from twisted.internet.interfaces import IStreamClientEndpoint
from zope.interface import implementer


@implementer(IStreamClientEndpoint)
@attr.s(frozen=True)
class FailureEndpoint(object):
    """
    Immediately fail every connection attempt.
    """
    reactor = attr.ib()
    host = attr.ib()
    port = attr.ib()

    def connect(self, protocolFactory):
        e = IndentationError('failing to connect {}'.format(protocolFactory))
        return defer.fail(e)


@implementer(IStreamClientEndpoint)
@attr.s(frozen=True)
class BlackholeEndpoint(object):
    """
    Black-hole every connection attempt by returning a deferred which never
    fires.
    """
    reactor = attr.ib()
    host = attr.ib()
    port = attr.ib()

    def connect(self, protocolFactory):
        return defer.Deferred()


class Connections(object):
    """Externally controllable endpoint factory

    Each time `Connections` is called to generate an endpoint it returns one
    which records each connection attempt
    """
    calls = attr.ib(init=False, default=attr.Factory(list))
    connects = attr.ib(init=False, default=attr.Factory(list))

    def __call__(self, reactor, host, port):
        """

        """
        self.calls.append((host, port))
        return _PuppetEndpoint(partial(self._connect, host, port))

    def _connect(self, host, port, protocolFactory):
        d = defer.Deferred()
        self.connects.append((host, port, protocolFactory))
        return d


@implementer(IStreamClientEndpoint)
@attr.s(frozen=True)
class _PuppetEndpoint(object):
    """
    Implementation detail of `Connections`.
    """
    connect = attr.ib(repr=False)
