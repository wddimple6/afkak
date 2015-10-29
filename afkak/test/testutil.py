# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import functools
import logging
import os
import random
import socket
import string
import time
import unittest2
import uuid

from nose.twistedtools import deferred

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from afkak import KafkaClient
from afkak.common import (OffsetRequest, SendRequest)

log = logging.getLogger(__name__)

__all__ = [
    'async_delay',
    'random_string',
    'get_open_port',
    'make_send_requests',
    'kafka_versions',
    'KafkaIntegrationTestCase',
]


# This must only be called from the reactor thread (that is, something
# decorated with @nose.twistedtools.deferred)
def async_delay(timeout=0.01, clock=None):
    if clock is None:
        from twisted.internet import reactor as clock

    timeout = timeout

    def succeed():
        d.callback(timeout)

    d = Deferred()
    clock.callLater(timeout, succeed)
    return d


def random_string(l):
    # Random.choice can be very slow for large amounts of data, so 'cheat'
    if l <= 50:
        s = "".join(random.choice(string.letters) for i in xrange(l))
    else:
        r = random_string(50)
        s = "".join(r for i in xrange(l / 50))
        if l % 50:
            s += r[0:(l % 50)]
    assert len(s) == l
    return s


def make_send_requests(msgs, topic=None, key=None):
    return [SendRequest(topic, key, msgs, None)]


def kafka_versions(*versions):
    def kafka_versions(func):
        @functools.wraps(func)
        def wrapper(self):
            kafka_version = os.environ.get('KAFKA_VERSION')

            if not kafka_version:
                self.skipTest("no kafka version specified")  # pragma: no cover
            elif 'all' not in versions and kafka_version not in versions:
                self.skipTest("unsupported kafka version")  # pragma: no cover

            return func(self)
        return wrapper
    return kafka_versions


@inlineCallbacks
def ensure_topic_creation(client, topic_name, timeout=5, reactor=None):
    '''
    With the default Kafka configuration, just querying for the metatdata
    for a particular topic will auto-create that topic.
    NOTE: This must only be called from the reactor thread (that is, something
    decorated with @nose.twistedtools.deferred)
    '''
    start_time = time.time()
    yield client.load_metadata_for_topics(topic_name)
    while not client.has_metadata_for_topic(topic_name):
        log.debug('Still waiting for metadata for topic: %s', topic_name)
        yield async_delay(clock=reactor)
        if time.time() > start_time + timeout:
            raise Exception(
                "Unable to create topic %s" % topic_name)  # pragma: no cover
        yield client.load_metadata_for_topics(topic_name)
    log.debug('Got metadata for topic: %s', topic_name)


def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class KafkaIntegrationTestCase(unittest2.TestCase):
    create_client = True
    topic = None

    @deferred(timeout=10)
    @inlineCallbacks
    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.error('KAFKA_VERSION unset!')
            return

        if not self.topic:
            self.topic = "%s-%s" % (
                self.id()[self.id().rindex(".") + 1:], random_string(10))

        if self.create_client:
            self.client = KafkaClient(
                '%s:%d' % (self.server.host, self.server.port),
                clientId=self.topic)

        yield ensure_topic_creation(self.client, self.topic,
                                    reactor=self.reactor)

        self._messages = {}

    @deferred(timeout=10)
    @inlineCallbacks
    def tearDown(self):
        super(KafkaIntegrationTestCase, self).tearDown()
        if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
            log.error('KAFKA_VERSION unset!')
            return

        if self.create_client:
            yield self.client.close()
            # Check for outstanding delayedCalls. Note, this may yield
            # spurious errors if the class's client has an outstanding
            # delayed call due to reconnecting.
            dcs = self.reactor.getDelayedCalls()
            if dcs:  # pragma: no cover
                log.error("Outstanding Delayed Calls at tearDown: %s\n\n",
                          ' '.join([str(dc) for dc in dcs]))
            self.assertFalse(dcs)

    @inlineCallbacks
    def current_offset(self, topic, partition):
        offsets, = yield self.client.send_offset_request(
            [OffsetRequest(topic, partition, -1, 1)])
        returnValue(offsets.offsets[0])

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s]
