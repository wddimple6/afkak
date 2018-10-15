# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.
# Copyright 2016, 2017, 2018 Ciena Corporation

from __future__ import print_function

import functools
import logging
import os
import random
import socket
import string
import sys
import time
import unittest
import uuid
from pprint import pformat

from nose.twistedtools import deferred
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue

from .. import KafkaClient
from ..common import (OffsetRequest, PartitionUnavailableError,
                      RetriableBrokerResponseError, SendRequest,
                      TopicAndPartition)

log = logging.getLogger(__name__)

__all__ = [
    'async_delay',
    'random_string',
    'get_open_port',
    'make_send_requests',
    'kafka_versions',
    'KafkaIntegrationTestCase',
]


def stat(key, value):
    print("##teamcity[buildStatisticValue key='{}' value='{}']".format(
        key, value), file=sys.stderr)


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


def random_string(length):
    # Random.choice can be very slow for large amounts of data, so 'cheat'
    if length <= 50:
        s = "".join(random.choice(string.ascii_letters) for _i in range(length))
    else:
        r = random_string(50)
        s = "".join(r for i in range(length // 50))
        if length % 50:
            s += r[0:(length % 50)]
    assert len(s) == length
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
def ensure_topic_creation(client, topic_name, fully_replicated=True, timeout=5):
    '''
    With the default Kafka configuration, just querying for the metadata
    for a particular topic will auto-create that topic.

    :param client: `afkak.client.KafkaClient` instance

    :param str topic_name: Topic name

    :param bool fully_replicated:
        If ``True``, check whether all partitions for the topic have been
        assigned brokers. This doesn't ensure that producing to the topic will
        succeed, though—there is a window after the partition is assigned
        before the broker can actually accept writes. In this case the broker
        will respond with a retriable error (see
        `KafkaIntegrationTestCase.retry_broker_errors()`).

        If ``False``, only check that any metadata exists for the topic.

    :param timeout: Number of seconds to wait.

    .. note::

        This must only be called from the reactor thread (that is,
        something decorated with ``@nose.twistedtools.deferred``).
    '''
    start_time = time.time()
    if fully_replicated:
        check_func = client.topic_fully_replicated
    else:
        check_func = client.has_metadata_for_topic
    yield client.load_metadata_for_topics(topic_name)

    def topic_info():
        if topic_name in client.topic_partitions:
            return "Topic {} exists. Partition metadata: {}".format(
                topic_name, pformat([client.partition_meta[TopicAndPartition(topic_name, part)]
                                     for part in client.topic_partitions[topic_name]]),
            )
        else:
            return "No metadata for topic {} found.".format(topic_name)

    while not check_func(topic_name):
        yield async_delay(clock=client.reactor)
        if time.time() > start_time + timeout:
            raise Exception((
                "Timed out waiting topic {} creation after {} seconds. {}"
            ).format(topic_name, timeout, topic_info()))
        else:
            log.debug('Still waiting topic creation: %s.', topic_info())
        yield client.load_metadata_for_topics(topic_name)
    log.info('%s', topic_info())


def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class KafkaIntegrationTestCase(unittest.TestCase):
    """
    :ivar bool create_client:
        A flag which may be set to ``False`` in a subclass of
        `KafkaIntegrationTestCase` which wishes to create its own `KafkaClient`
        instance.

    :ivar str topic:
        Kafka topic name. This may be set in subclasses. If ``None``, a random
        topic name is generated by `setUp()`.

    :ivar harness:
        `afkak.test.fixtures.KafkaHarness` instance. This must be created by
        the setUpClass method of a `KafkaIntegrationTestCase` subclass.

    :ivar reactor:
        Twisted reactor. This must be created by the setUpClass method of
        `KafkaIntegrationTestCase` subclasses.
    """
    create_client = True
    topic = None
    reactor = None

    if not os.environ.get('KAFKA_VERSION'):  # pragma: no cover
        skip = 'KAFKA_VERSION is not set'

    def shortDescription(self):
        """
        Show the ID of the test when nose displays its name, rather than
        a snippet of the docstring.
        """
        return self.id()

    @classmethod
    def assertNoDelayedCalls(cls):
        """
        Check for outstanding delayed calls in the reactor.
        """
        dcs = cls.reactor.getDelayedCalls()
        assert len(dcs) == 0, "Found {} outstanding delayed calls:\n{}".format(
            len(dcs),
            '\n'.join(str(dc) for dc in dcs),
        )

    @deferred(timeout=10)
    @inlineCallbacks
    def setUp(self):
        log.info("Setting up test %s", self.id())
        super(KafkaIntegrationTestCase, self).setUp()

        if not self.topic:
            self.topic = "%s-%s" % (
                self.id()[self.id().rindex(".") + 1:], random_string(10))

        if self.create_client:
            self.client = KafkaClient(self.harness.bootstrap_hosts, clientId=self.topic)

        yield ensure_topic_creation(self.client, self.topic,
                                    fully_replicated=True)

        self._messages = {}

    @deferred(timeout=10)
    @inlineCallbacks
    def tearDown(self):
        log.info("Tearing down test: %r", self)
        super(KafkaIntegrationTestCase, self).tearDown()

        if self.create_client:
            yield self.client.close()

        self.assertNoDelayedCalls()

    @inlineCallbacks
    def current_offset(self, topic, partition):
        offsets, = yield self.client.send_offset_request(
            [OffsetRequest(topic, partition, -1, 1)])
        returnValue(offsets.offsets[0])

    @inlineCallbacks
    def retry_while_broker_errors(self, f, *a, **kw):
        """
        Call a function, retrying on retriable broker errors.

        If calling the function fails with one of these exception types it is
        called again after a short delay:

        * `afkak.common.RetriableBrokerResponseError` (or a subclass thereof)
        * `afkak.common.PartitionUnavailableError`

        The net effect is to keep trying until topic auto-creation completes.

        :param f: callable, which may return a `Deferred`
        :param a: arbitrary positional arguments
        :param kw: arbitrary keyword arguments
        """
        while True:
            try:
                returnValue((yield f(*a, **kw)))
                break
            except (RetriableBrokerResponseError, PartitionUnavailableError):
                yield async_delay(0.1, clock=self.reactor)

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = (u'%s-%s-%s' % (s, self.id(), uuid.uuid4())).encode('utf-8')

        return self._messages[s]
