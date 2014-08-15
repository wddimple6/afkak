import functools
import logging
import os
import random
import socket
import string
import time
import unittest2
import uuid

import nose.twistedtools
from nose.twistedtools import deferred

from twisted.internet.defer import inlineCallbacks

from afkak import KafkaClient
from afkak.common import OffsetRequest

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('afkak.test.testutil')

__all__ = [
    'random_string',
    'ensure_topic_creation',
    'get_open_port',
    'kafka_versions',
    'KafkaIntegrationTestCase',
    'Timer',
]


def random_string(l):
    s = "".join(random.choice(string.letters) for i in xrange(l))
    return s


def kafka_versions(*versions):
    def kafka_versions(func):
        @functools.wraps(func)
        def wrapper(self):
            kafka_version = os.environ.get('KAFKA_VERSION')

            if not kafka_version:
                self.skipTest("no kafka version specified")
            elif 'all' not in versions and kafka_version not in versions:
                self.skipTest("unsupported kafka version")

            return func(self)
        return wrapper
    return kafka_versions


@deferred(timeout=5.0)
@inlineCallbacks
def ensure_topic_creation(client, topic_name, timeout=30):
    start_time = time.time()
    log.debug('ZORG: Ensuring topic creation')
    yield client.load_metadata_for_topics(topic_name)
    while not client.has_metadata_for_topic(topic_name):
        if time.time() > start_time + timeout:
            raise Exception("Unable to create topic %s" % topic_name)
        yield client.load_metadata_for_topics(topic_name)
        log.debug(
            'ZORG: Still no metadata for topic:%s. Sleeping 1', topic_name)
        time.sleep(1)


def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class KafkaIntegrationTestCase(unittest2.TestCase):
    create_client = True
    topic = None

    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        if not os.environ.get('KAFKA_VERSION'):
            log.debug('KAFKA_VERSION unset!')
            return

        if not self.topic:
            self.topic = "%s-%s" % (
                self.id()[self.id().rindex(".") + 1:], random_string(10))

        # Startup the twisted reactor in a thread. We need this before the
        # the KafkaClient can work, since KafkaBrokerClient relies on the
        # reactor for its TCP connection
        nose.twistedtools.threaded_reactor()

        if self.create_client:
            self.client = KafkaClient(
                '%s:%d' % (self.server.host, self.server.port))

        ensure_topic_creation(self.client, self.topic)

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationTestCase, self).tearDown()
        if not os.environ.get('KAFKA_VERSION'):
            log.debug('KAFKA_VERSION unset!')
            return

        if self.create_client:
            self.client.close()

        # Shutdown the twisted reactor
        nose.twistedtools.stop_reactor()

    def current_offset(self, topic, partition):
        offsets, = self.client.send_offset_request(
            [OffsetRequest(topic, partition, -1, 1)])
        return offsets.offsets[0]

    def msgs(self, iterable):
        return [self.msg(x) for x in iterable]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s]


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
