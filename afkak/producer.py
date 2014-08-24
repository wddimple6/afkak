from __future__ import absolute_import

import logging

from collections import namedtuple

from twisted.internet.defer import (
    Deferred, inlineCallbacks, returnValue,
    #       # returnValue, DeferredQueue, QueueUnderflow,
    )
from twisted.internet.task import LoopingCall

from .common import (
    ProduceRequest, UnsupportedCodecError, UnknownTopicOrPartitionError,
    check_error
)
from .partitioner import (RoundRobinPartitioner)
from .kafkacodec import CODEC_NONE, ALL_CODECS, create_message_set

log = logging.getLogger("afkak.producer")

BATCH_SEND_DEFAULT_INTERVAL = 30  # Seconds
BATCH_SEND_MSG_COUNT = 10  # Messages
BATCH_SEND_MSG_BYTES = 32 * 1024  # Bytes

SendRequest = namedtuple(
    "SendRequest", ["topic", "partition", "messages", "deferred"])


class Producer(object):
    """
    Params:
    client - The Kafka client instance to use
    partitioner_class - CLASS which will be used to instantiate partitioners
                 for topics, as needed. Constructor should take a topic and
                 list of partitions.
    req_acks - A value indicating the acknowledgements that the server must
               receive before responding to the request
    ack_timeout - Value (in milliseconds) indicating a how long the server
                  can wait for the above acknowledgements
    batch_send - If True, messages are sent in batches
    batch_every_n - If not None, messages are sent in batches of this many msgs
    batch_every_b - If not None, messages are sent when this many bytes of msgs
                    are waiting to be sent
    batch_every_t - If set, messages are sent after this timeout (secs)
    """

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed
    DEFAULT_ACK_TIMEOUT = 1000      # How long the server should wait (msec)

    def __init__(self, client,
                 partitioner_class=RoundRobinPartitioner,
                 req_acks=ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 batch_send=False,
                 batch_every_n=BATCH_SEND_MSG_COUNT,
                 batch_every_b=BATCH_SEND_MSG_BYTES,
                 batch_every_t=BATCH_SEND_DEFAULT_INTERVAL):

        # When messages are sent, the partition of the message is picked
        # by the partitioner object for that topic. The partitioners are
        # created as needed from the "partitioner_class" class and stored
        # by topic in self.partitioners
        self.partitioner_class = partitioner_class
        self.partitioners = {}

        # For efficiency, the producer can be set to send messages in
        # batches. In that case, the producer will wait until at least
        # batch_every_n messages are waiting to be sent, or batch_every_b
        # bytes of messages are waiting to be sent, or it has been
        # batch_every_t seconds since the last send
        self.batch_send = batch_send
        if batch_send:
            self._sendRequests = []
            self._waitingMsgCount = 0
            self.batchDesc = "{}cnt/{}secs".format(
                batch_every_n, batch_every_t)
            self.sendLooper = LoopingCall(self._sendWaiting)
            self.sendLooperD = self.sendLooper.start(
                batch_every_t, now=False)
            self.sendLooperD.addCallbacks(self._sendTimerStopped,
                                          self._sendTimerFailed)
        else:
            self.batchDesc = "Unbatched"
            batch_every_n = 1
            batch_every_t = None

        # Set our client, and our acks/timeout
        self.client = client
        self.req_acks = req_acks
        self.ack_timeout = ack_timeout

        # Are we compressing messages, or just sending 'raw'?
        if codec is None:
            codec = CODEC_NONE
        elif codec not in ALL_CODECS:
            raise UnsupportedCodecError("Codec 0x%02x unsupported" % codec)
        self.codec = codec

    def _sendTimerFailed(self, fail):
        """
        Our _sendWaiting() function called by the LoopingCall failed. Some
        error probably came back from Kafka and check_error() raised the
        exception
        For now, just log the failure and restart the loop
        """
        log.warning('_sendTimerFailed:%r: %s', fail, fail.getBriefTraceback())
        self.sendLooperD = self.sendLooper.start(
            self.batch_every_t, now=False)

    def _sendTimerStopped(self, lCall):
        """
        We're shutting down, clean up our looping call...
        """
        if self.sendLooper is not lCall:
            log.warning('commitTimerStopped with wrong timer:%s not:%s',
                        lCall, self.sendLooper)
        else:
            self.sendLooper = None
            self.sendLooperD = None

    @inlineCallbacks
    def _next_partition(self, topic, key=None):
        if topic not in self.client.topic_partitions:
            # client doesn't have partitions for topic. ask to fetch...
            yield self.client.load_metadata_for_topics(topic)
            # If we still don't have partitions for this topic, raise
            if topic not in self.client.topic_partitions:
                raise UnknownTopicOrPartitionError
        # if there is an error on the metadata for the topic, raise
        check_error(self.client.metadata_error_for_topic(topic))
        # Ok, should be safe to get the partitions now...
        partitions = self.client.topic_partitions[topic]
        # Do we have a partitioner for this topic already?
        if topic not in self.partitioners:
            # No, create a new paritioner for topic, partitions
            partitions = self.client.topic_partitions[topic]
            self.partitioners[topic] = \
                self.partitioner_class(topic, partitions)
        # Lookup the next partition
        returnValue(self.partitioners[topic].partition(key, partitions))

    def _sendWaiting(self):
        """
        Send the waiting messages, if there are any...
        """
        # We can be triggered by the LoopingCall, and have nothing to send...
        if not self._sendRequests:
            return
        #BUGBUG

    def __repr__(self):
        return '<Producer {}>'.format(self.batchDesc)

    @inlineCallbacks
    def send_messages(self, topic, key=None, msgs=[]):
        """
        send produce requests
        Two paths: batch or not.
        Non-batching: We just create a message set for the messages, a request
            with the topic, partition & that message set, and ask the client to
            send the request and return the deferred to the caller.
        Batching: We create a SendRequest with the topic, partition,
            messages, and a newly-created deferred. We store that in our
            sendRequests list and update the waitingMsgCount. Once enough msgs
            are waiting, or the timeout has elapsed, we send the messages.
            When the response comes back (assuming acks != 0), we correlate the
            topic/partition tuples with the SendRequest(s) and callback/errback
            the deferred(s) with the ProduceResponse for that topic/partition
        """
        if not msgs:
            raise ValueError("Empty messages list")
        if not self.batch_send:
            messages = create_message_set(msgs, self.codec)
            partition = yield self._next_partition(topic, key)
            req = ProduceRequest(topic, partition, messages)
            try:
                resp = yield self.client.send_produce_request(
                    [req], acks=self.req_acks, timeout=self.ack_timeout)
            except Exception:
                log.exception("Unable to send messages")
                raise
        else:
            d = Deferred()
            self._sendRequests.append(SendRequest(topic, partition, msgs, d))
            self._waitingMsgCount += len(msgs)
            for m in msgs:
                self._waitingByteCount += len(m)
            if (self._waitingMsgCount >= self.batch_every_n) or \
                    (self._waitingByteCount >= self.batch_every_b):
                self._sendWaiting()
            resp = yield d
        returnValue(resp)

    @inlineCallbacks
    def stop(self):
        """
        Cleanup our LoopingCall and any outstanding deferreds...
        """
        self.stopping = True
        if not self.batch_send:
            return

        # Stop our looping call, and wait for the deferred to be called
        if self.sendLooper is not None:
            self.sendLooper.stop()
        yield self.sendLooperD
        # Make sure there are no messages waiting to be sent.
        yield self._sendWaiting()
