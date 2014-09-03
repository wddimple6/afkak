from __future__ import absolute_import

from itertools import izip_longest, repeat
import logging
import numbers

from twisted.python.failure import Failure
from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredQueue, QueueUnderflow,
    )

from afkak.common import (
    check_error,
    ConsumerFetchSizeTooSmall, ConsumerNotReady,
    FetchRequest,
    OffsetRequest, OffsetCommitRequest,
    OffsetFetchRequest, FailedPayloadsError,
    UnknownTopicOrPartitionError,
)

log = logging.getLogger("afkak.consumer")

# How often we auto-commit (msgs, millisecs)
AUTO_COMMIT_MSG_COUNT = 100
AUTO_COMMIT_INTERVAL = 5000

FETCH_MIN_BYTES = 16 * 1024  # server waits for min. 16K bytes of messages
FETCH_MAX_WAIT_TIME = 100  # server waits 100 millisecs for messages
FETCH_BUFFER_SIZE_BYTES = 128 * 1024  # Our initial fetch buffer size
MAX_FETCH_BUFFER_SIZE_BYTES = 1024 * 1024  # Max the buffer can double to
QUEUE_LOW_WATERMARK = 64  # refetch when our internal queue gets below this
QUEUE_MAX_BACKLOG = 128  # Max outstanding message requests we will allow


class Consumer(object):
    """
    A simple consumer implementation that consumes all/specified partitions
    for a topic

    client: a connected KafkaClient
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume
    partitions: An optional list of partitions to consume the data from

    auto_commit: default True. Whether or not to auto commit the offsets
    auto_commit_every_n: default 100. How many messages to consume
                         before a commit
    auto_commit_every_t: default 5000. How much time (in milliseconds) to
                         wait before commit
    fetch_size_bytes:    number of bytes to request in a FetchRequest
    fetch_max_wait_time: max msecs the server should wait for that many bytes
    buffer_size:         default 128K. Initial number of bytes to tell kafka we
                         have available. This will double as needed up to...
    max_buffer_size:     default 4M. Max number of bytes to tell kafka we have
                         available. None means no limit. Must be larger than
                         the largest message we will find in our
                         topic/partitions
    queue_low_watermark  default 64. When the number of messages in the
                         consumer's internal queue is fewer than this, it will
                         initiate another fetch of more messages
    queue_max_backlog    default 128. Max number of unfullfilled get_message()
                         requests we will allow. Throw QueueUnderflow if more

    Auto commit details:
    If both auto_commit_every_n and auto_commit_every_t are set, they will
    reset one another when one is triggered. These triggers simply call the
    commit method on this class. A manual call to commit will also reset
    these triggers
    """
    def __init__(self, client, group, topic, auto_commit=True, partitions=None,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL,
                 fetch_size_bytes=FETCH_MIN_BYTES,
                 fetch_max_wait_time=FETCH_MAX_WAIT_TIME,
                 buffer_size=FETCH_BUFFER_SIZE_BYTES,
                 max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES,
                 queue_low_watermark=QUEUE_LOW_WATERMARK,
                 queue_max_backlog=QUEUE_MAX_BACKLOG):

        self._clock = None  # Settable reactor for testing
        self.client = client  # KafkaClient
        self.group = group  # Name of consumer group we may be a part of
        self.topic = topic  # The topic from which we consume
        self.stopping = False  # We're not shutting down yet...
        self.only_prefetched = False  # Should we raise StopIteration?
        # For tracking various async operations
        self.offsetFetchD = None  # fetching our offsets w/offset_fetch_request
        self._fetchD = None  # fetching messages
        self._autoCommitD = None  # performing an auto-commit

        # Variables for handling offset commits
        self.commit_looper = None  # LoopingCall handles auto_commit_every_t
        self.commit_looper_d = None  # deferred for the active call
        self.count_since_commit = 0
        self.auto_commit = auto_commit
        self.auto_commit_every_n = auto_commit_every_n
        self.auto_commit_every_t = auto_commit_every_t

        if max_buffer_size is not None and buffer_size > max_buffer_size:
            raise ValueError("buffer_size (%d) is greater than "
                             "max_buffer_size (%d)" %
                             (buffer_size, max_buffer_size))

        # Fetch related instance variables
        self.buffer_size = buffer_size
        self.max_buffer_size = max_buffer_size
        self.fetch_max_wait_time = int(fetch_max_wait_time)
        self.fetch_min_bytes = fetch_size_bytes
        self.fetch_offsets = {}  # fetch positions
        self.offsets = {}  # commit positions
        self.queue_low_watermark = queue_low_watermark
        self.queue = DeferredQueue(backlog=queue_max_backlog)

        # Set up the auto-commit timer
        if auto_commit is True and auto_commit_every_t is not None:
            self.auto_commit_every_t /= 1000.0  # msecs to secs
            self.commit_looper = LoopingCall(self.commit)
            self.commit_looper_d = self.commit_looper.start(
                (self.auto_commit_every_t), now=False)
            self.commit_looper_d.addCallbacks(self._commitTimerStopped,
                                              self._commitTimerFailed)

        # If the caller didn't supply a list of partitions, get all the
        # partitions of the topic from our client
        if not partitions:
            # Does our client have metadata for our topic? If not, then
            # ask it to load it...
            if self.client.has_metadata_for_topic(topic):
                partitions = self.client.topic_partitions[topic]
                self._setupPartitionOffsets(partitions)
            else:
                self.offsetFetchD = client.load_metadata_for_topics(topic)
                self.offsetFetchD.addCallbacks(
                    self._handleClientLoadMetadata,
                    self._handleClientLoadMetadataError)
        else:
            assert all(isinstance(x, numbers.Integral) for x in partitions)
            self._setupPartitionOffsets(partitions)

    def _getClock(self):
        # Reactor to use for callLater
        if self._clock is None:
            from twisted.internet import reactor
            self._clock = reactor
        return self._clock

    def _handleClientLoadMetadata(self, _):
        # Our client didn't have metadata for our topic when we were created
        # so we initiated an async request for the client to load its metadata
        self.offsetFetchD = None
        partitions = self.client.topic_partitions[self.topic]
        self._setupPartitionOffsets(partitions)
        return _

    def _handleClientLoadMetadataError(self, failure):
        # client can't load metadata, we're stuck...
        log.error("Client failed to load metadata:%r", failure)
        self.offsetFetchD = None
        return failure

    def _setupPartitionOffsets(self, partitions):
        # If we are auto_commiting, we need to pre-populate our offsets...
        # fetch them here. Otherwise, assume 0
        if self.auto_commit:
            payloads = []
            for partition in partitions:
                payloads.append(OffsetFetchRequest(self.topic, partition))
            if payloads:
                self.offsetFetchD = self.client.send_offset_fetch_request(
                    self.group, payloads, fail_on_error=False)
                self.offsetFetchD.addCallback(self._setupFetchOffsets)
            else:
                log.warning('setupPartitionOffsets got empty partition list.')
        else:
            for partition in partitions:
                self.offsets[partition] = 0
            self.fetch_offsets = self.offsets.copy()

    def _setupFetchOffsets(self, resps):
        self.offsetFetchD = None
        for resp in resps:
            try:
                check_error(resp)
                self.offsets[resp.partition] = resp.offset
            except UnknownTopicOrPartitionError:
                self.offsets[resp.partition] = 0
        self.fetch_offsets = self.offsets.copy()

    def __repr__(self):
        return '<afkak.Consumer group=%s, topic=%s, partitions=%s>' % \
            (self.group, self.topic, str(self.offsets.keys()))

    def _commitTimerFailed(self, fail):
        """
        Our commit() function called by the LoopingCall failed. Some error
        probably came back from Kafka and check_error() raised the exception
        For now, just log the failure and restart the loop
        """
        log.warning('commitTimerFailed:%r: %s', fail, fail.getBriefTraceback())
        self.commit_looper_d = self.commit_looper.start(
            self.auto_commit_every_t, now=False)

    def _commitTimerStopped(self, lCall):
        """
        We're shutting down, clean up our looping call...
        """
        if self.commit_looper is not lCall:
            log.warning('commitTimerStopped with wrong timer:%s not:%s',
                        lCall, self.commit_looper)
        else:
            self.commit_looper = None
            self.commit_looper_d = None

    def _check_fetch_msgs(self):
        """
        If we don't have enough messages, and we don't have an outstanding
        request for more right now, then start a new request
        """
        # If our queue pending is down to the low_watermark, and we
        # don't have an outstanding fetch request, start one now
        if not self.stopping and \
                (len(self.queue.pending) <= self.queue_low_watermark) \
                and self._fetchD is None:
            log.debug('Initiating new fetch:%d <= %d', len(self.queue.pending),
                      self.queue_low_watermark)
            self._fetchD = self.fetch()

    def waitForReady(self):
        return self.offsetFetchD

    @inlineCallbacks
    def commit(self, partitions=None):
        """
        Commit offsets for this consumer

        partitions: list of partitions to commit, default is to commit
                    all of them
        """

        # short circuit if nothing happened.
        if self.count_since_commit == 0:
            return

        # Have we completed our async-initialization?
        yield self.waitForReady()
        reqs = []
        if not partitions:  # commit all partitions
            partitions = self.offsets.keys()

        for partition in partitions:
            offset = self.offsets[partition]
            log.debug("Commit offset %d in SimpleConsumer: "
                      "group=%s, topic=%s, partition=%s" %
                      (offset, self.group, self.topic, partition))
            reqs.append(OffsetCommitRequest(self.topic, partition,
                                            offset, None))
        # If we don't have any requests we're done
        if not reqs:
            return
        # We want to start counting from now, not from when we get the response
        self.count_since_commit = 0
        resps = yield self.client.send_offset_commit_request(self.group, reqs)

        for resp in resps:
            check_error(resp)

        if self.commit_looper is not None:
            self.commit_looper.reset()

        # return the responses
        returnValue(resps)

    def _auto_commit(self):
        """
        Check if we have to commit based on number of messages and commit
        """
        def clearAutoCommitD(_):
            # Helper to cleanup deferred when it fires
            self._autoCommitD = None
            return _

        # Check if we are supposed to do an auto-commit
        if not self.auto_commit or self.auto_commit_every_n is None:
            return

        if self.count_since_commit >= self.auto_commit_every_n:
            if not self._autoCommitD:
                self._autoCommitD = self.commit()
                self._autoCommitD.addBoth(clearAutoCommitD)

    @inlineCallbacks
    def stop(self):
        self.stopping = True
        # Are we just starting, and waiting for our offsets to come back?
        yield self.waitForReady()
        # Do we have a looping call to handle autocommits by time?
        if self.commit_looper is not None:
            self.commit_looper.stop()
        yield self.commit_looper_d
        # Are we waiting for a fetch request to come back?
        if self._fetchD:
            try:
                yield self._fetchD
            except FailedPayloadsError as e:
                log.debug("Background fetch failed during stop:%r", e)
        # Are we waiting for an auto-commit commit to complete?
        if self._autoCommitD:
            yield self._autoCommitD
        # Ok, all our outstanding operations should be complete, commit
        # our offsets, if we're responsible for that, and then we're done
        if self.auto_commit:
            yield self.commit()

    @inlineCallbacks
    def pending(self, partitions=None):
        """
        Gets the pending message count

        partitions: list of partitions to check for, default is to check all
        """
        # Have we completed our async-initialization?
        yield self.waitForReady()
        if not partitions:
            partitions = self.offsets.keys()

        total = 0
        reqs = []

        for partition in partitions:
            # -1 means get offset of next message (most recent)
            reqs.append(OffsetRequest(self.topic, partition, -1, 1))

        # If we don't have any requests we're done
        if not reqs:
            return
        resps = yield self.client.send_offset_request(reqs)
        for resp in resps:
            partition = resp.partition
            pending = resp.offsets[0]
            offset = self.offsets[partition]
            total += pending - offset - (1 if offset > 0 else 0)

        returnValue(total)

    @inlineCallbacks
    def seek(self, offset, whence):
        """
        Alter the current offset in the consumer, similar to fseek

        offset: how much to modify the offset
        whence: where to modify it from
                0 is relative to the earliest available offset (head)
                1 is relative to the current offset
                2 is relative to the latest known offset (tail)
        """
        # Have we completed our async-initialization?
        yield self.waitForReady()
        if whence == 1:  # relative to current position
            for partition, _offset in self.offsets.items():
                self.offsets[partition] = _offset + offset
        elif whence in (0, 2):  # relative to beginning or end
            # divide the request offset by number of partitions,
            # distribute the remained evenly
            (delta, rem) = divmod(offset, len(self.offsets))
            deltas = {}
            for partition, r in izip_longest(self.offsets.keys(),
                                             repeat(1, rem), fillvalue=0):
                deltas[partition] = delta + r

            reqs = []
            for partition in self.offsets.keys():
                if whence == 0:
                    reqs.append(OffsetRequest(self.topic, partition, -2, 1))
                elif whence == 2:
                    reqs.append(OffsetRequest(self.topic, partition, -1, 1))
                else:
                    pass

            resps = yield self.client.send_offset_request(reqs)
            for resp in resps:
                self.offsets[resp.partition] = \
                    resp.offsets[0] + deltas[resp.partition]
        else:
            raise ValueError("Unexpected value for `whence`, %d" % whence)

        # Reset queue and fetch offsets since they are invalid
        self.queue = DeferredQueue()
        self.fetch_offsets = self.offsets.copy()
        if self.auto_commit:
            self.count_since_commit += 1  # Fake it to force commit
            yield self.commit()
        returnValue(resps)

    def get_messages(self, count=1, update_offset=True):
        """
        Fetch the specified number of messages

        count: Indicates the number of messages to be fetched
        Returns list of deferreds which callbacks with a
        (partition, message) tuples

        """
        messages = []
        for i in range(count):
            messages.append(self.get_message(update_offset=update_offset))
        return messages

    def get_message(self, update_offset=True):
        """
        returns a deferred from the queue which will fire with a
        (partition, message) tuples when a message is available.
        """
        # Have we completed our async-initialization?
        if self.offsetFetchD is not None:
            raise ConsumerNotReady("You must yield consumer.waitForReady()")

        try:
            d = self.queue.get()

            # Do we need more messages?
            self._check_fetch_msgs()

            # We really want to update the offsets after the caller has
            # processed the message, not before. So, we want the caller
            # to take the deferred we return, add their callback(s), and
            # then call add_update_offset(d) to add our update_offset
            # callback to the deferred _after_ theirs. That way we only
            # update the offset if they successfuly processed it. Adding
            # our callbacks here gives us 'at most once' semantics, when
            # we really probably want 'at least once' semantics
            if update_offset:
                # Update partition offset when the message is ready
                self.add_update_offset(d)
            return d
        except QueueUnderflow:
            log.exception(
                'afkak.consumer.get_message: Too Many outstanding gets:%d',
                len(self.queue.waiting))
            raise

    def __iter__(self):
        """
        NOTE: the consumer iterator returns _deferreds_, not messages, though
        in most cases the result for the deferred should be available, if
        there are messages in Kafka.
        If self.only_prefetched is True, StopIteration will be raised when
        no more prefetched messages are available. yield fetch() should be
        called on a new Consumer prior to iterating if using only_prefetched
        """
        while True:
            if self.only_prefetched and not self.queue.pending:
                break
            yield self.get_message()

    def add_update_offset(self, d):
        """
        Add callbacks to a deferred which should yield a
        (partition, message) tuple. When it does, we update our offset for
        that partition and pass the same tuple on to the next callback
        """
        d.addCallback(self._update_offset)

    def _update_offset(self, resp):
        """
        Update our notion of the current offset for the given partition
        from the message's offset
        """
        partition, message = resp
        self.offsets[partition] = message.offset + 1

        # Count, check and commit messages if necessary
        self.count_since_commit += 1
        self._auto_commit()

        # Return our arg, so the callback chain can continue to process it
        return resp

    @inlineCallbacks
    def fetch(self):
        # Create fetch request payloads for all our partitions
        # If we have an outstanding offset_fetch_request, wait for it here
        yield self.waitForReady()
        partitions = self.fetch_offsets.keys()
        while partitions:
            requests = []
            for partition in partitions:
                requests.append(FetchRequest(self.topic, partition,
                                             self.fetch_offsets[partition],
                                             self.buffer_size))
            # Send request, wait for response(s)
            responses = yield self.client.send_fetch_request(
                requests, max_wait_time=self.fetch_max_wait_time,
                min_bytes=self.fetch_min_bytes)

            retry_partitions = set()
            for resp in responses:
                partition = resp.partition
                try:
                    # resp.messages is a KafkaCodec._decode_message_set_iter
                    # Note that 'message' here is really an OffsetAndMessage
                    for message in resp.messages:
                        # Put the message in our queue
                        self.queue.put((partition, message))
                        self.fetch_offsets[partition] = message.offset + 1
                except ConsumerFetchSizeTooSmall:
                    if (self.max_buffer_size is not None and
                            self.buffer_size == self.max_buffer_size):
                        # We failed, and are already at our max...
                        # create a Failure and stuff that in the queue.
                        log.error("Max fetch size %d too small",
                                  self.max_buffer_size)
                        f = Failure(
                            ConsumerFetchSizeTooSmall(
                                "Max buffer size:%d too small for message",
                                self.max_buffer_size))
                        self.queue.put(f)
                        raise StopIteration
                    if self.max_buffer_size is None:
                        self.buffer_size *= 2
                    else:
                        self.buffer_size = min(self.buffer_size * 2,
                                               self.max_buffer_size)
                    log.debug("Next message larger than fetch size, "
                              "increasing to %d (2x) and retring",
                              self.buffer_size)
                    retry_partitions.add(partition)
                except StopIteration:
                    # Stop iterating through this partition
                    pass

                # Any partitions which failed to fetch a single message due to
                # the fetch size being too small need to be retried...
                partitions = retry_partitions
        # Now that we've put all the messages from the last response, we can
        # clear our 'fetching' flag and check if we need more. We can't do it
        # earlier than now, because we would fetch with the wrong offsets...
        self._fetchD = None
        # avoid infinite recursion by using callLater()
        self._getClock().callLater(0, self._check_fetch_msgs)
