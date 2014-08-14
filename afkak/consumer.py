from __future__ import absolute_import

from itertools import izip_longest, repeat
import logging
import numbers

from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredQueue, QueueUnderflow,
    )

from afkak.common import (
    check_error,
    ConsumerFetchSizeTooSmall,
    FetchRequest,
    OffsetRequest, OffsetCommitRequest,
    OffsetFetchRequest,
    UnknownTopicOrPartitionError,
)

log = logging.getLogger("afkak.consumer")

# How often we auto-commit (msgs, millisecs)
AUTO_COMMIT_MSG_COUNT = 100
AUTO_COMMIT_INTERVAL = 5000

FETCH_MIN_BYTES = 16 * 1024  # server waits for min. 16K bytes of messages
FETCH_MAX_WAIT_TIME = 30000  # server waits 30 secs for messages
FETCH_BUFFER_SIZE_BYTES = 128 * 1024  # Our initial fetch buffer size
MAX_FETCH_BUFFER_SIZE_BYTES = 4 * 1024 * 1024  # Max the buffer can double to
QUEUE_LOW_WATERMARK = 64  # refetch when our internal queue gets below this

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
    fetch_max_wait_time: max time the server should wait for that many bytes
    buffer_size:         default 128K. Initial number of bytes to tell kafka we
                         have available. This will double as needed up to...
    max_buffer_size:     default 4M. Max number of bytes to tell kafka we have
                         available. None means no limit. Must be larger than the
                         largest message we will find in our topic/partitions
    queue_low_watermark  default 64. When the number of messages in the
                         consumer's internal queue is fewer than this, it will
                         initiate another fetch of more messages

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
                 queue_low_watermark=QUEUE_LOW_WATERMARK):

        self.client = client  # KafkaClient
        self.group = group  # Name of consumer group we may be a part of
        self.topic = topic  # The topic from which we consume
        self.offsets = {}  # Our current offset(s) on each of our partitions

        # If the caller didn't supply a list of partitions, get all the
        # partitions of the topic from our client
        if not partitions:
            partitions = self.client.topic_partitions[topic]
        else:
            assert all(isinstance(x, numbers.Integral) for x in partitions)

        # Variables for handling offset commits
        self.commit_looper = None  # LoopingCall handles auto_commit_every_t
        self.count_since_commit = 0
        self.auto_commit = auto_commit
        self.auto_commit_every_n = auto_commit_every_n
        self.auto_commit_every_t = auto_commit_every_t

        if max_buffer_size is not None and buffer_size > max_buffer_size:
            raise ValueError("buffer_size (%d) is greater than "
                             "max_buffer_size (%d)" %
                             (buffer_size, max_buffer_size))

        self.buffer_size = buffer_size
        self.max_buffer_size = max_buffer_size
        self.fetch_max_wait_time = int(fetch_max_wait_time)
        self.fetch_min_bytes = fetch_size_bytes
        self.fetch_offsets = self.offsets.copy()
        self.queue_low_watermark = queue_low_watermark
        self.queue = DeferredQueue()

        # Set up the auto-commit timer
        if auto_commit is True and auto_commit_every_t is not None:
            self.commit_looper = LoopingCall(self.commit)
            self.commit_looper_d = self.commit_looper.start(
                (auto_commit_every_t / 1000), now=False)
            self.commit_looper_d.addCallbacks(self._commitTimerStopped,
                                             self._commitTimerFailed)

        # If we are auto_commiting, we need to pre-populate our offsets...
        # fetch them here. Otherwise, assume 0
        if auto_commit:
            payloads = []
            for partition in partitions:
                payloads.append(OffsetFetchRequest(topic, partition))

            resps = self.client.send_offset_fetch_request(
                 group, payloads, fail_on_error=False)
            for resp in resps:
                try:
                    check_error(resp)
                    self.offsets[resp.partition] = resp.offset
                except UnknownTopicOrPartitionError:
                    self.offsets[resp.partition] = 0
        else:
            for partition in partitions:
                self.offsets[partition] = 0

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
        self.commit_looper.start(self.auto_commit_every_t, now=False)

    def _commitTimerStopped(self, lCall):
        """
        We're shutting down, clean up our looping call...
        """
        if self.commit_looper is not lCall:
            log.warning('commitTimerStopped with wrong timer:%s not:%s',
                        lCall, self.commit_looper)
        else:
            self.commit_looper = None

    def commit(self, partitions=None):
        """
        Commit offsets for this consumer

        partitions: list of partitions to commit, default is to commit
                    all of them
        """

        # short circuit if nothing happened.
        if self.count_since_commit == 0:
            return

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

        resps = self.client.send_offset_commit_request(self.group, reqs)
        for resp in resps:
            check_error(resp)

        self.count_since_commit = 0

    def _auto_commit(self):
        """
        Check if we have to commit based on number of messages and commit
        """

        # Check if we are supposed to do an auto-commit
        if not self.auto_commit or self.auto_commit_every_n is None:
            return

        if self.count_since_commit >= self.auto_commit_every_n:
            self.commit()

    def stop(self):
        if self.commit_looper is not None:
            self.commit_looper.stop()
            self.commit()

    def pending(self, partitions=None):
        """
        Gets the pending message count

        partitions: list of partitions to check for, default is to check all
        """
        if not partitions:
            partitions = self.offsets.keys()

        total = 0
        reqs = []

        for partition in partitions:
            # -1 means get offset of next message (most recent)
            reqs.append(OffsetRequest(self.topic, partition, -1, 1))

        resps = self.client.send_offset_request(reqs)
        for resp in resps:
            partition = resp.partition
            pending = resp.offsets[0]
            offset = self.offsets[partition]
            total += pending - offset - (1 if offset > 0 else 0)

        return total


    def provide_partition_info(self):
        """
        Indicates that partition info must be returned by the consumer
        """
        self.partition_info = True

    def seek(self, offset, whence):
        """
        Alter the current offset in the consumer, similar to fseek

        offset: how much to modify the offset
        whence: where to modify it from
                0 is relative to the earliest available offset (head)
                1 is relative to the current offset
                2 is relative to the latest known offset (tail)
        """

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

            resps = self.client.send_offset_request(reqs)
            for resp in resps:
                self.offsets[resp.partition] = \
                    resp.offsets[0] + deltas[resp.partition]
        else:
            raise ValueError("Unexpected value for `whence`, %d" % whence)

        # Reset queue and fetch offsets since they are invalid
        self.fetch_offsets = self.offsets.copy()
        if self.auto_commit:
            self.count_since_commit += 1
            self.commit()
        self.queue = DeferredQueue()

    @inlineCallbacks
    def get_messages(self, count=1, update_offset=False):
        """
        Fetch the specified number of messages

        count: Indicates the number of messages to be fetched
        Returns a deferred which callbacks with a list of
        (partition, message) tuples

        """
        messages = []
        new_offsets = {}
        while count > 0:
            result = yield self.get_message(update_offset=update_offset)
            partition, message = result
            messages.append(result)
            new_offsets[partition] = message.offset + 1
            count -= 1

        returnValue(messages)

    def get_message(self, update_offset=False):
        """
        returns a deferred from the queue which will fire when a message
        is available.
        """
        try:
            d = self.queue.get()
            if len(self.queue.get.pending) <= self.queue_low_watermark:
                self._fetch()

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
                len(self.queue.pending))
            return None

    def __iter__(self):
        while True:
            yield self.get_message()

    def add_update_offset(self, d):
        """
        Add callbacks to a deferred which should yield a
        (partition, message) tuple. When it does, we update our offset for
        that partition and pass the same tuple on to the next callback
        """
        d.addCallback(self.update_offset)

    def update_offset(self, partMessTup):
        """
        Update our notion of the current offset for the given partition
        from the message's offset
        """
        partition, message = partMessTup
        self.offsets[partition] = message.offset + 1

        # Count, check and commit messages if necessary
        self.count_since_commit += 1
        self._auto_commit()

        # Return our arg, so the callback chain can continue to process it
        return partMessTup

    @inlineCallbacks
    def _fetch(self):
        # Create fetch request payloads for all the partitions
        requests = []
        partitions = self.fetch_offsets.keys()
        while partitions:
            for partition in partitions:
                requests.append(FetchRequest(self.topic, partition,
                                             self.fetch_offsets[partition],
                                             self.buffer_size))
            # Send request
            responses = yield self.client.send_fetch_request(
                requests, max_wait_time=self.fetch_max_wait_time,
                min_bytes=self.fetch_min_bytes)

            retry_partitions = set()
            for resp in responses:
                partition = resp.partition
                try:
                    for message in resp.messages:
                        # Put the message in our queue
                        self.queue.put((partition, message))
                        self.fetch_offsets[partition] = message.offset + 1
                except ConsumerFetchSizeTooSmall:
                    if (self.max_buffer_size is not None and
                            self.buffer_size == self.max_buffer_size):
                        log.error("Max fetch size %d too small",
                                  self.max_buffer_size)
                        raise
                    if self.max_buffer_size is None:
                        self.buffer_size *= 2
                    else:
                        self.buffer_size = max(self.buffer_size * 2,
                                               self.max_buffer_size)
                    log.warn("Fetch size too small, increase to %d (2x) "
                             "and retry", self.buffer_size)
                    retry_partitions.add(partition)
                except StopIteration:
                    # Stop iterating through this partition
                    log.debug("Done iterating over partition %s" % partition)
                partitions = retry_partitions

