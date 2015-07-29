# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

from __future__ import absolute_import

import sys
import logging
from numbers import Integral

from twisted.python.failure import Failure
from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    Deferred, maybeDeferred, CancelledError, succeed, fail,
    )

from afkak.common import (
    SourcedMessage, FetchRequest, OffsetRequest, OffsetFetchRequest,
    OffsetCommitRequest,
    KafkaError, ConsumerFetchSizeTooSmall, InvalidConsumerGroupError,
    OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_COMMITTED, TIMESTAMP_INVALID,
)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

REQUEST_RETRY_MIN_DELAY = 0.1  # Initial wait to retry a request after failure
REQUEST_RETRY_MAX_DELAY = 30.0  # When retrying a request, max delay (seconds)
REQUEST_RETRY_FACTOR = 1.20205  # Factor by which we increase our delay

FETCH_MIN_BYTES = 64 * 1024  # server waits for min. 64K bytes of messages
FETCH_MAX_WAIT_TIME = 100  # server waits 100 millisecs for messages
FETCH_BUFFER_SIZE_BYTES = 128 * 1024  # Our initial fetch buffer size

# How often we auto-commit (msgs, millisecs)
AUTO_COMMIT_MSG_COUNT = 100
AUTO_COMMIT_INTERVAL = 5000


class Consumer(object):
    """A simple Kafka consumer implementation
    This consumer consumes a single partition from a single topic, and can
    optionally commit offsets based on a commit policy specified via the
    auto_commit_every_* args.

    :ivar client:
        Kafka client used to make requests to the Kafka brokers
    :type client: :class:`afkak.client.KafkaClient`
    :ivar str topic: name of Kafak topic from which to consume
    :ivar int partition: Kafka partition from which to consume
    :ivar callable processor: Callable called with lists of
        :class:`afkak.common.SourcedMessage`

    .. Synopsis for usage:
        * Create a :class:`afkak.client.KafkaClient`
        * Create the Consumer, supplying the client, topic, partition,
          processor (a callback which may return a deferred), and optionally
          fetch specifics, a consumer group, and a commit policy.
        * Call :func:`start` with the offset within the partition at which to
          start consuming messages. See docs on :func:`start` for details.
        * Process the messages in your :func:`processor` callback, returning a
          deferred if needed.
        * Once :func:`processor` returns (or the returned deferred completes),
          :func:`processor` will be called again with a new batch of messages
        * When desired, call :func:`stop` on the Consumer and no more calls to
          the :func:`processor` will be made, and any outstanding requests to
          the client will be cancelled.
        * A Consumer may be restarted after stopping.
    """
    def __init__(self, client, topic, partition, processor,
                 consumer_group=None,
                 commit_metadata=None,
                 auto_commit_every_n=None,
                 auto_commit_every_ms=None,
                 fetch_size_bytes=FETCH_MIN_BYTES,
                 fetch_max_wait_time=FETCH_MAX_WAIT_TIME,
                 buffer_size=FETCH_BUFFER_SIZE_BYTES,
                 max_buffer_size=None,
                 request_retry_init_delay=REQUEST_RETRY_MIN_DELAY,
                 request_retry_max_delay=REQUEST_RETRY_MAX_DELAY):
        """Create a Consumer object for processing messages from Kafka

        Args:
          client (KafkaClient): connected client for submitting requests to the
            Kafka cluster
          topic (str): the topic from which to consume messages
          partition (int): the partition from which to consume
          processor (callable): the callback function to which lists of
            messages will be submitted for processing
          consumer_group (str): optional consumer group ID for committing
            offsets of processed messages back to Kafka
          commit_metadata (str): optional metadata to store with offsets commit
          auto_commit_every_n (int): number of messages after which the
            consumer will automatically commit the offset of the last processed
            message to Kafka. Zero disables, defaulted to AUTO_COMMIT_MSG_COUNT
          auto_commit_every_ms (int): time interval in milliseconds after which
            the consumer will automatically commit the offset of the last
            processed message to Kafka. Zero disables, defaulted to
            AUTO_COMMIT_INTERVAL
          fetch_size_bytes (int): number of bytes to request in a FetchRequest
          fetch_max_wait_time (int): max msecs the server should wait for that
            many bytes
          buffer_size (int): default 128K. Initial number of bytes to tell
            kafka we have available. This will double as needed up to...
          max_buffer_size (int): default None. Max number of bytes to tell
            kafka we have available. None means no limit. Must be larger than
            the largest message we will find in our topic/partitions
          request_retry_init_delay (float): seconds to wait before retrying a
            failed request to Kafka
          request_retry_max_delay (float): max seconds to wait before retrying
            a failed request to Kafka (delay is increased on each failure
            and reset to the initial delay upon success
        """
        # # Store away parameters
        self.client = client  # KafkaClient
        self.topic = topic  # The topic from which we consume
        self.partition = partition  # The partition within the topic we consume
        self.processor = processor  # The callback we call with the msg list
        # Commit related parameters (Ensure the attr. exist, even if None)
        self.consumer_group = consumer_group
        self.commit_metadata = commit_metadata
        self.auto_commit_every_n = None
        self.auto_commit_every_s = None
        if consumer_group:
            # Auto committing is possible...
            if auto_commit_every_n is None:
                auto_commit_every_n = AUTO_COMMIT_MSG_COUNT
            if auto_commit_every_ms is None:
                auto_commit_every_ms = AUTO_COMMIT_INTERVAL
            assert isinstance(auto_commit_every_n, Integral)
            assert isinstance(auto_commit_every_ms, Integral)
            assert ((auto_commit_every_ms >= 0) and
                    (auto_commit_every_n >= 0))
            self.auto_commit_every_n = auto_commit_every_n
            self.auto_commit_every_s = float(auto_commit_every_ms) / 1000
        else:
            if auto_commit_every_ms or auto_commit_every_n:
                raise ValueError('An auto_commit_every_x argument set without '
                                 'specifying consumer_group')
        # Fetch related instance variables
        self.fetch_min_bytes = fetch_size_bytes
        self.fetch_max_wait_time = int(fetch_max_wait_time)
        self.buffer_size = buffer_size
        self.max_buffer_size = max_buffer_size
        # request retry timing
        self.retry_delay = float(request_retry_init_delay)
        self.retry_init_delay = float(request_retry_init_delay)
        self.retry_max_delay = float(request_retry_max_delay)

        # # Internal state tracking attributes
        self._fetch_offset = None  # We don't know at what offset to fetch yet
        self._last_processed_offset = None  # Last msg processed offset
        self._last_committed_offset = None  # The last offset stored in Kafka
        self._stopping = False  # We're not shutting down yet...
        self._commit_looper = None  # Looping call for auto-commit
        self._commit_looper_d = None  # Deferred for running looping call
        self._clock = None  # Settable reactor for testing
        self._commit_d = None  # Track outstanding commit request
        # For tracking various async operations
        self._start_d = None  # deferred for alerting user of errors
        self._request_d = None  # outstanding KafkaClient request deferred
        self._retry_call = None  # IDelayedCall object for delayed retries
        self._commit_call = None  # IDelayedCall for delayed commit retries
        self._msg_block_d = None  # deferred for each block of messages
        self._processor_d = None  # deferred for a result from processor
        self._state = '[initialized]'  # Keep track of state for debugging
        # Check parameters for sanity
        if max_buffer_size is not None and buffer_size > max_buffer_size:
            raise ValueError("buffer_size (%d) is greater than "
                             "max_buffer_size (%d)" %
                             (buffer_size, max_buffer_size))
        assert isinstance(self.partition, Integral)

    def __repr__(self):
        """Return a string representation of the Consumer

        Returned string is for display only, not suitable for serialization

        Returns:
            str: A textual representation of the Consumer suitable for
              distinguishing it from other Consumers
        """
        # Setup our repr string
        fmtstr = '<afkak.Consumer topic={0}, partition={1}, processor={2} {3}>'
        return fmtstr.format(
            self.topic, self.partition, self.processor, self._state)

    def start(self, start_offset):
        """Start delivering message lists to the processor

        Starts fetching messages from Kafka from the configured topic and
          partition starting at the supplied start_offset.

        Args:
          start_offset (int): The offset within the partition from which to
            start fetching. Special values include: OFFSET_EARLIEST,
            OFFSET_LATEST, and OFFSET_COMMITTED. If the supplied offset is
            OFFSET_EARLIEST, or OFFSET_LATEST, the Consumer will use the
            OffsetRequest API to the Kafka cluster to retreive the actual
            offset used for fetching. In the case OFFSET_COMMITTED is used,
            :param:`commit_policy` MUST be set on the Consumer, and the
            Consumer will use the OffsetFetchRequest API to the Kafka cluster
            to retreive the actual offset used for fetching.

        Returns:
          Deferred: the Consumer will callback() the deferred when the Consumer
          is stopped, and will errback() if the Consumer encounters any error
          from which it is unable to recover
        """
        # Have we been started already, and not stopped?
        if self._start_d is not None:
            raise RuntimeError("Start called on already-started consumer")

        # Keep track of state for debugging
        self._state = '[started]'

        # Create and return a deferred for alerting on errors/stopage
        start_d = self._start_d = Deferred()

        # Start a new fetch request, possible just for the starting offset
        self._fetch_offset = start_offset
        self._do_fetch()

        # Set up the auto-commit timer, if needed
        if self.consumer_group and self.auto_commit_every_s:
            self._commit_looper = LoopingCall(self._auto_commit)
            self._commit_looper.clock = self._get_clock()
            self._commit_looper_d = self._commit_looper.start(
                self.auto_commit_every_s, now=False)
            self._commit_looper_d.addCallbacks(self._commit_timer_stopped,
                                               self._commit_timer_failed)
        return start_d

    def stop(self):
        """stop the consumer and return offset of last processed message

        raises RuntimeError if consumer is not running
        Cancels all outstanding async operations
        """
        if self._start_d is None:
            raise RuntimeError("Stop called on non-started consumer")

        self._stopping = True
        # Keep track of state for debugging
        self._state = '[stopping]'
        # Are we waiting for a request to come back?
        if self._request_d:
            self._request_d.cancel()
        # Are we working our way through a block of messages?
        if self._msg_block_d:
            # Need to add a cancel handler...
            self._msg_block_d.addErrback(
                lambda fail: fail.trap(CancelledError))
            self._msg_block_d.cancel()
        # Are we waiting for the processor to complete?
        if self._processor_d:
            self._processor_d.cancel()
        # Are we waiting to retry a request?
        if self._retry_call:
            self._retry_call.cancel()
        # Are we waiting on a commit request?
        if self._commit_d:
            self._commit_d.cancel()
        # Are we waiting to retry a commit?
        if self._commit_call:
            self._commit_call.cancel()
        # Do we have an auto-commit looping call?
        if self._commit_looper is not None:
            self._commit_looper.stop()
        # Done stopping
        self._stopping = False
        # Keep track of state for debugging
        self._state = '[stopped]'

        # Clear and possibly callback our start() Deferred
        self._start_d, d = None, self._start_d
        if not d.called:
            d.callback("Stopped")

        # Return the offset of the message we last processed
        return self._last_processed_offset

    def commit(self):
        """ Commit offsets for this consumer

        Commit the offset of the message we last processed if it is different
        from what we believe is the last offset comitted to Kafka.
        Note: It is possible to commit a smaller offset than Kafka has stored,
        and this is by design, so we can reprocess a Kafka msg stream if
        desired.
        Will retry until the delay matches the max retry delay.
        """
        def _clear_commit_d(_):
            # Helper to cleanup deferred when it fires
            self._commit_d = None
            return _

        # Can't commit without a consumer_group
        if not self.consumer_group:
            return fail(Failure(InvalidConsumerGroupError(
                "Bad Group_id:{0!r}".format(self.consumer_group))))
        # short circuit if we are 'up to date'
        if self._last_processed_offset == self._last_committed_offset:
            return succeed(self._last_committed_offset)

        # Send the request, save the deferred, add our callback to clear it
        self._commit_d = d = self._send_commit_request(self.retry_init_delay)
        d.addBoth(_clear_commit_d)

        # Reset the commit_looper here, rather than on success to give
        # more stability to the commit interval.
        if self._commit_looper is not None:
            self._commit_looper.reset()

        # return the deferred
        return d

    # # Private Methods # #

    def _auto_commit(self, by_count=False):
        """Check if we should start a new commit operation and commit"""
        # Check if we are even supposed to do any auto-committing
        if (self._stopping or (not self._start_d) or
                (self._last_processed_offset is None) or
                (not self.consumer_group) or
                (by_count and not self.auto_commit_every_n)):
            return

        # If we're auto_committing because the timer expired, or by count and
        # we don't have a record of our last_committed_offset, or we've
        # processed enough messages since our last commit, then try to commit
        if (not by_count or self._last_committed_offset is None or
            (self._last_processed_offset - self._last_committed_offset
             ) >= self.auto_commit_every_n):
            if not self._commit_d:
                commit_d = self.commit()
                commit_d.addErrback(self._handle_auto_commit_error)
            else:
                # We're waiting on the last commit to complete, so setup a
                # deferred to be called
                # attach a re-call of this function to the request completion
                self._commit_d.addCallback(lambda _:
                                           self._auto_commit(by_count))

    def _get_clock(self):
        # Reactor to use for callLater
        if self._clock is None:
            from twisted.internet import reactor
            self._clock = reactor
        return self._clock

    def _retry_fetch(self, after=None):
        """Schedule a delayed :func:`_do_fetch` call after a failure

        Args:
          after (float optional): The delay in seconds after which to do the
            retried fetch. If `None`, our internal :ivar:`retry_delay` is used,
            and adjusted by REQUEST_RETRY_FACTOR
        """
        if self._retry_call is None:
            if after is None:
                after = self.retry_delay
                self.retry_delay = min(self.retry_delay * REQUEST_RETRY_FACTOR,
                                       self.retry_max_delay)

            self._retry_call = self._get_clock().callLater(
                after, self._do_fetch)

    def _handle_offset_response(self, response):
        """Handle response to request to Kafka for offset

        Handles responses to both OffsetRequest and OffsetFetchRequest, since
        they are similar enough
        Args:
            response (tuple of a single OffsetFetchResponse or OffsetResponse):
        """
        # Got a response, clear our outstanding request deferred
        self._request_d = None

        # Successful request, reset our retry delay
        self.retry_delay = self.retry_init_delay

        response = response[0]
        if hasattr(response, 'offsets'):
            # It's a response to an OffsetRequest
            self._fetch_offset = response.offsets[0]
        else:
            # It's a response to an OffsetFetchRequest
            self._fetch_offset = response.offset + 1
            self._last_committed_offset = response.offset
        self._do_fetch()

    def _handle_offset_error(self, failure):
        """Retry the offset fetch request.

        Naively retries the offset fetch request until we reach the
        retry_max_delay (a proxy for a retry count)
        This should perhaps be extended to abort sooner on certain errors
        """
        # outstanding request got errback'd, clear it
        self._request_d = None

        if self._stopping and failure.check(CancelledError):
            # Not really an error
            return
        # Have we retried so many times we're at the retry_max_delay?
        if self.retry_delay == self.retry_max_delay:
            # We've retried enough, give up and errBack our start() deferred
            log.error("%r: Too many failures fetching offset from kafka: %r",
                      self, failure)
            self._start_d.errback(failure)
            return
        # We haven't reached our retry limit, so schedule a retry and increase
        # our retry interval
        log.error("%r: Failure fetching offset from kafka: %r\n%r", self,
                  failure, failure.getTraceback())
        self._retry_fetch()
        return

    def _clear_processor_deferred(self, _):
        self._processor_d = None  # It has fired, we can clear it
        return _

    def _update_committed_offset(self, result, offset):
        self._last_committed_offset = offset
        return result

    def _update_processed_offset(self, result, offset):
        self._last_processed_offset = offset
        self._auto_commit(by_count=True)

    def _send_commit_request(self, retry_delay, chain_d=None):
        """Send a commit request, and callback chain_d when it completes"""
        # If there's a _commit_call, and it's not active, clear it, it probably
        # just called us...
        if self._commit_call and not self._commit_call.active():
            self._commit_call = None

        # Create new OffsetCommitRequest with the latest processed offset
        commit_request = OffsetCommitRequest(
            self.topic, self.partition, self._last_processed_offset,
            TIMESTAMP_INVALID, self.commit_metadata)
        log.debug("Commit off=%d grp=%s tpc=%s part=%s req=%r, chn_d=%r",
                  self._last_processed_offset, self.consumer_group,
                  self.topic, self.partition, commit_request, chain_d)

        # Send the request, add our callbacks
        d = self.client.send_offset_commit_request(self.consumer_group,
                                                   [commit_request])
        d.addCallback(self._update_committed_offset,
                      self._last_processed_offset)
        d.addErrback(self._handle_commit_error, retry_delay)
        if chain_d:
            d.chainDeferred(chain_d)

        return d

    def _handle_commit_error(self, failure, retry_delay):
        """ Retry the commit request, depending on failure type

        Depending on the type of the failure, we retry the commit request with
        the latest processed offset.
        """
        # Check if we are stopping and the request was cancelled
        if self._stopping and failure.check(CancelledError):
            # Not really an error
            return
        # Check that the failure type is a Kafka error...this could maybe be
        # a tighter check to determine whether a retry will succeed...
        if not failure.check(KafkaError):
            log.error("Unhandleable failure during commit attempt: %r\n\t%r",
                      failure, failure.getBriefTraceback())
            return failure

        # Check the retry_delay to see if we should give up
        if retry_delay >= self.retry_max_delay:
            log.error("Failure during FINAL commit attempt: %r\n\t%r", failure,
                      failure.getBriefTraceback())
            return failure
        else:
            log.warn("Failure during commit attempt: %r\n\t%r\nWill Retry.",
                     failure, failure.getBriefTraceback())

        # Create a deferred to return
        chain_d = Deferred()

        # Schedule a delayed call to retry the commit, chaining to the deferred
        retry_delay = min(retry_delay * REQUEST_RETRY_FACTOR,
                          self.retry_max_delay)
        self._commit_call = self._get_clock().callLater(
            retry_delay, self._send_commit_request, retry_delay, chain_d)

        # return the deferred
        return chain_d

    def _handle_auto_commit_error(self, failure):
        if self._start_d is not None and not self._start_d.called:
            self._start_d.errback(failure)

    def _handle_processor_error(self, failure):
        """Handle a failure in the processing of a block of messages

        This method is called when the processor func fails while processing
        a block of messages. Since we can't know how best to handle a
        processor failure, we just :func:`errback` our :func:`start` method's
        deferred to let our user know about the failure.
        """
        # Check if we're stopping/stopped and the errback of the processor
        # deferred is just the cancelling we initiated.  If so, we skip
        # notifying via the _start_d deferred, as it will be 'callback'd at the
        # end of stop()
        if not (self._stopping and failure.check(CancelledError)):
            if self._start_d:  # Make sure we're not already stopped
                self._start_d.errback(failure)
        return

    def _handle_fetch_error(self, failure):
        """A fetch request resulted in an error. Retry after our current delay

        When a fetch error occurs, we wait our current :ivar:`retry_delay`, and
        retry the fetch. We also increase our retry_delay by Apery's constant
        (1.20205)
        NOTE: this will retry forever.
        TODO: Possibly make this differentiate based on the failure
        """
        # The _request_d deferred has fired, clear it.
        self._request_d = None
        if self._stopping and failure.check(CancelledError):
            # Not really an error
            return
        log.error("%r: Failure fetching from kafka: %r\n%r", self,
                  failure, failure.getTraceback())
        self._retry_fetch()
        return

    def _handle_fetch_response(self, responses):
        """The callback handling the successful response from the fetch request

        Delivers the message list to the processor, handles per-message errors
        (ConsumerFetchSizeTooSmall), triggers another fetch request

        If the processor is still processing the last batch of messages, we
        defer this processing until it's done.  Otherwise, we start another
        fetch request and submit the messages to the processor
        """
        # Successful fetch, reset our retry delay
        self.retry_delay = self.retry_init_delay

        # Check to see if we are still processing the last block we fetched...
        if self._msg_block_d:
            # We are still working through the last block of messages...
            # We have to wait until it's done, then process this response
            self._msg_block_d.addCallback(
                lambda _: self._handle_fetch_response(responses))
            return

        # No ongoing processing, great, let's get some started.
        # Request no longer outstanding, clear the deferred tracker so we
        # can refetch
        self._request_d = None
        messages = []
        try:
            for resp in responses:  # We should really only ever get one...
                if resp.partition != self.partition:
                    log.error(
                        "%r: Got response with partition: %r not our own: %r",
                        self, resp.partition, self.partition)
                    continue
                # resp.messages is a KafkaCodec._decode_message_set_iter
                # Note that 'message' here is really an OffsetAndMessage
                for message in resp.messages:
                    # Check for messages included which are from prior to our
                    # desired offset: can happen due to compressed message sets
                    if message.offset < self._fetch_offset:
                        log.debug(
                            'Skipping message at offset: %d, because its '
                            'offset is less that our fetch offset: %d.',
                            message.offset, self._fetch_offset)
                        continue
                    # Create a 'SourcedMessage' and add it to the messages list
                    messages.append(
                        SourcedMessage(
                            message=message.message,
                            offset=message.offset, topic=self.topic,
                            partition=self.partition))
                    # Update our notion of from where to fetch.
                    self._fetch_offset = message.offset + 1
        except ConsumerFetchSizeTooSmall:
            # A message was too large for us to receive, given our current
            # buffer size. Double it until it works, or we hit our max
            if self.max_buffer_size is None:
                # No limit. Double until we succeed or fail due to lack of RAM
                self.buffer_size *= 2
            elif (self.max_buffer_size is not None and
                    self.buffer_size < self.max_buffer_size):
                # Limited, but currently below it.
                self.buffer_size = min(
                    self.buffer_size * 2, self.max_buffer_size)
            else:
                # We failed, and are already at our max. Nothing we can do but
                # create a Failure and errback() our start() deferred
                log.error("Max fetch size %d too small", self.max_buffer_size)
                failure = Failure(
                    ConsumerFetchSizeTooSmall(
                        "Max buffer size:%d too small for message",
                        self.max_buffer_size))
                self._start_d.errback(failure)
                return

            log.debug(
                "Next message larger than fetch size, increasing "
                "to %d (~2x) and retrying", self.buffer_size)

        finally:
            # If we were able to extract any messages, deliver them to the
            # processor now.
            if messages:
                self._msg_block_d = Deferred()
                self._process_messages(messages)

        # start another fetch, if needed, but use callLater to avoid recursion
        self._retry_fetch(0)

    def _process_messages(self, messages):
        """Send messages to the `processor` callback to be processed

        In the case we have a CommitPolicy, we send messages to the processor
        in blocks no bigger than auto_commit_every_n (if set). Otherwise, we
        send the entire message block to be processed.

        """
        # Do we have any messages to process?
        if not messages:
            # No, we're done with this block. If we had another fetch result
            # waiting, this callback will trigger the processing thereof.
            if self._msg_block_d:
                _msg_block_d, self._msg_block_d = self._msg_block_d, None
                _msg_block_d.callback(True)
                return
        # Yes, we've got some messages to process.
        # Default to processing the entire block...
        proc_block_size = sys.maxsize
        # Unless our auto commit_policy restricts us to process less
        if self.auto_commit_every_n:
            proc_block_size = self.auto_commit_every_n

        # Divide messages into two lists: one to process now, and remainder
        msgs_to_proc = messages[:proc_block_size]
        msgs_remainder = messages[proc_block_size:]
        # Call our processor callable and handle the possibility it returned
        # a deferred...
        last_offset = msgs_to_proc[-1].offset
        self._processor_d = d = maybeDeferred(self.processor, msgs_to_proc)
        log.debug('self.processor return: %r, last_offset: %r', d, last_offset)
        # Once the processor completes, clear our _processor_d
        d.addBoth(self._clear_processor_deferred)
        # Record the offset of the last processed message and check autocommit
        d.addCallback(self._update_processed_offset, last_offset)
        # If we were stopped, cancel the processor deferred. Note, we have to
        # do this here, in addition to in stop() because the processor func
        # itself could have called stop(), and then when it returned, we re-set
        # self._processor_d to the return of maybeDeferred().
        if self._stopping or self._start_d is None:
            d.cancel()
        else:
            # Setup to process the rest of our messages
            d.addCallback(lambda _: self._process_messages(msgs_remainder))
        # Add an error handler
        d.addErrback(self._handle_processor_error)

    def _do_fetch(self):
        """Send a fetch request if there isn't a request outstanding

        Sends a fetch request to the Kafka cluster to get messages at the
        current offset.
        When the response comes back, if there are messages, it delivers
          them to the 'processor' callback and initiates another fetch request.
        If there is a recoverable error, the fetch is retried after
          :ivar:`retry_delay`.
        In the case of an unrecoverable error, :func:`errback` is called on the
          Deferred returned by the :func:`start` method.

        """
        # Check for outstanding request.
        if self._request_d:
            log.debug("_do_fetch: Outstanding request: %r", self._request_d)
            return

        # Cleanup our _retry_call, if we have one
        if self._retry_call is not None:
            if self._retry_call.active():
                self._retry_call.cancel()
            self._retry_call = None

        # Do we know our offset yet, or do we need to figure it out?
        if (self._fetch_offset == OFFSET_EARLIEST or
                self._fetch_offset == OFFSET_LATEST):
            # We need to fetch the offset for our topic/partition
            offset_request = OffsetRequest(
                self.topic, self.partition, self._fetch_offset, 1)
            self._request_d = self.client.send_offset_request([offset_request])
            self._request_d.addCallbacks(
                self._handle_offset_response, self._handle_offset_error)
        elif self._fetch_offset == OFFSET_COMMITTED:
            # We need to fetch the committed offset for our topic/partition
            # Note we use the same callbacks, as the responses are "close
            # enough" for our needs here
            if not self.consumer_group:
                # consumer_group must be set for OFFSET_COMMITTED
                failure = Failure(
                    InvalidConsumerGroupError("Bad Group_id:{0!r}".format(
                        self.consumer_group)))
                self._start_d.errback(failure)
            request = OffsetFetchRequest(self.topic, self.partition)
            self._request_d = self.client.send_offset_fetch_request(
                self.consumer_group, [request])
            self._request_d.addCallbacks(
                self._handle_offset_response, self._handle_offset_error)
        else:
            # Create fetch request payload for our partition
            request = FetchRequest(
                self.topic, self.partition, self._fetch_offset,
                self.buffer_size)
            # Send request and add handlers for the response
            self._request_d = self.client.send_fetch_request(
                [request], max_wait_time=self.fetch_max_wait_time,
                min_bytes=self.fetch_min_bytes)
            # We need a temp for this because if the response is already
            # available, _handle_fetch_response() will clear self._request_d
            d = self._request_d
            d.addCallback(self._handle_fetch_response)
            d.addErrback(self._handle_fetch_error)

    def _commit_timer_failed(self, fail):
        """Handle an error in the commit() function
        Our commit() function called by the LoopingCall failed. Some error
        probably came back from Kafka and check_error() raised the exception
        For now, just log the failure and restart the loop
        """
        log.warning(
            '_commit_timer_failed: uncaught error %r: %s in _auto_commit',
            fail, fail.getBriefTraceback())
        self._commit_looper_d = self._commit_looper.start(
            self.auto_commit_every_s, now=False)

    def _commit_timer_stopped(self, lCall):
        """We're shutting down, clean up our looping call..."""
        if self._commit_looper is not lCall:
            log.warning('_commit_timer_stopped with wrong timer:%s not:%s',
                        lCall, self._commit_looper)
        else:
            log.debug('_commit_timer_stopped: %s %s', lCall,
                      self._commit_looper)
            self._commit_looper = None
            self._commit_looper_d = None
