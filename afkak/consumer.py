from __future__ import absolute_import

import logging
from numbers import Integral

from twisted.python.failure import Failure
from twisted.internet.defer import (
    Deferred, maybeDeferred, CancelledError,
    )

from afkak.common import (
    SourcedMessage,
    FetchRequest,
    OffsetRequest,
    OffsetFetchRequest,
    ConsumerFetchSizeTooSmall,
    InvalidConsumerGroupError,
    OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_COMMITTED,
    # FailedPayloadsError,
    # OffsetCommitRequest,
)

log = logging.getLogger(__name__)

FETCH_RETRY_MIN_DELAY = 1  # When retrying a fetch after failure, initial wait
FETCH_RETRY_MAX_DELAY = 30  # When retrying a fetchs, max delay (seconds)
FETCH_RETRY_FACTOR = 1.20205  # Factor by which we increase our delay
FETCH_MIN_BYTES = 64 * 1024  # server waits for min. 64K bytes of messages
FETCH_MAX_WAIT_TIME = 100  # server waits 100 millisecs for messages
FETCH_BUFFER_SIZE_BYTES = 128 * 1024  # Our initial fetch buffer size


class Consumer(object):
    """A simple Kafka consumer implementation
    This consumer consumes a single partition from a single topic, and relies
    on an external committer object to commit offsets.

    :ivar client:
        Kafka client used to make requests to the Kafka brokers
    :type client: :class:`afkak.client.KafkaClient`
    :ivar str topic: name of Kafak topic from which to consume
    :ivar int partition: Kafka partition from which to consume
    :ivar

    .. Synopsis for usage:
        * Create a :class:`afkak.client.KafkaClient`
        * Create the Consumer, supplying the client, topic, partition,
          processor (a callback which may return a deferred), and optionally
          fetch specifics.
        * Call :func:`start` with the offset within the partition at which to
          start consuming messages. See docs on :func:`start` for details.
        * Process the messages in your :func:`processor` callback, returning a
          deferred if needed.
        * Optionally use a :class:`afkak.consumer.Committer` object to commit
          the offsets of the processed messages back to Kafka.
        * Once :func:`processor` returns (or the returned deferred completes),
          :func:`processor` will be called again with a new batch of messages
        * When desired, call :func:`stop` on the Consumer and no more calls to
          the :func:`processor` will be made, and any outstanding requests to
          the client will be cancelled.
        * A Consumer may be restarted after stopping.
    """
    def __init__(self, client, topic, partition, processor,
                 group_id=None,
                 fetch_size_bytes=FETCH_MIN_BYTES,
                 fetch_max_wait_time=FETCH_MAX_WAIT_TIME,
                 buffer_size=FETCH_BUFFER_SIZE_BYTES,
                 max_buffer_size=None,
                 fetch_retry_init_delay=FETCH_RETRY_MIN_DELAY,
                 fetch_retry_max_delay=FETCH_RETRY_MAX_DELAY):
        """Create a Consumer object for processing messages from Kafka

        Args:
          client (KafkaClient): connected client for submitting requests to the
            Kafka cluster
          topic (str): the topic from which to consume messages
          partition (int): the partition from which to consume
          processor (callable): the callback function to which lists of
            messages will be submitted for processing
          fetch_size_bytes (int): number of bytes to request in a FetchRequest
          fetch_max_wait_time (int): max msecs the server should wait for that
            many bytes
          buffer_size (int): default 128K. Initial number of bytes to tell
            kafka we have available. This will double as needed up to...
          max_buffer_size (int): default None. Max number of bytes to tell
            kafka we have available. None means no limit. Must be larger than
            the largest message we will find in our topic/partitions
          fetch_retry_init_delay (float): seconds to wait before retrying a
            failed fetch request to Kafka
          fetch_retry_max_delay (float): max seconds to wait before retrying a
            failed fetch request to Kafka (delay is increased on each failure
            and reset to the initial delay upon success
        """
        # # Store away parameters
        self.client = client  # KafkaClient
        self.topic = topic  # The topic from which we consume
        self.partition = partition  # The partition within the topic we consume
        self.processor = processor  # The callback we call with the msg list
        self.group_id = group_id
        # Fetch related instance variables
        self.fetch_min_bytes = fetch_size_bytes
        self.fetch_max_wait_time = int(fetch_max_wait_time)
        self.buffer_size = buffer_size
        self.max_buffer_size = max_buffer_size
        # Fetch retry timing
        self.retry_delay = float(fetch_retry_init_delay)
        self.retry_init_delay = float(fetch_retry_init_delay)
        self.retry_max_delay = float(fetch_retry_max_delay)

        # # Internal state tracking attributes
        self._fetch_offset = None  # We don't know at what offset to fetch yet
        self._stopping = False  # We're not shutting down yet...
        self._clock = None  # Settable reactor for testing
        # For tracking various async operations
        self._start_d = None  # deferred for alerting user of errors
        self._request_d = None  # outstanding KafkaClient request deferred
        self._retry_call = None  # IDelayedCall object for delayed retries
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
            :param:`group_id` MUST be set on the Consumer, and the Consumer
            will use the OffsetFetchRequest API to the Kafka cluster to
            retreive the actual offset used for fetching.

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
        self._start_d = Deferred()

        # Start a new fetch request, possible just for the starting offset
        self._fetch_offset = start_offset
        self._do_fetch()

        return self._start_d

    def stop(self):
        if self._start_d is None:
            raise RuntimeError("Stop called on non-started consumer")

        self._stopping = True
        # Keep track of state for debugging
        self._state = '[stopping]'
        # Are we waiting for a request to come back?
        if self._request_d:
            self._request_d.cancel()
        # Are we waiting for the processor to complete?
        if self._processor_d:
            self._processor_d.cancel()
        # Are we waiting to retry a request?
        if self._retry_call:
            self._retry_call.cancel()
        # Done stopping
        self._stopping = False
        # Keep track of state for debugging
        self._state = '[stopped]'

        # Clear and possibly callback our start() Deferred
        self._start_d, d = None, self._start_d
        if not d.called:
            d.callback("Stopped")

        # Return the offset where we left off
        return self._fetch_offset

    # # Private Methods # #

    def _getClock(self):
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
            and adjusted by FETCH_RETRY_FACTOR
        """
        if self._retry_call is None:
            if after is None:
                after = self.retry_delay
                self.retry_delay = min(self.retry_delay * FETCH_RETRY_FACTOR,
                                       self.retry_max_delay)

            self._retry_call = self._getClock().callLater(
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

        response = response[0]
        if hasattr(response, 'offsets'):
            # It's a response to an OffsetRequest
            self._fetch_offset = response.offsets[0]
        else:
            # It's a response to an OffsetFetchRequest
            self._fetch_offset = response.offset
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

    def _handle_processor_both(self, _):
        self._processor_d = None  # It has fired, we can clear it
        return _

    def _handle_processor_error(self, failure):
        """Handle a failure in the processing of a block of messages

        This method is called when the processor func fails while processing
        a block of messages, or when a :class:`Committer` attached to the block
        fails to commit the offsets. Since we can't know how best to handle a
        processor failure, we just :func:`errback` our :func:`start` method's
        deferred to let our user know about the failure.
        """
        # Check of we're stopping and the errback of the processor deferred is
        # just the cancelling we initiated.  If so, we skip notifying via the
        # _start_d deferred, as it will be 'callback'd at the end of stop()
        if not (self._stopping and failure.check(CancelledError)):
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
        log.error("%r: Failure fetching from kafka: %r", self,
                  failure)
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

        # Check to see if the processor is still busy
        if self._processor_d:
            # The processor is still processing the last batch we gave it.
            # We have to wait until it's done, then process this response
            self._processor_d.addCallback(
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
                self._processor_d = d = maybeDeferred(self.processor, messages)
                d.addBoth(self._handle_processor_both)
                d.addErrback(self._handle_processor_error)

        # start another fetch, if needed, but use callLater to avoid recursion
        self._retry_fetch(0)

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
            if not self.group_id:
                # group_id must be set for OFFSET_COMMITTED
                failure = Failure(
                    InvalidConsumerGroupError("Bad Group_id:{0!r}".format(
                        self.group_id)))
                self._start_d.errback(failure)
            request = OffsetFetchRequest(self.topic, self.partition)
            self._request_d = self.client.send_offset_fetch_request(
                self.group_id, [request])
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
            # Yes, twisted is terrible.
            d = self._request_d
            d.addCallback(self._handle_fetch_response)
            d.addErrback(self._handle_fetch_error)
