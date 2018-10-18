# -*- coding: utf-8 -*-
# Copyright 2015 Cyan, Inc.
# Copyright 2016, 2017, 2018 Ciena Corporation

"""KafkaClient class.

High level network client for an Apache Kafka Cluster.
"""
from __future__ import absolute_import

import logging
import random
import collections
from functools import partial
from numbers import Real
from twisted.names import client as DNSclient
from twisted.names import dns
from twisted.internet.abstract import isIPAddress
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList,
    CancelledError as t_CancelledError,
)
from twisted.python.compat import nativeString
from twisted.python.compat import unicode as _unicode

from .common import (
    BrokerResponseError,
    TopicAndPartition, FailedPayloadsError, BrokerMetadata,
    PartitionUnavailableError, LeaderUnavailableError, KafkaUnavailableError,
    UnknownTopicOrPartitionError, NotLeaderForPartitionError, _check_error,
    DefaultKafkaPort, RequestTimedOutError, KafkaError,
    NotCoordinatorForConsumerError, OffsetsLoadInProgressError, UnknownError,
    ConsumerCoordinatorNotAvailableError, CancelledError,
)
from .kafkacodec import KafkaCodec
from .brokerclient import _KafkaBrokerClient
from ._util import _coerce_topic
from ._util import _coerce_client_id
from ._util import _coerce_consumer_group

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class KafkaClient(object):
    """Cluster-aware Kafka client.

    This is the high-level client which most clients should use. It maintains
    a collection of :class:`~afkak.brokerclient._KafkaBrokerClient` objects,
    one each to the various hosts in the Kafka cluster and auto selects the
    proper one based on the topic and partition of the request. It maintains
    a map of topics/partitions to brokers.

    A KafkaClient object maintains connections (reconnected as needed) to the
    various brokers.  It must be bootstrapped with at least one host to
    retrieve the cluster metadata.

    :ivar reactor:
        Twisted reactor, as passed to the constructor. This must implement
        :class:`~twisted.internet.interfaces.IReactorTime` and
        :class:`~twisted.internet.interfaces.IReactorTCP`.
    :ivar str clientId:
        A short string used to identify the client to the server. This may
        appear in log messages on the server side.
    :ivar clients:
        Map of (host, port) tuples to :class:`_KafkaBrokerClient` instances.
    :type clients:
        :class:`dict` of (:class:`str`, :class:`int`) to
        :class:`_KafkaBrokerClient`
    """

    # This is the __CLIENT_SIDE__ timeout that's used when making requests
    # to our brokerclients. If a request doesn't return within this amount
    # of time, we errback() the deferred. This is _NOT_ the server-side
    # timeout which is passed into the send_{produce,fetch}_request methods
    # which have defaults set below. This one should be larger, btw :-)
    DEFAULT_REQUEST_TIMEOUT_MSECS = 10000
    # Default timeout msec for fetch requests. This is how long the server
    # will wait trying to get enough bytes of messages to fulfill the fetch
    # request. When this times out on the server side, it sends back a
    # response with as many bytes of messages as it has. See the docs for
    # more caveats on this timeout.
    DEFAULT_FETCH_SERVER_WAIT_MSECS = 5000
    # Default minimum amount of message bytes sent back on a fetch request
    DEFAULT_FETCH_MIN_BYTES = 4096
    # Default number of msecs the lead-broker will wait for replics to
    # ack Produce requests before failing the request
    DEFAULT_REPLICAS_ACK_MSECS = 1000

    clientId = u"afkak-client"
    _clientIdBytes = clientId.encode()

    def __init__(self, hosts, clientId=None,
                 timeout=DEFAULT_REQUEST_TIMEOUT_MSECS,
                 disconnect_on_timeout=False,
                 correlation_id=0,
                 reactor=None):

        if timeout is not None:
            if not isinstance(timeout, Real):
                raise TypeError(
                    "Timeout value: {!r} of type: {!s} is invalid. Must be "
                    "None or Real.".format(timeout, type(timeout)))
            timeout /= 1000.0  # msecs to secs
        self.timeout = timeout

        if clientId is not None:
            self.clientId = clientId
            self._clientIdBytes = _coerce_client_id(clientId)

        # Setup all our initial attributes
        self.clients = {}  # (host,port) -> _KafkaBrokerClient instance
        self.topics_to_brokers = {}  # TopicAndPartition -> BrokerMetadata
        self.partition_meta = {}  # TopicAndPartition -> PartitionMetadata
        self.consumer_group_to_brokers = {}  # consumer_group -> BrokerMetadata
        self.coordinator_fetches = {}  # consumer_group -> deferred
        self.topic_partitions = {}  # topic_id -> [0, 1, 2, ...]
        self.topic_errors = {}  # topic_id -> topic_error_code
        self.correlation_id = correlation_id
        self.close_dlist = None  # Deferred wait on broker client disconnects
        # Do we disconnect brokerclients when requests via them timeout?
        self._disconnect_on_timeout = disconnect_on_timeout
        self._brokers = {}  # Broker-NodeID -> BrokerMetadata
        self._topics = {}  # Topic-Name -> TopicMetadata
        self._closing = False  # Are we shutting down/shutdown?
        self.update_cluster_hosts(hosts)  # Store hosts and mark for lookup
        # clock/reactor for testing...
        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor

    @property
    def clock(self):
        # TODO: Deprecate this
        return self.reactor

    def __repr__(self):
        """return a string representing this KafkaClient."""
        return '<KafkaClient clientId={0} brokers={1} timeout={2}>'.format(
            self.clientId,
            sorted(self.clients.keys()),
            self.timeout,
        )

    def update_cluster_hosts(self, hosts):
        """Advise the Afkak client of possible changes to Kafka cluster hosts

        In general Afkak will keep up with changes to the cluster, but in
        a Docker environment where all the nodes in the cluster may change IP
        address at once or in quick succession Afkak may lose connections to
        all of the brokers.  This function lets you notify the Afkak client
        that some or all of the brokers may have changed. Afkak will compare
        the new list to the old and make new connections as needed.

        Parameters
        ==========
        hosts:
            (string|[string]) Hosts as a single comma separated
            "host[:port][,host[:port]]+" string, or a list of strings:
            ["host[:port]", ...]

        Return
        ======
        None
        """
        self._hosts = hosts
        self._collect_hosts_d = True

    def reset_topic_metadata(self, *topics):
        topics = tuple(_coerce_topic(t) for t in topics)
        for topic in topics:
            try:
                partitions = self.topic_partitions[topic]
            except KeyError:
                continue

            for partition in partitions:
                self.topics_to_brokers.pop(
                    TopicAndPartition(topic, partition), None)

            del self.topic_partitions[topic]
            if topic in self.topic_errors:
                del self.topic_errors[topic]

    def reset_consumer_group_metadata(self, *groups):
        """Reset cache of what broker manages the offset for specified groups

        Remove the cache of what Kafka broker should be contacted when
        fetching or updating the committed offsets for a given consumer
        group or groups.

        NOTE: Does not cancel any outstanding requests for updates to the
        consumer group metadata for the specified groups.
        """
        groups = tuple(_coerce_consumer_group(g) for g in groups)
        for group in groups:
            if group in self.consumer_group_to_brokers:
                del self.consumer_group_to_brokers[group]

    def reset_all_metadata(self):
        self.topics_to_brokers.clear()
        self.topic_partitions.clear()
        self.topic_errors.clear()
        self.consumer_group_to_brokers.clear()

    def has_metadata_for_topic(self, topic):
        return _coerce_topic(topic) in self.topic_partitions

    def metadata_error_for_topic(self, topic):
        return self.topic_errors.get(
            _coerce_topic(topic), UnknownTopicOrPartitionError.errno)

    def partition_fully_replicated(self, topic_and_part):
        if topic_and_part not in self.partition_meta:
            return False
        part_meta = self.partition_meta[topic_and_part]
        return len(part_meta.replicas) == len(part_meta.isr)

    def topic_fully_replicated(self, topic):
        """
        Determine if the given topic is fully replicated according to the
        currently known cluster metadata.

        .. note::

            This relies on cached cluster metadata. You may call
            :meth:`load_metadata_for_topics()` first to refresh this cache.

        :param str topic: Topic name

        :returns:
            A boolean indicating that:

            1. The number of partitions in the topic is non-zero.
            2. For each partition, all replicas are in the in-sync replica
               (ISR) set.
        :rtype: :class:`bool`
        """
        topic = _coerce_topic(topic)
        if topic not in self.topic_partitions:
            return False
        if not self.topic_partitions[topic]:
            # Don't consider an empty partition list 'fully replicated'
            return False
        return all(
            self.partition_fully_replicated(TopicAndPartition(topic, p))
            for p in self.topic_partitions[topic]
        )

    def close(self):
        # If we're already waiting on an/some outstanding disconnects
        # make sure we continue to wait for them...
        log.debug("%r: close", self)
        self._closing = True
        # Close down any clients we have
        self._close_brokerclients(self.clients.keys())
        # clean up other outstanding operations
        self.reset_all_metadata()
        return self.close_dlist

    def load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics.  This function is
        called lazily whenever metadata is unavailable.

        :param topics:
            The topics for which to fetch metadata (topic name as
            :class:`str`). Metadata for *all* topics is fetched when no topic
            is specified.
        :returns:
            :class:`Deferred` for the completion of the metadata fetch.
            This will resolve with ``True`` on success, ``None`` on
            cancellation, or fail with an exception on error.

            On success, topic metadata is available from the attributes of
            :class:`KafkaClient`: :data:`~KafkaClient.topic_partitions`,
            :data:`~KafkaClient.topics_to_brokers`, etc.
        """
        topics = tuple(_coerce_topic(t) for t in topics)
        log.debug("%r: load_metadata_for_topics: %r", self, topics)
        fetch_all_metadata = not topics

        # create the request
        requestId = self._next_id()
        request = KafkaCodec.encode_metadata_request(self._clientIdBytes,
                                                     requestId, topics)

        # Callbacks for the request deferred...
        def _handleMetadataResponse(response):
            # Decode the response
            (brokers, topics) = \
                KafkaCodec.decode_metadata_response(response)
            log.debug("%r: Broker/Topic metadata: %r/%r",
                      self, brokers, topics)

            # If we fetched the metadata for all topics, then store away the
            # received metadata for diagnostics.
            if fetch_all_metadata:
                self._brokers = brokers
                self._topics = topics

            # Iff we were fetching for all topics, and we got at least one
            # broker back, then remove brokers when we update our brokers
            ok_to_remove = (fetch_all_metadata and len(brokers))
            # Take the metadata we got back, update our self.clients, and
            # if needed disconnect or connect from/to old/new brokers
            self._update_brokers(
                [(nativeString(b.host), b.port) for b in brokers.values()],
                remove=ok_to_remove,
            )

            # Now loop through all the topics/partitions in the response
            # and setup our cache/data-structures
            for topic, topic_metadata in topics.items():
                _, topic_error, partitions = topic_metadata
                self.reset_topic_metadata(topic)
                self.topic_errors[topic] = topic_error
                if not partitions:
                    log.warning('No partitions for %s, Err:%d',
                                topic, topic_error)
                    continue

                self.topic_partitions[topic] = []
                for partition, meta in partitions.items():
                    self.topic_partitions[topic].append(partition)
                    topic_part = TopicAndPartition(topic, partition)
                    self.partition_meta[topic_part] = meta
                    if meta.leader == -1:
                        log.warning('No leader for topic %s partition %s',
                                    topic, partition)
                        self.topics_to_brokers[topic_part] = None
                    else:
                        self.topics_to_brokers[
                            topic_part] = brokers[meta.leader]
                self.topic_partitions[topic] = sorted(
                    self.topic_partitions[topic])
            return True

        def _handleMetadataErr(err):
            # This should maybe do more cleanup?
            if err.check(t_CancelledError, CancelledError):
                # Eat the error
                return None
            log.error("Failed to retrieve metadata:%s", err)
            raise KafkaUnavailableError(
                "Unable to load metadata from configured "
                "hosts: {!r}".format(err))

        # Send the request, add the handlers
        d = self._send_broker_unaware_request(requestId, request)
        d.addCallbacks(_handleMetadataResponse, _handleMetadataErr)
        return d

    def load_consumer_metadata_for_group(self, group):
        """
        Determine broker for the consumer metadata for the specified group

        Returns a deferred which callbacks with True if the group's coordinator
        could be determined, or errbacks with
        ConsumerCoordinatorNotAvailableError if not.

        Parameters
        ----------
        group:
            group name as `str`
        """
        group = _coerce_consumer_group(group)
        log.debug("%r: load_consumer_metadata_for_group: %r", self, group)

        # If we are already loading the metadata for this group, then
        # just return the outstanding deferred
        if group in self.coordinator_fetches:
            return self.coordinator_fetches[group]

        # No outstanding request, create a new one
        requestId = self._next_id()
        request = KafkaCodec.encode_consumermetadata_request(
            self._clientIdBytes, requestId, group)

        # Callbacks for the request deferred...
        def _handleConsumerMetadataResponse(response, group):
            # Clear the outstanding fetch
            self.coordinator_fetches.pop(group, None)
            # Decode the response (returns ConsumerMetadataResponse)
            c_m_resp = KafkaCodec.decode_consumermetadata_response(response)
            log.debug("%r: c_m_resp: %r", self, c_m_resp)
            if c_m_resp.error:
                # Raise the appropriate error
                resp_err = BrokerResponseError.errnos.get(
                    c_m_resp.error, UnknownError)(c_m_resp)
                raise resp_err

            self.consumer_group_to_brokers[group] = \
                BrokerMetadata(c_m_resp.node_id, c_m_resp.host,
                               c_m_resp.port)
            return True

        def _handleConsumerMetadataErr(err, group):
            # Clear the outstanding fetch
            self.coordinator_fetches.pop(group, None)
            log.error("Failed to retrieve consumer metadata "
                      "for group: %s Error:%r", group, err)
            # Clear any stored value for the group's coordinator
            self.reset_consumer_group_metadata(group)
            raise ConsumerCoordinatorNotAvailableError(
                "Coordinator for group: %s not available" % (group))

        # Send the request, add the handlers
        d = self._send_broker_unaware_request(requestId, request)
        # Save the deferred under the fetches for this group
        self.coordinator_fetches[group] = d
        d.addCallback(_handleConsumerMetadataResponse, group)
        d.addErrback(_handleConsumerMetadataErr, group)
        return d

    @inlineCallbacks
    def send_produce_request(self, payloads=None, acks=1,
                             timeout=DEFAULT_REPLICAS_ACK_MSECS,
                             fail_on_error=True, callback=None):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then
        sent to a specific broker. Output is a list of responses in the
        same order as the list of payloads specified

        Parameters
        ----------
        payloads:
            list of ProduceRequest
        acks:
            How many Kafka broker replicas need to write before
            the leader replies with a response
        timeout:
            How long the server has to receive the acks from the
            replicas before returning an error.
        fail_on_error:
            boolean, should we raise an Exception if we encounter an API error?
        callback:
            function, instead of returning the ProduceResponse,
            first pass it through this function

        Return
        ------
        a deferred which callbacks with a list of ProduceResponse

        Raises
        ------
        FailedPayloadsError, LeaderUnavailableError, PartitionUnavailableError
        """

        encoder = partial(
            KafkaCodec.encode_produce_request,
            acks=acks,
            timeout=timeout)

        if acks == 0:
            decoder = None
        else:
            decoder = KafkaCodec.decode_produce_response

        resps = yield self._send_broker_aware_request(
            payloads, encoder, decoder)

        returnValue(self._handle_responses(resps, fail_on_error, callback))

    @inlineCallbacks
    def send_fetch_request(self, payloads=None, fail_on_error=True,
                           callback=None,
                           max_wait_time=DEFAULT_FETCH_SERVER_WAIT_MSECS,
                           min_bytes=DEFAULT_FETCH_MIN_BYTES):
        """
        Encode and send a FetchRequest

        Payloads are grouped by topic and partition so they can be pipelined
        to the same brokers.

        Raises
        ======
        FailedPayloadsError, LeaderUnavailableError, PartitionUnavailableError
        """
        if self.timeout is not None and (
                max_wait_time / 1000) > (self.timeout - 0.1):
            raise ValueError(
                "%r: max_wait_time: %d must be less than client.timeout by "
                "at least 100 milliseconds.", self, max_wait_time)

        encoder = partial(KafkaCodec.encode_fetch_request,
                          max_wait_time=max_wait_time,
                          min_bytes=min_bytes)

        # resps is a list of FetchResponse() objects, each of which can hold
        # 1-n messages.
        resps = yield self._send_broker_aware_request(
            payloads, encoder,
            KafkaCodec.decode_fetch_response)

        returnValue(self._handle_responses(resps, fail_on_error, callback))

    @inlineCallbacks
    def send_offset_request(self, payloads=None, fail_on_error=True,
                            callback=None):
        resps = yield self._send_broker_aware_request(
            payloads,
            KafkaCodec.encode_offset_request,
            KafkaCodec.decode_offset_response)

        returnValue(self._handle_responses(resps, fail_on_error, callback))

    @inlineCallbacks
    def send_offset_fetch_request(self, group, payloads=None,
                                  fail_on_error=True, callback=None):
        """
        Takes a group (string) and list of OffsetFetchRequest and returns
        a list of OffsetFetchResponse objects
        """
        encoder = partial(KafkaCodec.encode_offset_fetch_request,
                          group=group)
        decoder = KafkaCodec.decode_offset_fetch_response
        resps = yield self._send_broker_aware_request(
            payloads, encoder, decoder, consumer_group=group)

        returnValue(self._handle_responses(
            resps, fail_on_error, callback, group))

    @inlineCallbacks
    def send_offset_commit_request(self, group, payloads=None,
                                   fail_on_error=True, callback=None,
                                   group_generation_id=-1,
                                   consumer_id=''):
        """Send a list of OffsetCommitRequests to the Kafka broker for the
        given consumer group.

        Args:
          group (str): The consumer group to which to commit the offsets
          payloads ([OffsetCommitRequest]): List of topic, partition, offsets
            to commit.
          fail_on_error (bool): Whether to raise an exception if a response
            from the Kafka broker indicates an error
          callback (callable): a function to call with each of the responses
            before returning the returned value to the caller.
          group_generation_id (int): Must currently always be -1
          consumer_id (str): Must currently always be empty string
        Returns:
          [OffsetCommitResponse]: List of OffsetCommitResponse objects.
          Will raise KafkaError for failed requests if fail_on_error is True
        """
        group = _coerce_consumer_group(group)
        encoder = partial(KafkaCodec.encode_offset_commit_request,
                          group=group, group_generation_id=group_generation_id,
                          consumer_id=consumer_id)
        decoder = KafkaCodec.decode_offset_commit_response
        resps = yield self._send_broker_aware_request(
            payloads, encoder, decoder, consumer_group=group)

        returnValue(self._handle_responses(
            resps, fail_on_error, callback, group))

    # # # Private Methods # # #

    def _handle_responses(self, responses, fail_on_error, callback=None,
                          consumer_group=None):
        out = []
        for resp in responses:
            try:
                _check_error(resp)
            except (UnknownTopicOrPartitionError, NotLeaderForPartitionError):
                log.error('Error found in response: %s', resp)
                self.reset_topic_metadata(resp.topic)
                if fail_on_error:
                    raise
            except (OffsetsLoadInProgressError,
                    NotCoordinatorForConsumerError,
                    ConsumerCoordinatorNotAvailableError):
                log.error('Error found in response: %s Consumer Group: %s',
                          resp, consumer_group)
                self.reset_consumer_group_metadata(consumer_group)
                if fail_on_error:
                    raise

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def _get_brokerclient(self, host, port):
        """
        Get or create a connection to a broker using host and port.
        Returns the broker immediately, but the broker may have just been
        created and be in an unconnected state. The broker will connect on
        an as-needed basis when processing a request.
        """
        host_key = (nativeString(host), port)
        if host_key not in self.clients:
            # We don't have a brokerclient for that host/port, create one,
            # ask it to connect
            log.debug("%r: creating client for %s:%d", self, host, port)
            self.clients[host_key] = _KafkaBrokerClient(
                self.reactor, host, port, self.clientId,
                subscriber=self._update_broker_state,
            )
        return self.clients[host_key]

    def _update_broker_state(self, broker, connected, reason):
        """
        Handle updates of a broker's connection state.  If we get an update
        with a state other than 'connected', reset our metadata, as it
        indicates that a connection to one of our brokers ended, or failed to
        come up correctly
        """
        def _md_load_on_disconnect_failure(result):
            log.debug('Attempt to fetch Kafka metadata after '
                      'disconnect failed with: %r', result)

        state = "Connected" if connected else "Disconnected"
        log.debug(
            "Broker:%r state changed:%s for reason:%r", broker, state, reason)
        # If one of our broker clients disconnected, there may be a metadata
        # change. Make sure we check...
        if not connected:
            self.reset_all_metadata()
            if not self._closing:
                # If we're not shutting down, and we're not already doing a
                # lookup, then mark ourselves as needing to re-resolve, and
                # then start a metadata lookup, which will do the lookup as
                # needed...
                if self._collect_hosts_d is None:
                    self._collect_hosts_d = True
                d = self.load_metadata_for_topics()
                d.addErrback(_md_load_on_disconnect_failure)

    def _close_brokerclients(self, brokers):
        """
        Pop each of the supplied brokers from self.clients
        Close that broker, and manage the completion of those operations
        """
        def _log_close_failure(failure, brokerclient):
            log.debug(
                'BrokerClient: %s close result: %s: %s', brokerclient,
                failure.type.__name__, failure.getErrorMessage())

        def _clean_close_dlist(result, close_dlist):
            # If there aren't any other outstanding closings going on, then
            # close_dlist == self.close_dlist, and we can reset it.
            if close_dlist == self.close_dlist:
                self.close_dlist = None

        if not self.close_dlist:
            dList = []
        else:
            log.debug("%r: _update_brokers has nested deferredlist: %r",
                      self, self.close_dlist)
            dList = [self.close_dlist]
        for broker in list(brokers):
            # broker better be in self.clients if not, weirdness
            brokerClient = self.clients.pop(broker)
            log.debug("Calling close on: %r", brokerClient)
            dList.append(
                brokerClient.close().addErrback(
                    _log_close_failure, brokerClient))
        self.close_dlist = DeferredList(dList)
        self.close_dlist.addBoth(_clean_close_dlist, self.close_dlist)

    def _update_brokers(self, new_brokers, remove=False):
        """ Update our self.clients based on brokers in received metadata
        Take the received dict of brokers and reconcile it with our current
        list of brokers (self.clients). If there is a new one, bring up a new
        connection to it, and if remove is True, and any in our current list
        aren't in the metadata returned, disconnect from it.
        """
        log.debug("%r: _update_brokers: %r remove: %r",
                  self, new_brokers, remove)

        # Work with the brokers as sets
        new_brokers = set(new_brokers)
        current_brokers = set(self.clients.keys())

        # set of added
        added_brokers = new_brokers - current_brokers
        # removed
        removed_brokers = current_brokers - new_brokers

        # Create any new brokers based on the new metadata
        for broker in added_brokers:
            self._get_brokerclient(*broker)

        # Disconnect and remove from self.clients any removed brokerclients
        if remove and removed_brokers:
            self._close_brokerclients(removed_brokers)

    @inlineCallbacks
    def _get_leader_for_partition(self, topic, partition):
        """
        Returns the leader for a partition or None if the partition exists
        but has no leader.

        PartitionUnavailableError will be raised if the topic or partition
        is not part of the metadata.
        """

        key = TopicAndPartition(topic, partition)
        # reload metadata whether the partition is not available
        # or has no leader (broker is None)
        if self.topics_to_brokers.get(key) is None:
            yield self.load_metadata_for_topics(topic)

        if key not in self.topics_to_brokers:
            raise PartitionUnavailableError("%s not available" % str(key))

        returnValue(self.topics_to_brokers[key])

    @inlineCallbacks
    def _get_coordinator_for_group(self, consumer_group):
        """Returns the coordinator (broker) for a consumer group

        Returns the broker for a given consumer group or
        Raises ConsumerCoordinatorNotAvailableError
        """
        if self.consumer_group_to_brokers.get(consumer_group) is None:
            yield self.load_consumer_metadata_for_group(consumer_group)

        returnValue(self.consumer_group_to_brokers.get(consumer_group))

    def _next_id(self):
        """Generate a new correlation id."""
        # modulo to keep within int32 (signed)
        self.correlation_id = (self.correlation_id + 1) % 2**31
        return self.correlation_id

    def _make_request_to_broker(self, broker, requestId, request, **kwArgs):
        """Send a request to the specified broker."""
        def _timeout_request(broker, requestId):
            """The time we allotted for the request expired, cancel it."""
            try:
                broker.cancelRequest(requestId, reason=RequestTimedOutError(
                    'Request: {} cancelled due to timeout'.format(requestId)))
            except KeyError:  # pragma: no cover This should never happen...
                log.exception('ERROR: Failed to find key for timed-out '
                              'request. Broker: %r Req: %d',
                              broker, requestId)
                raise
            if self._disconnect_on_timeout:
                broker.disconnect()

        def _alert_blocked_reactor(timeout, start):
            """Complain if this timer didn't fire before the timeout elapsed"""
            now = self.reactor.seconds()
            if now >= (start + timeout):
                log.error('Reactor was starved for %f seconds during request.',
                          now - start)

        def _cancel_timeout(_, dc):
            """Request completed/cancelled, cancel the timeout delayedCall."""
            if dc.active():
                dc.cancel()
            return _

        # Make the request to the specified broker
        log.debug('_mrtb: sending request: %d to broker: %r',
                  requestId, broker)
        d = broker.makeRequest(requestId, request, **kwArgs)
        if self.timeout is not None:
            # Set a delayedCall to fire if we don't get a reply in time
            dc = self.reactor.callLater(
                self.timeout, _timeout_request, broker, requestId)
            # Set a delayedCall to complain if the reactor has been blocked
            rc = self.reactor.callLater(
                (self.timeout * 0.9), _alert_blocked_reactor, self.timeout,
                self.reactor.seconds())
            # Setup a callback on the request deferred to cancel both callLater
            d.addBoth(_cancel_timeout, dc)
            d.addBoth(_cancel_timeout, rc)
        return d

    @inlineCallbacks
    def _send_broker_unaware_request(self, requestId, request):
        """
        Attempt to send a broker-agnostic request to one of the available
        brokers. Keep trying until you succeed, or run out of hosts to try
        """

        # Check if we've had a condition which indicates we might need to
        # re-resolve the IPs of our hosts
        if self._collect_hosts_d:
            if self._collect_hosts_d is True:
                # Lookup needed, but not yet started. Start it.
                self._collect_hosts_d = _collect_hosts(self._hosts)
            broker_list = yield self._collect_hosts_d
            self._collect_hosts_d = None
            if broker_list:
                self._update_brokers(broker_list, remove=True)
            else:
                # Lookup of all hosts returned no IPs. Log an error, setup
                # to retry lookup, and try to continue with the brokers we
                # already have...
                log.error('Failed to resolve hosts: %r', self._hosts)
                self._collect_hosts_d = True

        brokers = list(self.clients.values())
        # Randomly shuffle the brokers to distribute the load, but
        random.shuffle(brokers)
        # Prioritize connected brokers
        brokers.sort(reverse=True, key=lambda broker: broker.connected())
        for broker in brokers:
            try:
                log.debug('_sbur: sending request: %d to broker: %r',
                          requestId, broker)
                d = self._make_request_to_broker(broker, requestId, request)
                resp = yield d
                returnValue(resp)
            except KafkaError as e:
                log.warning("Could not makeRequest id:%d [%r] to server %s:%i, "
                            "trying next server. Err: %r", requestId,
                            request, broker.host, broker.port, e)

        # Anytime we fail a request to every broker, setup for a re-resolve
        self._collect_hosts_d = True
        raise KafkaUnavailableError(
            "All servers (%r) failed to process request" % brokers)

    @inlineCallbacks
    def _send_broker_aware_request(self, payloads, encoder_fn, decode_fn,
                                   consumer_group=None):
        """
        Group a list of request payloads by topic+partition and send them to
        the leader broker for that partition using the supplied encode/decode
        functions

        Params
        ======
        payloads: list of object-like entities with a topic and
                  partition attribute. payloads must be grouped by
                  (topic, partition) tuples.
        encode_fn: a method to encode the list of payloads to a request body,
                   must accept client_id, correlation_id, and payloads as
                   keyword arguments
        decode_fn: a method to decode a response body into response objects.
                   The response objects must be object-like and have topic
                   and partition attributes
        consumer_group: [string], optional. Indicates the request should be
                   directed to the Offset Coordinator for the specified
                   consumer_group.

        Return
        ======
        deferred yielding a list of response objects in the same order
        as the supplied payloads, or None if decode_fn is None.

        Raises
        ======
        FailedPayloadsError, LeaderUnavailableError, PartitionUnavailableError,

        """

        # Calling this without payloads is nonsensical
        if not payloads:
            raise ValueError("Payloads parameter is empty")

        # Group the requests by topic+partition
        original_keys = []
        payloads_by_broker = collections.defaultdict(list)

        # Go through all the payloads, lookup the leader/coordinator for that
        # payload's topic/partition or consumer group. If there's no
        # leader/coordinator (broker), raise. For each broker, keep
        # a list of the payloads to be sent to it. Also, for each payload in
        # the list of payloads, make a corresponding list (original_keys) with
        # the topic/partition in the same order, so we can lookup the returned
        # result(s) by that topic/partition key in the set of returned results
        # and return them in a list the same order the payloads were supplied
        for payload in payloads:
            # get leader/coordinator, depending on consumer_group
            if consumer_group is None:
                leader = yield self._get_leader_for_partition(
                    payload.topic, payload.partition)
                if leader is None:
                    raise LeaderUnavailableError(
                        "Leader not available for topic %s partition %s" %
                        (payload.topic, payload.partition))
            else:
                leader = yield self._get_coordinator_for_group(consumer_group)
                if leader is None:
                    raise ConsumerCoordinatorNotAvailableError(
                        "Coordinator not available for group: %s" %
                        (consumer_group))

            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # The kafka server doesn't send replies to produce requests
        # with acks=0. In that case, our decode_fn will be
        # None, and we need to let the brokerclient know not
        # to expect a reply. makeRequest() returns a deferred
        # regardless, but in the expectResponse=False case, it will
        # fire as soon as the request is sent, and it can errBack()
        # due to being cancelled prior to the broker being able to
        # send the request.
        expectResponse = decode_fn is not None

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # Keep track of outstanding requests in a list of deferreds
        inFlight = []
        # and the payloads that go along with them
        payloadsList = []
        # For each broker, send the list of request payloads,
        for broker_meta, payloads in payloads_by_broker.items():
            broker = self._get_brokerclient(broker_meta.host, broker_meta.port)
            requestId = self._next_id()
            request = encoder_fn(client_id=self._clientIdBytes,
                                 correlation_id=requestId, payloads=payloads)

            # Make the request
            d = self._make_request_to_broker(broker, requestId, request,
                                             expectResponse=expectResponse)
            inFlight.append(d)
            payloadsList.append(payloads)

        # Wait for all the responses to come back, or the requests to fail
        results = yield DeferredList(inFlight, consumeErrors=True)
        # We now have a list of (succeeded, response/Failure) tuples. Check 'em
        for (success, response), payloads in zip(results, payloadsList):
            if not success:
                # The brokerclient deferred was errback()'d:
                #   The send failed, or this request was cancelled (by timeout)
                log.debug("%r: request:%r to broker failed: %r", self,
                          payloads, response)
                failed_payloads.extend([(p, response) for p in payloads])
                continue
            if not expectResponse:
                continue
            # Successful request/response. Decode it and store by topic/part
            for response in decode_fn(response):
                acc[(response.topic, response.partition)] = response

        # Order the accumulated responses by the original key order
        # Note that this scheme will throw away responses which we did
        # not request.  See test_send_fetch_request, where the response
        # includes an error, but for a topic/part we didn't request.
        # Since that topic/partition isn't in original_keys, we don't pass
        # it back from here and it doesn't error out.
        # If any of the payloads failed, fail
        responses = [acc[k] for k in original_keys if k in acc] if acc else []
        if failed_payloads:
            self.reset_all_metadata()
            raise FailedPayloadsError(responses, failed_payloads)

        returnValue(responses)


@inlineCallbacks
def _collect_hosts(hosts):
    """
    Resolve hostnames into (IPv4, port) tuples.

    :param hosts:
        A list or comma-separated string of hostnames which may also include
        port numbers. All of the following are valid::

            b'host'
            u'host'
            b'host:1234'
            u'host:1234,host:2345'
            b'host:1234 , host:2345 '
            [u'host1', b'host2']
            [b'host:1234', b'host:2345']

        Hostnames must be ASCII (IDN is not supported). The default Kafka port
        of 9092 is implied when no port is given.

    :returns:
        A list of unique (IPv4, port) tuples. For example::

            [('127.0.0.1', 9092), ('127.0.0.2', 9092)]

        If DNS resolution fails, an empty list is returned.

    :rtype: :class:`list` of (:class:`str`, :class:`int`) instances
    """
    if isinstance(hosts, bytes):
        hosts = hosts.split(b',')
    elif isinstance(hosts, _unicode):
        hosts = hosts.split(u',')
    result = set()
    for host_port in hosts:
        # FIXME This won't handle IPv6 addresses
        res = nativeString(host_port).split(':')
        host = res[0].strip()
        port = int(res[1].strip()) if len(res) > 1 else DefaultKafkaPort

        if isIPAddress(host):
            ip_addresses = [host]
        else:
            ip_addresses = yield _get_IP_addresses(host)
        result.update((address, port) for address in ip_addresses)
    returnValue(list(result))


@inlineCallbacks
def _get_IP_addresses(hostname):
    """
    Resolve a hostname to a list of IPv4 addresses.

    :param str hostname: hostname or IP address
    :returns: :class:`list` of :class:`str` IPv4 addresses
    """
    try:
        answers, auth, addit = yield DNSclient.lookupAddress(hostname)
    except Exception as exc:  # Too many different DNS failures to catch...
        log.exception('DNS Resolution failure: %r for name: %r', exc, hostname)
        returnValue([])

    returnValue(
        [answer.payload.dottedQuad()
            for answer in answers if answer.type == dns.A])
