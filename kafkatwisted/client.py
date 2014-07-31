from __future__ import absolute_import

import logging
import collections
from functools import partial
from itertools import count

from .common import (
    TopicAndPartition, ConnectionError, FailedPayloadsError,
    PartitionUnavailableError, LeaderUnavailableError, KafkaUnavailableError,
    UnknownTopicOrPartitionError, NotLeaderForPartitionError, check_error,
    DefaultKafkaPort,
)

from .kafkacodec import KafkaCodec
from .brokerclient import KafkaBrokerClient

# Twisted-related imports
from twisted.internet.defer import inlineCallbacks, returnValue

log = logging.getLogger("kafkaclient")

class KafkaClient(object):
    """
    This is the high-level client which most clients should use. It maintains
      a collection of KafkaBrokerClient objects, one each to the various
      hosts in the Kafka cluster and auto selects the proper one based on the
      topic & partition of the request. It maintains a map of
      topics/partitions => broker.
    A KafkaBrokerClient object maintains connections (reconnected as needed) to
      the various brokers.
    Must be bootstrapped with at least one host to retrieve the cluster
      metadata.
    """

    ID_GEN = count()
    DEFAULT_REQUEST_TIMEOUT_SECONDS = 5
    clientId = "kafka-twisted-client"

    dMetaDataLoad = None

    def __init__(self, hosts, clientId=None,
                 timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS):

        self.hosts = collect_hosts(hosts)
        self.timeout = timeout
        if clientId is not None:
            self.clientId = clientId

        # create connections, brokers, etc on demand...
        self.clients = {}  # (host,port) -> KafkaBrokerClient instance
        self.brokers = {}  # broker_id -> BrokerMetadata
        self.topics_to_brokers = {}  # topic_id -> broker_id
        self.topic_partitions = {}  # topic_id -> [0, 1, 2, ...]

        # Start the load of the metadata
        self.dMetaDataLoad = self.load_metadata_for_topics()

    def _get_brokerclient(self, host, port):
        "Get or create a connection to a broker using host and port"
        host_key = (host, port)
        if host_key not in self.clients:
            # We don't have a brokerclient for that host/port, create one
            self.clients[host_key] = KafkaBrokerClient(
                host, port, timeout=self.timeout)

        return self.clients[host_key]

    @inlineCallbacks
    def _get_leader_for_partition(self, topic, partition):
        """
        Returns the leader for a partition or None if the partition exists
        but has no leader.

        PartitionUnavailableError will be raised if the topic or partition
        is not part of the metadata.
        """

        print "ZORG: get_leader_for_partition1"
        key = TopicAndPartition(topic, partition)
        # reload metadata whether the partition is not available
        # or has no leader (broker is None)
        if self.topics_to_brokers.get(key) is None:
            yield self.load_metadata_for_topics(topic)

        print "ZORG: get_leader_for_partition2"
        if key not in self.topics_to_brokers:
            print "ZORG: get_leader_for_partition3"
            raise PartitionUnavailableError("%s not available" % str(key))

        returnValue(self.topics_to_brokers[key])

    def _next_id(self):
        """
        Generate a new correlation id
        """
        return KafkaClient.ID_GEN.next()

    def _send_broker_unaware_request(self, requestId, request):
        """
        Attempt to send a broker-agnostic request to one of the available
        brokers. Keep trying until you succeed, or run out of hosts to try
        """
        hostlist = self.hosts
        for (host, port) in hostlist:
            try:
                broker = self._get_brokerclient(host, port)
                return broker.makeRequest(requestId, request)
            except Exception as e:
                log.warning("Could not makeRequest [%r] to server %s:%i, "
                            "trying next server. Err: %s",
                            request, host, port, e)

        raise KafkaUnavailableError("All servers failed to process request")

    @inlineCallbacks
    def _send_broker_aware_request(self, payloads, encoder_fn, decoder_fn):
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

        Return
        ======
        List of deferreds in the same order as the supplied payloads
        """

        # Group the requests by topic+partition
        original_keys = []
        payloads_by_broker = collections.defaultdict(list)

        for payload in payloads:
            leader = yield self._get_leader_for_partition(
                payload.topic, payload.partition)
            if leader is None:
                raise LeaderUnavailableError(
                    "Leader not available for topic %s partition %s" %
                    (payload.topic, payload.partition))

            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            broker = self._get_brokerclient(broker.host, broker.port)
            requestId = self._next_id()
            request = encoder_fn(client_id=self.clientId,
                                 correlation_id=requestId, payloads=payloads)

            # The kafka server doesn't send replies to produce requests
            # with acks=0. In that case, our decoder_fn will be
            # None, and we need to let the brokerclient know not
            # to expect a reply. makeRequest() returns a deferred
            # regardless, but in the expectReply=False case, it will
            # never fire, but it can errBack()
            d = broker.makeRequest(
                requestId, request, expectReply=(decoder_fn is not None))

            d.addCallbacks(self.handleResponse, self.handleRequestErr,
                           callbackArgs=(), errbackArgs=())

            # Nothing to process the reply
            if decoder_fn is None:
                continue

            for response in decoder_fn(response):
                acc[(response.topic, response.partition)] = response

        if failed_payloads:
            raise FailedPayloadsError(failed_payloads)

        # Order the accumulated responses by the original key order
        returnValue((acc[k] for k in original_keys) if acc else ())

    def __repr__(self):
        return '<KafkaClient clientId=%s>' % (self.clientId)

    def _raise_on_response_error(self, resp):
        try:
            check_error(resp)
        except (UnknownTopicOrPartitionError, NotLeaderForPartitionError) as e:
            log.exception('Error found in response:', resp, e)
            self.reset_topic_metadata(resp.topic)
            raise

    #################
    #   Public API  #
    #################
    def reset_topic_metadata(self, *topics):
        for topic in topics:
            try:
                partitions = self.topic_partitions[topic]
            except KeyError:
                continue

            for partition in partitions:
                self.topics_to_brokers.pop(
                    TopicAndPartition(topic, partition), None)

            del self.topic_partitions[topic]

    def reset_all_metadata(self):
        self.topics_to_brokers.clear()
        self.topic_partitions.clear()

    def has_metadata_for_topic(self, topic):
        return topic in self.topic_partitions

    def close(self):
        for conn in self.clients.values():
            conn.disconnect()

    def load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics.
        This function is called lazily whenever metadata is unavailable.
        """
        # First check if we're already trying to load...
        if self.dMetaDataLoad is not None and not self.dMetaDataLoad.called:
            # We're already loading, so just return the current deferred
            return self.dMetaDataLoad

        # create the request
        request_id = self._next_id()
        request = KafkaCodec.encode_metadata_request(
            self.clientId, request_id, topics)

        # Callbacks for the request deferred...
        def handleMetadataResponse(response):
            # Clear self.dMetaDataLoad so new calls will refetch
            self.dMetaDataLoad = None

            # Decode the response
            (brokers, topics) = \
                KafkaCodec.decode_metadata_response(response)

            log.debug("Broker metadata: %s", brokers)
            log.debug("Topic metadata: %s", topics)

            self.brokers = brokers

            for topic, partitions in topics.items():
                self.reset_topic_metadata(topic)

                if not partitions:
                    log.warning('No partitions for %s', topic)
                    continue

                self.topic_partitions[topic] = []
                for partition, meta in partitions.items():
                    self.topic_partitions[topic].append(partition)
                    topic_part = TopicAndPartition(topic, partition)
                    if meta.leader == -1:
                        log.warning('No leader for topic %s partition %s',
                                    topic, partition)
                        self.topics_to_brokers[topic_part] = None
                    else:
                        self.topics_to_brokers[topic_part] = \
                            brokers[meta.leader]

        def handleMetadataErr(err):
            # This should maybe do more cleanup?
            log.error("Failed to retrieve metadata", err)
            # Clear self.dMetaDataLoad so new calls will refetch
            self.dMetaDataLoad = None
            raise KafkaUnavailableError(
                "All servers failed to process request")


        # Send the request, add the handlers, save the deferred so we
        # can just return it if someone calls us again before the request
        # has completed...
        d = self._send_broker_unaware_request(request_id, request)
        d.addCallbacks(handleMetadataResponse, handleMetadataErr)
        self.dMetaDataLoad = d
        return d

    def send_produce_request(self, payloads=[], acks=1, timeout=1000,
                             fail_on_error=True, callback=None):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then
        sent to a specific broker. Output is a list of responses in the
        same order as the list of payloads specified

        Params
        ======
        payloads: list of ProduceRequest
        fail_on_error: boolean, should we raise an Exception if we
                       encounter an API error?
        callback: function, instead of returning the ProduceResponse,
                  first pass it through this function

        Return
        ======
        list of ProduceResponse or callback(ProduceResponse), in the
        order of input payloads
        """

        encoder = partial(
            KafkaCodec.encode_produce_request,
            acks=acks,
            timeout=timeout)

        if acks == 0:
            decoder = None
        else:
            decoder = KafkaCodec.decode_produce_response

        resps = self._send_broker_aware_request(payloads, encoder, decoder)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_fetch_request(self, payloads=[], fail_on_error=True,
                           callback=None, max_wait_time=100, min_bytes=4096):
        """
        Encode and send a FetchRequest

        Payloads are grouped by topic and partition so they can be pipelined
        to the same brokers.
        """

        encoder = partial(KafkaCodec.encode_fetch_request,
                          max_wait_time=max_wait_time,
                          min_bytes=min_bytes)

        resps = self._send_broker_aware_request(
            payloads, encoder,
            KafkaCodec.decode_fetch_response)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_request(self, payloads=[], fail_on_error=True,
                            callback=None):
        resps = self._send_broker_aware_request(
            payloads,
            KafkaCodec.encode_offset_request,
            KafkaCodec.decode_offset_response)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_commit_request(self, group, payloads=[],
                                   fail_on_error=True, callback=None):
        encoder = partial(KafkaCodec.encode_offset_commit_request,
                          group=group)
        decoder = KafkaCodec.decode_offset_commit_response
        resps = self._send_broker_aware_request(payloads, encoder, decoder)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_fetch_request(self, group, payloads=[],
                                  fail_on_error=True, callback=None):

        encoder = partial(KafkaCodec.encode_offset_fetch_request,
                          group=group)
        decoder = KafkaCodec.decode_offset_fetch_response
        resps = self._send_broker_aware_request(payloads, encoder, decoder)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionally
    randomize the returned list.
    """
    if isinstance(hosts, basestring):
        hosts = hosts.strip().split(',')

    result = []
    for host_port in hosts:
        res = host_port.split(':')
        host = res[0]
        port = int(res[1]) if len(res) > 1 else DefaultKafkaPort
        result.append((host.strip(), port))

    if randomize:
        from random import shuffle
        shuffle(result)

    return result
