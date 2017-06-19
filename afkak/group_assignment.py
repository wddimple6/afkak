import itertools
import collections
import six
import logging

log = logging.getLogger(__name__)


def round_robin_assignment(client, member_metadata):
    # the algorithm for this is copied from kafka-python
    all_topics = set()
    for metadata in six.itervalues(member_metadata):
        all_topics.update(metadata.subscriptions)

    all_topic_partitions = []
    for topic in all_topics:
        partitions = client.topic_partitions.get(topic)
        if partitions is None:
            log.warning('No partition metadata for topic %s', topic)
            continue
        for partition in partitions:
            all_topic_partitions.append((topic, partition))
    all_topic_partitions.sort()

    # construct {member_id: {topic: [partition, ...]}}
    assignment = collections.defaultdict(lambda: collections.defaultdict(list))

    member_iter = itertools.cycle(sorted(member_metadata.keys()))
    for (topic, partition) in all_topic_partitions:
        member_id = next(member_iter)

        # Because we constructed all_topic_partitions from the set of
        # member subscribed topics, we should be safe assuming that
        # each topic in all_topic_partitions is in at least one member
        # subscription; otherwise this could yield an infinite loop
        while topic not in member_metadata[member_id].subscriptions:
            member_id = next(member_iter)
        assignment[member_id][topic].append(partition)

    return assignment
