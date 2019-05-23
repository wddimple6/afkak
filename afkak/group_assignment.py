# Copyright 2018 Ciena Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
import itertools


def round_robin_assignment(client, member_metadata):
    # the algorithm for this is copied from kafka-python
    all_topics = set()
    for metadata in member_metadata.values():
        all_topics.update(metadata.subscriptions)

    all_topic_partitions = []
    for topic in all_topics:
        partitions = client.topic_partitions.get(topic)
        if not partitions:
            raise ValueError('Topic {} has no partitions'.format(topic))
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
