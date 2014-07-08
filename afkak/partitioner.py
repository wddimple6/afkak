from itertools import cycle
from random import randint


class Partitioner(object):
    """
    Base class for a partitioner
    """
    def __init__(self, topic, partitions):
        """
        Initialize the partitioner

        partitions - A list of available partitions (during startup)
        """
        self.topic = topic
        self.partitions = partitions

    def partition(self, key, partitions):
        """
        Takes a key (string) and partitions (list) as argument and returns
        a partition to be used for the message

        partitions - The list of partitions is passed in every call. This
                     may look like an overhead, but it will be useful
                     (in future) when we handle cases like rebalancing
        """
        raise NotImplementedError('partition function has to be implemented')


class RoundRobinPartitioner(Partitioner):
    """
    Implements a round robin partitioner which sends data to partitions
    in a round robin fashion. Also supports starting each new partitioner
    at a random offset into the cycle of partitions
    """
    randomStart = False

    @classmethod
    def set_random_start(cls, randomStart):
        cls.randomStart = randomStart

    def __init__(self, topic, partitions):
        super(RoundRobinPartitioner, self).__init__(topic, partitions)
        self._set_partitions(partitions)

    def __repr__(self):
        return '<RoundRobinPartitioner {}:{}>'.format(self.randomStart,
                                                      self.partitions)

    def _set_partitions(self, partitions):
        self.partitions = partitions
        self.iterpart = cycle(partitions)
        if self.randomStart:
            for _ in xrange(randint(0, len(partitions)-1)):
                self.iterpart.next()

    def partition(self, key, partitions):
        # Refresh the partition list if necessary
        if self.partitions != partitions:
            self._set_partitions(partitions)
        return self.iterpart.next()


class HashedPartitioner(Partitioner):
    """
    Implements a partitioner which selects the target partition based on
    the hash of the key
    """
    def partition(self, key, partitions):
        return partitions[hash(key) % len(partitions)]
