from __future__ import absolute_import

__title__ = 'afkak'
__version__ = '0.1.0'
__author__ = 'Robert Thille'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2014, Cyan Inc. under Apache License, v2.0'

from .client import KafkaClient
from .kafkacodec import (
    create_message, create_gzip_message, create_snappy_message
)
from .producer import Producer
from .partitioner import RoundRobinPartitioner, HashedPartitioner
from .consumer import Consumer

__all__ = [
    'KafkaClient', 'Producer', 'Consumer',
    'RoundRobinPartitioner', 'HashedPartitioner',
    'create_message', 'create_gzip_message',
    'create_snappy_message'
]
