# -*- coding: utf-8 -*-
# Copyright (C) 2016 Cyan, Inc.

from __future__ import absolute_import

from .client import KafkaClient
from .kafkacodec import (
    create_message, create_message_set,
    CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY,
)
from .producer import Producer
from .partitioner import RoundRobinPartitioner, HashedPartitioner
from .consumer import Consumer
from .common import (OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_COMMITTED,)

# Note, you need to bump the version in setup.py as well
__title__ = 'afkak'
__version__ = "2.4.0"  # Makefile parses this. Retain formatting.
__author__ = 'Robert Thille'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2015, Cyan Inc. under Apache License, v2.0'

__all__ = [
    'KafkaClient', 'Producer', 'Consumer',
    'RoundRobinPartitioner', 'HashedPartitioner',
    'create_message', 'create_message_set',
    'CODEC_NONE', 'CODEC_GZIP', 'CODEC_SNAPPY',
    'OFFSET_EARLIEST', 'OFFSET_LATEST', 'OFFSET_COMMITTED',
]
