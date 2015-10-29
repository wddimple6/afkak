# <img src="docs/_static/afkak.png" width="23" height="36" alt=""> Afkak: Twisted Python Kafka Client

This module provides low-level protocol support for [Apache Kafka][kafka] as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

[kafka]: http://kafka.apache.org/

# License

Copyright 2013, 2014, 2015 David Arthur under Apache License, v2.0. See `LICENSE`

Copyright 2014, 2015 Cyan, Inc. under Apache License, v2.0. See `LICENSE`

Copyright 2015 Ciena Corporation under Apache License, v2.0. See `LICENSE`

This project began as a port of the [kafka-python][kafka-python] library to Twisted.

[kafka-python]: https://github.com/mumrah/kafka-python

# Status

This version of the package is compatible with

Kafka broker versions
- 0.8.0
- 0.8.1
- 0.8.1.1
- 0.8.2.1

Python versions
- CPython 2.7.3
- PyPy 2.6.1

# Usage

## High level
[ Note: This code is not meant to be runable. See `producer_example`
and `consumer_example` for runable example code. ]

```python
from afkak.client import KafkaClient
from afkak.consumer import Consumer
from afkak.producer import Producer
from afkak.common import (OFFSET_EARLIEST, PRODUCER_ACK_ALL_REPLICAS,
    PRODUCER_ACK_LOCAL_WRITE)

kClient = KafkaClient("localhost:9092")

# To send messages
producer = Producer(kClient)
d1 = producer.send_messages("my-topic", msgs=["some message"])
d2 = producer.send_messages("my-topic", msgs=["takes a list", "of messages"])
# To get confirmations/errors on the sends, add callbacks to the returned deferreds
d1.addCallbacks(handleResponses, handleErrors)

# To wait for acknowledgements
# PRODUCER_ACK_LOCAL_WRITE : server will wait till the data is written to
#                         a local log before sending response
# [ the default ]
# PRODUCER_ACK_ALL_REPLICAS : server will block until the message is committed
#                            by all in sync replicas before sending a response
producer = Producer(kClient,
                    req_acks=Producer.PRODUCER_ACK_LOCAL_WRITE,
                    ack_timeout=2000)

responseD = producer.send_messages("my-topic", msgs=["message"])

# Using twisted's @inlineCallbacks:
responses = yield responseD
if response:
    print(response[0].error)
    print(response[0].offset)

# To send messages in batch: You can use a producer with any of the
# partitioners for doing this. The following producer will collect
# messages in batch and send them to Kafka after 20 messages are
# collected or every 60 seconds (whichever comes first). You can
# also batch by number of bytes.
# Notes:
# * If the producer dies before the messages are sent, the caller would
# * not have had the callbacks called on the send_messages() returned
# * deferreds, and so can retry.
# * Calling producer.stop() before the messages are sent will
# errback() the deferred(s) returned from the send_messages call(s)
producer = Producer(kClient, batch_send=True,
                    batch_send_every_n=20,
                    batch_send_every_t=60)
responseD1 = producer.send_messages("my-topic", msgs=["message"])
responseD2 = producer.send_messages("my-topic", msgs=["message 2"])

# To consume messages
# define a function which takes a list of messages to process and
# possibly returns a deferred which fires when the processing is
# complete.
def processor_func(messages):
    #  Store_Messages_In_Database may return a deferred
    return store_messages_in_database(messages)

the_partition = 3  # Consume only from partition 3.
consumer = Consumer(kClient, "my-topic", the_partition, processor_func)
d = consumer.start(OFFSET_EARLIEST)  # Start reading at earliest message
# The deferred returned by consumer.start() will fire when an error
# occurs that can't handled by the consumer, or when consumer.stop()
# is called
yield d

consumer.stop()
kClient.close()
```

## Keyed messages
```python
from afkak.client import KafkaClient
from afkak.producer import Producer
from afkak.partitioner import HashedPartitioner, RoundRobinPartitioner

kafka = KafkaClient("localhost:9092")

# Use the HashedPartitioner so that the producer will use the optional key
# argument on send_messages()
producer = Producer(kafka, partitioner_class=HashedPartitioner)
producer.send_messages("my-topic", "key1", ["some message"])
producer.send_messages("my-topic", "key2", ["this method"])


```

## Low level

```python
from afkak.client import KafkaClient
kafka = KafkaClient("localhost:9092")
req = ProduceRequest(topic="my-topic", partition=1,
    messages=[KafkaProdocol.encode_message("some message")])
resps = afkak.send_produce_request(payloads=[req], fail_on_error=True)
kafka.close()

resps[0].topic      # "my-topic"
resps[0].partition  # 1
resps[0].error      # 0 (hopefully)
resps[0].offset     # offset of the first message sent in this request
```

# Install

Afkak releases are [available on PyPI][afkak-pypi].

Because the Afkak dependencies [Twisted][twisted] and [python-snappy][python-snappy] have binary extension modules you will need to install the Python development headers for the interpreter you wish to use.  You'll need all of these to run Afkak's tests:

[afkak-pypi]: https://pypi.python.org/pypi/afkak
[twisted]: https://pypi.python.org/pypi/Twisted
[python-snappy]: https://pypi.python.org/pypi/python-snappy

<table>
<tr>
<td>Debian/Ubuntu:
<td><code>sudo apt-get install build-essential python-dev pypy-dev libsnappy-dev</code>
<tr>
<td>OS X
<td><code>brew install python-dev pypy-dev snappy</code>
</table>

Then Afkak can be [installed with pip as usual][pip-install]:

[pip-install]: https://packaging.python.org/en/latest/installing/

# Tests

## Run the unit tests

```shell
make toxu
```

## Run the integration tests

The integration tests will actually start up real local ZooKeeper
instance and Kafka brokers, and send messages in using the client.

build_integration.sh downloads and sets up the various Kafka releases
```shell
./build_integration.sh
```

Then run the tests against supported Kafka versions:
```shell
KAFKA_VERSION=0.8.1 tox
KAFKA_VERSION=0.8.1.1 tox
KAFKA_VERSION=0.8.2.1 tox
```
