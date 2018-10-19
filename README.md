# <img src="docs/_static/afkak.png" width="23" height="36" alt=""> Afkak: Twisted Python Kafka Client

This module provides low-level protocol support for [Apache Kafka][kafka] as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

[kafka]: http://kafka.apache.org/

# License

Copyright 2013, 2014, 2015 David Arthur under Apache License, v2.0. See `LICENSE`

Copyright 2014, 2015 Cyan, Inc. under Apache License, v2.0. See `LICENSE`

Copyright 2015, 2016, 2017, 2018 Ciena Corporation under Apache License, v2.0. See `LICENSE`

This project began as a port of the [kafka-python][kafka-python] library to Twisted.

[kafka-python]: https://github.com/mumrah/kafka-python

See [CONTRIBUTING.md](./CONTRIBUTING.md) for how to contribute.

# Status

Afkak supports these Pythons:

- CPython 2.7
- PyPy
- CPython 3.5

We aim to support Kafka 1.1.<var>x</var> and later.
Integration tests are run against these Kafka broker versions:

- 0.9.0.1
- 1.1.1

Testing against 2.0.0 is planned (see [#45](https://github.com/ciena/afkak/issues/45)).

Newer broker releases will generally function, but not all Afkak features will work on older brokers.
In particular, the coordinated consumer won’t work before Kafka 0.9.0.1.
We don’t recommend deploying such old releases anyway, as they have serious bugs.

# Usage

### High level

Note: This code is not meant to be runnable. See [producer\_example](./examples/producer_example)
and [consumer\_example](./examples/consumer_example) for runnable example code.

```python
from afkak.client import KafkaClient
from afkak.consumer import Consumer
from afkak.producer import Producer
from afkak.common import (OFFSET_EARLIEST, PRODUCER_ACK_ALL_REPLICAS,
    PRODUCER_ACK_LOCAL_WRITE)

kClient = KafkaClient("localhost:9092")

# To send messages
producer = Producer(kClient)
d1 = producer.send_messages("my-topic", msgs=[b"some message"])
d2 = producer.send_messages("my-topic", msgs=[b"takes a list", b"of messages"])
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

responseD = producer.send_messages("my-topic", msgs=[b"message"])

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
responseD1 = producer.send_messages("my-topic", msgs=[b"message"])
responseD2 = producer.send_messages("my-topic", msgs=[b"message 2"])

# To consume messages
# define a function which takes a list of messages to process and
# possibly returns a deferred which fires when the processing is
# complete.
def processor_func(consumer, messages):
    #  Store_Messages_In_Database may return a deferred
    result = store_messages_in_database(messages)
    # record last processed message
    consumer.commit()
    return result

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

#### Keyed messages
```python
from afkak.client import KafkaClient
from afkak.producer import Producer
from afkak.partitioner import HashedPartitioner, RoundRobinPartitioner

kafka = KafkaClient("localhost:9092")

# Use the HashedPartitioner so that the producer will use the optional key
# argument on send_messages()
producer = Producer(kafka, partitioner_class=HashedPartitioner)
producer.send_messages("my-topic", "key1", [b"some message"])
producer.send_messages("my-topic", "key2", [b"this method"])


```

### Low level

```python
from afkak.client import KafkaClient
kafka = KafkaClient("localhost:9092")
req = ProduceRequest(topic="my-topic", partition=1,
    messages=[KafkaProtocol.encode_message(b"some message")])
resps = afkak.send_produce_request(payloads=[req], fail_on_error=True)
kafka.close()

resps[0].topic      # b"my-topic"
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
<td><code>sudo apt-get install build-essential python-dev python3-dev pypy-dev libsnappy-dev</code>
<tr>
<td>OS X
<td><code>brew install python pypy snappy</code></br>
<code>pip install virtualenv</code></td>
</table>

Then Afkak can be [installed with pip as usual][pip-install]:

[pip-install]: https://packaging.python.org/en/latest/installing/

# Tests

### Run the unit tests

```shell
make toxu
```

### Run the integration tests

The integration tests will actually start up real local ZooKeeper
instance and Kafka brokers, and send messages in using the client.

The makefile knows how to download several versions of Kafka.
This will run just the integration tests against Kafka 1.1.1:

```shell
KAFKA_VER=1.1.1 make toxi
```

### Run all the tests against the default Kafka version

```shell
make toxa
```

### Run the integration tests against all the Kafka versions the Makefile knows about

```shell
make toxik
```
