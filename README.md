# Afkak: A twisted python client for Apache Kafka


This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

http://kafka.apache.org/

# License

Copyright 2013, David Arthur under Apache License, v2.0. See `LICENSE`

Copyright 2015, Cyan Inc. under Apache License, v2.0. See `LICENSE`

# Status

The current version of this package is **0.1.0** and is compatible with

Kafka broker versions
- 0.8.0
- 0.8.1
- 0.8.1.1
- 0.8.2.1

Python versions
- 2.7.3
- pypy 2.3.1

# Usage

## High level

```python
from afkak.client import KafkaClient
from afkak.consumer import Consumer
from afkak.producer import Producer

kClient = KafkaClient("localhost:9092")

# To send messages
producer = Producer(kClient)
d1 = producer.send_messages("my-topic", msgs=["some message"])
d2 = producer.send_messages("my-topic", msgs=["takes a list", "of messages"])
# To get confirmations/errors on the sends, add callbacks to the returned deferreds
d1.addCallbacks(handleResponses, handleErrors)

# To wait for acknowledgements
# ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
#                         a local log before sending response
# ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
#                            by all in sync replicas before sending a response
producer = Producer(kClient,
                    req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                    ack_timeout=2000)

responseD = producer.send_messages("my-topic", msgs=["message"])

# Using twisted's @inlineCallbacks:
responses = yield responseD
if response:
    print(response[0].error)
    print(response[0].offset)

# To send messages in batch. You can use a producer with any of the
# partitioners for doing this. The following producer will collect
# messages in batch and send them to Kafka after 20 messages are
# collected or every 60 seconds. You can also batch by number of bytes
# Notes:
# * If the producer dies before the messages are sent, the caller would
# * not have had the callbacks called on the send_messages() returned
# * deferreds, and so can retry.
# * Calling producer.stop() before the messages are sent will
# errback() the deferred(s) returned from the send_messages call(s)
producer = Producer(kClient, batch_send=True,
                    batch_send_every_n=20,
                    batch_send_every_t=60)

# To consume messages
# define a function which takes a list of messages to process and
# possibly returns a deferred which fires when the processing is
# complete.
def processor_func(messages):
    #  Store_Messages_In_Database may return a deferred
    return store_messages_in_database(messages)

consumer = Consumer(kClient, "my-group", "my-topic", processor_func)
d = consumer.start(0)  # Start reading at offset zero
# The deferred returned by consumer.start() will fire when an error
# occurs that can't handled by the consumer, or when consumer.stop()
# is called
yield d

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

Install with your favorite package manager

Pip:

```shell
git clone https://github.com/rthille/afkak
pip install ./afkak
```

Setuptools:
```shell
git clone https://github.com/rthille/afkak
easy_install ./afkak
```

Using `setup.py` directly:
```shell
git clone https://github.com/rthille/afkak
cd afkak
python setup.py install
```

## Optional Snappy install

Download and build Snappy from http://code.google.com/p/snappy/downloads/list

Linux:
```shell
wget http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz
tar xzvf snappy-1.0.5.tar.gz
cd snappy-1.0.5
./configure
make
sudo make install
```

OSX:
```shell
brew install snappy
```

Install the `python-snappy` module
```shell
pip install python-snappy
```

# Tests

## Run the unit tests

```shell
make toxu
```

## Run the integration tests

The integration tests will actually start up real local Zookeeper
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
