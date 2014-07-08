#!/usr/bin/env python
import threading
import logging
import time

from twisted.internet.defer import inlineCallbacks

from afkak.client import KafkaClient
from afkak.consumer import Consumer
from afkak.producer import Producer

msg_size = 524288


class ExampleProducer(threading.Thread):
    daemon = True
    big_msg = "1" * msg_size

    @inlineCallbacks
    def run(self):
        client = KafkaClient("localhost:9092")
        producer = Producer(client)
        self.sent = 0

        while True:
            yield producer.send_messages('my-topic', self.big_msg)
            self.sent += 1


class ExampleConsumer(threading.Thread):
    daemon = True

    @inlineCallbacks
    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = Consumer(
            client, "test-group", "my-topic",
            max_buffer_size=None,
            )
        self.valid = 0
        self.invalid = 0

        yield consumer.waitForReady()

        for messageD in consumer:
            message = yield messageD
            if len(message.message.value) == msg_size:
                self.valid += 1
            else:
                self.invalid += 1


def main():
    threads = [
        ExampleProducer(),
        ExampleConsumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)
    print 'Messages sent: %d' % threads[0].sent
    print 'Messages recvd: %d' % threads[1].valid
    print 'Messages invalid: %d' % threads[1].invalid

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
        '%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
