#!/usr/bin/env python
import threading
import logging
import time

from afkak.client import KafkaClient
from afkak.consumer import Consumer
from afkak.producer import Producer


class ExampleProducer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        producer = Producer(client)

        while True:
            producer.send_messages('my-topic', "test")
            producer.send_messages('my-topic', "\xc2Hola, mundo!")

            time.sleep(1)


class ExampleConsumer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = Consumer(client, "test-group", "my-topic")

        for message in consumer:
            print(message)


def main():
    threads = [
        ExampleProducer(),
        ExampleConsumer()
    ]

    for t in threads:
        t.start()

    time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:' +
        '%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
