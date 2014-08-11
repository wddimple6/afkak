import unittest2

class TestPackage(unittest2.TestCase):
    def test_top_level_namespace(self):
        import kafkatwisted as kafka1
        self.assertEquals(kafka1.KafkaClient.__name__, "KafkaClient")
        self.assertEquals(kafka1.client.__name__, "kafkatwisted.client")
        self.assertEquals(kafka1.codec.__name__, "kafkatwisted.codec")

    def test_submodule_namespace(self):
        import kafkatwisted.client as client1
        self.assertEquals(client1.__name__, "kafkatwisted.client")
        self.assertEquals(client1.KafkaClient.__name__, "KafkaClient")

        from kafkatwisted import client as client2
        self.assertEquals(client2.__name__, "kafkatwisted.client")
        self.assertEquals(client2.KafkaClient.__name__, "KafkaClient")

        from kafkatwisted.client import KafkaClient as KafkaClient1
        self.assertEquals(KafkaClient1.__name__, "KafkaClient")

        from kafkatwisted.codec import gzip_encode as gzip_encode1
        self.assertEquals(gzip_encode1.__name__, "gzip_encode")

        from kafkatwisted import KafkaClient as KafkaClient2
        self.assertEquals(KafkaClient2.__name__, "KafkaClient")

        from kafkatwisted.codec import snappy_encode
        self.assertEquals(snappy_encode.__name__, "snappy_encode")
