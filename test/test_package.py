import unittest2


class TestPackage(unittest2.TestCase):
    def test_top_level_namespace(self):
        import afkak as afkak1
        self.assertEquals(afkak1.KafkaClient.__name__, "KafkaClient")
        self.assertEquals(afkak1.client.__name__, "afkak.client")
        self.assertEquals(afkak1.codec.__name__, "afkak.codec")

    def test_submodule_namespace(self):
        import afkak.client as client1
        self.assertEquals(client1.__name__, "afkak.client")
        self.assertEquals(client1.KafkaClient.__name__, "KafkaClient")

        from afkak import client as client2
        self.assertEquals(client2.__name__, "afkak.client")
        self.assertEquals(client2.KafkaClient.__name__, "KafkaClient")

        from afkak.client import KafkaClient as KafkaClient1
        self.assertEquals(KafkaClient1.__name__, "KafkaClient")

        from afkak.codec import gzip_encode as gzip_encode1
        self.assertEquals(gzip_encode1.__name__, "gzip_encode")

        from afkak import KafkaClient as KafkaClient2
        self.assertEquals(KafkaClient2.__name__, "KafkaClient")

        from afkak.codec import snappy_encode
        self.assertEquals(snappy_encode.__name__, "snappy_encode")
