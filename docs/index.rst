Afkak: Twisted Python Kafka Client
==================================

Afkak is a `Twisted`_-native `Apache Kafka`_ client library.
It provides support for:

* Producing messages, with automatic batching and optional compression.
* Consuming messages, with automatic commit.

.. _Twisted: https://twistedmatrix.com/trac/
.. _Apache Kafka: https://kafka.apache.org/

Afkak |release| was tested against:

:Python: CPython 3.5+; PyPy3
:Twisted: 21.2.0
:Kafka: 0.9.0.1, 1.1.1

Newer broker releases will generally function, but not all Afkak features will work on older brokers.
In particular, the coordinated consumer won’t work before Kafka 0.9.0.1.
We don’t recommend deploying such old releases anyway, as they have serious bugs.

Topics
------

.. toctree::
    :maxdepth: 2

    api

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
