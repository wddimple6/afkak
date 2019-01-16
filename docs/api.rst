API Documentation
=================

.. default-role:: any

.. module:: afkak

KafkaClient objects
~~~~~~~~~~~~~~~~~~~

.. autoclass:: afkak.KafkaClient
    :members:
    :undoc-members:

Consumer objects
~~~~~~~~~~~~~~~~

.. autoclass:: afkak.Consumer
    :members:
    :undoc-members:

Producer objects
~~~~~~~~~~~~~~~~

.. automodule:: afkak.Producer
    :members:
    :undoc-members:

Message construction
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: afkak.create_message

.. autofunction:: afkak.create_message_set

Compression constants
~~~~~~~~~~~~~~~~~~~~~

.. data:: afkak.CODEC_NONE

    No compression.

.. data:: afkak.CODEC_GZIP

    Gzip compression.

.. data:: afkak.CODEC_SNAPPY

    Snappy compression.

    Snappy compression requires Afkak's ``snappy`` extra.
    For example::

        pip install afkak[snappy]

.. data:: afkak.CODEC_LZ4

    LZ4 compression. Not currently supported by Afkak.

Offset constants
~~~~~~~~~~~~~~~~

.. data:: afkak.OFFSET_EARLIEST

.. data:: afkak.OFFSET_LATEST

.. data:: afkak.OFFSET_COMMITTED

Common objects
~~~~~~~~~~~~~~

.. TODO: Less terrible docs for this stuff.

.. automodule:: afkak.common
    :members:
    :undoc-members:
