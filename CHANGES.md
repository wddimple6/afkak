Version 1.0.2
-------------

* Fixed bug where message keys weren't sent to Kafka
* Fixed bug where producer didn't retry metadata lookup
* Fixed hashing of keys in HashedPartitioner to use Murmur2, like Java
* Shuffle the broker list when sending broker-unaware requests
* Reduced Twisted runtime requirement to 13.2.0
* Consolidated tox configuration to one tox.ini file
* Added logo
* Cleanup of License, ReadMe, Makefile, etc.

Version 1.0.1
-------------

* Added Twisted as install requirement
* Readme augmented with better install instructions
* Handle testing properly without 'Snappy' installed

Version 1.0.0
-------------

* Working offset committing on 0.8.2.1
* Full coverage tests
* Examples for using producer & consumer

Version 0.1.0
-------------

* Large amount of rework of the base 'mumrah/kafka-python' to convert the APIs to async using Twisted
