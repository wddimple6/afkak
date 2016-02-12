Version 2.1.0
-------------

* Added `request_retry_max_attempts` parameter to Consumer objects
  which allows the caller to configure the maximum number of attempts
  the Consumer will make on any request to Kafka.
  NOTE: It defaults to zero which indicates the Consumer should retry
  forever. *This is a behavior change*.

* If a user-initiated Commit operation is attempted while a commit is
  ongoing (even a Consumer auto-initiated one), the new attempt will
  fail with a OperationInProgress error.

* Fixed an error where Commit requests would only be retried once
  before failing.

* Reduced the level of some log messages from ERROR to WARNING or
  lower.

Version 2.0.1
-------------

* Added `update_cluster_hosts` method to allow retargeting Afkak
  client in case all brokers are restarted on new IPs.

Version 2.0.0
-------------

* message processor callback will recieve Consumer object
  with which it was registered

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
