import sys

from setuptools import setup, Command


class Tox(Command):

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import tox
        sys.exit(tox.cmdline([]))


setup(
    name="afkak",
    version="0.1.0",

    tests_require=["tox", "mock"],
    cmdclass={"test": Tox},

    packages=["afkak"],

    zip_safe=True,

    author="Robert Thille",
    author_email="robert.thille@cyaninc.com",
    url="https://github.com/cyaninc/afkak",
    license="""
Copyright 2012, David Arthur under Apache License, v2.0
Copyright 2014, Cyan Inc under Apache License, v2.0
""",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Communications',
        'Topic :: System :: Distributed Computing',
    ],

    description="Twisted Python client for Apache Kafka",
    long_description="""
This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.
Modified from https://github.com/mumrah/kafka-python by Cyan Inc.
for twisted compatibility.
""",
    keywords=['Kafka client', 'twisted', 'distributed messaging',
              ]

)
