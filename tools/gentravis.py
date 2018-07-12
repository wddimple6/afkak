#!/usr/bin/env python3
# Copyright 2018 Ciena Corporation
#
# Generate a .travis.yml file based on the current tox.ini. Usage:
#
#   tox -l | tools/gentravis.py > .travis.yml
#   git add .travis.yml

import sys
import json
from itertools import groupby

envlist = sys.stdin.read().strip().split()
envlist.sort()

kafka_versions = ['0.8.2.2', '0.9.0.1']

envpy_to_travis = {
    'py27': '2.7',
    'pypy': 'pypy',
    'py35': '3.5',
    'py36': '3.6',
}

matrix_include = [{
    'python': '2.7',
    'env': 'TOXENV=docs',
}, {
    # Self-check: did you forget to regenerate .travis.yml after modifying this script?
    'python': '3.5',
    'script': [
        'tox -l | tools/gentravis.py > .travis.yml',
        'git diff --exit-code',
    ],
}]

for (envpy, category), envs in groupby(envlist, key=lambda env: env.split('-')[0:2]):
    toxenv = ','.join(envs)
    if category == 'unit':
        matrix_include.append({
            'python': envpy_to_travis[envpy],
            'env': 'TOXENV={}'.format(toxenv),
        })
    elif category == 'int':
        for kafka in kafka_versions:
            matrix_include.append({
                'python': envpy_to_travis[envpy],
                'jdk': 'openjdk8',
                'env': 'TOXENV={} KAFKA_VERSION={}'.format(toxenv, kafka),
            })
    else:
        raise ValueError("Expected Tox environments of the form pyXY-{unit,int}*, but got {!r}".format(toxenv))

json.dump({
    # Select a VM-based environment which provides more resources. See
    # https://docs.travis-ci.com/user/reference/overview/#Virtualisation-Environment-vs-Operating-System
    'sudo': 'required',
    'dist': 'trusty',
    'language': 'python',
    'install': [
        'pip install tox',
    ],
    'script': [
        'tox',
    ],
    'matrix': {
        'include': matrix_include,
    },
    'addons': {
        'apt': {
            'packages': [
                'libsnappy-dev',
            ],
        },
    },
    'cache': {
        'directories': [
            # Cache Kafka server tarballs to be nice to the Apache servers.
            'servers/dist',
        ],
    },
    # We need branch builds or the cache will never be populated, but we don't
    # want to run them for every PR branch in addition to the PR build.
    'branches': {
        'only': ['master'],
    },
}, sys.stdout, indent=2, sort_keys=True)
