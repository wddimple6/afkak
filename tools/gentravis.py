#!/usr/bin/env python3
# Generate a .travis.yml file.
# Usage:
#
#   tox -l | tools/gentravis.py > .travis.yml
#   git add .travis.yml

import sys
import json
from itertools import groupby

envlist = sys.stdin.read().strip().split()
envlist.sort()

kafka_versions = '0.8.0 0.8.1 0.8.1.1 0.8.2.1 0.8.2.2 0.9.0.1'.split()

envpy_to_travis = {
    'py27': '2.7',
    'pypy': 'pypy',
    'py35': '3.5',
    'py36': '3.6',
}

matrix_include = [{
    'python': '2.7',
    'env': 'TOXENV=docs',
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
    'sudo': False,
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
}, sys.stdout, indent=2, sort_keys=True)
