#!/usr/bin/env python3
# Copyright 2018, 2019 Ciena Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Generate a .travis.yml file based on the current tox.ini. Usage:

    tox -l | tools/gentravis.py > .travis.yml
    git add .travis.yml
"""

import json
import sys

envlist = sys.stdin.read().strip().split()
envlist.sort()

kafka_versions = ['0.9.0.1', '1.1.1']

envpy_to_travis = {
    'py27': {
        'python': '2.7',
    },
    'py35': {
        'python': '3.5',
    },
    'py36': {
        'python': '3.6',
    },
    'py37': {
        'python': '3.7',
    },
    'pypy': {
        'python': 'pypy',
    },
    'pypy3': {
        'python': 'pypy3',
    },
}

matrix_include = [{
    'name': 'Documentation and Self-Check',
    'python': '3.6',
    'script': [
        'tox -e docs',

        # Packaging check
        'tox -e twine -- python setup.py sdist bdist_wheel',
        'tox -e twine -- twine check dist/*',

        # Self-check: did you forget to regenerate .travis.yml after modifying
        # this script?
        'tox -l | tools/gentravis.py > .travis.yml',
        'git diff --exit-code',
    ],
}]


def group_envs(envlist):
    """Group Tox environments for Travis CI builds

    Separate by Python version so that they can go in different Travis jobs:

    >>> group_envs('py37-int-snappy', 'py36-int')
    [('py36', 'int', ['py36-int']), ('py37', 'int', ['py37-int-snappy'])]

    Group unit tests and linting together:

    >>> group_envs(['py27-unit', 'py27-lint'])
    [('py27', 'unit', ['py27-unit', 'py27-lint'])]
    """
    groups = {}
    for env in envlist:
        envpy, category = env.split('-')[0:2]

        if category == 'lint':
            category = 'unit'

        try:
            groups[envpy, category].append(env)
        except KeyError:
            groups[envpy, category] = [env]

    return sorted((envpy, category, envs) for (envpy, category), envs in groups.items())


for envpy, category, envs in group_envs(envlist):
    toxenv = ','.join(envs)
    if category == 'unit':
        if any(env.endswith('-lint') for env in envs):
            name = "Unit and Lint: {}".format(envpy)
        else:
            name = "Unit: {}".format(envpy)
        matrix_include.append({
            'name': name,
            'env': 'TOXENV={}'.format(toxenv),
            **envpy_to_travis[envpy],
        })
    elif category == 'int':
        for kafka in kafka_versions:
            matrix_include.append({
                'name': "Integration: {} + Kafka {}".format(envpy, kafka),
                'jdk': 'openjdk8',
                'env': 'TOXENV={} KAFKA_VERSION={}'.format(toxenv, kafka),
                **envpy_to_travis[envpy],
            })
    else:
        raise ValueError("Expected Tox environments of the form pyXY-{unit,int}*, but got {!r}".format(toxenv))

json.dump({
    # Select a VM-based environment which provides more resources. See
    # https://docs.travis-ci.com/user/reference/overview/#Virtualisation-Environment-vs-Operating-System
    'sudo': 'required',
    'dist': 'xenial',
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
            # Cache intersphinx inventories to make the build more robust in
            # the face of upstreams going down.
            'docs/_cache',
        ],
    },
    # We need branch builds or the cache will never be populated, but we don't
    # want to run them for every PR branch in addition to the PR build.
    'branches': {
        'only': ['master'],
    },
}, sys.stdout, indent=2, sort_keys=True)
