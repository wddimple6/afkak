#!/usr/bin/env python3
# Copyright 2018 Ciena Corporation
#
# Download and extract a Kafka distribution tarball.
# To list the supported Kafka versions:
#
#     tools/download-kafka.py list
#
# To download a given version:
#
#     tools/download-kafka.py get <KAFKA_VERSION>

import argparse
import hashlib
import os
from shutil import copyfileobj
import sys
import tarfile
from tempfile import TemporaryFile
from urllib.request import urlopen

CHUNK_SIZE = 32 * 1024 * 1024

versions = {
    '0.8.0': (
        'kafka_2.8.0-0.8.0',
        'https://archive.apache.org/dist/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz',
        hashlib.md5,  # FIXME
        bytes.fromhex('59 3E 0C F9 66 E6 B8 CD 1B BF F5 BF F7 13 C4 B3'),
    ),
    '0.8.1': (
        'kafka_2.9.2-0.8.1',
        'https://archive.apache.org/dist/kafka/0.8.1/kafka_2.9.2-0.8.1.tgz',
        hashlib.md5,  # FIXME
        bytes.fromhex('BF 02 96 AE 67 12 4A 76 96 64 67 E5 6D 01 DE 3E'),
    ),
    '0.8.1.1': (
        'kafka_2.9.2-0.8.1.1',
        'https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz',
        hashlib.md5,  # FIXME
        bytes.fromhex('75 41 ED 16 0F 1B 3A A1 A5 33 4D 4E 78 2B A4 D3'),
    ),
    '0.8.2.1': (
        'kafka_2.10-0.8.2.1',
        'https://archive.apache.org/dist/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz',
        hashlib.md5,  # FIXME
        bytes.fromhex('44 6E AB 1F 53 29 EB 03 66 29 26 AA 1C B0 84 5D'),
    ),
    '0.8.2.2': (
        'kafka_2.10-0.8.2.2',
        'https://archive.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz',
        hashlib.md5,  # FIXME
        bytes.fromhex('5D 2B C9 65 CF 3D F8 48 CD 1F 8E 33 E2 94 F2 9E'),
    ),
    '0.9.0.1': (
        'kafka_2.11-0.9.0.1',
        'https://archive.apache.org/dist/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz',
        hashlib.md5,  # FIXME
        bytes.fromhex('B7 1E 5C BC 78 16 5C 1C A4 83 27 9C 27 40 26 63'),
    ),
    '1.0.0': (
        'kafka_2.11-1.0.0',
        'https://archive.apache.org/dist/kafka/1.0.0/kafka_2.11-1.0.0.tgz',
        hashlib.sha512,
        bytes.fromhex('B6980284 85D460C4 94AF942B 2DFA41C7 342A2AD6 052B543C 95289CD1 E832A1EB CF679B7E 568DCABC 342C7997 65337B94 F91BC0CF 0EE91553 4CDF82D1 635A622A'),
    ),
}

parser = argparse.ArgumentParser()
parser.add_argument('--list', action='store_true', help="List all know Kafka versions", default=False)
parser.add_argument('--all', action='store_true', help="Download all known Kafka verisons", default=False)
parser.add_argument('version', nargs='*', help="Kafka version to download (unless --all is specified)", default=[])
args = parser.parse_args()

if args.list:
    print('\n'.join(versions.keys()))
    sys.exit(0)
elif args.all:
    download_versions = list(versions.items())
else:
    download_versions = [(v, versions[v]) for v in args.version]


def check_hash(filepath, hashtype, digest):
    """
    Verify that the given file matches the given digest.

    :raises ValueError: on failed hash verification
    """
    hasher = hashtype()
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    if hasher.digest() != digest:
        raise ValueError('Digest {} of {} does not match expected value {}'.format(
            hasher.digest().hex(), filepath, digest.hex()))


server_dir = os.path.normpath(os.path.join(__file__, '..', '..', 'servers'))
for version, (archive_root, url, hashtype, digest) in download_versions:
    dist_name = url.rsplit('/', 1)[1]
    dist_path = os.path.join(server_dir, 'dist', dist_name)
    extract_path = os.path.join(server_dir, version)
    bin_path = os.path.join(extract_path, 'kafka-bin')

    if os.path.isdir(bin_path):
        print("Kafka", version, "is available in", bin_path, "(remove this tree to force re-extraction)")
        continue

    failures = 0
    while True:
        try:
            check_hash(dist_path, hashtype, digest)
        except FileNotFoundError:
            pass  # No cached file.
        except ValueError:
            # The file failed hash verification.
            failures += 1
            if failures >= 3:
                raise Exception("Failed to download Kafka {}.".format(version, dist_path))
            else:
                print("Kafka", version, "download", dist_path, "is corrupt; deleting it.")
                os.unlink(dist_path)
        else:
            break

        print('Downloading Kafka', version, 'to', dist_path)
        with open(dist_path, 'wb') as fout:
            with urlopen(url, timeout=30) as fin:
                if fin.status != 200:
                    print('Request failed: HTTP', fin.status, fin.reason, fin.geturl())
                    print('Will retry in 10 seconds')
                    time.sleep(10)
                    continue
                copyfileobj(fin, fout)

    # And also extract it.
    print('Extracting Kafka', version, 'to', extract_path)
    with tarfile.open(dist_path) as archive:
        archive.extractall(extract_path)
    # Then rename it to the well-known "kafka-bin" location.
    os.rename(os.path.join(extract_path, archive_root), bin_path)
