#!/bin/bash
# Clear coverage data, run tests, then generate a coverage report.
#
# Usage:
#
#   KAFKA_VERSION=... tools/coverage.sh [TOX]
#
# KAFKA_VERSION - Version of Kafka to run integration tests against. See the
#                 table of Kafka releases in tools/download-kafka.py for a list
#                 of known versions. If not set, only unit tests will be run.
#
# TOX           - tox executable to use. Defaults to whatever is available on
#                 your PATH.

KAFKA_VERSION="$1"
TOX="${2:-tox}"

if [[ -n $KAFKA_VERSION ]]
then
    export KAFKA_VERSION
else
    echo "No Kafka version provided: only unit tests will be run."
fi

env_filter() {
    if [[ -n $KAFKA_VERSION ]]
    then
        cat
    else
        egrep -v '[-]int'
    fi
}

if ! "$TOX" -e cov_erase
then
    exit 1
fi

"$TOX" tox -l | env_filter | awk '{ print($0 "-coverage"); }' | xargs -n1 "$TOX" -e
status=$?

if ! "$TOX" -e cov_report
then
    exit 1
fi

exit $status
