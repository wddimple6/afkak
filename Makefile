
# This must be set before we include any other makefiles
TOP := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILDSTART:=$(shell date +%s)

RELEASE_DIR := $(TOP)/build
TOXDIR := $(TOP)/.tox
VENV := $(TOP)/.env
TOX := $(VENV)/bin/tox
SERVERS := $(TOP)/servers
KAFKA_ALL_VERS := 0.8.1 0.8.1.1 0.8.2.1
KAFKA_VER ?= 0.8.2.1
KAFKA_RUN := $(SERVERS)/$(KAFKA_VER)/kafka-bin/bin/kafka-run-class.sh
UNAME := $(shell uname)
PYPI ?= 'https://pypi.python.org/simple/'

ifeq ($(UNAME),Darwin)
  _CPPFLAGS := -I/opt/local/include -L/opt/local/lib
  _LANG := en_US.UTF-8
endif

AFKAK_PYFILES := \
	afkak/client.py \
	afkak/util.py \
	afkak/producer.py \
	afkak/__init__.py \
	afkak/protocol.py \
	afkak/partitioner.py \
	afkak/consumer.py \
	afkak/kafkacodec.py \
	afkak/brokerclient.py \
	afkak/common.py \
	afkak/codec.py

UNITTEST_PYFILES := \
	afkak/test/__init__.py \
	afkak/test/fixtures.py \
	afkak/test/service.py \
	afkak/test/testutil.py \
	afkak/test/test_brokerclient.py \
	afkak/test/test_client.py \
	afkak/test/test_codec.py \
	afkak/test/test_common.py \
	afkak/test/test_consumer.py \
	afkak/test/test_kafkacodec.py \
	afkak/test/test_package.py \
	afkak/test/test_partitioner.py \
	afkak/test/test_producer.py \
	afkak/test/test_protocol.py \
	afkak/test/test_util.py

INTTEST_PYFILES := \
	afkak/test/test_client_integration.py \
	afkak/test/test_consumer_integration.py \
	afkak/test/test_failover_integration.py \
	afkak/test/test_producer_integration.py

SETUP_PYFILES := setup.py

MISC_PYFILES := \
	consumer_example \
	producer_example

ALL_PYFILES := $(AFKAK_PYFILES) $(UNITTEST_PYFILES) \
    $(INTTEST_PYFILES) $(MISC_PYFILES) $(SETUP_PYFILES)

# We don't currently ignore any pep8 errors
PEP8_IGNORES :=
PYLINT_IGNORES :=  # --disable=invalid-name,no-member

# We lint all python files
PYLINTERS_TARGETS += $(foreach f,$(ALL_PYFILES),build/pyflakes/$f.flag)
# Unittests
UNITTEST_TARGETS += $(PYLINTERS_TARGETS)

# Files to cleanup
UNITTEST_CLEANS  += build/pyflakes $(PYL_ACK_ERRS)
EGG := $(TOP)/afkak.egg-info
TRIAL_TEMP := $(TOP)/_trial_temp
COVERAGE_CLEANS := $(TOP)/.coverage
CLEAN_TARGETS += $(UNITTEST_CLEANS) $(EGG) $(COVERAGE_CLEANS) $(TRIAL_TEMP)

# We don't yet use this, but will eventually check for Python3 compatibility
# But Twisted needs full Python3 support first...
PY3CHK_TARGETS += $(foreach f,$(ALL_PYFILES),build/python3/$f.todo)

###########################################################################
## Start of system makefile
###########################################################################
.PHONY: all clean pyc-clean timer build venv
.PHONY: toxi toxu toxr toxc

all: timer

timer: build
	@echo "---( Make $(MAKECMDGOALS) Complete (time: $$((`date +%s`-$(BUILDSTART)))s) )---"

build: toxa # Not Yet python3check
	@echo "Done"

clean: pyc-clean
	$(AT)rm -rf $(CLEAN_TARGETS)
	@echo "Done cleaning"

dist-clean: clean
	$(AT)rm -rf $(TOXDIR) $(VENV) $(TOP)/.noseids build
	$(AT)$(foreach VERS,$(KAFKA_ALL_VERS), rm -rf $(SERVERS)/$(VERS)/kafka-bin)
	@echo "Done dist-cleaning"

pyc-clean:
	@echo "Removing '*.pyc' from all subdirs"
	$(AT)find $(TOP) -name '*.pyc' -delete

$(KAFKA_RUN): export KAFKA_VERSION = $(KAFKA_VER)
$(KAFKA_RUN):
	$(AT)$(TOP)/build_integration.sh
	$(AT)[ -x $(KAFKA_RUN) ] || false

venv: $(VENV)
	@echo "Done creating virtualenv"

$(VENV): export CPPFLAGS = $(_CPPFLAGS)
$(VENV): export LANG = $(_LANG)
$(VENV): requirements_venv.txt
	$(AT)virtualenv --python python2.7 $(VENV)
	$(AT)$(VENV)/bin/pip install --upgrade --index-url $(PYPI) -r requirements_venv.txt

lint: export LANG = $(_LANG)
lint: $(VENV) $(PYLINTERS_TARGETS)
	$(AT)$(VENV)/bin/pyroma $(TOP)
	$(AT)$(TOX) -e lint
	@echo Done

# This could run straight 'tox' without the config arg, since it doesn't set
# KAFKA_VERSION, but it could be set in the env already, and this tests the
# tox_unit.ini which the teamcity builder uses.
toxu: export CPPFLAGS = $(_CPPFLAGS)
toxu: $(UNITTEST_TARGETS)
	$(TOX) -c tox_unit.ini

# Integration tests rely on a KAFKA_VERSION environment variable, otherwise the
# integration tests are skipped. Also, use integration-only tox config which
# teamcity builder uses, to ensure it gets tested during dev.
toxi: export CPPFLAGS = $(_CPPFLAGS)
toxi: export TMPDIR = $(TOP)/tmp
toxi: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX) -c tox_int.ini

# Run the full test suite
toxa: export CPPFLAGS = $(_CPPFLAGS)
toxa: export LANG = $(_LANG)
toxa: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX)
	$(AT)$(TOX) -e coverage

# Run the full test suite until it fails
toxr: export CPPFLAGS = $(_CPPFLAGS)
toxr: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) sh -c "while $(TOX); do : ; done"

# Run just the tests selected in tox_cur.ini
toxc: export CPPFLAGS = $(_CPPFLAGS)
toxc: $(UNITTEST_TARGETS)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX) -c $(TOP)/tox_cur.ini

# Run the just the tests selected in tox_cur.ini until they fail
toxrc: export CPPFLAGS = $(_CPPFLAGS)
toxrc: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) sh -c "while time $(TOX) -c tox_cur.ini; do : ; done"

# We use flag files so that we only need to run the lint stage if the file
# changes.
build/pyflakes/%.flag: % $(VENV)
	$(AT)$(VENV)/bin/pyflakes $<
	$(AT)$(VENV)/bin/pep8 --ignore=$(PEP8_IGNORES) $<
	# $(AT)pylint $(PYLINT_IGNORES) $< || true
	# $(AT)pep257 $<
	# $(AT)dodgy $<
	# $(AT)frosted $<
	@mkdir -p $(dir $@)
	@touch "$@"
