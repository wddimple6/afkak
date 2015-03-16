
# This must be set before we include any other makefiles
TOP := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILDSTART:=$(shell date +%s)

TOOLS := $(HOME)/dev/pv/cyportal/tools
RELEASE_DIR := $(TOP)/build
TOXDIR := $(TOP)/.tox
SERVERS := $(TOP)/servers
KAFKA_VER := 0.8.1.1
UNAME := $(shell uname)

ifeq ($(UNAME),Darwin)
  _CPPFLAGS := -I/opt/local/include -L/opt/local/lib
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
	test/__init__.py \
	test/fixtures.py \
	test/service.py \
	test/testutil.py \
	test/test_brokerclient.py \
	test/test_client.py \
	test/test_codec.py \
	test/test_common.py \
	test/test_consumer.py \
	test/test_kafkacodec.py \
	test/test_package.py \
	test/test_partitioner.py \
	test/test_protocol.py \
	test/test_util.py

INTTEST_PYFILES := \
	test/test_client_integration.py \
	test/test_consumer_integration.py \
	test/test_failover_integration.py \
	test/test_producer_integration.py

SETUP_PYFILES := setup.py

MISC_PYFILES := \
	example.py \
	load_example.py

ALL_PYFILES := $(AFKAK_PYFILES) $(UNITTEST_PYFILES) \
    $(INTTEST_PYFILES) $(MISC_PYFILES) $(SETUP_PYFILES)

# We don't currently ignore any pep8 errors
PEP8_IGNORES :=

# We lint all python files
PYLINTERS_TARGETS += $(foreach f,$(ALL_PYFILES),build/pyflakes/$f.flag)
# Unittests
UNITTEST_TARGETS += $(PYLINTERS_TARGETS)
# Itegration tests

# Files to cleanup
UNITTEST_CLEANS  += build/pyflakes $(PYL_ACK_ERRS)
EGG := $(TOP)/afkak.egg-info
TRIAL_TEMP := $(TOP)/_trial_temp
COVERAGE_CLEANS := $(TOP)/.coverage
CLEAN_TARGETS += $(UNITTEST_CLEANS) $(EGG) $(COVERAGE_CLEANS) $(TRIAL_TEMP)

# We don't yet use this, but will eventually check for Python3 compatibility
PY3CHK_TARGETS += $(foreach f,$(ALL_PYFILES),build/python3/$f.todo)

###########################################################################
## Start of system makefile
###########################################################################
.PHONY: all clean pyc-clean timer build
.PHONY: toxi toxu toxr toxc

all: timer

timer: build
	@echo "---( Make $(MAKECMDGOALS) Complete (time: $$((`date +%s`-$(BUILDSTART)))s) )---"

build: toxa # Not Yet python3check
	@echo "Done"

clean: pyc-clean
	$(AT)rm -rf $(CLEAN_TARGETS)
	$(AT)echo "Done cleaning"

dist-clean: clean
	$(AT)rm -rf $(TOXDIR)
	$(AT)echo "Done dist-cleaning"

pyc-clean:
	@echo "Removing '*.pyc' from all subdirs"
	$(AT)find . -name '*.pyc' -delete

python3check: $(PY3CHK_TARGETS)
	$(AT)$(TOOLS)/python3postcheck $(PY3CHKARGS) build/python3

# This could run straight 'tox' without the config arg, since it doesn't set
# KAFKA_VERSION, but it could be set in the env already, and this tests the
# tox_unit.ini which the teamcity builder uses.
toxu: export CPPFLAGS = $(_CPPFLAGS)
toxu: $(UNITTEST_TARGETS)
	tox -c tox_unit.ini

# Integration tests rely on a KAFKA_VERSION environment variable, otherwise the
# integration tests are skipped. Also, use integration-only tox config which
# teamcity builder uses, to ensure it gets tested during dev.
toxi: $(UNITTEST_TARGETS)
	KAFKA_VERSION=$(KAFKA_VER) tox -c tox_int.ini

# Run the full test suite
toxa: $(UNITTEST_TARGETS)
	KAFKA_VERSION=$(KAFKA_VER) tox

# Run the full test suite until it fails
toxr: $(UNITTEST_TARGETS)
	KAFKA_VERSION=$(KAFKA_VER) sh -c "while tox; do : ; done"

# Run just the tests selected in tox_cur.ini
toxc: $(UNITTEST_TARGETS)
	KAFKA_VERSION=$(KAFKA_VER) tox -c $(TOP)/tox_cur.ini

# We use flag files so that we only need to run the python3 check
# stage if the file changes. Also, record if the file contains args.
# The postprocess stage will consider it an error if the .todo file
# contains "2to3Args" and the file requires changes.
build/python3/%.todo: %
	@mkdir -p $(dir $@)
	$(AT)twotothreeargs=$$(awk -F: '/^# 2to3_args:/{print $$2}' $<); \
	if [ -n "$$twotothreeargs" ]; then \
	    echo "2to3Args:$$twotothreeargs" > $@; \
	fi; \
	2to3 $$twotothreeargs $< >>$@ 2>&1

# We use flag files so that we only need to run the lint stage if the file
# changes.
build/pyflakes/%.flag: %
	$(AT)pyflakes $<
	$(AT)pep8 --ignore=$(PEP8_IGNORES) $<
	$(AT)$(TOOLS)/check-pybang $<
	@mkdir -p $(dir $@)
	@touch "$@"
