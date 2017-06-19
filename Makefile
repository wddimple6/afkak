
# This must be set before we include any other makefiles
TOP := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILDSTART:=$(shell date +%s)

RELEASE_DIR := $(TOP)/build
TOXDIR := $(TOP)/.tox
VENV := $(TOP)/.env
TOX := $(VENV)/bin/tox
SERVERS := $(TOP)/servers
KAFKA_ALL_VERS := 0.8.0 0.8.1 0.8.1.1 0.8.2.1 0.8.2.2 0.9.0.1
KAFKA_VER ?= 0.9.0.1
KAFKA_RUN := $(SERVERS)/$(KAFKA_VER)/kafka-bin/bin/kafka-run-class.sh
UNAME := $(shell uname)
PYPI ?= 'https://pypi.python.org/simple/'
AT ?= @

AFKAK_VERSION := $(shell awk '$$1 == "version" { gsub("\"", "", $$3); print($$3) }' setup.py)
CHANGES_VERSION := $(shell awk 'NR == 1 { print($$2); }' CHANGES.md)
INIT_VERSION := $(shell awk '$$1 == "__version__" { gsub("\"", "", $$3); print($$3) }' afkak/__init__.py)
ifneq ($(AFKAK_VERSION),$(CHANGES_VERSION))
  ifneq ($(AFKAK_VERSION)-SNAPSHOT,$(CHANGES_VERSION))
    $(error Version on first line of CHANGES.md ($(CHANGES_VERSION)) does not match the version in setup.py ($(AFKAK_VERSION)))
  endif
endif

ifneq ($(INIT_VERSION),$(AFKAK_VERSION))
  $(error Version in setup.py ($(AFKAK_VERSION)) does not match afkak/__init__.py ($(INIT_VERSION)))
endif

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
	afkak/codec.py \
	afkak/group.py \
	afkak/group_assignment.py

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
	afkak/test/test_util.py \
	afkak/test/test_group.py \
	afkak/test/test_group_assignment.py

INTTEST_PYFILES := \
	afkak/test/test_client_integration.py \
	afkak/test/test_consumer_integration.py \
	afkak/test/test_failover_integration.py \
	afkak/test/test_producer_integration.py \
	afkak/test/test_group_integration.py

SETUP_PYFILES := setup.py

MISC_PYFILES := \
	consumer_example \
	producer_example

ALL_PYFILES := $(AFKAK_PYFILES) $(UNITTEST_PYFILES) \
    $(INTTEST_PYFILES) $(MISC_PYFILES) $(SETUP_PYFILES)

# We lint all python files
PYLINTERS_TARGETS += $(foreach f,$(ALL_PYFILES),build/pyflakes/$f.flag)
# Unittests
UNITTEST_TARGETS += $(PYLINTERS_TARGETS)

# Files to cleanup
UNITTEST_CLEANS  += build/pyflakes $(PYL_ACK_ERRS)
EGG := $(TOP)/afkak.egg-info
TRIAL_TEMP := $(TOP)/_trial_temp
COVERAGE_CLEANS := $(TOP)/.coverage $(TOP)/coverage.xml $(TOP)/htmlcov
CLEAN_TARGETS += $(UNITTEST_CLEANS) $(EGG) $(COVERAGE_CLEANS) $(TRIAL_TEMP)
CLEAN_TARGETS +=

# We don't yet use this, but will eventually check for Python3 compatibility
# But Twisted needs full Python3 support first...
PY3CHK_TARGETS += $(foreach f,$(ALL_PYFILES),build/python3/$f.todo)

###########################################################################
## Start of system makefile
###########################################################################
.PHONY: all clean pyc-clean timer build venv release documentation
.PHONY: lint toxik toxa toxr toxi toxu toxc toxrc toxcov

all: timer

timer: build
	@echo "---( Make $(MAKECMDGOALS) Complete (time: $$((`date +%s`-$(BUILDSTART)))s) )---"

build: toxa
	@echo "Done"

clean: pyc-clean
	$(AT)rm -rf $(CLEAN_TARGETS)
	@echo "Done cleaning"

dist-clean: clean
	$(AT)rm -rf $(TOXDIR) $(VENV) $(TOP)/.noseids $(RELEASE_DIR)
	$(AT)$(foreach VERS,$(KAFKA_ALL_VERS), rm -rf $(SERVERS)/$(VERS)/kafka-bin)
	@echo "Done dist-cleaning"

git-clean:
	$(AT)git clean -fdx
	@echo "Done git-cleaning"

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
	$(AT)$(VENV)/bin/pip install --upgrade pip
	$(AT)$(VENV)/bin/pip install --upgrade --index-url $(PYPI) -r requirements_venv.txt

lint: export LANG = $(_LANG)
lint: $(VENV) $(PYLINTERS_TARGETS)
	$(AT)$(VENV)/bin/pyroma $(TOP)
	$(AT)$(TOX) -e lint
	@echo Done

# Run the integration test suite under all Kafka versions
toxik:
	$(AT)$(foreach VERS,$(KAFKA_ALL_VERS), KAFKA_VER=$(VERS) $(MAKE) toxi && ) echo "Done"

# Run the full test suite
toxa: export CPPFLAGS = $(_CPPFLAGS)
toxa: export LANG = $(_LANG)
toxa: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX)

# Run the full test suite until it fails
toxr: export CPPFLAGS = $(_CPPFLAGS)
toxr: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) sh -c "while $(TOX); do : ; done"

# Run just the integration tests
toxi: export CPPFLAGS = $(_CPPFLAGS)
toxi: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX) -e int

# Run just the unit tests
toxu: export CPPFLAGS = $(_CPPFLAGS)
toxu: $(UNITTEST_TARGETS)
	$(TOX) -e unit

# Run just the tests selected in the 'cur' tox environment
toxc: export CPPFLAGS = $(_CPPFLAGS)
# When iterating, don't bother with slightly long lines
toxc: PEP8_MAX_LINE := --max-line-length=120
toxc: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX) -e cur

# Run the just the tests selected in tox_cur.ini until they fail
toxrc: export CPPFLAGS = $(_CPPFLAGS)
toxrc: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) sh -c "while time $(TOX) -e cur; do : ; done"

# Run just the 'coverage' tox environment
toxcov: export CPPFLAGS = $(_CPPFLAGS)
toxcov: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	KAFKA_VERSION=$(KAFKA_VER) $(TOX) -e coverage

# We use flag files so that we only need to run the lint stage if the file
# changes.
build/pyflakes/%.flag: % $(VENV)
	$(AT)$(VENV)/bin/pyflakes $<
	$(AT)$(VENV)/bin/flake8 $<
	# $(AT)pep257 $<
	# $(AT)dodgy $<
	# $(AT)frosted $<
	@mkdir -p $(dir $@)
	@touch "$@"

# Targets to push a release to artifactory
checkver:
	@if [[ $(CHANGES_VERSION) =~ SNAPSHOT ]]; then \
	  echo 'FATAL: Cannot tag/release as "SNAPSHOT" version: $(CHANGES_VERSION)'; \
	  false; \
	fi

release: checkver toxa
	$(AT)$(TOX) -e release
