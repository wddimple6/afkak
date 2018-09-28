
# This must be set before we include any other makefiles
TOP := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILDSTART:=$(shell date +%s)

RELEASE_DIR := $(TOP)/build
TOXDIR := $(TOP)/.tox
VENV := $(TOP)/.env
PYPI ?= https://pypi.python.org/simple/
TOX := $(VENV)/bin/tox -i $(PYPI)
SERVERS := $(TOP)/servers
KAFKA_ALL_VERS := 0.9.0.1 1.1.1
KAFKA_VER ?= 0.9.0.1
KAFKA_RUN := $(SERVERS)/$(KAFKA_VER)/kafka-bin/bin/kafka-run-class.sh
UNAME := $(shell uname)
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

# Files to cleanup
EGG := $(TOP)/afkak.egg-info
TRIAL_TEMP := $(TOP)/_trial_temp
COVERAGE_CLEANS := $(TOP)/.coverage $(TOP)/coverage.xml $(TOP)/htmlcov
CLEAN_TARGETS += $(UNITTEST_CLEANS) $(EGG) $(COVERAGE_CLEANS) $(TRIAL_TEMP)

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
	rm -rf $(CLEAN_TARGETS)
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
	$(AT)$(TOP)/tools/download-kafka.py $(KAFKA_VER)
	$(AT)[ -x $(KAFKA_RUN) ]

venv: $(VENV)
	@echo "Done creating virtualenv"

$(VENV): export CPPFLAGS = $(_CPPFLAGS)
$(VENV): export LANG = $(_LANG)
$(VENV):
	virtualenv --python python2.7 --no-download $(VENV)
	$(VENV)/bin/pip install --index-url $(PYPI) tox==2.9.1

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
	$(TOX) -l | grep -e-int- | KAFKA_VERSION=$(KAFKA_VER) xargs -n1 $(TOX) -e

# Run just the unit tests
toxu: export CPPFLAGS = $(_CPPFLAGS)
toxu: $(UNITTEST_TARGETS)
	$(TOX) -l | grep -e-unit- | xargs -n1 $(TOX) -e

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

# Union the test coverage of all Tox environments.
toxcov: export CPPFLAGS = $(_CPPFLAGS)
toxcov: export KAFKA_VERSION = $(KAFKA_VER)
toxcov: $(UNITTEST_TARGETS) $(KAFKA_RUN)
	$(TOP)/tools/coverage.sh '$(TOX)'

# Targets to push a release to artifactory
checkver:
	@if [[ $(CHANGES_VERSION) =~ SNAPSHOT ]]; then \
	  echo 'FATAL: Cannot tag/release as "SNAPSHOT" version: $(CHANGES_VERSION)'; \
	  false; \
	fi

release: checkver toxa
	$(AT)$(TOX) -e release
