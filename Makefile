
# This must be set before we include any other makefiles
TOP := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILDSTART:=$(shell date +%s)

TOOLS := /home/rthille/dev/pv/cyportal/tools
RELEASE_DIR := $(TOP)/build
RELEASE_DEB_DIR := $(RELEASE_DIR)/debs
BUILD_OUTPUT_DIR := deb_dist

# Use ack-grep to find files of a certain type
ACK := ack-grep --noenv --ignore-dir=$(RELEASE_DIR) --ignore-dir=.tox < /dev/null
ifeq ($(shell which ack-grep),)
  $(error Please install ack-grep)
endif


# We don't currently ignore any pep8 errors
PEP8_IGNORES :=

# what files should we run all the linters on?
PYL_ACK_ERRS := $(TOP)/.PYLINTERS_ACK_ERRORS.log
PYL_ACK_ERROUT := $(shell rm -f $(PYL_ACK_ERRS))
PYLINTERS_FILES += $(shell $(ACK) -f --python $(TOP) 2>> $(PYL_ACK_ERRS))
PYL_ACK_ERROUT := $(shell cat $(PYL_ACK_ERRS) 2>/dev/null)
ifneq ($(strip $(PYL_ACK_ERROUT)),)
  $(error Error setting:$$PYLINTERS_FILES "$(PYL_ACK_ERROUT)")
endif

EGG := $(TOP)/afkak.egg-info
TRIAL_TEMP := $(TOP)/_trial_temp
COVERAGE_CLEANS := $(TOP)/.coverage
TOXDIR := $(TOP)/.tox

PYLINTERS_TARGETS += $(foreach f,$(PYLINTERS_FILES),build/pyflakes/$f.flag)
UNITTEST_TARGETS += $(PYLINTERS_TARGETS)
UNITTEST_CLEANS  += build/pyflakes $(PYL_ACK_ERRS)
CLEAN_TARGETS += $(UNITTEST_CLEANS) $(EGG) $(COVERAGE_CLEANS) $(TRIAL_TEMP)

# Piggyback on the fact that 'PYLINTERS_FILES' has pretty much every python file
# in our system, or at least the ones we care about...
PYTHON3_TARGETS += $(foreach f,$(PYLINTERS_FILES),build/python3/$f.todo)

###########################################################################
## Start of system makefile
###########################################################################
.PHONY: all clean pyc-clean timer build
.PHONY: all-debs deb-clean deb-build-dir-clean

all: all-debs

all-debs: timer

timer: build
	@echo "---( Make $(MAKECMDGOALS) Complete (time: $$((`date +%s`-$(BUILDSTART)))s) )---"

build: toxi # Not Yet python3check
	@echo "Done"

clean: pyc-clean deb-build-dir-clean
	$(AT)rm -rf $(CLEAN_TARGETS)
	$(AT)echo "Done cleaning"

dist-clean: clean
	$(AT)rm -rf $(TOX)
	$(AT)echo "Done dist-cleaning"

pyc-clean:
	@echo "Removing '*.pyc' from all subdirs"
	$(AT)find -name '*.pyc' -delete

deb-build-dir-clean:
	$(AT)rm -rf ./build

build/check-setuppy/%.flag: %
	$(AT)$(TOOLS)/check-setuppy $(dir $<)

check-setuppy: $(SETUPPY_TARGETS)
UNITTEST_TARGETS += check-setuppy

python3check: $(PYTHON3_TARGETS)
	$(AT)$(TOOLS)/python3postcheck $(PY3CHKARGS) build/python3

# Tox run with all the tests, but without integration due to lack of $KAFKA_VERSION
toxu: $(UNITTEST_TARGETS)
	tox -c $(TOP)/tox_all.ini

# Integration tests rely on a a KAFKA_VERSION environment variable
toxi: $(UNITTEST_TARGETS)
	KAFKA_VERSION=0.8.1 tox -c $(TOP)/tox_all.ini

# Run the full test suite until it fails
toxr: $(UNITTEST_TARGETS)
	KAFKA_VERSION=0.8.1 sh -c "while tox -c $(TOP)/tox_all.ini; do : ; done"

# Run just the tests selected in tox_cur.ini
toxc: $(UNITTEST_TARGETS)
	KAFKA_VERSION=0.8.1 tox -c $(TOP)/tox_cur.ini

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
