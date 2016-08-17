ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
YAHOO_CFG=/home/y/share/yahoo_cfg
include $(YAHOO_CFG)/Make.defs

ifndef HTF_VERSION
TIMESTAMP := $(shell date +%y%m%d%H%M)
export HTF_VERSION := 1.0.0.${TIMESTAMP}
endif

# Rules has to be included after vars are set, or it thinks you didn't set PACKAGE_CONFIG or whatever, and makes it *.yicf.
include $(YAHOO_CFG)/Make.rules

.PHONY: screwdriver

cleanplatforms::

platforms:: releaseHTF cleanHTF build

screwdriver: releaseHTF cleanHTF build

.PHONY: build 

releaseHTF:
	@echo "Creating HTF version"
	@echo ${HTF_VERSION} > ${SOURCE_DIR}/RELEASE

cleanHTF:
	@echo "Cleaning HTF"

build:
	@echo "Building HTF"

testCommit:
	@echo "HTF Commit tests"
