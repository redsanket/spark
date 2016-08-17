ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
YAHOO_CFG=/home/y/share/yahoo_cfg
include $(YAHOO_CFG)/Make.defs

ifndef QEVM_VERSION
TIMESTAMP := $(shell date +%y%m%d%H%M)
export QEVM_VERSION := 1.0.0.${TIMESTAMP}
endif

# Rules has to be included after vars are set, or it thinks you didn't set PACKAGE_CONFIG or whatever, and makes it *.yicf.
include $(YAHOO_CFG)/Make.rules

.PHONY: screwdriver

cleanplatforms::

platforms:: releaseQEVM cleanQEVM build

screwdriver: releaseQEVM cleanQEVM build

.PHONY: build 

releaseQEVM:
	@echo "Creating QEVM version"
	@echo ${QEVM_VERSION} > ${SOURCE_DIR}/RELEASE

cleanQEVM:
	@echo "Cleaning QEVM"

build:
	@echo "Building QEVM"

testCommit:
	@echo "QEVM Commit tests"
