ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
YAHOO_CFG=/home/y/share/yahoo_cfg
include $(YAHOO_CFG)/Make.defs

ifndef DEPLOY_VERSION
BUILD_DATE := $(shell date)
TIMESTAMP := $(shell date +%y%m%d%H%M)
export DEPLOY_VERSION := 1.0.0.${TIMESTAMP}
endif

export MALLOC_ARENA_MAX=4

# Rules has to be included after vars are set, or it thinks you didn't set PACKAGE_CONFIG or whatever, and makes it *.yicf.
include $(YAHOO_CFG)/Make.rules

.PHONY: screwdriver
screwdriver: release_version build jtest

cleanplatforms::

platforms:: release_version build jtest

.PHONY: build 


release_version:
	@echo "DEPLOY_VERSION ${DEPLOY_VERSION}"  
	@echo "DEPLOY_VERSION=${DEPLOY_VERSION}" > ${WORKSPACE}/param.properties

build: 
	@echo "Running build..."
	@echo "Build completed..."

.PHONY: jtest
jtest:
	@echo "Setting up tests..."
	@echo "Tests completed..."
