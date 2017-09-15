ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
YAHOO_CFG=/home/y/share/yahoo_cfg
include $(YAHOO_CFG)/Make.defs

export UPLOAD_ARTIFACTORY=true
export TEST_TAG=HADOOP_2_8_0_LATEST
export CERTIFIED_TAG=hadoop_2_8_latest_certified_release
export CLUSTERS=all
export CLUSTER_ROLES=grid.clusters.sandbox,grid.clusters.research,grid.clusters.prod,grid_re.clusters.staging
export CLUSTERS_TO_PROMOTE=axonitered
export COMPONENTS=hadoop-2.8
export packages='hadoop conf'
# will be uncommented after makerelease on re107 is disabled 
# export UPDATE_RELEASE=true


ifndef VERSION
TIMESTAMP := $(shell date +%y%m%d%H%M)
export VERSION := 1.0.0.${TIMESTAMP}
endif

# Rules has to be included after vars are set, or it thinks you didn't set PACKAGE_CONFIG or whatever, and makes it *.yicf.
include $(YAHOO_CFG)/Make.rules

.PHONY: screwdriver

cleanplatforms::

platforms:: release cleanbuild build

screwdriver: release cleanbuild build

.PHONY: build 

release:
	@echo "Creating version ${VERSION}"
	@echo ${VERSION} > ${SOURCE_DIR}/RELEASE

cleanbuild:
	@echo "Cleaning build environment"

build:
	@echo "Building..."
	sudo cp bin/mtree /usr/sbin/mtree

testCommit:
	@echo "Commit tests"
