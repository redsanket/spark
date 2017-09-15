ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
YAHOO_CFG=/home/y/share/yahoo_cfg
include $(YAHOO_CFG)/Make.defs

export SKIP_VALIDATION=true
export UPLOAD_ARTIFACTORY=true
export TEST_TAG=HADOOP_2_8_0_LATEST
export CERTIFIED_TAG=hadoop_2_8_latest_certified_release
export CLUSTERS=all
export CLUSTER_ROLES=grid.clusters.sandbox,grid.clusters.research,grid.clusters.prod,grid_re.clusters.staging
export CLUSTERS_TO_PROMOTE=axonitered
export COMPONENTS=hadoop-2.8
export packages='hadoop conf'


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
	# sudo yum install --enablerepo=repoidglob mtree
	sudo cp bin/mtree /usr/sbin/mtree

testCommit:
	@echo "QEVM Commit tests"
