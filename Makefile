ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
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

.PHONY: screwdriver

platforms:: release cleanbuild build

.PHONY: build 

pr_build:
	@echo "Pull Request build..."

release:
	@echo "Creating version ${VERSION}"
	@echo ${VERSION} > ${SOURCE_DIR}/RELEASE

cleanbuild:
	@echo "Cleaning build environment"

build:
	@echo "Building..."
	yinst i rocl
	sudo cp bin/mtree /usr/sbin/mtree

testCommit:
	@echo "Commit tests"
