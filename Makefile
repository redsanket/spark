ROOT=/home/y
MVN=/home/y/bin/mvn

SRCTOP = .
YAHOO_CFG=/home/y/share/yahoo_cfg
include $(YAHOO_CFG)/Make.defs
# YAHOO_CFG=$(ROOT)/share/yahoo_cfg

-include $(YAHOO_CFG)/screwdriver/Make.rules

ifndef HTF_VERSION
TIMESTAMP := $(shell date +%y%m%d%H%M)
export HTF_VERSION := 1.0.0.${TIMESTAMP}
endif

# Rules has to be included after vars are set, or it thinks you didn't set PACKAGE_CONFIG or whatever, and makes it *.yicf.
include $(YAHOO_CFG)/Make.rules

.PHONY: screwdriver

cleanplatforms::

platforms:: releaseHTF cleanHTF compileHTF

screwdriver: releaseHTF cleanHTF compileHTF

releaseHTF:
	@echo "Creating HTF version"
	@echo ${HTF_VERSION} > ${WORKSPACE}/app_root/RELEASE

cleanHTF:
	@echo "Cleaning HTF"
	mvn -f hadooptest/pom.xml clean

compileHTF:
	@echo "Building HTF"
	rm -rf hadooptest/htf-common/src/test/scala/hadooptest/spark/regression/spark1_2
	mvn -f hadooptest/pom.xml package -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8 -gs hadooptest/resources/yjava_maven/settings.xml.orig -Pprofile-all -Pprofile-corp -DskipTests

testCommit:
	@echo "HTF Commit tests"
	hadooptest/scripts/run_hadooptest_remote --screwdriver -c openorange -u hadoopqa -r openorangegw.ygridvm.yahoo.com -s ./hadooptest/ -m -n -f /homes/hadoopqa/hadooptest_conf/hadooptest-ci.conf --resultsdir $(TEST_RESULTS_DIR) -t TestVersion,TestFS,TestConf,TestSleepJobCLI,TestYarnClient,TestIgorLookup

