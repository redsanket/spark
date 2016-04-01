ROOT=/home/y
YAHOO_CFG=$(ROOT)/share/yahoo_cfg

-include $(YAHOO_CFG)/screwdriver/Make.rules

screwdriver: cleanHTF compileHTF

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

