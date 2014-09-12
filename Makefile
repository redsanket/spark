ROOT=/home/y
YAHOO_CFG=$(ROOT)/share/yahoo_cfg

-include $(YAHOO_CFG)/screwdriver/Make.rules

screwdriver: cleanHTF compileHTF testCommit

cleanHTF:
	@echo "Cleaning HTF"
	mvn -f hadooptest/pom.xml clean

compileHTF:
	@echo "Building HTF"
	mvn -f hadooptest/pom.xml package -gs hadooptest/resources/yjava_maven/settings.xml.orig -Pprofile-all -Pprofile-corp -DskipTests

testCommit:
	@echo "HTF Commit tests"
	hadooptest/scripts/run_hadooptest_remote -c openorange -u hadoopqa -r openorangegw.ygridvm.yahoo.com -s ./hadooptest/ -m -n -f /homes/hadoopqa/hadooptest_conf/hadooptest-ci.conf -t TestVersion

