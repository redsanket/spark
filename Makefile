ROOT=/home/y
YAHOO_CFG=$(ROOT)/share/yahoo_cfg

-include $(YAHOO_CFG)/screwdriver/Make.rules

screwdriver: cleanHTF compileHTF

cleanHTF:
	@echo "Cleaning HTF"
	mvn -f hadooptest/pom.xml clean

compileHTF:
	@echo "Building HTF"
	mvn -f hadooptest/pom.xml package -gs hadooptest/resources/yjava_maven/settings.xml.orig -Pprofile-all -Pprofile-corp -DskipTests
    
