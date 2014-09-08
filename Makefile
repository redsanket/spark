include /home/y/share/yahoo_cfg/screwdriver/Make.rules

screwdriver: clean compile

clean:
	@echo "Cleaning HTF"
	mvn -f hadooptest/pom.xml clean

compile:
	@echo "Building HTF"
	mvn -f hadooptest/pom.xml package -gs hadooptest/resources/yjava_maven/settings.xml.orig -Pprofile-all -Pprofile-corp -DskipTests
    
