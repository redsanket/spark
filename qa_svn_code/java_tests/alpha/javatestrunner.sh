#!/bin/sh

testcluster=$1
testclass=$2
workspace=$3

echo "************************************************"
echo "Initilizing kerboros tickets"
echo "************************************************"
. ${workspace}/java_tests/alpha/kerborosinit.sh
. ${workspace}/lib/user_kerb_lib.sh
getAllUserTickets

HADOOP_CONF_DIR=/grid/0/gshome/gridre/yroot.$1/conf/hadoop

TEST_LIB_CLASSPATH=/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/yconcordion/lib/junit-4.8.2.jar:
TEST_LIB_CLASSPATH=${TEST_LIB_CLASSPATH}:/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/yconcordion/lib/ognl-2.6.9.jar
TEST_LIB_CLASSPATH=${TEST_LIB_CLASSPATH}:/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/yconcordion/lib/xom-1.2.5.jar
TEST_LIB_CLASSPATH=${TEST_LIB_CLASSPATH}:/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/yconcordion/lib/commons-logging-1.1.1.jar

#The following classpath is not needed, this library is already included in target/classes
#CONCORDION_CLASSPATH=/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/yconcordion/target/concordion-1.4.2-SNAPSHOT.jar

HADOOP_CLASSPATH=/home/gs/gridre/yroot.$testcluster/share/hadoop/share/hadoop/common/*
HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/home/gs/gridre/yroot.$testcluster/share/hadoop/share/hadoop/hdfs/*
HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/home/gs/gridre/yroot.$testcluster/share/hadoop/share/hadoop/mapreduce/*
HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/home/gs/gridre/yroot.$testcluster/share/hadoop/share/hadoop/tools/*

TEST_CLASSPATH=/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/alpha/target/classes/:${TEST_LIB_CLASSPATH}:${HADOOP_CLASSPATH}

echo "TEST_CLASSPATH=$TEST_CLASSPATH"

TEST_OPTS="-DHADOOP_HOME=$HADOOP_HOME"
TEST_OPTS="${TEST_OPTS} -DHADOOP_CONF_DIR=$HADOOP_CONF_DIR"
TEST_OPTS="${TEST_OPTS} -DclusterType=real"
TEST_OPTS="${TEST_OPTS} -DtestList=/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/alpha/src/main/resource/testconfig/testlist.txt"
TEST_OPTS="${TEST_OPTS} -DTEST_RESOURCE_DIR=/homes/bachbui/Works/HadoopQEAutomation/branch-23/java_tests/alpha/src/main/resource/data"
TEST_OPTS="${TEST_OPTS} -DclusterName=eomer"
#KERBEROS_TICKETS_DIR="/grid/0/tmp/${testcluster}.kerberosTickets.1335214804"
TEST_OPTS="${TEST_OPTS} -DKERBEROS_TICKET_DIR=${KERBEROS_TICKETS_DIR}"

echo "************************************************"
echo "Starting the tests"
echo "************************************************"

java ${TEST_OPTS} -cp $TEST_CLASSPATH org.junit.runner.JUnitCore $testclass