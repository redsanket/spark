#!/bin/sh

### This is the Configuration file where only Hadoop Product related Environment variable will be set up
echo "========== Hadoop Related Exported variables ================="
export CLUSTER=$1; echo "CLUSTER=$CLUSTER"
export WORKSPACE=$2; echo "WORKSPACE=$WORKSPACE"
export ARTIFACTS=$3; echo "ARTIFACTS=$ARTIFACTS"
export HADOOP_QA_ROOT=$4; echo "HADOOP_QA_ROOT=$HADOOP_QA_ROOT"
export HADOOP_LOG_DIR=$5; echo "HADOOP_LOG_DIR=$HADOOP_LOG_DIR"
export OWNER="Not Mentioned";
export CLUSTER_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER; echo "CLUSTER_HOME=$CLUSTER_HOME"
# export JAVA_HOME=${CLUSTER_HOME}/share/gridjdk64-1.6.0_21; echo "JAVA_HOME=$JAVA_HOME"
## Special Variables for Hadoop 23
export HADOOP_PREFIX=${CLUSTER_HOME}/share/hadoop ; echo "HADOOP_PREFIX = $HADOOP_PREFIX"
## Get Hadoop version for internal use 
HADOOP_VERSION=$(ls -ld $HADOOP_PREFIX | sed 's/^.*hadoop-\(.*\)$/\1/')

export HADOOP_COMMON_HOME=$HADOOP_PREFIX ; echo "HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME"
export HADOOP_HDFS_HOME=$HADOOP_PREFIX; echo "HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME"
export HADOOP_MAPRED_HOME=$HADOOP_PREFIX; echo "HADOOP_MAPRED_HOME=$HADOOP_MAPRED_HOME"
export HADOOP_YARN_HOME=$HADOOP_PREFIX; echo "HADOOP_YARN_HOME=$HADOOP_YARN_HOME"
## Common for both Hadoop 20 and Hadoop 23
export HADOOP_CONF_DIR=${CLUSTER_HOME}/conf/hadoop/; echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
export HADOOP_YARN_CONF_DIR=$HADOOP_CONF_DIR; echo "HADOOP_YARN_CONF_DIR=$HADOOP_YARN_CONF_DIR"
export HADOOP_MAPRED_EXAMPLES_JAR=${HADOOP_PREFIX}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar; echo "HADOOP_MAPRED_EXAMPLES_JAR=$HADOOP_MAPRED_EXAMPLES_JAR"
# export HADOOP_MAPRED_TEST_JAR=${HADOOP_PREFIX}/hadoop-mapreduce-test-*.jar; echo "HADOOP_MAPRED_TEST_JAR=$HADOOP_MAPRED_TEST_JAR"
export HADOOP_MAPRED_TEST_JAR=${HADOOP_PREFIX}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-${HADOOP_VERSION}-tests.jar; echo "HADOOP_MAPRED_TEST_JAR=$HADOOP_MAPRED_TEST_JAR"
export HADOOP_MAPREDUCE_JOBCLIENT_JAR=$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-${HADOOP_VERSION}.jar;echo "HADOOP_MAPREDUCE_JOBCLIENT_JAR=$HADOOP_MAPREDUCE_JOBCLIENT_JAR"
export HADOOP_TEST_JAR=$HADOOP_MAPRED_TEST_JAR; echo "HADOOP_TEST_JAR=$HADOOP_TEST_JAR";
export HADOOP_STREAMING_JAR=${HADOOP_PREFIX}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar; echo "HADOOP_STREAMING_JAR=$HADOOP_STREAMING_JAR"
export HADOOP_EXAMPLES_JAR=$HADOOP_MAPRED_EXAMPLES_JAR; echo "HADOOP_EXAMPLES_JAR=$HADOOP_EXAMPLES_JAR"
export HADOOP_COMMON_CMD=${HADOOP_PREFIX}/bin/hadoop
export HADOOP_MAPRED_CMD=${HADOOP_PREFIX}/bin/mapred
export HADOOP_HDFS_CMD=${HADOOP_PREFIX}/bin/hdfs
# export HADOOP_TOOLS_JAR=${HADOOP_PREFIX}/hadoop-tools.jar

#env var for different users
export HADOOPQA_USER="hadoopqa"
export HADOOP1_USER="hadoop1"
export HADOOP2_USER="hadoop2"
export HADOOP3_USER="hadoop3"
export HADOOP4_USER="hadoop4"
export HADOOP5_USER="hadoop5"
export HADOOP6_USER="hadoop6"
export HADOOP7_USER="hadoop7"
export HADOOP8_USER="hadoop8"
export HADOOP9_USER="hadoop9"
export HADOOP10_USER="hadoop10"
export HADOOP11_USER="hadoop11"
export HADOOP12_USER="hadoop12"
export HADOOP13_USER="hadoop13"
export HADOOP14_USER="hadoop14"
export HADOOP15_USER="hadoop15"
export HADOOP16_USER="hadoop16"
export HADOOP17_USER="hadoop17"
export HADOOP18_USER="hadoop18"
export HADOOP19_USER="hadoop19"
export HADOOP20_USER="hadoop20"
export HDFS_USER="hdfs"
export HDFSQA_USER="hdfsqa"
export MAPRED_USER="mapred"
export MAPREDQA_USER="mapredqa"

#super users for 22
export HDFS_SUPER_USER=$HDFSQA_USER
export MAPRED_SUPER_USER=$MAPREDQA_USER


#variables that specify the location of the keytab files
export KEYTAB_FILES_HOMES_DIR="/homes"
export KEYTAB_FILES_DIR="/homes/hdfsqa/etc/keytabs"
export KEYTAB_FILE_NAME_SUFFIX=".dev.headless.keytab"

#variables that store the location of the kerb tickets
timestamp=`date +%s`
export KERBEROS_TICKETS_DIR="/grid/0/tmp/${CLUSTER}.kerberosTickets.${timestamp}"
export KERBEROS_TICKET_SUFFIX=".kerberos.ticket"

#variables for the different keys files
export HDFS_USER_KEY_FILE="/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfs"
export HDFSQA_USER_KEY_FILE="/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa"
export MAPRED_USER_KEY_FILE="/homes/hadoopqa/.ssh/flubber_hadoopqa_as_mapred"
export MAPREDQA_USER_KEY_FILE="/homes/hadoopqa/.ssh/flubber_hadoopqa_as_mapredqa"

#super user for 22
export HDFS_SUPER_USER_KEY_FILE=$HDFSQA_USER_KEY_FILE
export MAPRED_SUPER_USER_KEY_FILE=$MAPREDQA_USER_KEY_FILE

# static variable to indicate the test branch/test version
export HADOOPQE_TEST_BRANCH=`$HADOOP_COMMON_CMD version|grep Hadoop|awk '{print $2}'|cut -d'.' -f1-2`

#timeout in secondfs to wait for the namenode in safemode in seconds
export NN_SAFEMODE_TIMEOUT=300

#location of the JMX jar file
export JMX_JAR_FILE="/homes/mapred/jmxterm-1.0-SNAPSHOT-uber.jar"
