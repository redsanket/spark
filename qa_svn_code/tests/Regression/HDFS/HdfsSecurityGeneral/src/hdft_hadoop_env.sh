#!/bin/sh

export HDFT_HADOOP_ENV_SH=1
 
# Input: SG_WORKSPACE & CLUSTER need to be set
# Exporting all the HADOOP environment variables 

if [ -z "$SG_WORKSPACE" ]  ||  [ ! -d $SG_WORKSPACE ] ; then
	echo "ERROR: Key Env Variable SG_WORKSPACE is either not set or is not a valid directory - [$SG_WORKSPACE]"
	exit 1
fi
if [ -z "$CLUSTER" ]  || [ ! -d "${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER" ] ; then
	echo "ERROR: Key Env Variable CLUSTER is either not set or is not a valid cluster - [$CLUSTER]"
	exit 1
fi

if [ -z "$CLUSTER" ] && [  -f $SG_WORKSPACE/../HadoopPushButtonAutomation/clusterName ] ; then
  export CLUSTER=`cat $SG_WORKSPACE/../HadoopPushButtonAutomation/clusterName`
fi


export HADOOP_CONF_DIR=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/conf/hadoop/

export YARN_CONF_DIR=$HADOOP_CONF_DIR
if [ -z "$HADOOP_VERSION" ] ; then
	# not as reliable as running hadoop version, but you need this before setting up the path
	if [ -f $HADOOP_CONF_DIR/${CLUSTER}.namenodeconfigs.xml ] ; then
		export HADOOP_VERSION="22"
	else
		export HADOOP_VERSION="20"
	fi
fi

export HADOOP_VERSION=22

# To solve: http://bug.corp.yahoo.com/show_bug.cgi?id=2426820
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true ${HADOOP_OPTS}"


if [ $HADOOP_VERSION == '22' ] ; then
	# hadoop 22 settings
	#export HADOOP_COMMON_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoopcommon/
	#export HADOOP_MAPRED_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoopmapred
	export HADOOP_YARN_HOME=$HADOOP_MAPRED_HOME
	#export HADOOP_HDFS_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoophdfs/
	export HADOOP_CMD="hdfs"
	unset HADOOP_HOME

	#export HADOOP_TEST_JAR=$HADOOP_MAPRED_HOME/hadoop-test.jar
	#export HADOOP_TOOLS_JAR=$HADOOP_MAPRED_HOME/hadoop-tools.jar
	#export HADOOP_EXAMPLES_JAR=$HADOOP_MAPRED_HOME/lib/hadoop-mapred-examples-0.22.0.*.jar 
	#export HADOOP_STREAMING_JAR=$HADOOP_MAPRED_HOME/contrib/streaming/hadoop-0.22.0.*-streaming.jar



else if [ $HADOOP_VERSION == '20' ] ; then
	# hadoop 20 settings
	export HADOOP_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoop-current
	export HADOOP_CMD="hadoop"

	export HADOOP_TEST_JAR=$HADOOP_HOME/hadoop-test.jar
	export HADOOP_TOOLS_JAR=$HADOOP_HOME/hadoop-tools.jar
	export HADOOP_EXAMPLES_JAR=$HADOOP_HOME/hadoop-examples.jar
	export HADOOP_STREAMING_JAR=$HADOOP_HOME/hadoop-streaming.jar

     else 
	echo "ERROR: HADOOP_VERSION is set to unknown [$HADOOP_VERSION]"
	echo "ERROR: pls set HADOOP_VERSION to either 22 or 20"
	exit 1
	# hadoop version setting
      fi
fi

if [ -n "$CLUSTER" ]  && [ "$CLUSTER" == "omegab" ] ; then
	export HFDT_NAMENODE="gsbl90772.blue.ygrid"
	export HFDT_JOBTRACKER="gsbl90771.blue.ygrid"

	export HADOOP_DFS_CMD="hadoop dfs"
fi

