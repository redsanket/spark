#!/bin/sh

## Driving script to call other script to do real testing.

PROG="hdfsRegression.sh"
export HDFT_TOP_DIR=`pwd`
export HDFT_HOME=$HDFT_TOP_DIR

#export HADOOP_COMMON_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoopcommon
#export HADOOP_HDFS_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoophdfs
#export HADOOP_MAPRED_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoopmapred
#export HADOOP_CONF_DIR=/grid/0/gs/gridre/yroot.$CLUSTER/conf/hadoop
export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH

echo "================================================="
echo "${PROG} Env:: Running on cluster $CLUSTER"
echo "${PROG} Env:: Host name: `hostname`"
echo "${PROG} Env:: Current Time: `date`"
echo "${PROG} Env:: Current Directory: `pwd`"
echo "${PROG} Env:: Current user: `whoami`"

echo "${PROG} Env:: CLUSTER=$CLUSTER"
echo "${PROG} Env:: WORKSPACE=$WORKSPACE"
echo "${PROG} Env:: HADOOP_HOME=$HADOOP_HOME"
echo "${PROG} Env:: HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME"
echo "${PROG} Env:: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "${PROG} Env:: HDFT_TOP_DIR=$HDFT_TOP_DIR"
echo "${PROG} Env:: HDFT_HOME=$HDFT_HOME"
echo ""

# not as reliable as running hadoop version, but you need this before setting up the path
if [ -f $HADOOP_CONF_DIR/${CLUSTER}.namenodeconfigs.xml ] ; then
	export HADOOP_VERSION="22"
else
	export HADOOP_VERSION="20"
fi

echo "${PROG} Dump of other env setting:"
env | egrep 'HADOOP|MAPRED|HDFS|HDFL_HDFT_|HDFD_' | sort

echo "${PROG} Call prepData/prepData.sh to prepare data ... "
(cd prepData; sh $HDFT_TOP_DIR/prepData/prepData.sh hadoopqa)
 ##sh RunTest.sh

sh RunAll.sh
