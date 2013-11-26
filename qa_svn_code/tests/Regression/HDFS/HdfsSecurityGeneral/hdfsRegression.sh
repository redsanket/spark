#!/bin/sh

## Driving script to call other script to do real testing.

PROG="hdfsRegression.sh"
export HDFT_TOP_DIR=`pwd`
export HDFT_HOME=$HDFT_TOP_DIR

#if [ -z "$HADOOP_QA_ROOT" ] ; then
#	if [ -e /home/gs/gridre ] ; then
#		HADOOP_QA_ROOT=/home
#	else
#		HADOOP_QA_ROOT=/grid/0
#	fi
#fi

#export HADOOP_COMMON_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoopcommon
#export HADOOP_HDFS_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoophdfs
#export HADOOP_MAPRED_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/share/hadoopmapred
#export HADOOP_CONF_DIR=${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER/conf/hadoop
export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH

echo "================================================="
echo "${PROG} Env:: Running on cluster $CLUSTER"
echo "${PROG} Env:: Host name: `hostname`"
echo "${PROG} Env:: Current Time: `date`"
echo "${PROG} Env:: Current Directory: `pwd`"
echo "${PROG} Env:: Current user: `whoami`"

echo "${PROG} Env:: CLUSTER=$CLUSTER"
echo "${PROG} Env:: HADOOP_QA_ROOT=$HADOOP_QA_ROOT"
echo "${PROG} Env:: SG_WORKSPACE=$SG_WORKSPACE"
echo "${PROG} Env:: HADOOP_HOME=$HADOOP_HOME"
echo "${PROG} Env:: HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME"
echo "${PROG} Env:: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "${PROG} Env:: HDFT_TOP_DIR=$HDFT_TOP_DIR"
echo "${PROG} Env:: HDFT_HOME=$HDFT_HOME"
echo ""

echo "${PROG} Dump of other env setting:"
env | egrep 'HADOOP|MAPRED|HDFS|HDFL_HDFT_|HDFD_' | sort

echo "${PROG} Call prepData/prepData.sh to prepare data ... "
(cd prepData; sh prepData.sh hadoopqa)
# sh runTest.sh

sh RunAll.sh

(cd prepData; sh cleanup.sh hadoopqa)