#!/bin/bash

# Usage: prepareCsmt.sh user [namenode] 
# copy data to 

source ${HDFT_TOP_DIR}/src/hdft_util_lib.sh
source ${WORKSPACE}/lib/library.sh

PROG=prepData.sh
DEST_USER=$1
DEST_NN=$2

DEFAULT_NN=`getDefaultNameNode`
DATA_SRC=/homes/hdfsqa/hdfsRegressionData/

DFS_CMD=`hdft_getDfsCommand`

if [ $# == 0 ] ; then
    echo "Usage: $0 user [namenode]"
    exit 0
fi

if [ $# == 2 ] ; then
    DEST_URI="hdfs://${DEST_NN}.blue.ygrid"
else 
    DEST_URI="hdfs://${DEFAULT_NN}"
fi

DEST_DIR=${DEST_URI}/user/$DEST_USER/hdfsRegressionData

echo "${PROG}:: Input Param DEST_USER=$DEST_USER"
echo "${PROG}:: Input Param DEST_NN=$DEST_NN"
echo "${PROG}:: Env DEFAULT_NN=$DEFAULT_NN"
echo "${PROG}:: Env DATA_SRC=$DATA_SRC"
echo "${PROG}:: Env DEST_DIR=$DEST_DIR"
echo "${PROG}:: Env DFS_CMD=$DFS_CMD"

echo "${PROG}:: Preparing Data to copy from src $DATA_SRC for user $DEST_USER to $DEST_DIR"

set -x
hdftKInitAsUser $DEST_USER


echo "${PROG} Env:: HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME"
echo "${PROG} Env:: HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME"
echo "${PROG} Env:: HADOOP_MAPRED_HOME=$HADOOP_MAPRED_HOME"
echo "${PROG} Env:: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "${PROG} Env:: Which hdfs running: $DFS_CMD"
echo "${PROG} Env:: Which hadoop running: `which hadoop` "
echo "${PROG} Env:: Hadoop version: `hadoop version`"

echo "${PROG} Env:: Copy HDFSRegressionData from $DATA_SRC"
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls  $DEST_DIR
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -test -d $DEST_DIR
if [ $? == 0 ]; then
	$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rmr -skipTrash $DEST_DIR
fi

echo "${PROG} doit:: $DFS_CMD dfs -copyFromLocal $DATA_SRC   $DEST_DIR"
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -copyFromLocal $DATA_SRC   $DEST_DIR
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -mkdir -p  $DEST_DIR/tmpWrite
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -chmod 777    $DEST_DIR/tmpWrite

echo "${PROG} setup: $DFS_CMD dfs -lsr   $DEST_DIR"
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -lsr   $DEST_DIR

# echo "Now prepare permTest"
# sh prepPermData.sh $DEST_URI/user/$DEST_USER/hdfsRegressionData/PermTest2
