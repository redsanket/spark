#!/bin/bash

# Usage: cleanup.sh user [namenode] 
# copy data to 

source ${HDFT_TOP_DIR}/src/hdft_util_lib.sh
source ${WORKSPACE}/lib/library.sh
source ${WORKSPACE}/lib/user_kerb_lib.sh

PROG=cleanup.sh
DEST_USER=$1
DEST_NN=$2

DEFAULT_NN=`getDefaultNameNode`

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
echo "${PROG}:: Env DEST_DIR=$DEST_DIR"
echo "${PROG}:: Env DFS_CMD=$DFS_CMD"

echo "${PROG}:: Clean up all data using user hdfs"

set -x
setKerberosTicketForUser hdfs
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r -skipTrash $DEST_DIR
setKerberosTicketForUser $DEST_USER