#!/bin/sh
export JAVA_HOME=$GSHOME/java8/jdk64/current

[ -z "$HADOOP_CONF_DIR" ] && export HADOOP_CONF_DIR=${yroothome}/conf/hadoop

[ -z "$HADOOP_HOME" ]  && export HADOOP_HOME=${GSHOME}/hadoop/current

[ -z "$HDFSUSER" ] && export HDFSUSER=hdfs

echo $0 -- HDFSUSER=$HDFSUSER

if [ `whoami` != $HDFSUSER ]
then
	echo "failure: need to run $0 as $HDFSUSER." 1>&2
	exit 2
fi
case $1 in
    start+erase)
       export ERASEENABLED=true
       CMD=start
       echo "starting and erasing first."
       ;;
   startonly)
       export ERASEENABLED=false
       CMD=start
       echo "starting but not erasing."
       ;;
   stop)
       echo "stopping, only."
       CMD=stop
       ;;
   *)
       echo "unknown option to $0."
       exit 1
       ;;
esac
echo "Part 1: beginning."

# For SBN, the regular namenode is started.
if [ "$ENABLE_HA" = true ]; then
    TARGET_CMD=namenode
else
    TARGET_CMD=secondarynamenode
fi

if [ $CMD == "start" ]; then
    # cleangrid.sh deletes all data in the name dir in case it is for SBN.
    # So there is no need to format SBN. For 2NN, the working dir is 
    # unconditionally deleted here.
    if [ -e /grid/0/tmp/hadoop-$HDFSUSER ]; then
        rm -rf /grid/0/tmp/hadoop-$HDFSUSER
    fi
    if [ "$ENABLE_HA" = true ] && [ "$ERASEENABLED" = true ]; then
        # SBN needs bootstrapping to populate the local storage dir by
        # reading from the shared dir. PNN must be formated before this.
        $HADOOP_HDFS_HOME/bin/hdfs namenode -bootstrapStandby -force
    fi
        
    if [ -e ${GSHOME}/conf/local/masters ]; then
        $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --hosts masters --script "$HADOOP_HDFS_HOME"/bin/hdfs start $TARGET_CMD $nameStartOpt
    fi
    echo "Part 1 finishing immediately after start of secondary name node."
elif [ $CMD == "stop" ]; then 
    if [ -e ${GSHOME}/conf/local/masters ]; then
        $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --hosts masters --script "$HADOOP_HDFS_HOME"/bin/hdfs stop $TARGET_CMD $nameStartOpt
    fi
    echo "Part 1 finishing immediately after stop of secondary name node."
else
    echo "Usage: namenodescript.sh [startonly|stop|start+erase]"
fi
echo "Part 1: done."
