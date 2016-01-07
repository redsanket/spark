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
if [ $CMD == "start" ]; then
    if [ "$ERASEENABLED" = true ]
    then
          if [ "$ENABLE_HA" = true ]; then
              $HADOOP_HDFS_HOME/bin/hdfs namenode -format -force -clusterid $CLUSTERID
          else
              echo Y | $HADOOP_HDFS_HOME/bin/hdfs namenode -format -clusterid $CLUSTERID
          fi
    fi

    if [ "$ENABLE_HA" = true ]; then
           echo Initializing shared edit directory.
           # "-nonInteractive" will make it bail out if there is existing data in the shared dir.
           $HADOOP_HDFS_HOME/bin/hdfs namenode -initializeSharedEdits -nonInteractive
    fi
    if [[ "$HADOOP_27" == "true" ]]; then
        nameStartOpt="-upgrade $nameStartOpt"
    fi

    echo "${HADOOP_HDFS_HOME}/bin/hdfs start namenode -upgrade ${nameStartOpt}"
    $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs start namenode $nameStartOpt

    # transition ha1 to active. wait until it comes up in standby mode.
    # rather than sleep, we could use hadmin to query the namenode state.
    if [ "$ENABLE_HA" = true ]; then
        sleep 10
        echo "Transitioning ha1 to active."
        $HADOOP_HDFS_HOME/bin/hdfs --config $HADOOP_CONF_DIR haadmin -transitionToActive ha1
    fi

#if [ -e ${GSHOME}/conf/local/masters ]; then
#        $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --hosts masters --script "$HADOOP_HDFS_HOME"/bin/hdfs start secondarynamenode $nameStartOpt
#    fi
    echo "Part 1 finishing immediately after start of name node."
elif [ $CMD == "stop" ]; then 
#    if [ -e ${GSHOME}/conf/local/masters ]; then
#        $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --hosts masters --script "$HADOOP_HDFS_HOME"/bin/hdfs stop secondarynamenode $nameStartOpt
#    fi
    $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs stop namenode $nameStartOpt
    echo "Part 1 finishing immediately after stop of name node."
else
    echo "Usage: namenodescript.sh [startonly|stop|start+erase]"
fi
echo "Part 1: done."
