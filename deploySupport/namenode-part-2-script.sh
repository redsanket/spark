#!/bin/sh

action=$1
cluster=$2

export JAVA_HOME=$GSHOME/java8/jdk64/current

[ -z "$HADOOP_CONF_DIR" ] && export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
[ -z "$HDFSUSER" ] && export HDFSUSER=hdfs

if [ `whoami` != root ]
then
	echo "failure: need to run $0 as $HDFSUSER." 1>&2
fi


case $1 in
    start+erase)
       export ERASEENABLED=true
       CMD=start
       # echo "starting and erasing first."
       ;;
   startonly)
       export ERASEENABLED=false
       CMD=start
       # echo "starting but not erasing."
       ;;
   stop)
       # echo "stopping, only."
       CMD=stop
       ;;
   *)
       echo "unknown option to $0."
       exit 1
       ;;
esac
# echo "Part 2: beginning."
if [ $CMD == "start" ]; then
    # echo "Part 2 started and we really should be running as root, for this."
    if [ "`whoami`" != "root" ]
    then
	$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs start datanode $nameStartOpt
        echo "*** Note: not running as root. How to start data nodes?"
    fi
    if [ -f $HADOOP_HDFS_HOME/bin/hdfs  ]
    then
	[ -f /var/run/jsvc.pid ] && rm -rf /var/run/jsvc.pid
	export HADOOP_SECURE_DN_USER=$HDFSUSER
	$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs start datanode $nameStartOpt
    else
        echo ================================
        echo ================================
        echo ================================
        echo ================================
        echo ================================
        echo "=== major mistake, cannot start data nodes"
        echo "=== Cannot find \$HADOOP_HDFS_HOME/bin/hdfs  "
        echo ================================
        echo ================================
        echo ================================
        echo ================================
        echo ================================
    fi
elif [ $CMD == "stop" ]; then 
    if [ "`whoami`" != "root" ]
    then
        echo "*** Note: not running as root. How to stop data nodes?"
    fi
    if [ -f $HADOOP_HDFS_HOME/bin/hdfs  ]
    then
        # echo "EXACT CMD: $HADOOP_HDFS_HOME/bin/hdfs  --config  $HADOOP_CONF_DIR  stop datanode"
	export HADOOP_SECURE_DN_USER=$HDFSUSER
        $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs stop datanode $nameStartOpt
    fi
else
    echo "Usage: namenode-part-2-script.sh.sh [startonly|stop|start+erase]"
fi
# echo "Part 2: done."
