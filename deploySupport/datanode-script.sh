#!/bin/sh

action=$1
cluster=$2

# we use 64-bit for datanode
export JAVA_HOME=$GSHOME/java/jdk64/current
export HADOOP_CLASSPATH=$HADOOP_COMMON_HOME/share/hadoop/server/*:$HADOOP_COMMON_HOME/share/hadoop/server/lib/*

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

    # GRIDCI-443 - CID out of sync
    if [[ $ERASEENABLED == "true" ]]; then
        if [ -e /grid/0/tmp/hadoop-$HDFSUSER ]; then
            rm -rf /grid/0/tmp/hadoop-$HDFSUSER
        fi
    fi

    if [ "`whoami`" != "root" ]
    then
        # $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs start datanode $nameStartOpt

        if [[ "$HADOOPVERSION" =~ ^3. ]]; then
	  $HADOOP_HDFS_HOME/bin/hdfs --daemon start datanode $nameStartOpt
        elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
          $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR start datanode $nameStartOpt
        else
            echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
            exit 1
        fi

        echo "*** Note: not running as root. How to start data nodes?"
    fi

    if [ -f $HADOOP_HDFS_HOME/bin/hdfs  ]
    then
        [ -f /var/run/jsvc.pid ] && rm -rf /var/run/jsvc.pid
        export HADOOP_SECURE_DN_USER=$HDFSUSER
        if [[ "$HADOOPVERSION" =~ ^3. ]]; then
	  $HADOOP_HDFS_HOME/bin/hdfs --daemon start datanode $nameStartOpt
        elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
          $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR start datanode $nameStartOpt
        else
            echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
            exit 1
        fi

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

        if [[ "$HADOOPVERSION" =~ ^3. ]]; then
	  $HADOOP_HDFS_HOME/bin/hdfs --daemon stop datanode $nameStartOpt
        elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
          $HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script "$HADOOP_HDFS_HOME"/bin/hdfs stop datanode $nameStartOpt
        else
            echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
            exit 1
        fi
    fi
else
    echo "Usage: datanode-script.sh [startonly|stop|start+erase]"
fi
