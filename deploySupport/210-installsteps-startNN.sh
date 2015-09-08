#
# "start the namenode(s), including data nodes and HDFS-level mkdirs."
#
# (1) We have scripts to run. See 189-installsteps-namenodeExplanation.sh
#    (a) namenode-part-1-script.sh makes the filesystem (runs on NN)
#    (b) datanode-script.sh starts all datanodes
#        (runs on all slaves)
#    (c) namenode-part-3-script.sh runs HDFS-level mkdir/chown/etc
#        (runs on NN.)
# (2) We run "fanoutNN" to do the exact-same things on each NN.
# (3) We put the exact-same filesystem on each NN, which is probably a
#     long-term bad-thing but okay for the short-term.
#
# Inputs: $STARTNAMENODE	(boolean)
# Inputs: $REMOVEEXISTINGDATA	(boolean)
# Inputs: $namenode (set by cluster-list.sh)
# Inputs: $cluster	(set by cluster-list.sh)
#
if [ "$STARTNAMENODE" = true ]
then
    if [ "$REMOVEEXISTINGDATA" = true ]
    then
        arg=start+erase
    else
        arg=startonly
    fi

    fanoutNN "rm /tmp/namenode-part-*-script.sh"
    (
    echo "export CLUSTERID=$CLUSTERID"
    echo "export GSHOME=$GSHOME"
    echo "export yroothome=$yroothome"
    echo "export HDFSUSER=$HDFSUSER"
    echo "export MAPREDUSER=$MAPREDUSER"
    echo "export JAVA_HOME=$GSHOME/java/jdk64/current"
    echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export ENABLE_HA=$ENABLE_HA"
     #workaround for one-day with older name for common, expires sept 9
    echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
    echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
    echo "export YARN_HOME=${yroothome}/share/hadoop"
    echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_HOME=${yroothome}/share/hadoop-combined-folder"
    echo "sh /tmp/namenode-part-1-script.sh $arg " 
    ) > /grid/0/tmp/scripts.deploy.$cluster/startnn1.sh
    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/namenode-part-1-script.sh /grid/0/tmp/scripts.deploy.$cluster/startnn1.sh __HOSTNAME__:/tmp/" "$ALLNAMENODESLIST"
    # Run startnn1.sh as HDFS user
    fanoutNN "su $HDFSUSER -c 'sh /tmp/startnn1.sh'"
    st=$?
    [ "$st" -ne 0 ] && echo "Failed to run namenode-part-1-script.sh" && exit $st
    fanoutNN "rm /tmp/namenode-part-1-script.sh"

    if [ -n "$secondarynamenode" ]
    then

    (
    echo "export CLUSTERID=$CLUSTERID"
    echo "export GSHOME=$GSHOME"
    echo "export yroothome=$yroothome"
    echo "export HDFSUSER=$HDFSUSER"
    echo "export MAPREDUSER=$MAPREDUSER"
    echo "export JAVA_HOME=$GSHOME/java/jdk64/current"
    echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export ENABLE_HA=$ENABLE_HA"
    echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
    # workaround for one-day with older name for common, expires sept 9
    echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
    echo "export YARN_HOME=${yroothome}/share/hadoop"
    echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_HOME=${yroothome}/share/hadoop-combined-folder   "
    echo "sh /tmp/namenode2-part-1-script.sh $arg " 
    ) > /grid/0/tmp/scripts.deploy.$cluster/startsecondary.sh
    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/namenode2-part-1-script.sh /grid/0/tmp/scripts.deploy.$cluster/startsecondary.sh __HOSTNAME__:/tmp/" "$ALLSECONDARYNAMENODESLIST"
    # Run startnn1.sh as HDFS user
    fanoutSecondary "su $HDFSUSER -c 'sh /tmp/startsecondary.sh'"
#    st=$?
#    [ "$st" -ne 0 ] && echo "Failed to run namenode2-part-1-script.sh" && exit $st
    fi
    echo "======= short-term workaround Nov 15: start up DN as $HDFSUSER"
    ## dnstartupuser=$HDFSUSER
    ## $PDSH -w "$SLAVELIST"  "scp $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$cluster/datanode-script.sh  /tmp/datanode-script.sh  && export HADOOP_COMMON_HOME=${yroothome}/share/hadoop && export HADOOP_HOME=${yroothome}/share/hadoop-combined-folder   && export HADOOP_HDFS_HOME=${yroothome}/share/hadoop && export HDFSUSER=$HDFSUSER && export HADOOP_CONF_DIR=${yroothome}/conf/hadoop && su $dnstartupuser  -c 'sh /tmp/datanode-script.sh $arg $cluster' "
    fanoutSecondary "rm /tmp/namenode2-part-1-script.sh"

    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/datanode-script.sh __HOSTNAME__:/tmp/datanode-script.sh" "$SLAVELIST"
    $PDSH -w "$SLAVELIST" "export GSHOME=$GSHOME && export yroothome=$yroothome export HADOOP_COMMON_HOME=${yroothome}/share/hadoop && echo "export HADOOP_PREFIX=${yroothome}/share/hadoop" && export HADOOP_HOME=${yroothome}/share/hadoop-combined-folder   && export HADOOP_HDFS_HOME=${yroothome}/share/hadoop && export HDFSUSER=$HDFSUSER && export HADOOP_CONF_DIR=${yroothome}/conf/hadoop && export  JAVA_HOME=$GSHOME/java/jdk64/current && sh /tmp/datanode-script.sh $arg $cluster  && rm -f /tmp/datanode-script.sh"

    (
    echo "export GSHOME=$GSHOME"
    echo "export yroothome=$yroothome"
    echo "export HDFSUSER=$HDFSUSER"
    echo "export MAPREDUSER=$MAPREDUSER"
    echo "export JAVA_HOME=$GSHOME/java/jdk64/current"
    echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
    # workaround for one-day with older name for common, expires sept 9
    echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
    echo "export YARN_HOME=${yroothome}/share/hadoop"
    echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_HOME=${yroothome}/share/hadoop-combined-folder   "
    echo "sh /tmp/namenode-part-3-script.sh $arg "
    ) > /grid/0/tmp/scripts.deploy.$cluster/finishNN.sh
    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/namenode-part-3-script.sh /grid/0/tmp/scripts.deploy.$cluster/finishNN.sh __HOSTNAME__:/tmp/" "$ALLNAMENODESLIST"
    fanoutNN "su $HDFSUSER -c 'sh /tmp/finishNN.sh'"
    st=$?
    [ "$st" -ne 0 ] && echo "Failed to run namenode-part-3-script.sh" && exit $st

    (
    echo "export GSHOME=$GSHOME"
    echo "export yroothome=$yroothome"
    echo "export HDFSUSER=$HDFSUSER"
    echo "export MAPREDUSER=$MAPREDUSER"
    echo "export JAVA_HOME=$GSHOME/java/jdk64/current"
    echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
    # workaround for one-day with older name for common, expires sept 9
    echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
    echo "export YARN_HOME=${yroothome}/share/hadoop"
    echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_HOME=${yroothome}/share/hadoop-combined-folder   "
    echo "sh /tmp/namenode2-part-3-script.sh $arg "
    ) > /grid/0/tmp/scripts.deploy.$cluster/finishNN2.sh
    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/namenode2-part-3-script.sh /grid/0/tmp/scripts.deploy.$cluster/finishNN2.sh __HOSTNAME__:/tmp/" "$ALLSECONDARYNAMENODESLIST"
    fanoutSecondary "su $HDFSUSER -c 'sh /tmp/finishNN2.sh'"
fi
