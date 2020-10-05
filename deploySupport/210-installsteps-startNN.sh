set +x
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
if [ "$STARTNAMENODE" != true ]; then
    echo "STARTNAMENODE is not enabled. Nothing to do."
    return 0
fi

if [ "$REMOVEEXISTINGDATA" = true ]; then
    arg=start+erase
else
    arg=startonly
fi

set +e
# Continue even if remove files returns non zero exit code
fanout_nn_root "rm /tmp/namenode-part-*-script.sh"
set -e

#################################################################################
# Run startnn1.sh as HDFS user
#################################################################################

# USER is used in /home/gs/gridre/yroot.openphil1blue/conf/hadoop/hadoop-env.sh, which is called by
# /home/gs/gridre/yroot.openphil1blue/share/hadoop/sbin/hadoop-daemon.sh
JAVA_HOME="$GSHOME/java/jdk64/current"
(
echo "export CLUSTERID=$CLUSTERID"
echo "export GSHOME=$GSHOME"
echo "export yroothome=$yroothome"
echo "export HDFSUSER=$HDFSUSER"
echo "export USER=$HDFSUSER"
echo "export MAPREDUSER=$MAPREDUSER"
echo "export JAVA_HOME=$JAVA_HOME"
echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
echo "export ENABLE_HA=$ENABLE_HA"
#workaround for one-day with older name for common, expires sept 9
echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
echo "export YARN_HOME=${yroothome}/share/hadoop"
echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_DEFAULT_LIBEXEC_DIR=${HADOOP_HOME}/libexec"
echo "export HADOOPVERSION=$HADOOPVERSION "
echo "sh /tmp/namenode-part-1-script.sh $arg "
) > $scriptdir/startnn1.sh

fanoutscp \
"$scriptdir/namenode-part-1-script.sh $scriptdir/startnn1.sh" \
"/tmp/" \
"$ALLNAMENODESLIST"

fanout_nn_hdfsuser "sh /tmp/startnn1.sh"
st=$?
[ "$st" -ne 0 ] && echo "Failed to run namenode-part-1-script.sh" && exit $st
fanout_nn_root "rm /tmp/namenode-part-1-script.sh"

#################################################################################
# Run startsecondary.sh as HDFS user if secondary namenode exists
#################################################################################
if [ -n "$secondarynamenode" ]; then
    (
    echo "export CLUSTERID=$CLUSTERID"
    echo "export GSHOME=$GSHOME"
    echo "export yroothome=$yroothome"
    echo "export HDFSUSER=$HDFSUSER"
    echo "export USER=$HDFSUSER"
    echo "export MAPREDUSER=$MAPREDUSER"
    echo "export JAVA_HOME=$JAVA_HOME"
    echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export ENABLE_HA=$ENABLE_HA"
    echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
    # workaround for one-day with older name for common, expires sept 9
    echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
    echo "export YARN_HOME=${yroothome}/share/hadoop"
    echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_HOME=${yroothome}/share/hadoop   "
    echo "export HADOOPVERSION=$HADOOPVERSION "
    echo "sh /tmp/namenode2-part-1-script.sh $arg "
    ) > $scriptdir/startsecondary.sh

    fanoutscp \
"$scriptdir/namenode2-part-1-script.sh $scriptdir/startsecondary.sh" \
"/tmp/" \
"$ALLSECONDARYNAMENODESLIST"

    fanoutSecondary_hdfsuser "sh /tmp/startsecondary.sh"
    st=$?
    [ "$st" -ne 0 ] && echo "Failed to run namenode2-part-1-script.sh" && exit $st

    fanoutSecondary_root "rm /tmp/namenode2-part-1-script.sh"
fi

#################################################################################
# Start datanodes
#################################################################################
echo "======= short-term workaround Nov 15: start up DN as $HDFSUSER"
## $PDSH -w "$SLAVELIST"  \
#"scp $ADMIN_HOST:$scriptdir/datanode-script.sh  /tmp/datanode-script.sh && \
#export HADOOP_COMMON_HOME=${yroothome}/share/hadoop && export HADOOP_HOME=${yroothome}/share/hadoop && \
#export HADOOP_HDFS_HOME=${yroothome}/share/hadoop && \
#export HDFSUSER=$HDFSUSER && \
#export HADOOP_CONF_DIR=${yroothome}/conf/hadoop && \
#su $HDFSUSER -c 'sh /tmp/datanode-script.sh $arg $cluster' "

fanoutscp \
"$scriptdir/datanode-script.sh" \
"/tmp/datanode-script.sh" \
"$SLAVELIST"

fanout_workers_root "\
export GSHOME=$GSHOME && \
export yroothome=$yroothome export HADOOP_COMMON_HOME=${yroothome}/share/hadoop && \
export HADOOP_PREFIX=${yroothome}/share/hadoop && \
export HADOOP_HOME=${yroothome}/share/hadoop && \
export HADOOP_HDFS_HOME=${yroothome}/share/hadoop && \
export HDFSUSER=$HDFSUSER && \
export USER=$HDFSUSER && \
export HADOOP_CONF_DIR=${yroothome}/conf/hadoop && \
export JAVA_HOME=$JAVA_HOME && \
export HADOOPVERSION=$HADOOPVERSION && \
sh /tmp/datanode-script.sh $arg $cluster; \
[[ \$? -eq 0 ]] && rm -f /tmp/datanode-script.sh\
"
st=$?
[ "$st" -ne 0 ] && echo "Failed to run datanode-script.sh" && exit $st

#################################################################################
# Run finishNN.sh as HDFS user
#################################################################################
(
echo "export GSHOME=$GSHOME"
echo "export yroothome=$yroothome"
echo "export HDFSUSER=$HDFSUSER"
echo "export USER=$HDFSUSER"
echo "export MAPREDUSER=$MAPREDUSER"
echo "export JAVA_HOME=$JAVA_HOME"
echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
# workaround for one-day with older name for common, expires sept 9
echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
echo "export YARN_HOME=${yroothome}/share/hadoop"
echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_HOME=${yroothome}/share/hadoop   "
echo "export HADOOPVERSION=$HADOOPVERSION "
echo "sh /tmp/namenode-part-3-script.sh $arg "
) > $scriptdir/finishNN.sh

fanoutscp \
"$scriptdir/namenode-part-3-script.sh $scriptdir/finishNN.sh" \
"/tmp/" \
"$ALLNAMENODESLIST"

fanout_nn_hdfsuser "sh /tmp/finishNN.sh"
st=$?
[ "$st" -ne 0 ] && echo "Failed to run finishNN.sh / namenode-part-3-script.sh" && exit $st

#################################################################################
# Run finishNN2.sh as HDFS user
#################################################################################
(
echo "export GSHOME=$GSHOME"
echo "export yroothome=$yroothome"
echo "export HDFSUSER=$HDFSUSER"
echo "export USER=$HDFSUSER"
echo "export MAPREDUSER=$MAPREDUSER"
echo "export JAVA_HOME=$JAVA_HOME"
echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
# workaround for one-day with older name for common, expires sept 9
echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
echo "export YARN_HOME=${yroothome}/share/hadoop"
echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
echo "export HADOOP_HDFS_HOME=${yroothome}/share/hadoop"
echo "export HADOOP_HOME=${yroothome}/share/hadoop   "
echo "export HADOOPVERSION=$HADOOPVERSION "
echo "sh /tmp/namenode2-part-3-script.sh $arg "
) > $scriptdir/finishNN2.sh

fanoutscp \
"$scriptdir/namenode2-part-3-script.sh $scriptdir/finishNN2.sh" \
"/tmp/" \
"$ALLSECONDARYNAMENODESLIST"

fanoutSecondary_hdfsuser "sh /tmp/finishNN2.sh"
st=$?
[ "$st" -ne 0 ] && echo "Failed to run finishNN2.sh / namenode2-part-3-script.sh" && exit $st
