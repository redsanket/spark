set +x

#
# "get the cluster ID before starting any namenodes."
#
#
# (1) We choose to get the cluster-id via this:
#       $hadoop_hdfs_home/bin/hdfs   namenode   -genclusterid
# (2) We run a small perl(1) script to get the output and parse it.
# (2) We run a small perl(1) script to get the output and parse it.
#
# Inputs: $STARTNAMENODE	(boolean)
# Inputs: $REMOVEEXISTINGDATA	(boolean)
# Inputs: $NAMENODE_Primary (set by installgrid.sh)
# Inputs: $cluster
#
# Outputs: $CLUSTERID	(used by namenode startups)
#
#

if [ "$STARTNAMENODE" != true  -o  "$REMOVEEXISTINGDATA" != true ]; then
    echo "STARTNAMENODE='$STARTNAMENODE' REMOVEEXISTINGDATA='$REMOVEEXISTINGDATA': Nothing to do."
    return 0
fi

if [ -z "$NAMENODE_Primary" ]; then
    echo "$0: Internal error: cannot decide what primary namenode is."
    exit 1
fi

clusterid_file="/tmp/$cluster.clusterid.txt"
[ -f $clusterid_file ] && rm -rf $clusterid_file

JAVA_HOME="$GSHOME/java/jdk64/current"
exec_nn_hdfsuser \
"set -x && cd ${yroothome} && \
JAVA_HOME=$JAVA_HOME HADOOP_PREFIX=${yroothome}/share/hadoop \
perl /tmp/getclusterid.pl > $clusterid_file"
st=$?
if [ $st -eq 0 ]; then
    scp $NAMENODE_Primary:$clusterid_file $clusterid_file
    export CLUSTERID=`cat $clusterid_file`
    echo "CLUSTERID=$CLUSTERID"
else
    echo "Run getclusterid failed! Exit code $st"
    exit $st
fi
exec_nn_root "rm -f $clusterid_file"
