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

nn=$NAMENODE_Primary
JAVA_HOME="$GSHOME/java/jdk64/current"
(
    set -x
    echo "cd ${yroothome}"
    echo "JAVA_HOME=$JAVA_HOME HADOOP_PREFIX=${yroothome}/share/hadoop  perl /tmp/getclusterid.pl > /tmp/$cluster.clusterid.txt"
) | $SSH $nn su - $HDFSUSER
st=$?
echo "Exit status of ssh for getclusterid was $st"

[ -f /tmp/$cluster.clusterid.txt ] && rm -rf /tmp/$cluster.clusterid.txt
if [ $st -eq 0 ]; then
    set -x
    scp ${nn}:/tmp/$cluster.clusterid.txt   /tmp/
    export CLUSTERID=`cat /tmp/$cluster.clusterid.txt`
    set +x
fi

$SSH $nn  rm  -f  /tmp/$cluster.clusterid.txt
[ -f /tmp/$cluster.clusterid.txt ] && rm -rf /tmp/$cluster.clusterid.txt
