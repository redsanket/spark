
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


if [ "$STARTNAMENODE" = true  -a  "$REMOVEEXISTINGDATA" = true ]
then

    if [ -z "$NAMENODE_Primary" ]
    then
        echo "$0: Internal error: cannot decide what primary namenode is."
        exit 1
    fi
    nn=$NAMENODE_Primary
    if [[ "$HADOOP_27" == "true" ]]; then
        JAVA_HOME="$GSHOME/java8/jdk64/current"
    else
        JAVA_HOME="$GSHOME/java/jdk64/current"
    fi
    (
        set -x
        echo "cd ${yroothome}"
        echo "JAVA_HOME=$JAVA_HOME HADOOP_PREFIX=${yroothome}/share/hadoop  perl /tmp/getclusterid.pl > /tmp/$cluster.clusterid.txt"
    ) | ssh $nn su - $HDFSUSER
    st=$?
    echo Exit status of ssh for getclusterid was $st
    [ -f /tmp/$cluster.clusterid.txt ] && rm -rf /tmp/$cluster.clusterid.txt
    if [ $st -eq 0 ]
    then
        scp ${nn}:/tmp/$cluster.clusterid.txt   /tmp/
        export CLUSTERID=`cat /tmp/$cluster.clusterid.txt`
    fi
    ssh $nn  rm  -f  /tmp/$cluster.clusterid.txt
    [ -f /tmp/$cluster.clusterid.txt ] && rm -rf /tmp/$cluster.clusterid.txt
fi
