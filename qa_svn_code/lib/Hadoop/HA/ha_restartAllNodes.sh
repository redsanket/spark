#!/bin/bash
########
## Restart all nodes in a freshly deployed (and updated) flubber cluster with HA 
##  Needs to run from adm102, or an admin box
##  Needs to be run AFTER ha_updateAllNodes.sh 
##  20131206phw
########

if [ $# -ne 1 ]; then
  echo "Need the cluster name "
  exit 1
fi
CLUSTER=$1
 
IGOR_PROXY=`yinst which-dist`
#echo "My igor proxy is: $IGOR_PROXY"

##
# Function: stop/start the given datanode, need to be exec'ed as root
##
function toggleNode {
  ssh $1 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/sbin/hadoop-daemon.sh --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  --script /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs $2 datanode"
}

##
# Function: stop/start the given namenode, need to be exec'ed as hdfsqa
##
function toggleNameNode {
  ssh $1  "su hdfsqa -c \"/home/gs/gridre/yroot.$CLUSTER/share/hadoop/sbin/hadoop-daemon.sh --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  --script /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs $2  namenode\""
}

##
# Function: stop/start the given resourcemanager, need to be exec'ed as mapredqa
##
function toggleResourceManager {
  ssh $1  "su mapredqa -c \"/home/gs/gridre/yroot.$CLUSTER/share/hadoop/sbin/yarn-daemon.sh --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  $2  resourcemanager\""
}


##
# restart all the compute nodes, based on the igor members list for the base role
##
# get the complete list of members for the cluster
LISTFILE="/tmp/list_members_$CLUSTER"
LISTFILE2="/tmp/list_members_clean_$CLUSTER"
`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER | grep blue.ygrid.yahoo.com > $LISTFILE | cat $LISTFILE |grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2 ; cat $LISTFILE | grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2 > $LISTFILE2`

# stop all nodes
for host in `cat $LISTFILE2`; do
  HOSTNAME=`echo $host |grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2`
  echo "Stopping: $HOSTNAME"
  toggleNode $HOSTNAME stop
done

# start all nodes
for host in `cat $LISTFILE2`; do
  HOSTNAME=`echo $host |grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2`
  echo "Starting: $HOSTNAME"
  toggleNode $HOSTNAME start
done

rm $LISTFILE $LISTFILE2

####
## restart the namenodes
####
# namenode
NAMENODE=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.namenode|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`

echo "Stopping namenode $NAMENODE"
toggleNameNode $NAMENODE stop

sleep 2
echo "Starting namenode $NAMENODE"
toggleNameNode $NAMENODE start
 
# namenode2
NAMENODE2=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.namenode2|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`

echo "Stopping namenode2 $NAMENODE2"
toggleNameNode $NAMENODE2 stop

sleep 2
echo "Starting namenode2 $NAMENODE2"
toggleNameNode $NAMENODE2 start


####
## restart the resourcemanager
####
# get the RM host
RM=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.jobtracker|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`

echo "Stopping resourcemanager $RM"
toggleResourceManager $RM stop
sleep 2
echo "Starting resourcemanager $RM"
toggleResourceManager $RM start

