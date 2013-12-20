#!/bin/bash
########
## Setup a freshly deployed flubber cluster with HA 
##  Needs to run from adm102, or an admin box
##  20131206phw
########

# check if we got the cluster's name
if [ $# -ne 1 ]; then
  echo "Need the cluster name "
  exit 1
fi
CLUSTER=$1
 
# get the proxy we need to talk to igor from within the colo
IGOR_PROXY=`yinst which-dist`
#echo "My igor proxy is: $IGOR_PROXY"

## get the cluster's namenodes
# namenode
NAMENODE=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.namenode|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`
echo "Namenode is: $NAMENODE"
# namenode2
NAMENODE2=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.namenode2|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`
echo "Namenode2 is: $NAMENODE2"

# get the list of all cluster members from igor
LISTFILE="/tmp/list_members_$CLUSTER"
LISTFILE2="/tmp/list_members_clean_$CLUSTER"
`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER | grep blue.ygrid.yahoo.com > $LISTFILE | cat $LISTFILE |grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2 ; cat $LISTFILE | grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2 > $LISTFILE2`

####
## Function: check the given node for 64bit JDK 
####
function checkJdk64 {
  RESULT=`ssh $1 "rm /home/gs/java/jdk; ln -s /home/gs/java/jdk64/current /home/gs/java/jdk;readlink /home/gs/java/jdk"`
  if [ "$RESULT" = "/home/gs/java/jdk64/current" ]; then
    echo "Info, $1 is using 64bit JDK"
  else
    echo "Error, cannot set 64bit JDK on $1" 
    exit 1
  fi
}


####
## main entry point
####

## make sure hadoop_qe_runasroot pkg with ifdown/ifup support is installed on ha1 and ha2
# ha1
ssh $NAMENODE "/usr/local/bin/yinst i hadoop_qe_runasroot -root /home/gs/gridre/yroot.$CLUSTER -br test"
PKGCHECK=`ssh $NAMENODE "/usr/local/bin/yinst ls hadoop_qe_runasroot -root /home/gs/gridre/yroot.$CLUSTER | grep hadoop_qe_runasroot-0.1.0.1384989520"`
if [ "$PKGCHECK" != "hadoop_qe_runasroot-0.1.0.1384989520" ]; then
  echo "Error, failed to install hadoop_qe_runasroot-0.1.0.1384989520 on $NAMENODE"
  exit 1
fi
# ha2
ssh $NAMENODE2 "/usr/local/bin/yinst i hadoop_qe_runasroot -root /home/gs/gridre/yroot.$CLUSTER -br test"
PKGCHECK=`ssh $NAMENODE2 "/usr/local/bin/yinst ls hadoop_qe_runasroot -root /home/gs/gridre/yroot.$CLUSTER | grep hadoop_qe_runasroot-0.1.0.1384989520"`
if [ "$PKGCHECK" != "hadoop_qe_runasroot-0.1.0.1384989520" ]; then
  echo "Error, failed to install hadoop_qe_runasroot-0.1.0.1384989520 on $NAMENODE2"
  exit 1
fi

# for each member, make sure the jdk link points to 64bit and overwrite the confs
# needed for HA changes 
for host in `cat $LISTFILE2`; do
  HOSTNAME=`echo $host |grep blue.ygrid.yahoo.com | cut --delimiter=\'  -f2`
  echo "Host is: $HOSTNAME"
  # check that JDK is 64bit 
  checkJdk64 $HOSTNAME
  # cp HA specific confs over
  ssh $HOSTNAME "cp /homes/patwhite/work/HadoopHA/confs/core-site.xml.$CLUSTER.HAphw  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/core-site.xml"
  ssh $HOSTNAME "cp /homes/patwhite/work/HadoopHA/confs/hdfs-site.xml.$CLUSTER.HAphw  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-site.xml"
  ssh $HOSTNAME "cp /homes/patwhite/work/HadoopHA/confs/hdfs-ha.xml.$CLUSTER.HAphw  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-ha.xml"
done

# cleanup
rm $LISTFILE $LISTFILE2

