# script to install pig on the cluster's gateway node
#
# inputs: cluster being installed 
# outputs: 0 on success

if [ $# -ne 1 ]; then
  echo "ERROR: need the cluster name"
  exit 1
fi

CLUSTER=$1
PIGNODE=`hostname`
PIGNODE_SHORT=`echo $PIGNODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Pig node being installed: $PIGNODE"

#
## install pig
#
# check if we need to use a reference cluster, else use 'current'
echo "STACK_COMP_REFERENCE_CLUSTER is: $STACK_COMP_REFERENCE_CLUSTER"
if [ "$STACK_COMP_REFERENCE_CLUSTER" == "none" ]; then
  yinst i pig -br current
else
  PIG_VERSION_REFERENCE_CLUSTER=`${WORKSPACE}/deploySupport/query_releases -c $STACK_COMP_REFERENCE_CLUSTER -b pig -p pig_current`
  echo PIG_VERSION_REFERENCE_CLUSTER is: $PIG_VERSION_REFERENCE_CLUSTER
  #
  yinst install pig-$PIG_VERSION_REFERENCE_CLUSTER
fi

yinst set pig.PIG_HOME=/home/y/share/pig

# make the grid links for pig
PIGVERSION=`yinst ls | grep pig-`
echo PIGVERSION=$PIGVERSION

yinst install ygrid_pig_multi -br current -set ygrid_pig_multi.CURRENT=$PIGVERSION -same -live

