# script to install pig on the cluster's gateway node
#
# inputs: cluster being installed, reference cluster 
# outputs: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name and reference cluster"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

PIGNODE=`hostname`
PIGNODE_SHORT=`echo $PIGNODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Pig node being installed: $PIGNODE"

# check if we need to use a reference cluster, else use 'current'
echo "STACK_COMP_REFERENCE_CLUSTER is: $REFERENCE_CLUSTER"
if [ "$REFERENCE_CLUSTER" == "none" ]; then
  PACKAGE_VERSION_PIG=`yinst package -br current pig`
else
  yinst i hadoop_releases_utils
  RC=$?
  if [ "$RC" -ne 0 ]; then
    echo "Error: failed to install hadoop_releases_utils on $PIGNODE!"
    exit 1
  fi
  PACKAGE_VERSION_PIG=pig-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b pig -p pig_current`
fi

#
## install pig
#
yinst install $PACKAGE_VERSION_PIG
RC=$?
if [ $RC -ne 0 ]; then
  echo "Error: failed to install $PACKAGE_VERSION_PIG on $PIGNODE!"
  exit 1
fi

yinst set pig.PIG_HOME=/home/y/share/pig

# make the grid links for pig
PIGVERSION=`yinst ls | grep pig-`
echo PIGVERSION=$PIGVERSION

yinst install ygrid_pig_multi -br current -set ygrid_pig_multi.CURRENT=$PIGVERSION -same -live

