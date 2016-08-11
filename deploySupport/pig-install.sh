# script to install pig on the cluster's gateway node
#
# inputs: cluster being installed, reference version 
# outputs: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name and reference version"
  exit 1
fi

CLUSTER=$1
REFERENCE_VERSION=$2

PIGNODE=`hostname`
PIGNODE_SHORT=`echo $PIGNODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Pig node being installed: $PIGNODE"

# check what comp version we need to use
echo "STACK_COMP_VERSION_PIG is: $REFERENCE_VERSION"

if [ "$REFERENCE_VERSION" == "test" ]; then
  # need to use dist_tag to get pig test version per Rohini
  PACKAGE_VERSION_PIG=`dist_tag list  pig_0_14_test | grep pig- |cut -d' ' -f1`
elif [ "$REFERENCE_VERSION" == "axonitered" ]; then
  yinst i hadoop_releases_utils
  RC=$?
  if [ "$RC" -ne 0 ]; then
    echo "Error: failed to install hadoop_releases_utils on $PIGNODE!"
    exit 1
  fi
  PACKAGE_VERSION_PIG=pig-`/home/y/bin/query_releases -c $REFERENCE_VERSION -b pig -p pig_current`
else
  echo "ERROR: unknown reference component version $REFERENCE_VERSION!!"
  exit 1
fi

#
## install pig
#
yinst install -same -live -downgrade  $PACKAGE_VERSION_PIG
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

