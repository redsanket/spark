# script to install pig on the cluster's gateway node
#
# inputs: cluster being installed, reference cluster name 
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

# check what comp version we need to use
echo "STACK_COMP_VERSION_PIG is: $REFERENCE_CLUSTER"

# make sure we have tools to talk to artifactory
yinst i hadoop_releases_utils
RC=$?
if [ "$RC" -ne 0 ]; then
  echo "Error: failed to install hadoop_releases_utils on $PIGNODE!"
  exit 1
fi

# check we got a valid reference cluster
RESULT=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER`
RC=$?
if [ $RC -eq 0 ]; then 
  # get Artifactory URI and log it
  ARTI_URI=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER  -v | grep downloadUri |cut -d\' -f4`
  echo "Artifactory URI with most recent versions:"
  echo $ARTI_URI

  # look up pig version for AR in artifactory
  PACKAGE_VERSION_PIG=pig-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b pig -p pig_latest`

else
  echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT" 
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

