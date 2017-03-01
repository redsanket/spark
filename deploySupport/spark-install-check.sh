# Launcher script to fire off spark install on gateway and spark history server
# install on jobtracker. 
#
# This is called from hudson-startslave-openstack if jenkins option
# to install spark is set true.
#
# Note that this component install is different than that of Core,
# Core is yinst packaged and dropped on admin node where the install
# package is executed as root. This install does not need root and can 
# be delivered directly to the component node from the RE node.
#
# inputs: cluster to install spark on and reference cluster name,
# note that LATEST is a virtual cluster for latest push to artifactory
# returns: 0 on success

#-------------------------------------------------------------------------------
### functions
function run_on_target_node () {
  target_node=$1
  env_variables=$2
  install_script=$3
  
  # copy the installer to the target node and run it.
  $SCP $install_script $target_node:/tmp/
  $SSH $target_node $env_variables  "/tmp/$install_script $CLUSTER $REFERENCE_CLUSTER"
  
  st=$?
  [[ $st -ne 0 ]] && echo "ERROR: Failed to execute $install_script on $target_node" && exit 1 
  
  # Clean up install scripts
  echo "INFO: remove $target_node:/tmp/$install_script"
  $SSH $target_node "rm /tmp/$install_script"
}

#-------------------------------------------------------------------------------
### main

[[ $# -ne 2 ]] && echo "ERROR: need the cluster to install spark onto and reference cluster name" && exit 1

CLUSTER=$1
REFERENCE_CLUSTER=$2

# If reference cluster is not provided, spark and spark history versions should be provided explicitly.
if [[ $REFERENCE_CLUSTER == "none" ]]; then
  if [[ $SPARKVERSION == "none" ]] && [[ $SPARK_HISTORY_VERSION == "none" ]]; then
    echo "ERROR: Missing info needs to be provided. Specify a reference cluster or SPARKVERSION & SPARK_HISTORY_VERSION"
    exit 1
  fi
fi

echo "=== Spark version = $SPARKVERSION ==="
echo "=== Spark History version = $SPARK_HISTORY_VERSION ==="

# setup ssh cmd with parameters
SSH_OPT=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"

#-------------------------------------------------------------------------------
# Install spark on gateway node
SPARKNODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.gateway)"`;
if [ -z "$SPARKNODE" ]; then
  echo "ERROR: No Spark node defined, SPARKNODE is empty! Is the Rolesdb role correctly set for gateway?"
  exit 1
fi
echo "INFO: Going to call Spark installer for gateway node $SPARKNODE..."

run_on_target_node $SPARKNODE "SPARKVERSION=$SPARKVERSION" spark-install.sh 
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install spark on $SPARKNODE <<<<<<<<<<" && exit $st
#-------------------------------------------------------------------------------
# Install spark history server on jobtracker node
SPARK_HISTORY_SERVER_NODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.jobtracker)"`
if [ -z "$SPARK_HISTORY_SERVER_NODE" ]; then
  echo "ERROR: No Spark History Server node defined, SPARK_HISTORY_SERVER_NODE is empty! Is the Rolesdb role correctly set for jobtracker?"
  exit 1
fi
echo "INFO: Going to call Spark History Server installer for jobtracker node $SPARK_HISTORY_SERVER_NODE..."

run_on_target_node $SPARK_HISTORY_SERVER_NODE "SPARK_HISTORY_VERSION=$SPARK_HISTORY_VERSION" spark-history-server-install.sh 
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install spark history server on $SPARK_HISTORY_SERVER_NODE <<<<<<<<<<" && exit $st
#-------------------------------------------------------------------------------
# Run SparkPi example on gateway node as a basic sanity check for the installation.

run_on_target_node $SPARKNODE "yroothome=$yroothome SPARK_QUEUE=$SPARK_QUEUE" sparkPiExample.sh 
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to run sparkPiExample on $SPARKNODE <<<<<<<<<<" && exit $st

exit 0
