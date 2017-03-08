# Launcher script to fire off the oozie installer
#
# This is called from hudson-startslave-openstack if jenkins option
# to install oozie is set true. This will also call the hive installer
# to install hive and pig on the oozie node since oozie requires
# these components 
#
# This will also copy over pig-install.sh and hive-install.sh to install 
# pig and hive, since these are needed by oozie 
#
# The oozie installation relies on keytabs which are generated in
# the Configure job.
#
# Note that this component install is different than that of Core,
# Core is yinst packaged and dropped on admin node where the install
# package is executed as root. This install does not need root and can 
# be delivered directly to the component node from the RE node.
#
# inputs: cluster to install oozie on and reference cluster name,
# note that LATEST is a virtual cluster for latest push to artifactory 
# returns: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster to install oozie onto and reference cluster name"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

OOZIENODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.oozie)"`;
if [ -z "$OOZIENODE" ]; then
  echo "ERROR: No Oozie node defined, OOZIENODE for role grid_re.clusters.$CLUSTER.oozie is empty!"
  echo "Is the Rolesdb role correctly set?"
  exit 1
fi
echo "INFO: Going to call Oozie installer for node $OOZIENODE..."

# setup ssh cmd with parameters
SSH_OPT=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"

##
# install hive and pig on the oozie node
##

PIG_INSTALL_SCRIPT=pig-install.sh
HIVE_INSTALL_SCRIPT=hive-install.sh

# copy the installers to the target node and run them
$SCP $PIG_INSTALL_SCRIPT  $OOZIENODE:/tmp/
$SCP $HIVE_INSTALL_SCRIPT  $OOZIENODE:/tmp/

$SSH $OOZIENODE "/tmp/$PIG_INSTALL_SCRIPT $CLUSTER $REFERENCE_CLUSTER"
RC=$?
if [ $RC -ne 0 ]; then
  echo "ERROR: Pig install to Oozie node failed!"
  exit 1
fi

# gridci-1937 check if hive wants to use current branch
if [[ "$STACK_COMP_VERSION_HIVE" == "current" ]]; then
  echo "INFO: Hive install on Oozie node will use current branch"
  REFERENCE_CLUSTER_FOR_HIVE=current
else
  echo "INFO: Hive install on Oozie node will use $REFERENCE_CLUSTER"
  REFERENCE_CLUSTER_FOR_HIVE="$REFERENCE_CLUSTER"
fi
$SSH $OOZIENODE "/tmp/$HIVE_INSTALL_SCRIPT $CLUSTER $REFERENCE_CLUSTER_FOR_HIVE"
RC=$?
if [ $RC -ne 0 ]; then
  echo "ERROR: Hive install to Oozie node failed!"
  exit 1
fi

# Clean up hive and pig install
echo "INFO: remove $OOZIENODE:/tmp/$HIVE_INSTALL_SCRIPT and $OOZIENODE:/tmp/$PIG_INSTALL_SCRIPT"
$SSH $OOZIENODE "rm /tmp/$HIVE_INSTALL_SCRIPT /tmp/$PIG_INSTALL_SCRIPT"

##
# install oozie on oozie node and oozie_client on gateway with required packages.
##

INSTALL_SCRIPT=oozie-install.sh

# copy the installer to the target node and run it
$SCP $INSTALL_SCRIPT  $OOZIENODE:/tmp/
  
$SSH $OOZIENODE "/tmp/$INSTALL_SCRIPT $CLUSTER $REFERENCE_CLUSTER"
RC=$?

if [ $RC -ne 0 ]; then
  echo "ERROR: Oozie installer failed!"
  exit 1
fi

# Clean up oozie install
echo "INFO: remove $OOZIENODE:/tmp/$INSTALL_SCRIPT"
$SSH $OOZIENODE "rm /tmp/$INSTALL_SCRIPT"

exit 0

