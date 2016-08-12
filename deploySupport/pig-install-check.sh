# Launcher script to fire off pig install on gateway 
#
# This is called from hudson-startslave-openstack if jenkins option
# to install pig is set true.
#
# Note that this component install is different than that of Core,
# Core is yinst packaged and dropped on admin node where the install
# package is executed as root. This install does not need root and can 
# be delivered directly to the component node from the RE node.
#
# inputs: cluster to install pig on and reference cluster name,
# note that LATEST is a virtual cluster for latest push to artifactory
# returns: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster to install pig onto and reference cluster name"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

PIGNODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.gateway)"`;
if [ -z "$PIGNODE" ]; then
  echo "ERROR: No Pig node defined, PIGNODE is empty! Is the Rolesdb role correctly set for gateway?"
  exit 1
fi
echo "INFO: Going to call Pig installer for gateway node $PIGNODE..."


# setup ssh cmd with parameters
SSH_OPT=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"


INSTALL_SCRIPT=pig-install.sh

# copy the installer to the target node and run it
$SCP $INSTALL_SCRIPT  $PIGNODE:/tmp/
  
$SSH $PIGNODE "cd /tmp/ && /tmp/$INSTALL_SCRIPT $CLUSTER $REFERENCE_CLUSTER"
RC=$?

if [ $RC -ne 0 ]; then
  echo "ERROR: Pig installer failed!"
  exit 1
fi

# Clean up pig install
echo "INFO: remove $PIGNODE:/tmp/$INSTALL_SCRIPT"
$SSH $PIGNODE "rm /tmp/$INSTALL_SCRIPT"

exit 0

