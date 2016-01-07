# Launcher script to fire off the oozie installer
#
# This is called from hudson-startslave-openstack if jenkins option
# to install oozie is set true. This will also call the hive installer
# to install hive and pig on the oozie node since oozie requires
# these components 
#
# The oozie installation relies on keytabs which are generated in
# the Configure job.
#
# Note that this component install is different than that of Core,
# Core is yinst packaged and dropped on admin node where the install
# package is executed as root. This install does not need root and can 
# be delivered directly to the component node from the RE node.
#
# inputs: cluster to install oozie on
# returns: 0 on success

if [ $# -ne 1 ]; then
  echo "ERROR: need the cluster to install oozie onto"
  exit 1
fi

CLUSTER=$1

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

HIVE_INSTALL_SCRIPT=hive-install.sh

# copy the installer to the target node and run it
$SCP $HIVE_INSTALL_SCRIPT  $OOZIENODE:/tmp/

$SSH $OOZIENODE "cd /tmp/ && /tmp/$HIVE_INSTALL_SCRIPT $CLUSTER"
RC=$?

if [ $RC -ne 0 ]; then
  echo "ERROR: Hive install to Oozie node failed!"
  exit 1
fi

# Clean up hive install
echo "INFO: remove $OOZIENODE:/tmp/$HIVE_INSTALL_SCRIPT"
$SSH $OOZIENODE "rm /tmp/$HIVE_INSTALL_SCRIPT"

##
# install oozie
##

INSTALL_SCRIPT=oozie-install.sh

# copy the installer to the target node and run it
$SCP $INSTALL_SCRIPT  $OOZIENODE:/tmp/
  
$SSH $OOZIENODE "cd /tmp/ && /tmp/$INSTALL_SCRIPT $CLUSTER"
RC=$?

if [ $RC -ne 0 ]; then
  echo "ERROR: Oozie installer failed!"
  exit 1
fi

# Clean up oozie install
echo "INFO: remove $OOZIENODE:/tmp/$INSTALL_SCRIPT"
$SSH $OOZIENODE "rm /tmp/$INSTALL_SCRIPT"

exit 0

