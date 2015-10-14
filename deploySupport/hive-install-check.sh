# Launcher script to fire off the hive installer
#
# This is called from hudson-startslave-openstack if jenkins option
# to install hive is set true. From the automated jobs this will run
# as hadoopqa, the reason is the hive credentials are setup to use
# hadoopqa and dfsload, dfsload is kinited by the installed when
# needed.
#
# The hive installation relies on keytabs which are generated in
# the Build and Configure jobs.
#
# Note that this component install is different than that of Core,
# Core is yinst packaged and dropped on admin node where the install
# package is executed as root. This install does not need root and can 
# be delivered directly to the component node from the RE node.
#
# inputs: node to install hive on
# returns: 0 on success

if [ $# -ne 1 ]; then
  echo "ERROR: need the node to install hive onto"
  exit 1
fi

HIVENODE=$1
echo "INFO: Installing Hive component on node $HIVENODE"

INSTALL_SCRIPT=hive-install.sh

# setup ssh cmd with parameters
SSH_OPT=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"

# copy the installer to the target node and run it
$SCP $INSTALL_SCRIPT  $HIVENODE:/tmp/
  
set -x
$SSH $HIVENODE "cd /tmp/ && /tmp/$INSTALL_SCRIPT $HIVENODE"
RC=$?
set +x

if [ $RC -ne 0 ]; then
  echo "ERROR: Hive installer failed!"
  exit 1
fi

# Clean up hive install
echo "INFO: remove $HIVENODE:/tmp/$INSTALL_SCRIPT"
$SSH $HIVENODE "rm /tmp/$INSTALL_SCRIPT"

exit 0

