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
  echo "ERROR: No Oozie node defined, OOZIENODE is empty! Is the Rolesdb role correctly set?"
  exit 1
fi
echo "INFO: Going to call Oozie installer for node $OOZIENODE..."


# setup ssh cmd with parameters
SSH_OPT=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"


# check that the oozie node's local-superuser-conf.xml is correctly
# setup with doAs users, if not then oozie operations will fail
# check if hadoopqa properties are set
EC=0
$SSH $OOZIENODE grep "hadoop.proxyuser.oozie.hosts" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

$SSH $OOZIENODE grep "hadoop.proxyuser.oozie.groups" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

$SSH $OOZIENODE grep "hadoop.proxyuser.hcat.hosts" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

$SSH $OOZIENODE grep "hadoop.proxyuser.hcat.groups" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

if [ $EC -ne 0 ]; then
  echo "ERROR: oozie node $OOZIENODE /home/gs/conf/local/local-superuser-conf.xml is missing doAs users!"
  echo "       See the section \"Local Node Conf File\" in the Build/Configure Jenkins job's README.md at:" 
  echo "       https://git.corp.yahoo.com/HadoopQE/qeopenstackdist/blob/master/README.md"
  exit 1
else
  echo "INFO: oozie node $OOZIENODE /home/gs/conf/local/local-superuser-conf.xml is correct"
fi

##
# install hive and pig on the oozie node
##

HIVE_INSTALL_SCRIPT=hive-install.sh

# copy the installer to the target node and run it
$SCP $HIVE_INSTALL_SCRIPT  $OOZIENODE:/tmp/

set -x
$SSH $OOZIENODE "cd /tmp/ && /tmp/$HIVE_INSTALL_SCRIPT $CLUSTER"
RC=$?
set +x

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
  
set -x
$SSH $OOZIENODE "cd /tmp/ && /tmp/$INSTALL_SCRIPT $CLUSTER"
RC=$?
set +x

if [ $RC -ne 0 ]; then
  echo "ERROR: Oozie installer failed!"
  exit 1
fi

# Clean up oozie install
echo "INFO: remove $OOZIENODE:/tmp/$INSTALL_SCRIPT"
$SSH $OOZIENODE "rm /tmp/$INSTALL_SCRIPT"

exit 0

