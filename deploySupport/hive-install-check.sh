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
# inputs: cluster to install hive on and reference cluster version,
# note that LATEST is a virtual cluster for latest push to artifactory
# returns: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster to install hive onto and reference cluster version"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

HIVENODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.hive)"`;
if [ -z "$HIVENODE" ]; then
  echo "ERROR: No Hive node defined, HIVENODE is empty! Is the Rolesdb role correctly set?"
  exit 1
fi
echo "INFO: Going to call Hive installer for node $HIVENODE..."


# setup ssh cmd with parameters
SSH_OPT=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"


# check that the hive node's local-superuser-conf.xml is correctly
# setup with doAs users, if not then hive operations will fail
# check if hadoopqa properties are set
EC=0
$SSH $HIVENODE grep "hadoop.proxyuser.hadoopqa.hosts" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

$SSH $HIVENODE grep "hadoop.proxyuser.hadoopqa.groups" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

$SSH $HIVENODE grep "hadoop.proxyuser.dfsload.hosts" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

$SSH $HIVENODE grep "hadoop.proxyuser.dfsload.groups" /home/gs/conf/local/local-superuser-conf.xml
RC=$?
EC=$((EC+RC))

if [ $EC -ne 0 ]; then
  echo "ERROR: hive node $HIVENODE /home/gs/conf/local/local-superuser-conf.xml is missing doAs users!"
  echo "       See the section \"Local Node Conf File\" in the Build/Configure Jenkins job's README.md at:" 
  echo "       https://git.corp.yahoo.com/HadoopQE/qeopenstackdist/blob/master/README.md"
  exit 1
else
  echo "INFO: hive node $HIVENODE /home/gs/conf/local/local-superuser-conf.xml is correct"
fi


INSTALL_SCRIPT=hive-install.sh

# copy the installer to the target node and run it
$SCP $INSTALL_SCRIPT  $HIVENODE:/tmp/
  
$SSH $HIVENODE "cd /tmp/ && /tmp/$INSTALL_SCRIPT $CLUSTER $REFERENCE_CLUSTER"
RC=$?

if [ $RC -ne 0 ]; then
  echo "ERROR: Hive installer failed!"
  exit 1
fi

# Clean up hive install
echo "INFO: remove $HIVENODE:/tmp/$INSTALL_SCRIPT"
$SSH $HIVENODE "rm /tmp/$INSTALL_SCRIPT"

exit 0

