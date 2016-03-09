# script to install pig on the cluster's gateway node
#
# inputs: cluster being installed 
# outputs: 0 on success

if [ $# -ne 1 ]; then
  echo "ERROR: need the cluster name"
  exit 1
fi

CLUSTER=$1
PIGNODE=`hostname`
PIGNODE_SHORT=`echo $PIGNODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Pig node being installed: $PIGNODE"

# install pig
yinst install pig -br current
yinst set pig.PIG_HOME=/home/y/share/pig

