# script to install spark on the cluster's gateway node
#
# inputs: cluster being installed
# outputs: 0 on success

if [ $# -ne 1 ]; then
  echo "ERROR: need the cluster name"
  exit 1
fi

CLUSTER=$1
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Spark node being installed: $gateway"
echo "INFO: Spark version being installed: $SPARKVERSION"

# Install Spark
cmd="yinst i yspark_yarn_install -br current \
   -set yspark_yarn_install.DOT_SIX=yspark_yarn-$SPARKVERSION \
   -set yspark_yarn_install.LATEST=yspark_yarn-$SPARKVERSION \
   -same -live"

fanoutSpark "$cmd"

