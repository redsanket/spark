# script to install spark history server on the cluster's jobtracker node
#
# inputs: cluster being installed, reference cluster name 
# outputs: 0 on success

#-------------------------------------------------------------------------------
### functions

function get_spark_history_server_version_from_artifactory () {
  # make sure we have tools to talk to artifactory
  yinst i hadoop_releases_utils
  st=$?
  [[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install hadoop_releases_utils on $SPARK_HISTORY_NODE <<<<<<<<<<" && exit $st  

  SPARK_HISTORY_VERSION=`query_releases -c $REFERENCE_CLUSTER -b spark -p yspark_yarn_history_server`
}

#-------------------------------------------------------------------------------
### main

[[ $# -ne 2 ]] && echo "ERROR: need the cluster name and reference cluster." && exit 1

CLUSTER=$1
REFERENCE_CLUSTER=$2

SPARK_HISTORY_NODE=`hostname`
SPARK_HISTORY_NODE_SHORT=`echo $SPARK_HISTORY_NODE | cut -d'.' -f1`

HADOOP="/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop"

echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Spark history server node being installed: $SPARK_HISTORY_NODE"

# Get history version from artifactory if reference cluster is not specified. 
# Else it is available as an ENV variable.
[[ $REFERENCE_CLUSTER != "none" ]] && get_spark_history_server_version_from_artifactory

#-------------------------------------------------------------------------------
# Install Spark history server and set required hdfs directories.
echo "INFO: Installing yspark_yarn_history_server-$SPARK_HISTORY_VERSION & setting up hdfs://mapred/sparkhistory/"
cmd="yinst i yspark_yarn_history_server-$SPARK_HISTORY_VERSION -same -live -down ; \
kinit -k -t /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
echo Creating hdfs://mapred/sparkhistory ; \
$HADOOP fs -mkdir -p /mapred/sparkhistory/ ; \
echo chmoding -R 1777 /mapred/sparkhistory/ ; \
$HADOOP fs -chmod -R 1777 /mapred/sparkhistory/ ; \
echo chowning mapredqa:hadoop /mapred/sparkhistory/ ; \
$HADOOP fs -chown -R mapredqa:hadoop /mapred/sparkhistory/ ; \
echo listing /mapred/ directory ; \
$HADOOP fs -ls /mapred/ ; \
echo Set spark_daemon_user to mapredqa ; \
yinst set yspark_yarn_history_server.spark_daemon_user=mapredqa ; \
echo Starting history server: http://$jobtrackernode:18080 ; \
SUDO_USER=hadoopqa sudo yinst start yspark_yarn_history_server"

echo "$cmd"
eval "$cmd"
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install yspark_yarn_history_server-$SPARK_HISTORY_VERSION <<<<<<<<<<" && exit $st

# Setup the environment for Spark History Server
echo "INFO: Setting up the environment variables for yspark_yarn_history_server-$SPARK_HISTORY_VERSION"
cmd="export SPARK_CONF_DIR=/home/gs/conf/spark/latest ; \
export SPARK_HOME=/home/gs/spark/latest ; \
export HADOOP_HOME=$GSHOME/hadoop/current ; \
export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
export HADOOP_CONF_DIR=/home/gs/conf/current ; \
export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*" ; \
export JAVA_HOME="$GSHOME/java8/jdk64/current""

echo "$cmd"
eval "$cmd"
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to set environment for yspark_yarn_history_server-$SPARK_HISTORY_VERSION <<<<<<<<<<" && exit $st

echo "Spark history server installation completed!"
