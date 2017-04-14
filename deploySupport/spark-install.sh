# script to install spark on the cluster's gateway node
#
# inputs: cluster being installed, reference cluster name 
# outputs: 0 on success

#-------------------------------------------------------------------------------
### functions

function get_spark_label_version_from_artifactory () {
  # make sure we have tools to talk to artifactory
  yinst i hadoop_releases_utils
  st=$?
  [[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install hadoop_releases_utils on $SPARKNODE <<<<<<<<<<" && exit $st  

  # check we got a valid reference cluster
  RESULT=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER`
  st=$?
  if [[ $st -eq 0 ]]; then
    # get Artifactory URI and log it
    ARTI_URI=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER  -v | grep downloadUri |cut -d\' -f4`
    echo "Artifactory URI with most recent versions:"
    echo $ARTI_URI
  else
    echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT" 
    exit 1
  fi

  label_version_arr[0]=DOT_SIX=`query_releases -c $REFERENCE_CLUSTER -b spark -p SPARK_DOT_SIX`
  label_version_arr[1]=TWO_ZERO=`query_releases -c $REFERENCE_CLUSTER -b spark -p SPARK_TWO_ZERO`
  label_version_arr[2]=CURRENT=`query_releases -c $REFERENCE_CLUSTER -b spark -p SPARK_DOT_CURRENT`
  label_version_arr[3]=LATEST=`query_releases -c $REFERENCE_CLUSTER -b spark -p SPARK_DOT_LATEST`
}

#-------------------------------------------------------------------------------
### main

[[ $# -ne 2 ]] && echo "ERROR: need the cluster name and reference cluster." && exit 1

CLUSTER=$1
REFERENCE_CLUSTER=$2

SPARKNODE=`hostname`
SPARKNODE_SHORT=`echo $SPARKNODE | cut -d'.' -f1`

HADOOP="/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop"

echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Spark node being installed: $SPARKNODE"

if [[ ${REFERENCE_CLUSTER:=none} != none ]]; then
  # check what comp version we need to use
  echo "STACK_COMP_VERSION_SPARK is: $REFERENCE_CLUSTER"
  get_spark_label_version_from_artifactory 
elif [[ $SPARKVERSION == "2."* ]]; then
  label_version_arr[0]="TWO_ZERO=$SPARKVERSION"
  label_version_arr[1]="LATEST=$SPARKVERSION"
elif [[ $SPARKVERSION == "1.6"* ]]; then
  label_version_arr[0]="DOT_SIX=$SPARKVERSION"
  label_version_arr[1]="LATEST=$SPARKVERSION"
else
  echo "ERROR: Aborting installation for an unexpected version of Spark" && exit 1
fi

#-------------------------------------------------------------------------------

# Uninstall any existing yspark_yarn_install package to ensure we run in a clean env.
cmd="yinst ls yspark_yarn_install"

echo "$cmd"
eval "$cmd"

st=$?
if [[ $st -eq 0 ]]; then
  cmd="yinst rm yspark_yarn_install -live"

  echo "INFO: Removing existing yspark_yarn_install package."
  echo "$cmd"
  eval "$cmd"

  st=$?
  [[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to remove existing yspark_yarn_install package <<<<<<<<<<" && exit $st
else
  echo "INFO: No existing yspark_yarn_install package found."
fi

#Explicitly clean any existing settings.
cmd="yinst clean -settings yspark_yarn_install -yes"
echo "INFO: Removing any existing settings for yspark_yarn_install."

echo "$cmd"
eval "$cmd"

#-------------------------------------------------------------------------------
spark_install_cmd="yinst i yspark_yarn_install -br current -same -live"

for i in "${label_version_arr[@]}"
do
  label=$(echo $i | cut -d= -f1)
  version=$(echo $i | cut -d= -f2)

  echo "INFO: Spark version being installed: $version"
  echo "INFO: Installing yspark_yarn-$version"

  spark_install_cmd+=" -set yspark_yarn_install.$label=yspark_yarn-$version"
done

echo "$spark_install_cmd"
eval "$spark_install_cmd"
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install yspark_yarn-$SPARKVERSION <<<<<<<<<<" && exit $st

#-------------------------------------------------------------------------------

# Obtain the kerberos tokens to talk to hdfs.
kinit -k -t /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa

# Setup the sharelib in hdfs for the current and latest spark version.
for i in "${label_version_arr[@]}"
do
  label=$(echo $i | cut -d= -f1)
  version=$(echo $i | cut -d= -f2)

  if [[ $label == "CURRENT" || $label == "LATEST" ]]; then
    # convert the label to lowercase to match the directory
    label=$(echo $label | tr '[:upper:]' '[:lower:]')

    spark_install_jars_cmds="$HADOOP fs -put /home/gs/spark/$label/python/lib/pyspark.zip /sharelib/v1/spark/yspark_yarn-$version/share/spark/python/lib/ ; \
    $HADOOP fs -put /home/gs/spark/$label/python/lib/py4j-*-src.zip /sharelib/v1/spark/yspark_yarn-$version/share/spark/python/lib/"
    
    if [[ $version == "2."* ]]; then
      spark_install_jars_cmds=$spark_install_jars_cmds" ; \
      $HADOOP fs -put /home/gs/spark/$label/lib/ /sharelib/v1/spark/yspark_yarn-$version/share/spark/ ; \
      $HADOOP fs -put /home/gs/spark/$label/yspark-jars-*.tgz /sharelib/v1/spark/yspark_yarn-$version/share/spark/"
    else
      spark_install_jars_cmds=$spark_install_jars_cmds" ; \
      $HADOOP fs -put /home/gs/spark/$label/lib/spark-assembly.jar /sharelib/v1/spark/yspark_yarn-$version/share/spark/lib/ ; \
      $HADOOP fs -put /home/gs/spark/$label/lib/datanucleus-api-jdo.jar /sharelib/v1/spark/yspark_yarn-$version/share/spark/lib/ ; \
      $HADOOP fs -put /home/gs/spark/$label/lib/datanucleus-core.jar /sharelib/v1/spark/yspark_yarn-$version/share/spark/lib/ ; \
      $HADOOP fs -put /home/gs/spark/$label/lib/datanucleus-rdbms.jar /sharelib/v1/spark/yspark_yarn-$version/share/spark/lib/"
    fi

    echo "INFO: Copying yspark_yarn-$version jars to hdfs://sharelib/v1/spark/"

    cmd="echo Creating hdfs://sharelib/v1/spark/yspark_yarn-$version/share/spark/lib/ ; \
    $HADOOP fs -mkdir -p /sharelib/v1/spark/yspark_yarn-$version/share/spark/lib/ ; \
    $HADOOP fs -mkdir -p /sharelib/v1/spark/yspark_yarn-$version/share/spark/python/lib/ ; \
    $spark_install_jars_cmds"

    echo "$cmd"
    eval "$cmd"
    st=$?
    [[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install yspark_yarn-$version jars to sharelib <<<<<<<<<<" && exit $st

    echo "INFO: Copying yspark_yarn-$version conf to hdfs://sharelib/v1/spark_conf/"
    cmd="echo Creating hdfs://sharelib/v1/spark_conf/yspark_yarn_conf-$version/ ; \
    $HADOOP fs -mkdir -p /sharelib/v1/spark_conf/yspark_yarn_conf-$version/conf/spark/ ; \
    $HADOOP fs -put /home/gs/conf/spark/$label/spark-defaults.conf /sharelib/v1/spark_conf/yspark_yarn_conf-$version/conf/spark/"

    echo "$cmd"
    eval "$cmd"
    set=$?
    [[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to install yspark_yarn-$version conf to sharelib <<<<<<<<<<" && exit $st
  fi
done

$HADOOP fs -chmod -R 755 /sharelib/

#-------------------------------------------------------------------------------
# Setup the gateway environment
cmd="echo INFO: Setting up the environment variables ; \
export SPARK_CONF_DIR=/home/gs/conf/spark/latest ; \
export SPARK_HOME=/home/gs/spark/latest ; \
export HADOOP_HOME=$GSHOME/hadoop/current ; \
export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
export HADOOP_CONF_DIR=/home/gs/conf/current ; \
export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*" ; \
export JAVA_HOME="$GSHOME/java8/jdk64/current""

echo "$cmd"
eval "$cmd"
st=$?
[[ $st -ne 0 ]] && echo ">>>>>>>> ERROR: Failed to setup the gateway environment.<<<<<<<<<<" && exit $st

echo "Spark installation completed!"
