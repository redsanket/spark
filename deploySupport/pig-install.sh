# script to install pig on the cluster's gateway node
#
# inputs: cluster being installed, reference cluster name 
# outputs: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name and reference cluster"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

PIGNODE=`hostname`
PIGNODE_SHORT=`echo $PIGNODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Pig node being installed: $PIGNODE"

# check what comp version we need to use
echo "STACK_COMP_VERSION_PIG is: $REFERENCE_CLUSTER"

# gridci-1937 allow hive to use branch 'current' but keep Pig using
# axonite-red version from artifactory
if [[ "$REFERENCE_CLUSTER" == "current" ]]; then
  echo "INFO: Hive is using branch current but will use AR version for Pig"
  REFERENCE_CLUSTER="axonitered"
fi

# make sure we have tools to talk to artifactory
yinst i hadoop_releases_utils
RC=$?
if [ "$RC" -ne 0 ]; then
  echo "Error: failed to install hadoop_releases_utils on $PIGNODE!"
  exit 1
fi

# check we got a valid reference cluster
RESULT=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER`
RC=$?
if [ $RC -eq 0 ]; then 
  # get Artifactory URI and log it
  ARTI_URI=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER  -v | grep downloadUri |cut -d\' -f4`
  echo "Artifactory URI with most recent versions:"
  echo $ARTI_URI

  # look up pig version for AR in artifactory
  PACKAGE_VERSION_PIG=pig-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b pig -p pig_latest`

else
  echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT" 
  exit 1
fi

#
## install pig
#
yinst install -same -live -downgrade  $PACKAGE_VERSION_PIG
RC=$?
if [ $RC -ne 0 ]; then
  echo "Error: failed to install $PACKAGE_VERSION_PIG on $PIGNODE!"
  exit 1
fi

yinst set pig.PIG_HOME=/home/y/share/pig

# make the grid links for pig
PIGVERSION=`yinst ls | grep pig-`
echo PIGVERSION=$PIGVERSION

# yinst bug, old pkgs disted before rhel7 yinst support was added won't install unless explicit
# vers given, and not from quarantine either. Fix would be to re-dist the pkg(s)
yinst install ygrid_pig_multi-1.3.2  -br current -set ygrid_pig_multi.CURRENT=$PIGVERSION -same -live


# GRIDCI-3269: Integration deployment, add BaseFeed.jar and FETLProjector.jar
echo "check if {BaseFeed.jar, FETLProjector.jar} are present, else copy"
INT_JAR_NAMES=("BaseFeed.jar" "FETLProjector.jar")
BASE_TGT_JAR_PATH="/tmp/integration_test_files/lib/"
/usr/bin/kinit -k -t /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa
echo "kinit success status: $?"
for i in "${INT_JAR_NAMES[@]}"
do
  TGT_OUTPUT=$(hadoop fs -ls "${BASE_TGT_JAR_PATH}${i}")
  echo "${TGT_OUTPUT}"
  if [ -z "${TGT_OUTPUT}" ]
  then
    echo "[INFO] ABSENT: ${BASE_TGT_JAR_PATH}${i}"
    BASE_SRC_JAR_PATH="hdfs://openqe5blue-n2/tmp/integration_test_files/lib/"
    SRC_OUTPUT=$(hadoop fs -ls "${BASE_SRC_JAR_PATH}${i}")
    if [ -z "${SRC_OUTPUT}" ]
    then
      echo "[ERROR] MISSING: ${BASE_SRC_JAR_PATH}${i}"
    else
      echo "[INFO] COPYING: ${BASE_SRC_JAR_PATH}${i}"
      hadoop fs -mkdir -p /tmp/integration_test_files
      hadoop fs -mkdir -p /tmp/integration_test_files/lib
      hadoop fs -chmod -R 755 /tmp/integration_test_files/lib/
      hadoop fs -cp "${BASE_SRC_JAR_PATH}${i}"    "${BASE_TGT_JAR_PATH}."
    fi
  else
    echo "[INFO] PRESENT: ${BASE_TGT_JAR_PATH}${i}"
  fi
done
