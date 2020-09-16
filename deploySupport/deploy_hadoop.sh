#!/bin/bash

# This is the top level script called by the Jenkin's job for hadoop deployment.

# Set the CLUSTER value using the first command line argument if it is passed in.
# Otherwise, CLUSTER will default to the environment variable value.
if [ $# -gt 0 ]; then
    CLUSTER=$1
    export CLUSTER=$CLUSTER
fi

# Remove spaces in cluster name
CLUSTER=`echo $CLUSTER|tr -d ' '`

# Set DEBUG to 0 if not defined via the environment
DEBUG=${DEBUG:=0}

case "$CLUSTER" in
   *Fill*in*your*cluster*)
      echo "ERROR: Required CLUSTER value is not defined.  Exiting!!!"
      exit 1
      ;;
   open*)
      export confpkg=HadoopConfigopenstacklargedisk
      export localconfpkg=hadooplocalconfigsopenstacklarge
      ;;
   *)
      export confpkg=HadoopConfiggeneric10nodeblue
      export localconfpkg=hadooplocalconfigs
      ;;
esac

# Check for other required parameters
if [ -z "$HADOOP_RELEASE_TAG" ]; then
    echo "ERROR: Required HADOOP_RELEASE_TAG value is not defined.  Exiting!!!"
    exit 1
fi

export PATH=$PATH:/home/y/bin64:/home/y/bin:/usr/bin:/usr/local/bin:/bin:/sroot:/sbin
export DATESTRING=`date +%y%m%d%H%M`
set -o pipefail

SCRIPT_DIR=`dirname $(readlink -f $0)`
source $SCRIPT_DIR/setenv.sh

banner "Starting Hadoop deployment job for cluster '$CLUSTER' from `hostname`"
echo "PATH = '$PATH'"

# show environment variables
[ $DEBUG ] && /bin/env

# Setup a new artifacts directory
artifacts_dir="${WORKSPACE}/artifacts"
[ -d $artifacts_dir ] && rm -rf $artifacts_dir
mkdir -p $artifacts_dir

cd deploySupport

[ -z "$LOCAL_CONFIG_PKG_NAME" ] && export LOCAL_CONFIG_PKG_NAME=$localconfpkg

# Check if dist_tag is valid. If not, exit.
# dist_tag list take on average about 3 minutes to run
# /home/y/bin/dist_tag basically fetch the content from
# http://edge.dist.corp.yahoo.com:8000/dist_get_tag?t=HADOOP_2_8_0_LATEST&os=rhel&q=1&ls=1
# The '&ls=1' is the part that takes a very long time.
# So we will curl the content directly ourselves.

# cmd="dist_tag list $HADOOP_RELEASE_TAG -timeout 300 -os rhel"
cmd="curl \"http://edge.dist.corp.yahoo.com:8000/dist_get_tag?t=${HADOOP_RELEASE_TAG}&os=rhel&q=1\"|cut -d' ' -f2- | sed 's/ /-/'"
note "Process dist tag '$HADOOP_RELEASE_TAG': $cmd"
DIST_TAG_LIST=`eval "$cmd"`
if [[ $? != "0" ]] || [[ -z $DIST_TAG_LIST ]]; then
    echo "ERROR: dist_tag list '$HADOOP_RELEASE_TAG' failed: '$DIST_TAG_LIST'; Exiting!!!"
    exit 1;
fi

# Parse the hadoop version
export FULLHADOOPVERSION=`echo $DIST_TAG_LIST | grep -o hadoopcoretree-[^\ ]* | cut -d'-' -f2`
if [ -z "$FULLHADOOPVERSION" ]; then
    echo "ERROR: Cannot determine hadoop version!!! Exiting!!!"
    exit 1
fi

# Parse the hadoop short version: e.g 2.6
export HADOOPVERSION=`echo $FULLHADOOPVERSION|cut -d. -f1,2`

YJAVA_JDK_VERSION=${YJAVA_JDK_VERSION:='qedefault'}

# need to get test version of coretree and hadoopCommonsDaemon for rhel7, 
# so pass it in explicitly later on, rhel6 installs should be fine with this...
# HADOOP_CORE_PKGS="hadoopcoretree hadoopgplcompression hadoopCommonsDaemon ytez_yarn_shuffle"
HADOOP_CORE_PKGS="hadoopgplcompression ytez_yarn_shuffle"

# For stack component deploys, make sure we have tools to talk to artifactory.
# We also dertermine the yspark_yarn_shuffle version using artifactory.
yinst i hadoop_releases_utils
if [ $? -ne 0 ]; then
  echo "Error: failed to install hadoop_releases_utils!"
  exit 1
fi

#################################################################################
# Set default values for SPARK parameters if not set via Jenkins
#################################################################################
# STACK_COMP_VERSION_SPARK
# Install Spark component on the Spark node using a reference cluster's package version(s)
# none - do not install (DEFAULT VALUE)
# LATEST - use version from Artifactory LATEST
# axonitered - use same version as on AR cluster.
STACK_COMP_VERSION_SPARK=${STACK_COMP_VERSION_SPARK:='none'}
SPARK_SHUFFLE_DIST_TAG=${SPARK_SHUFFLE_DIST_TAG:='same_as_STACK_COMP_VERSION_SPARK'}

if [[ $SPARK_SHUFFLE_DIST_TAG != "same_as_STACK_COMP_VERSION_SPARK" ]]; then
    export SPARK_SHUFFLE_VERSION=`dist_tag list $SPARK_SHUFFLE_DIST_TAG | head -1 | awk '{print $1}' | cut -d- -f2`
elif [[ $STACK_COMP_INSTALL_SPARK == true && $STACK_COMP_VERSION_SPARK != "none" ]]; then
    export SPARK_SHUFFLE_VERSION=`/home/y/bin/query_releases -c $STACK_COMP_VERSION_SPARK -b spark -p SPARK_DOT_LATEST`
elif [[ $SPARK_SHUFFLE_VERSION == "" ]]; then
    HADOOP_CORE_PKGS+=" yspark_yarn_shuffle"
fi

# if jdk is coming from Dist tag, add it to the HADOOP_CORE_PKGS list now so that its
# version can be pulled along with rest of core pkgs
if [[ $YJAVA_JDK_VERSION == "disttag" ]]; then
  echo "Using JDK version from Dist tag"
  HADOOP_CORE_PKGS+=" yjava_jdk"
fi

export HADOOP_MVN_PKGS="hadoop_mvn_auth hadoop_mvn_common hadoop_mvn_hdfs"

HADOOP_INSTALL_STRING=''
HADOOP_MVN_INSTALL_STRING_PKG=''
# JDK_QEDEFAULT=yjava_jdk-1.8.0_102.70
# need rhel7 compatible flavor of jdk
JDK_QEDEFAULT=yjava_jdk-8.0_8u232b09.2641196

for i in $HADOOP_CORE_PKGS; do
    HADOOP_INSTALL_STRING_PKG=`echo $DIST_TAG_LIST|grep -o $i-[^\ ]*`
    HADOOP_INSTALL_STRING+=" $HADOOP_INSTALL_STRING_PKG"
done

# gridci-1557, make jdk8 u102 the 'qedefault', so changing YJAVA_JDK_VERSION behavior to be:
#              if param empty OR 'qedefault', use u102
#              elif param has 'disttag', use Dist tagged pkg
#              else use the version sent in from jenkins
# gridci-1465, allow testing yjava_jdk version 8u102
if [[ $YJAVA_JDK_VERSION == "qedefault"  ]]; then
  echo "Using yjava_jdk $JDK_QEDEFAULT"
  HADOOP_INSTALL_STRING+=" $JDK_QEDEFAULT"
elif [[ $YJAVA_JDK_VERSION != "disttag" ]]; then 
  # use arbitrary jdk version sent in from jenkins
  echo "Using yjava_jdk $YJAVA_JDK_VERSION"
  # note: can't use '^', jenkins is inserting some chars in front of the choice list item
  if [[ $YJAVA_JDK_VERSION =~ "yjava_jdk-8" ]]; then
    HADOOP_INSTALL_STRING+=" $YJAVA_JDK_VERSION"
  else
    echo "Error: invalid YJAVA_JDK_VERSION value, expected starts-with yjava_jdk-8, got: $YJAVA_JDK_VERSION"
    exit 1
  fi
# if neither qedefault or an arbitrary jdk was sent in, the base pkg 'yjava_jdk' was
# set earlier and has already been added to HADOOP_CORE_PKGS
fi

# explicitly set coretree and hadoopCommonsDaemon for rhel7
# must do this to get the rhel7 compat version on test branch
# Again, rhel6 installs should be good with this
HADOOP_INSTALL_STRING+=" hadoopcoretree-$FULLHADOOPVERSION hadoopCommonsDaemon "

if [ -n "$SPARK_SHUFFLE_VERSION" ]; then
    HADOOP_INSTALL_STRING+=" yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION"
fi

# gridci-1566, remove the unneeded "|sed 's/ *//'" from below 'echo', this does 
# nothing and 'echo' itself reduces whitespace
HADOOP_INSTALL_STRING=`echo $HADOOP_INSTALL_STRING`
export HADOOP_INSTALL_STRING=$HADOOP_INSTALL_STRING

for i in $HADOOP_MVN_PKGS; do
    HADOOP_MVN_INSTALL_STRING_PKG=`echo $DIST_TAG_LIST|grep -o $i-[^\ ]*`
    HADOOP_MVN_INSTALL_STRING+=" $HADOOP_MVN_INSTALL_STRING_PKG"
done
HADOOP_MVN_INSTALL_STRING=`echo $HADOOP_MVN_INSTALL_STRING`
export HADOOP_MVN_INSTALL_STRING=$HADOOP_MVN_INSTALL_STRING

export HADOOP_CORETREE_INSTALL_STRING=`echo $DIST_TAG_LIST | grep -o hadoopcoretree-[^\ ]*`
export HADOOP_CONFIG_INSTALL_STRING=`echo $DIST_TAG_LIST | grep -o $confpkg-[^\ ]*`
export LOCAL_CONFIG_INSTALL_STRING=`echo $DIST_TAG_LIST | grep -o $LOCAL_CONFIG_PKG_NAME-[^\ ]*`

note "Process dist tags for any applicable components..."
if [ -n "$TEZ_DIST_TAG" ]; then
    export TEZVERSION=`dist_tag list $TEZ_DIST_TAG | grep ytez- | cut -d' ' -f1 | cut -d'-' -f2`
fi

if [ -n "$SPARK_DIST_TAG" ]; then
    export SPARKVERSION=`dist_tag list $SPARK_DIST_TAG | head -1 |awk '{print $1}' | cut -d- -f2`
fi

if [ -n "$SPARK_HISTORY_SERVER_DIST_TAG" ]; then
    export SPARK_HISTORY_VERSION=`dist_tag list $SPARK_HISTORY_SERVER_DIST_TAG | head -1 | awk '{print $1}' | cut -d- -f2`
fi

AUTO_CREATE_RELEASE_TAG=${AUTO_CREATE_RELEASE_TAG:=0}
if [ $AUTO_CREATE_RELEASE_TAG = 1 ]; then
    if [ -n "$CUST_DIST_TAG" ]; then
        export NEW_DIST_TAG="$CUST_DIST_TAG"_${DATESTRING}
    else
        export NEW_DIST_TAG=hadoop_2_0_${DATESTRING}
    fi
    echo "`date +%H:%M:%S` Clone dist tag '$HADOOP_RELEASE_TAG' to '$NEW_DIST_TAG':"
    dist_tag clone $HADOOP_RELEASE_TAG $NEW_DIST_TAG
    dist_tag add $NEW_DIST_TAG $HADOOP_INSTALL_STRING $HADOOP_CONFIG_INSTALL_STRING $LOCAL_CONFIG_INSTALL_STRING
fi
note "Completed dist tag processing."

echo "==="
echo "===  Dist Tag='$HADOOP_RELEASE_TAG'"
echo "===  Hadoop Version (full)='$FULLHADOOPVERSION'"
echo "===  Hadoop Version (short)='$HADOOPVERSION'"
[ -n $TEZVERSION ] && echo "===  Tez Version='$TEZVERSION'"
[ -n $SPARKVERSION ] && echo "===  Spark Version='$SPARKVERSION'"
[ -n $SPARK_HISTORY_VERSION ] && echo "===  Spark History Version='$SPARK_HISTORY_VERSION'"
echo "===  Requested packages='$HADOOP_INSTALL_STRING'"
echo "===  Requested configs='$HADOOP_CONFIG_INSTALL_STRING'"
echo "===  Requested MVN pkgs='$HADOOP_MVN_INSTALL_STRING'"
echo "==="
export RUNSIMPLETEST=true

# side note: this removes any leftover cruft from a previous run. Hudson does not start 'clean'.
rm -f *.tgz > /dev/null 2>&1

# Make sure there is sufficient disk space before we install
banner "Make sure there is sufficient disk space before installation"
set -x
DU_THRESHOLD=${DU_THRESHOLD:=80}
set +x
# Proceed only if DU_THRESHOLD is a number between 0 and 100
if [[ "$DU_THRESHOLD" =~ ^[0-9]+$ ]] && [[ $DU_THRESHOLD -lt 100 ]] && [[ $DU_THRESHOLD -gt 0 ]] ; then
    banner "Make sure there is sufficient disk space before installation: Use% should be less than threshold of ${DU_THRESHOLD}% "
    set -x
    PDSH_SSH_ARGS_APPEND="$SSH_OPT" \
        /home/y/bin/pdsh -S -r @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway "\
[[ \$(cat /etc/redhat-release | cut -d' ' -f7) =~ ^7 ]] && sudo yum install -y perl-Test-Simple; \
yinst install -br test -yes hadoop_qa_utils && sudo /home/y/bin/disk_usage -c -t $DU_THRESHOLD"
    RC=$?
    set +x
    if [[ $RC -ne 0 ]]; then
        echo "ERROR: Insufficient disk space on the cluster for install!!!"
        exit 1
    fi
fi

# Make sure rocl is installed on all nodes
banner "Make sure rocl is installed on all the nodes"
set -x
whoami
env|grep SSH_AUTH_SOCK
set +x

source /home/hadoopqa/.bashrc
set -x
env|grep SSH_AUTH_SOCK
PDSH_SSH_ARGS_APPEND="$SSH_OPT" \
/home/y/bin/pdsh -S -r @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway 'if [ ! -x /home/y/bin/rocl ]; then yinst install -br test -yes rocl; fi'
RC=$?
set +x
if [[ $RC -ne 0 ]]; then
    echo "ERROR: rocl failed to installed on all the nodes"
    exit 1
fi

# Default values, if not set by a Hudson/user environment variable.
[ -z "$ADMIN_HOST" ] && export ADMIN_HOST=devadm102.blue.ygrid.yahoo.com
[ -z "$CLUSTER" ] && export CLUSTER=none
[ -z "$JOB_NAME" ] && export JOB_NAME=none
[ -z "$BUILD_NUMBER" ] && export BUILD_NUMBER=none
[ -z "$CREATEGSHOME" ] && export CREATEGSHOME=true
[ -z "$REMOVE_EXISTING_LOGS" ] && export REMOVE_EXISTING_LOGS=true
[ -z "$REMOVEEXISTINGDATA" ] && export REMOVEEXISTINGDATA=true
# GRIDCI-1495 Remove only YARN data. This is a subset of REMOVEEXISTINGDATA.
[ -z "$REMOVE_YARN_DATA" ] && export REMOVE_YARN_DATA=true
[ -z "$CLEANLOCALCONFIG" ] && export CLEANLOCALCONFIG=false
[ -z "$GRIDJDK_VERSION" ] && export GRIDJDK_VERSION=1.7.0_17
[ -z "$GRIDJDK_INSTALL_STRING" ] && export GRIDJDK_INSTALL_STRING=gridjdk:hadoopXXX20X104Xlatest
[ -z "$USERNAMES" ] && export USERNAMES=mapredqa:hdfsqa
[ -z "$HDFSUSER" ] && export HDFSUSER=`echo $USERNAMES | cut -f2 -d:`
[ -z "$MAPREDUSER" ] && export MAPREDUSER=`echo $USERNAMES | cut -f1 -d:`
[ -z "$GRIDJDK64_INSTALL_STRING" ] && export GRIDJDK64_INSTALL_STRING=gridjdk64:hadoopXXX2X0X5Xlatest
[ -z "$HADOOP_INSTALL_STRING" ] && export HADOOP_INSTALL_STRING=HADOOP_2_LATEST
[ -z "$LOCAL_CONFIG_INSTALL_STRING" ] && export LOCAL_CONFIG_INSTALL_STRING=$LOCAL_CONFIG_PKG_NAME:HADOOP_2_LATEST

[ -z "$HADOOP_CONFIG_INSTALL_STRING" ] && export HADOOP_CONFIG_INSTALL_STRING=HadoopConfigopenstacklargedisk:hadoopXXX2X0X5Xlatest
[ -z "$KILLALLPROCESSES" ] && export KILLALLPROCESSES=true
[ -z "$RUNKINIT" ] && export RUNKINIT=true
[ -z "$RUNSIMPLETEST" ] && export RUNSIMPLETEST=true
[ -z "$STARTYARN" ] && export STARTYARN=true
[ -z "$CONFIGUREJOBTRACKER" ] && export CONFIGUREJOBTRACKER=true
CLUSTER_LIST="monsters hbasedev"
for i in $CLUSTER_LIST; do
    if [ $i = $CLUSTER ]; then
        export CONFIGUREJOBTRACKER=false
        export STARTYARN=false
        export RUNSIMPLETEST=false
    fi
done
[ -z "$INSTALL_GW_IN_YROOT" ] && export INSTALL_GW_IN_YROOT=false
[ -z "$USE_DEFAULT_QUEUE_CONFIG" ] && export USE_DEFAULT_QUEUE_CONFIG=true
[ -z "$ENABLE_HA" ] && export ENABLE_HA=false
[ -z "$ENABLE_KMS" ] && export ENABLE_KMS=true
[ -z "$STARTNAMENODE" ] && export STARTNAMENODE=true
[ -z "$INSTALLLOCALSAVE" ] && export INSTALLLOCALSAVE=true
[ -z "HIT_DEPLOY" ] && export HIT_DEPLOY=false
[ -z "KEEP_HIT_YROOT" ] && export KEEP_HIT_YROOT=false
[ -z "$HITVERSION" ] && export HITVERSION=none
[ -z "$INSTALL_HIT_TEST_PACKAGES" ] && export INSTALL_HIT_TEST_PACKAGES=false
[ -z "$EXCLUDE_HIT_TESTS" ] && export EXCLUDE_HIT_TESTS=none
[ -z "$INSTALL_TEZ" ] && export INSTALL_TEZ=false
[ -z "$TEZ_QUEUE" ] && export TEZ_QUEUE=default
[ -z "$TEZVERSION" ] && export TEZVERSION=none
[ -z "$SPARKVERSION" ] && export SPARKVERSION=none
[ -z "$SPARK_SHUFFLE_VERSION" ] && export SPARK_SHUFFLE_VERSION=none
[ -z "$SPARK_HISTORY_VERSION" ] && export SPARK_HISTORY_VERSION=none
[ -z "$SPARK_QUEUE" ] && export SPARK_QUEUE=default
[ -z "$PIGVERSION" ] && export PIGVERSION=none
[ -z "$OOZIEVERSION" ] && export OOZIEVERSION=none
[ -z "$OOZIE_SERVER" ] && export OOZIE_SERVER=default
[ -z "$HIVEVERSION" ] && export HIVEVERSION=none
[ -z "$HIVE_VERSION" ] && export HIVE_VERSION=none
[ -z "$HIVE_SERVER2_VERSION" ] && export HIVE_SERVER2_VERSION=none
[ -z "$STARLINGVERSION" ] && export STARLINGVERSION=none
[ -z "$NOVAVERSION" ] && export NOVAVERSION=none
[ -z "$GDM_PKG_NAME" ] && export GDM_PKG_NAME=none
[ -z "$GDMVERSION" ] && export GDMVERSION=none
[ -z "$HCATVERSION" ] && export HCATVERSION=none
[ -z "$HBASEVERSION" ] && export HBASEVERSION=none
[ -z "$VAIDYAVERSION" ] && export VAIDYAVERSION=none
[ -z "$DISTCPVERSION" ] && export DISTCPVERSION=none
[ -z "$LOG_COLLECTORVERSION" ] && export LOG_COLLECTORVERSION=none
[ -z "$HDFSPROXYVERSION" ] && export HDFSPROXYVERSION=none
[ -z "$HDFSPROXY_TEST_PKG" ] && export HDFSPROXY_TEST_PKG=none
[ -z "$HIT_DEPLOYMENT_TAG" ] && export HIT_DEPLOYMENT_TAG=none
[ -z "$RHEL7_DOCKER_DISABLED" ] && export RHEL7_DOCKER_DISABLED=true
[ -z "$DOCKER_IMAGE_TAG_TO_USE" ] && export DOCKER_IMAGE_TAG_TO_USE=rhel6
[ -z "$IS_INTEGRATION_CLUSTER" ] && export IS_INTEGRATION_CLUSTER=true
# Additional packages maintained by Hadoop Core QA team
# [ -z "$QA_PACKAGES" ] && export QA_PACKAGES=none
[ -z "$QA_PACKAGES" ] && export QA_PACKAGES="\
hadoop_qe_runasroot-test \ 
datanode-test \ 
hadoop_qa_restart_config-test \ 
namenode-test \ 
secondarynamenode-test \ 
resourcemanager-test \ 
nodemanager-test \ 
historyserver-test"
[ -z "$SEND_LOG_TO_STDOUT" ] && export SEND_LOG_TO_STDOUT=false
[ -z "$NO_CERTIFICATION" ] && export NO_CERTIFICATION=false
[ -z "$HBASE_SHORTCIRCUIT" ] && export HBASE_SHORTCIRCUIT=false
[ -z "$CREATE_NEW_CLUSTER_KEYTAB" ] && export CREATE_NEW_CLUSTER_KEYTAB=false
[ -z "$HCATIGORTAG" ] && export HCATIGORTAG=none
[ -z "$HIVEIGORTAG" ] && export HIVEIGORTAG=none
[ -z "$OOZIEIGORTAG" ] && export OOZIEIGORTAG=none

#
# stack component install settings
# potential stack components to install
# these are jenkins version select controls, or 'none'
#
[ -z "$STACK_COMP_VERSION_PIG" ] && export STACK_COMP_VERSION_PIG=none
[ -z "$STACK_COMP_VERSION_HIVE" ] && export STACK_COMP_VERSION_HIVE=none
[ -z "$STACK_COMP_VERSION_OOZIE" ] && export STACK_COMP_VERSION_OOZIE=none
# spark is a boolean jenkins control
[ -z "$STACK_COMP_INSTALL_SPARK" ] && export STACK_COMP_INSTALL_SPARK=false
[ -z "$STACK_COMP_VERSION_SPARK" ] && export STACK_COMP_VERSION_SPARK=none

## HIT test pkg
[ -z "$PIG_TEST_PKG" ] && export PIG_TEST_PKG=none
[ -z "$HCAT_TEST_PKG" ] && export HCAT_TEST_PKG=none
[ -z "$HIVE_TEST_PKG" ] && export HIVE_TEST_PKG=none
[ -z "$DISTCP_TEST_PKG" ] && export DISTCP_TEST_PKG=none
[ -z "$LOG_COLLECTOR_TEST_PKG" ] && export LOG_COLLECTOR_TEST_PKG=none
[ -z "$VAIDYA_TEST_PKG" ] && export VAIDYA_TEST_PKG=none
[ -z "$PIG_TEST_PKG" ] && export PIG_TEST_PKG=none
[ -z "$GDM_TEST_PKG" ] && export GDM_TEST_PKG=none
[ -z "$OOZIE_TEST_PKG" ] && export OOZIE_TEST_PKG=none
[ -z "$NOVA_TEST_PKG" ] && export NOVA_TEST_PKG=none
[ -z "$HBASE_TEST_PKG" ] && export HBASE_TEST_PKG=none

[ -z "$HADOOPCORE_TEST_PKG" ] && export HADOOPCORE_TEST_PKG=none
## HIT test pkg

# Fetch build artifacts from the admin box
function fetch_artifacts() {
    banner "FETCH ARTIFACTS"
    set -x
    $SCP $ADMIN_HOST:$ADMIN_WORKSPACE/manifest.txt $artifacts_dir/manifest.txt
    cat $artifacts_dir/manifest.txt
    # $SCP $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$CLUSTER/timeline.log $artifacts_dir/timeline.log
    # $SCP $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$CLUSTER/${CLUSTER}-test.log $artifacts_dir/${CLUSTER}-test.log
    $SCP $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$CLUSTER/*.log $artifacts_dir/

    # Add to the build artifact handy references to the NN and RM webui
    webui_file="$artifacts_dir/webui.html"
    echo "<Pre>" > $webui_file;

    echo "Get the namenode and resourcemanager"
    namenode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode)"|head -1`;
    URL="https://$namenode:50504/dfshealth.html"
    echo "WEBUI: $cluster NN $URL"
    printf "%-12s %s %s %s\n" "$CLUSTER" "NN" "-" "<a href=$URL>$URL</a>" >> $webui_file;

    rm=`yinst range -ir "(@grid_re.clusters.$CLUSTER.jobtracker)"|tr -s '\n' ','|sed -e  's/,$//'`;
    URL="https://$rm:50505/cluster"
    echo "WEBUI: $cluster RM $URL"
    printf "%-12s %s %s %s\n" "$CLUSTER" "RM" "-" "<a href=$URL>$URL</a>"  >> $webui_file;

    # if hive was selected, add the hive node's thrift URI
    if [ "$STACK_COMP_VERSION_HIVE" != "none" ]; then
      hivenode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.hive)"|head -1`;
      URL="thrift://$hivenode:9080/"
      echo "WEBUI: $cluster Hive $URL"
      printf "%-12s %s %s %s\n" "$CLUSTER" "Hive" "-" "<a href=$URL>$URL</a>" >> $webui_file;
    fi

    # if oozie was selected, add the oozie node's GUI URI
    if [ "$STACK_COMP_VERSION_OOZIE" != "none" ]; then
      oozienode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.oozie)"|head -1`;
      URL="https://$oozienode:4443/oozie"
      echo "WEBUI: $cluster Oozie $URL"
      printf "%-12s %s %s %s\n" "$CLUSTER" "Oozie" "-" "<a href=$URL>$URL</a>" >> $webui_file;
    fi
    echo "</Pre>" >> $webui_file;
}

#################################################################################
# The 'set -e' option will cause the script to terminate immediately on error.
# The intent is to exit when the first build errors occurs.
# We do want to trap the error so it can be handle more gracefully, and provide
# information on the origination of the error.
#################################################################################
set -e
function error_handler {
   LASTLINE="$1"
   echo "ERROR: Trapped error signal from caller [${BASH_SOURCE} line ${LASTLINE}]"
   fetch_artifacts
}
trap 'error_handler ${LINENO}' ERR

#################################################################################
# Deploy Hadoop
#################################################################################
export BUILD_DESC="Deploy to $CLUSTER $FULLHADOOPVERSION ($HADOOP_RELEASE_TAG)"
echo "$BUILD_DESC"
echo "$BUILD_DESC" > $artifacts_dir/timeline.log
exit

#################################################################################
# CHECK IF WE NEED TO INSTALL STACK COMPONENTS
#
# gridci-1040, make component version selectable
# gridci-1300, use cluster names passed in from jenkins, to lookup component
#              versions from artifactory
#
# PIG - gridci-747 install pig on gw
#
# HIVE - gridci-481 install hive server and client
# this relies on hive service keytab being generated and pushed out in the
# cluster configure portion
# of cluster building (cluster-build/configure_cluster)
#
# gridci-1937, allow hive/hcat to install using current branch
#
# OOZIE - gridci-561 install yoozie server
# this relies on oozie service keytab being generated and pushed out in the
# cluster configure portion of cluster building (cluster-build/configure_cluster)
#################################################################################
function deploy_stack() {
    STACK_COMP=$1
    STACK_COMP_VERSION=$2
    STACK_COMP_SCRIPT=$3

    start=`date +%s`
    h_start=`date +%Y/%m/%d-%H:%M:%S`
    echo "INFO: Install stack component ${STACK_COMP} on $h_start"
    if [ "$STACK_COMP_VERSION" == "none" ]; then
        echo "INFO: Nothing to do since STACK_COMP_VERSION is set to 'none'"
        return 0
    else
        # check we got a valid reference cluster
        REFERENCE_CLUSTER=$STACK_COMP_VERSION
        if [[ "$STACK_COMP" == "pig" ]]; then
            packagename='pig_latest'
        elif [[ "$STACK_COMP" == "oozie" ]]; then
            packagename='yoozie'
        else
            packagename=${STACK_COMP}
        fi

        # gridci-1937 allow install from current branch
        if [[ "$STACK_COMP_VERSION" == "current" ]]; then
          REFERENCE_CLUSTER=current
        else
          RESULT=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER`
          if [ $? -eq 0 ]; then
              # get Artifactory URI and log it
              ARTI_URI=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER  -v | grep downloadUri |cut -d\' -f4`
              echo "Artifactory URI with most recent versions:"
              echo $ARTI_URI
              # look up stack component version for AR in artifactory
              set -x
              if [[ ${STACK_COMP} != "spark" ]]; then
                PACKAGE_VERSION=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b ${STACK_COMP} -p $packagename`
              fi
              set +x
          else
              echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT"
              exit 1
          fi
        fi

        banner "START INSTALL STEP: Stack Component ${STACK_COMP}"
        set -x
        time ./$STACK_COMP_SCRIPT $CLUSTER $STACK_COMP_VERSION 2>&1 | tee $artifacts_dir/deploy_stack_${STACK_COMP}-${PACKAGE_VERSION}.log
        st=$?
        set +x
        if [ $st -ne 0 ]; then
            echo "ERROR: component install for ${STACK_COMP} failed!"
        fi
        banner "END INSTALL STEP: Stack Component ${STACK_COMP}: status='$st'"
    fi
    end=`date +%s`
    h_end=`date +%Y/%m/%d-%H:%M:%S`
    runtime=$((end-start))
    printf "%-124s : %.0f min (%.0f sec) : %s : %s : %s\n" $STACK_COMP_SCRIPT $(echo "scale=2;$runtime/60" | bc) $runtime $h_start $h_end $st >> $artifacts_dir/timeline.log
    cat $artifacts_dir/timeline.log
}

#################################################################################
# Spark installation is flexible. You can either specify a reference cluster or
# you can specify the version of spark and spark history server to be installed.
#################################################################################
function deploy_spark () {
  if [[ $STACK_COMP_INSTALL_SPARK == true ]]; then
    if [[ $STACK_COMP_VERSION_SPARK != "none" ]]; then
      # call the default deploy behavior.
      deploy_stack spark $STACK_COMP_VERSION_SPARK spark-install-check.sh
    else
      echo "INFO: Installing a individual spark version which is different from the gateway setup."
      start=`date +%s`
      h_start=`date +%Y/%m/%d-%H:%M:%S`

      STACK_COMP=spark
      echo "INFO: Install stack component ${STACK_COMP} on $h_start"
      banner "START INSTALL STEP: Stack Component ${STACK_COMP}"
      set -x
      time ./spark-install-check.sh $CLUSTER $STACK_COMP_VERSION_SPARK 2>&1 | tee $artifacts_dir/deploy_stack_${STACK_COMP}-${PACKAGE_VERSION}.log
      st=$?
      set +x
      if [ $st -ne 0 ]; then
        echo "ERROR: component install for ${STACK_COMP} failed!"
      fi
      banner "END INSTALL STEP: Stack Component ${STACK_COMP}: status='$st'"
    fi
  fi
}

banner "CHECK IF WE NEED TO INSTALL STACK COMPONENTS: pig, hive, spark, oozie"
deploy_stack pig $STACK_COMP_VERSION_PIG pig-install-check.sh
deploy_stack hive $STACK_COMP_VERSION_HIVE hive-install-check.sh
deploy_spark
#oozie should be installed after installing other stack components as it relies on them.
deploy_stack oozie $STACK_COMP_VERSION_OOZIE oozie-install-check.sh

fetch_artifacts

# Review: note that the exit-status of the deploy is indeterminate, and seems to reflect the success of that final 'yinst-remove'.
exit $?
