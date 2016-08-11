#!/bin/bash

# hudson-startslave.sh
# 	The first script called by Hudson. It massages the arguments
# 	given, then creates a yinst-package of the scripts needed (by
# 	calling yinstify.sh), then copies that package to the destination
# 	machine and runs it, which runs installgrid.sh on ADMIN_HOST.

case "$CLUSTER" in
   *Fill*in*your*cluster*)
      echo ====================================================
      echo ERROR: CLUSTER was not defined.  Exiting.
      echo Please make sure you specify CLUSTER from hudson UI
      echo ====================================================
      exit 1
      ;;
   dense*)
      export scriptnames=generic10node12disk
      export localnames=12disk
      ;;
   *) export scriptnames=generic10node ;;
esac
export confpkg=HadoopConfig${scriptnames}blue
export localconfpkg=hadooplocalconfigsopenstacklarge
export PATH=$PATH:/home/y/bin64:/home/y/bin:/usr/bin:/usr/local/bin:/bin:/sroot:/sbin

echo =========================================
echo Beginning of Hudson-driven deployment job.
echo hostname = `hostname`
echo "PATH='$PATH'"
echo date = `TZ=PDT8PDT date `
echo date = `TZ= date`
echo =========================================
echo

export DATESTRING=`date +%y%m%d%H%M`


# echo environment follows:
# /bin/env
cd deploySupport

# 	From above:  "It massages the arguments given"
#
# Note that we might add one additional thing: a choice-list.

[ -z "$LOCAL_CONFIG_PKG_NAME" ] && export LOCAL_CONFIG_PKG_NAME=$localconfpkg

# Check if dist_tag is valid. If not, exit.
cmd="dist_tag list $HADOOP_RELEASE_TAG"
echo "$cmd"
DIST_TAG_LIST=`eval "$cmd"`
if [[ $? != "0" ]];then
    echo "ERROR: dist_tag list '$HADOOP_RELEASE_TAG' failed: '$DIST_TAG_LIST'; Exiting!!!"
    exit 1;
fi

# Parse the hadoop version
export FULLHADOOPVERSION=`echo $DIST_TAG_LIST | tr ' ' '\n' | grep hadoopcoretree | cut -d'-' -f2`
if [ -z "$FULLHADOOPVERSION" ]; then
    echo "ERROR: Cannot determine hadoop version!!! Exiting!!!"
    exit 1
fi

# Parse the hadoop short version: e.g 2.6
export HADOOPVERSION=`echo $FULLHADOOPVERSION|cut -d. -f1,2`
if [[ "$HADOOPVERSION" > "2.6" ]]; then
    HADOOP_27="true"
else
    HADOOP_27="false"
fi
export HADOOP_27=$HADOOP_27

HADOOP_CORE_BASE_PKGS="hadoopcoretree hadoopgplcompression hadoopCommonsDaemon"
if [[ "$HADOOP_27" == "true" ]]; then
    export HADOOP_CORE_PKGS="$HADOOP_CORE_BASE_PKGS yjava_jdk yspark_yarn_shuffle"
else
    export HADOOP_CORE_PKGS="$HADOOP_CORE_BASE_PKGS yjava_jdk gridjdk"
fi
export HADOOP_MVN_PKGS="hadoop_mvn_auth hadoop_mvn_common hadoop_mvn_hdfs"

if [ -n "$HADOOP_RELEASE_TAG" ]; then
    for i in $HADOOP_CORE_PKGS; do
        HADOOP_INSTALL_STRING_PKG=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $i- | cut -d ' ' -f 1`
        HADOOP_INSTALL_STRING="$HADOOP_INSTALL_STRING $HADOOP_INSTALL_STRING_PKG "
    done
    HADOOP_INSTALL_STRING=`echo $HADOOP_INSTALL_STRING|sed 's/ *//'`
    export HADOOP_INSTALL_STRING=$HADOOP_INSTALL_STRING

    for i in $HADOOP_MVN_PKGS; do
        HADOOP_MVN_INSTALL_STRING_PKG=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $i- | cut -d ' ' -f 1`
        HADOOP_MVN_INSTALL_STRING="$HADOOP_MVN_INSTALL_STRING $HADOOP_MVN_INSTALL_STRING_PKG "
    done
    HADOOP_MVN_INSTALL_STRING=`echo $HADOOP_MVN_INSTALL_STRING|sed 's/ *//'`
    export HADOOP_MVN_INSTALL_STRING=$HADOOP_MVN_INSTALL_STRING

    export HADOOP_CORETREE_INSTALL_STRING=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep hadoopcoretree | cut -d ' ' -f 1`
    export HADOOP_CONFIG_INSTALL_STRING=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $confpkg- | cut -d ' ' -f 1`
    export LOCAL_CONFIG_INSTALL_STRING=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $LOCAL_CONFIG_PKG_NAME- | cut -d ' ' -f 1`
else
    if [ ! -z "$HIT_DEPLOYMENT_TAG" ]
    then
        # if HIT_DEPLOYMENT_TAG is provided from hudson UI, we will install all the following pkgs
        # included in the $HIT_DEPLOYMENT_TAG
        # - gridjdk
        # - gridjdk64
        # - hadoopcoretree
        # - HadoopConfiggeneric10nodeblue
        # - HadoopConfiggeneric500nodeblue
        tag=$HIT_DEPLOYMENT_TAG
        export HADOOP_CONFIG_INSTALL_STRING=`dist_tag list $HIT_DEPLOYMENT_TAG |grep $confpkg- | cut -d ' ' -f 1`
        for i in $HADOOP_CORE_PKGS
        do
            export HADOOP_INSTALL_STRING_PKG=`dist_tag list $HIT_DEPLOYMENT_TAG |grep $i- | cut -d ' ' -f 1`
            export HADOOP_INSTALL_STRING="$HADOOP_INSTALL_STRING $HADOOP_INSTALL_STRING_PKG "
        done
        for i in $HADOOP_MVN_PKGS
        do
            export HADOOP_MVN_INSTALL_STRING_PKG=`dist_tag list $HADOOP_RELEASE_TAG |grep $i- | cut -d ' ' -f 1`
            export HADOOP_MVN_INSTALL_STRING="$HADOOP_MVN_INSTALL_STRING $HADOOP_MVN_INSTALL_STRING_PKG "
        done
        export HADOOP_CORETREE_INSTALL_STRING=`dist_tag list $HADOOP_RELEASE_TAG |grep hadoopcoretree | cut -d ' ' -f 1`
        export LOCAL_CONFIG_INSTALL_STRING=`dist_tag list $HIT_DEPLOYMENT_TAG |grep $LOCAL_CONFIG_PKG_NAME- | cut -d ' ' -f 1`


        # now constructing the following variables based on HIT_DEPLOYMENT_TAG
        perl retrieveHitPkgFromTag.pl
        if [ $? = 0 ]; then
            . exportHITpkgs.sh
        else
            echo "Error: cannot construct hadoop service pkg string from HIT_DEPLOYMENT_TAG=$tag"
        fi
    else
        echo "Error: You have to select a dist tag for deployment!!"
        exit 1
    fi
fi

if [ ! -z "$TEZ_DIST_TAG" ]
then
    export TEZVERSION=`dist_tag list $TEZ_DIST_TAG | grep ytez_full | cut -c11-28`
fi

if [ ! -z "$SPARK_DIST_TAG" ]
then
    export SPARKVERSION=`dist_tag list $SPARK_DIST_TAG | awk '{print $1}' | cut -d- -f2`
fi

if [ ! -z "$SPARK_HISTORY_SERVER_DIST_TAG" ]
then
    export SPARK_HISTORY_VERSION=`dist_tag list $SPARK_HISTORY_SERVER_DIST_TAG | awk '{print $1}' | cut -d- -f2`
fi

if [ ! -z "$AUTO_CREATE_RELEASE_TAG" ]
then
    if [ $AUTO_CREATE_RELEASE_TAG = 1 ] && [ ! -z "$HADOOP_RELEASE_TAG" ]
    then
        if [ ! -z "$CUST_DIST_TAG" ]
        then
            echo "Using custom dist tag to clone..."
            export NEW_DIST_TAG="$CUST_DIST_TAG"_${DATESTRING}
        else
            export NEW_DIST_TAG=hadoop_2_0_${DATESTRING}
        fi

        dist_tag clone $HADOOP_RELEASE_TAG $NEW_DIST_TAG
        dist_tag add $NEW_DIST_TAG $HADOOP_INSTALL_STRING $HADOOP_CONFIG_INSTALL_STRING $LOCAL_CONFIG_INSTALL_STRING
    fi
fi

echo ===
echo ===
echo ===
echo ===
echo "===  Dist Tag='$HADOOP_RELEASE_TAG'"
echo "===  Hadoop Version (full)='$FULLHADOOPVERSION'"
echo "===  Hadoop Version (short)='$HADOOPVERSION'"
echo "===  HADOOP_27='$HADOOP_27'"
[ -n $TEZVERSION ] && echo "===  Tez Version='$TEZVERSION'"
[ -n $SPARKVERSION ] && echo "===  Spark Version='$SPARKVERSION'"
echo "===  Requested packages='$HADOOP_INSTALL_STRING'"
echo "===  Requested configs='$HADOOP_CONFIG_INSTALL_STRING'"
echo "===  Requested MVN pkgs='$HADOOP_MVN_INSTALL_STRING'"
echo ===
echo ===
echo ===
echo ===
export RUNSIMPLETEST=true

#		side note: this removes any leftover cruft from a previous run. Hudson does not start 'clean'.

rm -f *.tgz > /dev/null 2>&1

# Remove spaces in cluster name
CLUSTER=`echo $CLUSTER|tr -d ' '`

# Make sure rocl is installed on all nodes
PDSH_SSH_ARGS_APPEND="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
                    pdsh -S -r @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway 'yinst install -yes rocl'

#		default values, if not set by a Hudson/user environment variable.
[ -z "$ADMIN_HOST" ] && export ADMIN_HOST=adm102.blue.ygrid.yahoo.com
[ -z "$CLUSTER" ] && export CLUSTER=none
[ -z "$JOB_NAME" ] && export JOB_NAME=none
[ -z "$BUILD_NUMBER" ] && export BUILD_NUMBER=none
[ -z "$CREATEGSHOME" ] && export CREATEGSHOME=true
[ -z "$REMOVE_EXISTING_LOGS" ] && export REMOVE_EXISTING_LOGS=true
[ -z "$REMOVEEXISTINGDATA" ] && export REMOVEEXISTINGDATA=true
[ -z "$CLEANLOCALCONFIG" ] && export CLEANLOCALCONFIG=false
[ -z "$GRIDJDK_VERSION" ] && export GRIDJDK_VERSION=1.7.0_17
[ -z "$GRIDJDK_INSTALL_STRING" ] && export GRIDJDK_INSTALL_STRING=gridjdk:hadoopXXX20X104Xlatest
[ -z "$USERNAMES" ] && export USERNAMES=mapredqa:hdfsqa
[ -z "$HDFSUSER" ] && export HDFSUSER=`echo $USERNAMES | cut -f2 -d:`
[ -z "$MAPREDUSER" ] && export MAPREDUSER=`echo $USERNAMES | cut -f1 -d:`
[ -z "$GRIDJDK64_INSTALL_STRING" ] && export GRIDJDK64_INSTALL_STRING=gridjdk64:hadoopXXX2X0X5Xlatest
if [[ "$HADOOP_27" == "true" ]]; then
    [ -z "$HADOOP_INSTALL_STRING" ] && export HADOOP_INSTALL_STRING=HADOOP_2_LATEST
    [ -z "$LOCAL_CONFIG_INSTALL_STRING" ] && export LOCAL_CONFIG_INSTALL_STRING=$LOCAL_CONFIG_PKG_NAME:HADOOP_2_LATEST
else
    [ -z "$HADOOP_INSTALL_STRING" ] && export HADOOP_INSTALL_STRING=hadoop:hadoopXXX2X0X5Xlatest
    [ -z "$LOCAL_CONFIG_INSTALL_STRING" ] && export LOCAL_CONFIG_INSTALL_STRING=$LOCAL_CONFIG_PKG_NAME:hadoop_23_localconfig_latest
fi

[ -z "$HADOOP_CONFIG_INSTALL_STRING" ] && export HADOOP_CONFIG_INSTALL_STRING=HadoopConfigopenstacklargedisk:hadoopXXX2X0X5Xlatest
[ -z "$KILLALLPROCESSES" ] && export KILLALLPROCESSES=true
[ -z "$RUNKINIT" ] && export RUNKINIT=true
[ -z "$RUNSIMPLETEST" ] && export RUNSIMPLETEST=true
[ -z "$STARTYARN" ] && export STARTYARN=true
[ -z "$CONFIGUREJOBTRACKER" ] && export CONFIGUREJOBTRACKER=true
if [[ "$HADOOP_27" == "true" ]]; then
    CLUSTER_LIST="monsters hbasedev"
else
    CLUSTER_LIST="monsters adhoc2"
fi
for i in $CLUSTER_LIST
do
    if [ $i = $CLUSTER ]; then
        export CONFIGUREJOBTRACKER=false
        export STARTYARN=false
        export RUNSIMPLETEST=false
    fi
done
[ -z "$INSTALL_GW_IN_YROOT" ] && export INSTALL_GW_IN_YROOT=false
[ -z "$USE_DEFAULT_QUEUE_CONFIG" ] && export USE_DEFAULT_QUEUE_CONFIG=true
[ -z "$ENABLE_HA" ] && export ENABLE_HA=false

[ -z "$STARTNAMENODE" ] && export STARTNAMENODE=true
[ -z "$INSTALLLOCALSAVE" ] && export INSTALLLOCALSAVE=true

[ -z "HIT_DEPLOY" ] && export HIT_DEPLOY=false
[ -z "KEEP_HIT_YROOT" ] && export KEEP_HIT_YROOT=false
[ -z "$HITVERSION" ] && export HITVERSION=none
[ -z "$INSTALL_HIT_TEST_PACKAGES" ] && export INSTALL_HIT_TEST_PACKAGES=false
[ -z "$EXCLUDE_HIT_TESTS" ] && export EXCLUDE_HIT_TESTS=none
[ -z "$RUN_HIT_TESTS" ] && export RUN_HIT_TESTS=false
[ -z "$INSTALL_TEZ" ] && export INSTALL_TEZ=false
[ -z "$TEZ_QUEUE" ] && export TEZ_QUEUE=default
[ -z "$TEZVERSION" ] && export TEZVERSION=none
[ -z "$SPARKVERSION" ] && export SPARKVERSION=none
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
[ -z "$QA_PACKAGES" ] && export QA_PACKAGES=none
[ -z "$SEND_LOG_TO_STDOUT" ] && export SEND_LOG_TO_STDOUT=false
[ -z "$NO_CERTIFICATION" ] && export NO_CERTIFICATION=false
[ -z "$HBASE_SHORTCIRCUIT" ] && export HBASE_SHORTCIRCUIT=false
[ -z "$CREATE_NEW_CLUSTER_KEYTAB" ] && export CREATE_NEW_CLUSTER_KEYTAB=false
[ -z "$HCATIGORTAG" ] && export HCATIGORTAG=none
[ -z "$HIVEIGORTAG" ] && export HIVEIGORTAG=none
[ -z "$OOZIEIGORTAG" ] && export OOZIEIGORTAG=none

#
## stack component install settings
#
# potential stack components to install
#
# these are jenkins version select controls, or 'none'
[ -z "$STACK_COMP_VERSION_PIG" ] && export STACK_COMP_VERSION_PIG=none
[ -z "$STACK_COMP_VERSION_HIVE" ] && export STACK_COMP_VERSION_HIVE=none
[ -z "$STACK_COMP_VERSION_OOZIE" ] && export STACK_COMP_VERSION_OOZIE=none
# spark is a boolean jenkins control
[ -z "$STACK_COMP_INSTALL_SPARK" ] && export STACK_COMP_INSTALL_SPARK=false


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
}
trap 'error_handler ${LINENO}' ERR

export DATESTRING=`date +%y%m%d%H%M`
sh yinstify.sh  -v 0.0.1.${CLUSTER}.$DATESTRING
filelist=`ls  *.${CLUSTER}.*.tgz`

# From above: "then copies that package to the destination machine and runs it..."

scp $filelist  $ADMIN_HOST:/tmp/
ssh $ADMIN_HOST "cd /tmp/ && /usr/local/bin/yinst  install  -root /tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING -yes /tmp/$filelist -set root.propagate_start_failures=1"
ssh $ADMIN_HOST "/usr/local/bin/yinst  start  -root /tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING  hadoopgridrollout"
# (
# echo "cd /tmp/ && /usr/local/bin/yinst  install  -root /tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING -yes /tmp/$filelist "
# echo "/usr/local/bin/yinst  start  -root /tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING  hadoopgridrollout"
# echo 'finalstatus=$?'
# echo 'echo finalstatus=$finalstatus'
# echo 'exit $finalstatus'
# )| ssh $ADMIN_HOST

st=$?
echo finalstatus=$st
echo "Running ssh $ADMIN_HOST /usr/local/bin/yinst  start  -root /tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING  hadoopgridrollout status: $st"

if [ "$st" -ne 0 ]
then
    exit $st
fi

(
echo "/usr/local/bin/yinst  remove -all -live   -root /tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING  hadoopgridrollout"
echo "cd /tmp && rm -rf $filelist"
)| ssh $ADMIN_HOST

scp $ADMIN_HOST:/tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING/manifest.txt  manifest.txt

cp  manifest.txt ${WORKSPACE}/
cat manifest.txt

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
    else
        set -x
        time ./$STACK_COMP_SCRIPT $CLUSTER $STACK_COMP_VERSION
        st=$?
        set +x
        if [ $st -ne 0 ]; then
            echo "ERROR: component install for ${STACK_COMP} failed!"
        fi
    fi
    end=`date +%s`
    h_end=`date +%Y/%m/%d-%H:%M:%S`
    runtime=$((end-start))
    printf "%-124s : %.0f min (%.0f sec) : %s : %s : %s\n" $STACK_COMP_SCRIPT $(echo "scale=2;$runtime/60" | bc) $runtime $h_start $h_end $st >> $artifacts_dir/timeline.log
    cat $artifacts_dir/timeline.log
}

deploy_stack pig $STACK_COMP_VERSION_PIG pig-install-check.sh
deploy_stack hive $STACK_COMP_VERSION_HIVE hive-install-check.sh
deploy_stack oozie $STACK_COMP_VERSION_OOZIE oozie-install-check.sh

# Copy HIT test results back if there is any
if [ $RUN_HIT_TESTS = "true" ]; then
    echo "Clean up workspace and remove old HIT test results from previous runs.."
    set -e
    set -x
    rm -rf ${WORKSPACE}/hit_results
    mkdir -p ${WORKSPACE}/hit_results
    scp -r $ADMIN_HOST:/grid/0/tmp/${CLUSTER}.${DATESTRING} ${WORKSPACE}/hit_results/
    case "$MAILTO" in
        none)
            echo "Skip HIT deployment notification email..."
            ;;
        *yahoo-inc*)
            echo "Sending HIT deployment notification email to $MAILTO now ..."
            perl HITEmailReport.pl --mailto="$MAILTO"
            ;;
        *)
            echo "Ignore HIT deployment notification because $MAILTO does not seem to be valid...."
            ;;
    esac
fi

# Review: note that the exit-status of the deploy is indeterminate, and seems to reflect the success of that final 'yinst-remove'.
exit $?
