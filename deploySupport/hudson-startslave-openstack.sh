#!/bin/bash

export scriptnames=openstacklargedisk
export confpkg=HadoopConfigopenstacklargedisk
export localconfpkg=hadooplocalconfigsopenstacklarge

echo =========================================
echo Beginning of Openstack deployment job.
echo hostname = `hostname`
echo date = `TZ=PDT8PDT date `
echo date = `TZ= date`
echo =========================================

export PATH=$PATH:/usr/bin:/usr/local/bin:/bin:/home/y/bin:/sroot:/sbin
export DATESTRING=`date +%y%m%d%H%M`

# Setup and cleanup artifacts directory
artifacts_dir="${WORKSPACE}/artifacts"
if [[ -d $artifacts_dir ]]; then
    rm -rf $artifacts_dir
fi
mkdir -p $artifacts_dir

# echo environment follows:
# /bin/env
cd deploySupport

# 	From above:  "It massages the arguments given"
#
# Note that we might add one additional thing: a choice-list.

[ -z "$LOCAL_CONFIG_PKG_NAME" ] && export LOCAL_CONFIG_PKG_NAME=$localconfpkg

# Check if dist_tag is valid. If not, exit.
DIST_TAG_LIST=`dist_tag list $HADOOP_RELEASE_TAG`
if [[ $? != "0" ]];then
    echo "ERROR: dist_tag list '$HADOOP_RELEASE_TAG' failed: '$DIST_TAG_LIST'; Exiting!!!"
    exit 1;
fi

# Fetch the hadoop version
set -x
export FULLHADOOPVERSION=`dist_tag list $HADOOP_RELEASE_TAG hadoopcoretree | cut -d'-' -f2`
set +x
if [ -z "$FULLHADOOPVERSION" ]; then
    echo "ERROR: Cannot determine hadoop version!!! Exiting!!!"
    exit 1
fi
# short version: e.g 2.6
set -x
export HADOOPVERSION=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG hadoopcoretree | cut -f2,3 -d'-' | cut -f1,2 -d.`
set +x

if [[ "$HADOOPVERSION" > "2.6" ]]; then
    HADOOP_27="true"
else
    HADOOP_27="false"
fi
set -x
export HADOOP_27=$HADOOP_27
set +x


HADOOP_CORE_BASE_PKGS="hadoopcoretree hadoopgplcompression hadoopCommonsDaemon"
if [[ "$HADOOP_27" == "true" ]]; then
    export HADOOP_CORE_PKGS="$HADOOP_CORE_BASE_PKGS yjava_jdk yspark_yarn_shuffle"
else
    export HADOOP_CORE_PKGS="$HADOOP_CORE_BASE_PKGS gridjdk64 gridjdk"
fi
export HADOOP_MVN_PKGS="hadoop_mvn_auth hadoop_mvn_common hadoop_mvn_hdfs"

if [ -n "$HADOOP_RELEASE_TAG" ]
then
    export HADOOP_CONFIG_INSTALL_STRING=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $confpkg- | cut -d ' ' -f 1`
    for i in $HADOOP_CORE_PKGS
    do
        export HADOOP_INSTALL_STRING_PKG=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $i- | cut -d ' ' -f 1`
        export HADOOP_INSTALL_STRING="$HADOOP_INSTALL_STRING $HADOOP_INSTALL_STRING_PKG "
    done
    for i in $HADOOP_MVN_PKGS
    do
        export HADOOP_MVN_INSTALL_STRING_PKG=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $i- | cut -d ' ' -f 1`
        export HADOOP_MVN_INSTALL_STRING="$HADOOP_MVN_INSTALL_STRING $HADOOP_MVN_INSTALL_STRING_PKG "
    done
    export HADOOP_CORETREE_INSTALL_STRING=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep hadoopcoretree | cut -d ' ' -f 1`
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
        export HADOOP_CONFIG_INSTALL_STRING=`/home/y/bin/dist_tag list $HIT_DEPLOYMENT_TAG |grep $confpkg- | cut -d ' ' -f 1`
        for i in $HADOOP_CORE_PKGS
        do
            export HADOOP_INSTALL_STRING_PKG=`/home/y/bin/dist_tag list $HIT_DEPLOYMENT_TAG |grep $i- | cut -d ' ' -f 1`
            export HADOOP_INSTALL_STRING="$HADOOP_INSTALL_STRING $HADOOP_INSTALL_STRING_PKG "
        done
        for i in $HADOOP_MVN_PKGS
        do
            export HADOOP_MVN_INSTALL_STRING_PKG=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep $i- | cut -d ' ' -f 1`
            export HADOOP_MVN_INSTALL_STRING="$HADOOP_MVN_INSTALL_STRING $HADOOP_MVN_INSTALL_STRING_PKG "
        done
        export HADOOP_CORETREE_INSTALL_STRING=`/home/y/bin/dist_tag list $HADOOP_RELEASE_TAG |grep hadoopcoretree | cut -d ' ' -f 1`
        export LOCAL_CONFIG_INSTALL_STRING=`/home/y/bin/dist_tag list $HIT_DEPLOYMENT_TAG |grep $LOCAL_CONFIG_PKG_NAME- | cut -d ' ' -f 1`


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
    echo "readback tez version as:$TEZVERSION"
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
echo ===  New Dist Tag: $NEW_DIST_TAG
echo ===  Dist Tag: $HADOOP_RELEASE_TAG
echo ===  Hadoop Version: $FULLHADOOPVERSION
echo ===  Requested to install $HADOOP_INSTALL_STRING
echo ===  Requested configs: $HADOOP_CONFIG_INSTALL_STRING
echo ===  Requested MVN pkgs: $HADOOP_MVN_INSTALL_STRING
echo ===
echo ===
echo ===
echo ===
export RUNSIMPLETEST=true

#		side note: this removes any leftover cruft from a previous run. Hudson does not start 'clean'.

rm -f *.tgz > /dev/null 2>&1

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
for i in monsters
do
    if [ $i = $CLUSTER ]; then
        export CONFIGUREJOBTRACKER=false
        export STARTYARN=false
        export RUNSIMPLETEST=false
    fi
done
[ -z "$INSTALL_GW_IN_YROOT" ] && export INSTALL_GW_IN_YROOT=false
[ -z "$USE_DEFAULT_QUEUE_CONFIG" ] && export USE_DEFAULT_QUEUE_CONFIG=false
[ -z "$ENABLE_HA" ] && export ENABLE_HA=false

[ -z "$STARTNAMENODE" ] && export STARTNAMENODE=true
[ -z "$INSTALLLOCALSAVE" ] && export INSTALLLOCALSAVE=true

[ -z "$HITVERSION" ] && export HITVERSION=none
[ -z "$INSTALL_HIT_TEST_PACKAGES" ] && export INSTALL_HIT_TEST_PACKAGES=false
[ -z "$EXCLUDE_HIT_TESTS" ] && export EXCLUDE_HIT_TESTS=none
[ -z "$RUN_HIT_TESTS" ] && export RUN_HIT_TESTS=false
[ -z "$PIGVERSION" ] && export PIGVERSION=none
[ -z "$OOZIEVERSION" ] && export OOZIEVERSION=none
[ -z "$OOZIE_SERVER" ] && export OOZIE_SERVER=default
[ -z "$HIVEVERSION" ] && export HIVEVERSION=none
[ -z "$HIVE_VERSION" ] && export HIVE_VERSION=none
[ -z "$HIVE_SERVER2_VERSION" ] && export HIVE_SERVER2_VERSION=none
[ -z "$STARLINGVERSION" ] && export STARLINGVERSION=none
[ -z "$NOVAVERSION" ] && export NOVAVERSION=none
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

# stack component install settings
[ -z "$STACK_COMP_INSTALL_HIVE" ] && export STACK_COMP_INSTALL_HIVE=true



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
    set -x
    scp $ADMIN_HOST:$ADMIN_WORKSPACE/manifest.txt $artifacts_dir/manifest.txt
    scp $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$CLUSTER/timeline.log $artifacts_dir/timeline.log
    cat $artifacts_dir/manifest.txt

    # Add to the build artifact handy references to the NN and RM webui
    webui_file="$artifacts_dir/webui.html"
    echo "<Pre>" > $webui_file;

    echo "Get the namenode and resourcemanager"
    namenode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode)"|head -1`;
    URL="http://$namenode:50070/dfshealth.jsp"
    echo "WEBUI: $cluster NN $URL"
    printf "%-12s %s %s %s\n" "$CLUSTER" "NN" "-" "<a href=$URL>$URL</a>" >> $webui_file;

    rm=`yinst range -ir "(@grid_re.clusters.$CLUSTER.jobtracker)"|tr -s '\n' ','|sed -e  's/,$//'`;
    URL="http://$rm:8088/cluster"
    echo "WEBUI: $cluster RM $URL"
    printf "%-12s %s %s %s\n" "$CLUSTER" "RM" "-" "<a href=$URL>$URL</a>"  >> $webui_file;
    echo "</Pre>" >> $webui_file;

    set +x
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

export BUILD_DESC="Deploy to $CLUSTER $FULLHADOOPVERSION ($HADOOP_RELEASE_TAG)"
echo "$BUILD_DESC"

#################################################################################
# RUN THE INSTALL SCRIPT ON THE ADM HOST
# From above: "then copies that package to the destination machine and runs it..."
#################################################################################
export DATESTRING=`date +%y%m%d%H%M`
# GRIDCI-426: component name cannot exceed 10 characters.
component=${CLUSTER:0:10}
set -x
sh yinstify.sh  -v 0.0.1.${component}.$DATESTRING
set +x
filelist=`ls  *.${component}.*.tgz`
scp $filelist  $ADMIN_HOST:/tmp/

# Install and start the deployment package on the adm admin box to commence
# deployment as root.
ADMIN_WORKSPACE="/tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING"
set -x
ssh $ADMIN_HOST "\
cd /tmp/ && /usr/local/bin/yinst install -root $ADMIN_WORKSPACE -yes /tmp/$filelist; \
yinst set -root $ADMIN_WORKSPACE root.propagate_start_failures=1; \
/usr/local/bin/yinst start -root $ADMIN_WORKSPACE hadoopgridrollout \
"
st=$?;
set +x
echo "Running ssh $ADMIN_HOST /usr/local/bin/yinst start -root $ADMIN_WORKSPACE hadoopgridrollout status: $st"
if [ "$st" -ne 0 ]
then
    echo "Exit on non-zero yinst exit status: $st"
    get_cluster_exit_status
    exit $st
fi

# Clean up hadoopgridrollout
CLEANUP_ON_EXIT=${CLEANUP_ON_EXIT:="true"}
if [ "$CLEANUP_ON_EXIT" = "true" ]; then
    (
        echo "/usr/local/bin/yinst  remove -all -live -root $ADMIN_WORKSPACE hadoopgridrollout"
        echo "cd /tmp && rm -rf $filelist"
    )| ssh $ADMIN_HOST
fi

fetch_artifacts

#################################################################################
# CHECK IF NEED TO RUN THE HIVE INSTALL SCRIPT ON THE HIVE NODE 
#################################################################################
# gridci-481 install hive server and client
# this relies on hive service keytab being generated and pushed out in the cluster configure portion
# of cluster building (cluster-build/configure_cluster)

if [ "$STACK_COMP_INSTALL_HIVE" == true ]; then

  HIVENODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.hive)"`;
  echo "INFO: Installing Hive component on node $HIVENODE"

  ./hive-install-check.sh $HIVENODE
  if [ $? -ne 0 ]; then
    echo "ERROR: Hive component installer failed!"
  fi

else
  echo "INFO: Not installing Hive component"
fi

  
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
