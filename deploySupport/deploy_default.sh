#################################################################################
# From installgrid.sh
#################################################################################
# options - many are hard-coded for now (maybe always).
export INSTALLYUMMERGE=false
export REMOVEOLDYINSTPKGS=true
export INSTALLNEWPACKAGES=true
export CONFIGURENAMENODE=true
confpkginstalloptions=

[ -z "$CHECKSSHCONNECTIVITY" ] && export CHECKSSHCONNECTIVITY=
[ -z "$YINSTSELFUPDATE" ] && export YINSTSELFUPDATE=false
[ -z "$PREFERREDJOBPROCESSOR" ] && export PREFERREDJOBPROCESSOR=
[ -z "$REMOVERPMPACKAGES" ] && export REMOVERPMPACKAGES=
[ -z "$HERRIOT_CONF_ENABLED" ] && export HERRIOT_CONF_ENABLED=
[ -z "$OOZIE_TEST_SUITE" ] && export OOZIE_TEST_SUITE=
[ -n "$YARNTAG" ] && export YARNTAG=

export TIMESTAMP=$DATESTRING

#################################################################################
# From hudson-startslave.sh
#################################################################################
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
[ -z "$LOCAL_CONFIG_PKG_NAME" ] && export LOCAL_CONFIG_PKG_NAME=$localconfpkg

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
[ -z "$RUN_HIT_TESTS" ] && export RUN_HIT_TESTS=false
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
[ -z "$GDM_TEST_PKG" ] && export GDM_TEST_PKG=none
[ -z "$OOZIE_TEST_PKG" ] && export OOZIE_TEST_PKG=none
[ -z "$NOVA_TEST_PKG" ] && export NOVA_TEST_PKG=none
[ -z "$HBASE_TEST_PKG" ] && export HBASE_TEST_PKG=none

[ -z "$HADOOPCORE_TEST_PKG" ] && export HADOOPCORE_TEST_PKG=none
## HIT test pkg
