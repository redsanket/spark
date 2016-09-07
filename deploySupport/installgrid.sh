#!/bin/bash

#
# This script installs onto multiple machines.
#
# (1) The argument is the name of a grid:
#		bigc / ankh / biga / bigb / bigd  / smalle
# (2) The grid info is filled in, in a 'case' statement at the top.
# (3) you need to make sure that the package-name for the config package
#     exists before running.
#     3a: the source for making a new package is in
#            svn+ssh://svn.corp.yahoo.com/yahoo/platform/grid/projects/branches/hudson-0.20/internal/confSupport
#         There are three files to create in "cluster values" (subdir);
#         follow the pattern there. There is also a file ("clusterlists.sh")
#         with the list of clusters.
#     3b: a Hudson job remakes the configs and puts them in dist.
# (4) You will need to comment or uncomment to make it run only the sections you want.
#
#
export PATH="/home/y/bin64:/home/y/bin:$PATH"
echo `hostname`": PATH=$PATH"

confpkginstalloptions=
export yinst=/usr/local/bin/yinst

# options - many are hard-coded for now (maybe always).
export KILLALLPROCESSES=true
export INSTALLYUMMERGE=false
export REMOVEOLDYINSTPKGS=true
export INSTALLNEWPACKAGES=true
export INSTALLLOCALSAVE=true
export RUNKINIT=true
export CONFIGURENAMENODE=true
export STARTNAMENODE=true

export SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
export SCP="scp $SSH_OPT"
export SSH="ssh $SSH_OPT"
export PDSH_SSH_ARGS_APPEND="$SSH_OPT"

# confpkg=HadoopConfigopenstacklargedisk
[ -z "$confpkg" ] && export confpkg=$hadoopgridrollout__confpkg
[ -z "$STARTYARN" ] && export STARTYARN=$hadoopgridrollout__STARTYARN
[ -z "$CONFIGUREJOBTRACKER" ] && export CONFIGUREJOBTRACKER=$hadoopgridrollout__CONFIGUREJOBTRACKER
[ -z "$ADMIN_HOST" ] && export ADMIN_HOST=$hadoopgridrollout__ADMIN_HOST
[ -z "$CHECKSSHCONNECTIVITY" ] && export CHECKSSHCONNECTIVITY=$hadoopgridrollout__CHECKSSHCONNECTIVITY
[ -z "$YINSTSELFUPDATE" ] && export YINSTSELFUPDATE=$hadoopgridrollout__YINSTSELFUPDATE
[ -z "$PREFERREDJOBPROCESSOR" ] && export PREFERREDJOBPROCESSOR=$hadoopgridrollout__PREFERREDJOBPROCESSOR

[ -z "$LOCAL_CONFIG_INSTALL_STRING" ] && export LOCAL_CONFIG_INSTALL_STRING=$hadoopgridrollout__LOCAL_CONFIG_INSTALL_STRING
[ -z "$LOCAL_CONFIG_PKG_NAME" ] && export LOCAL_CONFIG_PKG_NAME=$hadoopgridrollout__LOCAL_CONFIG_PKG_NAME
[ -z "$HDFSUSER" ] && export HDFSUSER=$hadoopgridrollout__HDFSUSER
[ -z "$MAPREDUSER" ] && export MAPREDUSER=$hadoopgridrollout__MAPREDUSER
echo HDFSUSER = $HDFSUSER
echo MAPREDUSER = $MAPREDUSER

[ -z "$REMOVE_EXISTING_LOGS" ] && export REMOVE_EXISTING_LOGS=$hadoopgridrollout__REMOVE_EXISTING_LOGS
[ -z "$CREATEGSHOME" ] && export CREATEGSHOME=$hadoopgridrollout__CREATEGSHOME
[ -z "$CLEANLOCALCONFIG" ] && export CLEANLOCALCONFIG=$hadoopgridrollout__CLEANLOCALCONFIG
[ -z "$REMOVEEXISTINGDATA" ] && export REMOVEEXISTINGDATA=$hadoopgridrollout__REMOVEEXISTINGDATA
[ -z "$REMOVE_YARN_DATA" ] && export REMOVE_YARN_DATA=$hadoopgridrollout__REMOVE_YARN_DATA
[ -z "$REMOVERPMPACKAGES" ] && export REMOVERPMPACKAGES=$hadoopgridrollout__REMOVERPMPACKAGES
[ -z "$HERRIOT_CONF_ENABLED" ] && export HERRIOT_CONF_ENABLED=$hadoopgridrollout__HERRIOT_CONF_ENABLED

[ -z "$INSTALL_TEZ" ] && INSTALL_TEZ="$hadoopgridrollout__INSTALL_TEZ"
[ -z "$STACK_COMP_INSTALL_SPARK" ] && STACK_COMP_INSTALL_SPARK="$hadoopgridrollout__STACK_COMP_INSTALL_SPARK"
[ -z "$HADOOP_INSTALL_STRING" ] && HADOOP_INSTALL_STRING="$hadoopgridrollout__HADOOP_INSTALL_STRING"
[ -z "$HADOOP_MVN_INSTALL_STRING" ] && HADOOP_MVN_INSTALL_STRING="$hadoopgridrollout__HADOOP_MVN_INSTALL_STRING"
[ -z "$HADOOP_CORETREE_INSTALL_STRING" ] && HADOOP_CORETREE_INSTALL_STRING="$hadoopgridrollout__HADOOP_CORETREE_INSTALL_STRING"
[ -z "$HADOOP_CONFIG_INSTALL_STRING" ] && HADOOP_CONFIG_INSTALL_STRING="$hadoopgridrollout__HADOOP_CONFIG_INSTALL_STRING"
[ -z "$RUNSIMPLETEST" ] && RUNSIMPLETEST=$hadoopgridrollout__RUNSIMPLETEST
[ -z "$INSTALL_GW_IN_YROOT" ] && INSTALL_GW_IN_YROOT=$hadoopgridrollout__INSTALL_GW_IN_YROOT
[ -z "$USE_DEFAULT_QUEUE_CONFIG" ] && USE_DEFAULT_QUEUE_CONFIG=$hadoopgridrollout__USE_DEFAULT_QUEUE_CONFIG
[ -z "$OOZIEVERSION" ] && export OOZIEVERSION=$hadoopgridrollout__OOZIEVERSION
[ -z "$OOZIE_SERVER" ] && export OOZIE_SERVER=$hadoopgridrollout__OOZIE_SERVER
[ -z "$OOZIE_TEST_SUITE" ] && export OOZIE_TEST_SUITE=$hadoopgridrollout__OOZIE_TEST_SUITE

[ -z "$JOB_NAME" ] && export JOBNAME=$hadoopgridrollout__JOB_NAME
[ -z "$BUILD_NUMBER" ] && export BUILD_NUMBER=$hadoopgridrollout__BUILD_NUMBER

[ -z "$SEND_LOG_TO_STDOUT" ] && export SEND_LOG_TO_STDOUT=$hadoopgridrollout__SEND_LOG_TO_STDOUT
[ -z "$NO_CERTIFICATION" ] && export NO_CERTIFICATION=$hadoopgridrollout__NO_CERTIFICATION

[ -z "$TEZVERSION" ] && export TEZVERSION=$hadoopgridrollout__TEZVERSION
[ -z "$TEZ_QUEUE" ] && export TEZ_QUEUE=$hadoopgridrollout__TEZ_QUEUE
[ -z "$SPARKVERSION" ] && export SPARKVERSION=$hadoopgridrollout__SPARKVERSION
[ -z "$SPARK_HISTORY_VERSION" ] && export SPARK_HISTORY_VERSION=$hadoopgridrollout__SPARK_HISTORY_VERSION
[ -z "$SPARK_QUEUE" ] && export SPARK_QUEUE=$hadoopgridrollout__SPARK_QUEUE
[ -z "$PIGVERSION" ] && export PIGVERSION=$hadoopgridrollout__PIGVERSION
[ -z "$HITVERSION" ] && export HITVERSION=$hadoopgridrollout__HITVERSION
[ -z "$STARLINGVERSION" ] && export STARLINGVERSION=$hadoopgridrollout__STARLINGVERSION
[ -z "$EXCLUDE_HIT_TESTS" ] && export EXCLUDE_HIT_TESTS=$hadoopgridrollout__EXCLUDE_HIT_TESTS
[ -z "$INSTALL_HIT_TEST_PACKAGES" ] && export INSTALL_HIT_TEST_PACKAGES=$hadoopgridrollout__INSTALL_HIT_TEST_PACKAGES
[ -z "$RUN_HIT_TESTS" ] && export RUN_HIT_TESTS=$hadoopgridrollout__RUN_HIT_TESTS
[ -z "$KEEP_HIT_YROOT" ] && export KEEP_HIT_YROOT=$hadoopgridrollout__KEEP_HIT_YROOT
[ -z "$HIT_DEPLOY" ] && export HIT_DEPLOY=$hadoopgridrollout__HIT_DEPLOY
[ -z "$HIT_DEPLOYMENT_TAG" ] && export HIT_DEPLOYMENT_TAG=$hadoopgridrollout__HIT_DEPLOYMENT_TAG
[ -z "$DATESTRING" ] && export DATESTRING=$hadoopgridrollout__DATESTRING
[ -z "$QA_PACKAGES" ] && export DATESTRING=$hadoopgridrollout__QA_PACKAGES

[ -z "$NOVAVERSION" ] && export NOVAVERSION=$hadoopgridrollout__NOVAVERSION
[ -z "$HIVEVERSION" ] && export HIVEVERSION=$hadoopgridrollout__HIVEVERSION
[ -z "$HIVE_VERSION" ] && export HIVEVERSION=$hadoopgridrollout__HIVE_VERSION
[ -z "$HIVE_SERVER2_VERSION" ] && export HIVE_SERVER2_VERSION=$hadoopgridrollout__HIVE_SERVER2_VERSION
[ -z "$HCATVERSION" ] && export HCATVERSION=$hadoopgridrollout__HCATVERSION
[ -z "$HBASEVERSION" ] && export HBASEVERSION=$hadoopgridrollout__HBASEVERSION
[ -z "$VAIDYAVERSION" ] && export VAIDYAVERSION=$hadoopgridrollout__VAIDYAVERSION
[ -z "$DISTCPVERSION" ] && export DISTCPVERSION=$hadoopgridrollout__DISTCPVERSION
[ -z "$LOG_COLLECTORVERSION" ] && export LOG_COLLECTORVERSION=$hadoopgridrollout__LOG_COLLECTORVERSION
[ -z "$HDFSPROXYVERSION" ] && export HDFSPROXYVERSION=$hadoopgridrollout__HDFSPROXYVERSION
[ -z "$GDMVERSION" ] && export GDMVERSION=$hadoopgridrollout__GDMVERSION
[ -z "$HBASE_SHORTCIRCUIT" ] && export HBASE_SHORTCIRCUIT=$hadoopgridrollout__HBASE_SHORTCIRCUIT
[ -z "$CREATE_NEW_CLUSTER_KEYTAB" ] && export CREATE_NEW_CLUSTER_KEYTAB=$hadoopgridrollout__CREATE_NEW_CLUSTER_KEYTAB

# HIT test pkgs
[ -z "$HCAT_TEST_PKG" ] && export HCAT_TEST_PKG=$hadoopgridrollout__HCAT_TEST_PKG
[ -z "$HIVE_TEST_PKG" ] && export HIVE_TEST_PKG=$hadoopgridrollout__HIVE_TEST_PKG
[ -z "$DISTCP_TEST_PKG" ] && export DISTCP_TEST_PKG=$hadoopgridrollout__DISTCP_TEST_PKG
[ -z "$LOG_COLLECTOR_TEST_PKG" ] && export LOG_COLLECTOR_TEST_PKG=$hadoopgridrollout__LOG_COLLECTOR_TEST_PKG
[ -z "$VAIDYA_TEST_PKG" ] && export VAIDYA_TEST_PKG=$hadoopgridrollout__VAIDYA_TEST_PKG
[ -z "$PIG_TEST_PKG" ] && export PIG_TEST_PKG=$hadoopgridrollout__PIG_TEST_PKG
[ -z "$GDM_TEST_PKG" ] && export GDM_TEST_PKG=$hadoopgridrollout__GDM_TEST_PKG
[ -z "$HBASE_TEST_PKG" ] && export HBASE_TEST_PKG=$hadoopgridrollout__HBASE_TEST_PKG
[ -z "$NOVA_TEST_PKG" ] && export NOVA_TEST_PKG=$hadoopgridrollout__NOVA_TEST_PKG
[ -z "$HDFSPROXY_TEST_PKG" ] && export HDFSPROXY_TEST_PKG=$hadoopgridrollout__HDFSPROXY_TEST_PKG
[ -z "$OOZIE_TEST_PKG" ] && export OOZIE_TEST_PKG=$hadoopgridrollout__OOZIE_TEST_PKG
[ -z "$HADOOPCORE_TEST_PKG" ] && export HADOOPCORE_TEST_PKG=$hadoopgridrollout__HADOOPCORE_TEST_PKG
[ -z "$GRIDJDK_VERSION" ] && export GRIDJDK_VERSION=$hadoopgridrollout__GRIDJDK_VERSION
# HIT test pkgs

[ -n "$YARNTAG" ] && export YARNTAG=$hadoopgridrollout__YARNTAG
[ -n "$HCATIGORTAG" ] && export HCATIGORTAG=$hadoopgridrollout__HCATIGORTAG
[ -n "$HIVEIGORTAG" ] && export HIVEIGORTAG=$hadoopgridrollout__HIVEIGORTAG
[ -n "$OOZIEIGORTAG" ] && export OOZIEIGORTAG=$hadoopgridrollout__OOZIEIGORTAG

export TIMESTAMP=$DATESTRING

# Even though 'set -o' is set from the upstream hudson-startslave scripts, it
# needs to be set here because this script is run via the yinst start. If not
# set, the script will continue downstream after failure is encountered.
set -e
function error_handler {
   LASTLINE="$1"
   echo "ERROR: Trapped error signal in [${BASH_SOURCE}]: line ${LASTLINE} of current or downstream script"
}
trap 'error_handler ${LINENO}' ERR

base=${YINST_ROOT}/conf/hadoop/hadoopAutomation

# Initialize the nodes variables from rolesdb roles
if [ -f ${base}/cluster-list.sh ]
then
    # TODO: Pass STACK_COMP_INSTALL settings to cluster-list.sh so they can be
    # used to determine if component nodes should be added to the host list.
    set -x
    GDMVERSION=$GDMVERSION \
HBASEVERSION=$HBASEVERSION \
HCATVERSION=$HCATVERSION \
HDFSPROXYVERSION=$HDFSPROXYVERSION \
OOZIEVERSION=$OOZIEVERSION \
. ${base}/cluster-list.sh
    set +x
else
	echo "failure: cluster-list.sh is not in same directory as $0. Exiting." 1>&2
	exit 1
fi
st=$?
[ "$st" -ne 0 ] && exit $st
setGridParameters   "$1"

if [ -z "$cluster" ] 
then
	echo "failure: grid name not specified!"
	exit 1
fi

#
# Review: See note in cluster-list.sh. There is a better version of the slaves-file or master-file to
# use, that has had "pdsh....... hostname" run to verify that it is working.
#
ALLHOSTS=`cat hostlist.$1.txt`
#echo "hostlist = '$ALLHOSTS'" | fmt
ALLNAMENODES=`cat namenodes.$cluster.txt`
ALLSECONDARYNAMENODES=`cat secondarynamenodes.$cluster.txt`
ALLNAMENODESAndSecondaries=`cat allnamenodes.$cluster.txt`
NAMENODE_Primary=`head -1 namenodes.$cluster.txt`
NAMENODEHAALIAS=`cat namenodehaalias.$cluster.txt`
HOSTLISTNOGW1=`grep -v $gateway  hostlist.$1.txt`
HOSTLISTNOGW=`echo $HOSTLISTNOGW1 | tr ' ' ,`

[ -e hbasemasternodes.$cluster.txt ] &&  export HBASEMASTERNODE=`cat hbasemasternodes.$cluster.txt`
[ -e hbasezookeepernodes.$cluster.txt ] &&  export HBASEZOOKEEPERNODE=`cat hbasezookeepernodes.$cluster.txt`
[ -e regionservernodes.$cluster.txt ] && export REGIONSERVERNODES=`cat regionservernodes.$cluster.txt`

ALLSLAVES=`cat slaves.$1.txt`
# echo "slavelist = '$ALLSLAVES'" | fmt

if [ -z "$NAMENODE_Primary" ] 
then
	echo "failure: No namenodes indicated in list."
	exit 1
#else
#        echo "namenode (primary): " $NAMENODE_Primary
#        echo "namenodes: " $ALLNAMENODES
fi

# cp slaves.$cluster.txt  /tmp/slaves.$cluster.txt
cp slaves.$1.txt  $scripttmp/slaves.$cluster.txt

export WCALL=hostlist.$1.txt

yr=`date +%Y`
errs=0

export HOSTLIST=`echo $ALLHOSTS| tr ' ' ,`
export SLAVELIST=`echo $ALLSLAVES| tr ' ' ,`
export ALLNAMENODESLIST=`echo $ALLNAMENODES  | tr ' ' ,`
export ALLSECONDARYNAMENODESLIST=`echo $ALLSECONDARYNAMENODES  | tr ' ' ,`
export ALLNAMENODESAndSecondariesList=`echo $ALLNAMENODESAndSecondaries  | tr ' ' ,`

echo =====================================================
echo ===  installing grid: $cluster
echo =====================================================
echo "===  gateway='$gateway'"
echo "===  namenode='$NAMENODE_Primary'"
echo "===  namenodes='$ALLNAMENODES'"
[ -n "$NAMENODEHAALIAS" ] && echo  "=== namenode_alias=$NAMENODEHAALIAS"
#[ -n "$yroots" ] && echo "===  gateways/yroots='$yroots'"
[ -n "$gateways" ] && echo "===  gateways/yroots='$gateways'"
[ -n "$hitnodes" ] && echo "gateways/hit-yroots='$hitnodes'"
echo "===  jobtrackernode='$jobtrackernode'"
echo "===  confpkg='$confpkg'"
echo "===  HOSTLIST='$HOSTLIST' (all nodes)"
echo "===  SLAVELIST='$SLAVELIST' (slave nodes)"
echo =====================================================
echo =====================================================
scripttmp=/grid/0/tmp/scripts.deploy.$cluster
scriptaddr=$ADMIN_HOST::tmp/scripts.deploy.$cluster
grossworkaroundaddr=$ADMIN_HOST::tmp/gross-0.22-dev-workaround
[ -d $scripttmp ] || mkdir -p $scripttmp

#
#  The following section creates a few shell-scripts to be copied to the target
# machines and run.
#
#  One's for starting the job tracker, and three are for starting NN / DN / NN
#		. start-NN
#		. then start-DN (as root!)
#		. then do some NN operations to make directories and so on.
# They are written to $ADMIN_HOST:/grid/0/tmp for retrieval.
#
base=${YINST_ROOT}/conf/hadoop/hadoopAutomation
export MANIFEST=${YINST_ROOT}/manifest.txt
.  ${base}/000-shellfunctions.sh
	
echo "================================================================================="
echo "installgrid.sh: run create conf scripts to auto-generate scripts to be run later:"
echo "================================================================================="
for script in ${base}/[0-9][0-9]*-create-conf-*.sh
do
    echo "eval . $script"
    eval ". $script"
done

#
# Review:  All scripts are copied to /grid/0/tmp.  This should be no problem, even if multiple deploys are happening,
# unless someone updates one of the scripts at just the 'right' moment before a second job starts. Still,
# it could be a slight exposure for concurrency.
#
echo ========== Copying namenode scripts to /grid/0/tmp

cp ${YINST_ROOT}/conf/hadoop/hadoopAutomation/*.sh $scripttmp
cp ${YINST_ROOT}/conf/hadoop/hadoopAutomation/*.pl $scripttmp



echo installing onto $1....
echo HIT_DEPLOY: ${HIT_DEPLOY}

export EXIT_ON_ERROR=true

if [[ "${INSTALL_TEZ}" == only ]]; then
    f=${base}/229-installsteps-installTez.sh
    banner running $f
    . "$f"
    st=$?
    echo "Running $f Status: $st"
    if [ "$EXIT_ON_ERROR" = "true" ]; then
        [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running '" $f "': Exit $st <<<<<<<<<<" && exit $st
    else
       [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running '" $f "' <<<<<<<<<<"
    fi
    ## After successful Tez installation, run wordcount for sanity test
    f=${base}/258-installsteps-runTezWordCount.sh
    banner running $f
    . "$f"
    st=$?
    echo "Running $f Status: $st"
    [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running Tez Wordcount example '" $f "' <<<<<<<<<<"
    exit $st
fi

timeline="$scripttmp/timeline.log"
cat /dev/null > $timeline
pwd=`pwd`
hostname=`hostname`
whoami=`whoami`
index=1
START_STEP=${START_STEP:="0"}
echo "==============================================="
echo "installgrid.sh: run install steps scripts:"
echo "==============================================="
# Be careful not to name any variable in any downstream scripts with the name
# $script because it will override the $script variable here.
for script in ${base}/[0-9][0-9]*-installsteps-*.sh
do
  if [[  -e $script ]]
  then

    script_sn=`basename $script`
    current_step=`echo $script_sn|cut -d'-' -f1|bc`
    if [[ $current_step -lt $START_STEP ]];then
       echo "SKIP deploy script: ${script_sn}: less than starting step '$START_STEP'"
       continue;
    fi

    if ([[ $RUN_HIT_TESTS == "false" ]] && [[ $INSTALL_HIT_TEST_PACKAGES == "false" ]]); then
        if [[ $script_sn =~ "-HIT-" ]]; then
            echo "RUN_HIT_TESTS and INSTALL_HIT_TEST_PACKAGES are false: SKIP HIT deployment script: $script"
            continue
        fi
    fi

    #banner running $f
    set +x
    sleep 1
    banner2 "START INSTALL STEP #$index: '$script'" "Called from $hostname:$pwd/installgrid.sh as $whoami"

    start=`date +%s`
    h_start=`date +%Y/%m/%d-%H:%M:%S`

    # For general shutdown and cleanup scripts, temporarily disable exit on failure.
    SKIP_ERROR_ON_STEP="false"
    if ([[ $script =~ "100-" ]] || [[ $script =~ "101-" ]] || [[ $script =~ "140-" ]]); then
        SKIP_ERROR_ON_STEP="true"
        set +e
    fi

    set -x
    time . "$script"
    st=$?
    set +x
    end=`date +%s`
    h_end=`date +%Y/%m/%d-%H:%M:%S`
    runtime=$((end-start))

    if [[ "$SKIP_ERROR_ON_STEP" == "true" ]]; then
        set -e
    fi

    echo "CURRENT COMPLETED EXECUTION STEPS:"
    printf "%-2s %-124s : %.0f min (%.0f sec) : %s : %s : %s\n" $index $script $(echo "scale=2;$runtime/60" | bc) $runtime $h_start $h_end $st >> $timeline
    cat $timeline

    banner "END INSTALL STEP #$index: '$script': status='$st'"
    if [ "$st" -ne 0 ]; then
        echo "EXIT_ON_ERROR=$EXIT_ON_ERROR"
        if [ "$EXIT_ON_ERROR" = "true" ]; then
            echo ">>>>>>>> EXIT ON ERROR <<<<<<<<<<" && exit $st
        fi
    fi
    index=$((index+1))
  else
    echo "WARNING!!! deploy script $script not found!!!"
  fi
done
echo FinalStatus: $st
# Review: Note that exit-status is not dealt with, either here or in the hudson-startup script.
