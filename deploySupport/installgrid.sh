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
confpkg=HadoopConfiggeneric10nodeblue
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
[ -z "$REMOVERPMPACKAGES" ] && export REMOVERPMPACKAGES=$hadoopgridrollout__REMOVERPMPACKAGES
[ -z "$HERRIOT_CONF_ENABLED" ] && export HERRIOT_CONF_ENABLED=$hadoopgridrollout__HERRIOT_CONF_ENABLED

[ -z "$HADOOP_INSTALL_STRING" ] && HADOOP_INSTALL_STRING="$hadoopgridrollout__HADOOP_INSTALL_STRING"
[ -z "$HADOOP_MVN_INSTALL_STRING" ] && HADOOP_MVN_INSTALL_STRING="$hadoopgridrollout__HADOOP_MVN_INSTALL_STRING"
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

[ -z "$PIGVERSION" ] && export PIGVERSION=$hadoopgridrollout__PIGVERSION
[ -z "$HITVERSION" ] && export HITVERSION=$hadoopgridrollout__HITVERSION
[ -z "$STARLINGVERSION" ] && export STARLINGVERSION=$hadoopgridrollout__STARLINGVERSION
[ -z "$EXCLUDE_HIT_TESTS" ] && export EXCLUDE_HIT_TESTS=$hadoopgridrollout__EXCLUDE_HIT_TESTS
[ -z "$INSTALL_HIT_TEST_PACKAGES" ] && export INSTALL_HIT_TEST_PACKAGES=$hadoopgridrollout__INSTALL_HIT_TEST_PACKAGES
[ -z "$RUN_HIT_TESTS" ] && export RUN_HIT_TESTS=$hadoopgridrollout__RUN_HIT_TESTS
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

base=${YINST_ROOT}/conf/hadoop/hadoopAutomation
if [ -f ${base}/cluster-list.sh ]
then
	. ${base}/cluster-list.sh
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
echo hostlist = $ALLHOSTS | fmt
ALLNAMENODES=`cat namenodes.$cluster.txt`
ALLSECONDARYNAMENODES=`cat secondarynamenodes.$cluster.txt`
ALLNAMENODESAndSecondaries=`cat allnamenodes.$cluster.txt`
NAMENODE_Primary=`head -1 namenodes.$cluster.txt`
HOSTLISTNOGW1=`grep -v $gateway  hostlist.$1.txt`
HOSTLISTNOGW=`echo $HOSTLISTNOGW1 | tr ' ' ,`

[ -e hbasemasternodes.$cluster.txt ] &&  export HBASEMASTERNODE=`cat hbasemasternodes.$cluster.txt`
[ -e hbasezookeepernodes.$cluster.txt ] &&  export HBASEZOOKEEPERNODE=`cat hbasezookeepernodes.$cluster.txt`
[ -e regionservernodes.$cluster.txt ] && export REGIONSERVERNODES=`cat regionservernodes.$cluster.txt`

ALLSLAVES=`cat slaves.$1.txt`
echo slavelist = $ALLSLAVES | fmt

if [ -z "$NAMENODE_Primary" ] 
then
	echo "failure: No namenodes indicated in list."
	exit 1
else
        echo ".... namenode (primary): " $NAMENODE_Primary
        echo ".... namenodes: " $ALLNAMENODES
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

echo $HOSTLIST
echo =====================================================
echo ===  installing grid: $cluster
echo =====================================================
echo ===  gateway=$gateway
echo ===  namenode=$NAMENODE_Primary
echo ===  namenodes="$ALLNAMENODES"
#[ -n "$yroots" ] && echo ===  gateways/yroots="$yroots"
[ -n "$gateways" ] && echo ===  gateways/yroots="$gateways"
[ -n "$hitnodes" ] && echo              gateways/hit-yroots="$hitnodes"
echo ===  jobtrackernode=$jobtrackernode
echo ===  confpkg=$confpkg
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
	
for f in ${base}/[0-9][0-9]*-create-conf-*.sh
do
    eval ". $f"
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

export EXIT_ON_ERROR=true

for f in ${base}/[0-9][0-9]*-installsteps-*.sh
do
if [[  -e $f ]]
then

    banner running $f
    . "$f"
    st=$?
    echo "Running $f Status: $st"
    if [ "$EXIT_ON_ERROR" = "true" ]; then
        [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running and exit '" $f "' <<<<<<<<<<" && exit $st
    else
        [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running '" $f "' <<<<<<<<<<"   
    fi

fi
done
echo FinalStatus: $st
# Review: Note that exit-status is not dealt with, either here or in the hudson-startup script.
