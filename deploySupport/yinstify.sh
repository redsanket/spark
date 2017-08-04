rm -f *.tgz
export debug=echo

if [[ -z $DATESTRING ]]; then
    export DATESTRING=`date +%y%m%d%H%M`
fi

die() {
	echo $* 1>&2
	exit 1
}

# If HOMEDIR is not set or passed in via Jenkns job, set it.
if [ -z "$HOMEDIR" ]; then
    # set the home directory based on the openstack node's zone,
    # this is needed for the grid-backplane nodes because they use
    # 'home' instead of 'homes', and export this as HOMEDIR
    RM_NODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.jobtracker)"`;
    NODE_DOMAIN=`echo $RM_NODE | cut -d'.' -f2-`
    if [[ "$NODE_DOMAIN" == "ygrid.yahoo.com" ]]; then
        export HOMEDIR="/home"
    elif [[ -n "$NODE_DOMAIN" ]]; then
        export HOMEDIR="/homes"
    else
        echo "Error: unable to determine NODE_DOMAIN: HOMEDIR is not set!"
        exit 1
    fi
fi
export HOMEDIR=$HOMEDIR
echo "export HOMEDIR=$HOMEDIR"

mkyicf() {
	local pkgname=$1
	local outfile=$2
	local scriptline=$3
	(
	echo "# Do not edit - this file is auto-generated by yinstify.sh" 
	echo PRODUCT_NAME = $pkgname
	echo SRCTOP = .
	echo VERSION = $version
	echo SHORT_DESC = Automation-task for $filename.
	echo LONG_DESC = Hadoop configuration for $pkg. Contains config-files but not .jar files.
	echo CUSTODIAN = hadoop-devel@yahoo-inc.com \ http://devel.yahoo.com/yahoo/doc/fileutils/
	echo OWNER = root
	echo PACKAGE_OS_SPECIFIC=no
        echo YINST bug-product Hadoop
        echo YINST bug-component General

	echo GROUP = root
	echo PERM = 0444

        allfields="\
confpkg \
ADMIN_HOST \
BUILD_NUMBER \
CHECKSSHCONNECTIVITY \
CLEANLOCALCONFIG \
CLUSTER \
CONFIGUREJOBTRACKER \
CONFIGURENAMENODE \
CREATEGSHOME \
CREATE_NEW_CLUSTER_KEYTAB \
DATESTRING \
DISTCPVERSION \
DISTCP_TEST_PKG \
ENABLE_HA \
EXCLUDE_HIT_TESTS \
FULLHADOOPVERSION \
GDMVERSION \
GDM_TEST_PKG \
GRIDJDK_VERSION \
HADOOPCORE_TEST_PKG \
HADOOPVERSION \
HADOOP_27 \
HADOOP_CONFIG_INSTALL_STRING \
HADOOP_CORETREE_INSTALL_STRING \
HADOOP_INSTALL_STRING \
HADOOP_MVN_INSTALL_STRING \
HBASEVERSION \
HBASE_SHORTCIRCUIT \
HBASE_TEST_PKG \
HCATIGORTAG \
HCATVERSION \
HCAT_TEST_PKG \
HDFSPROXYVERSION \
HDFSPROXY_TEST_PKG \
HDFSUSER \
HERRIOT_CONF_ENABLED \
HITVERSION \
HIT_DEPLOYMENT_TAG \
HIVEIGORTAG \
HIVEVERSION \
HIVE_SERVER2_VERSION \
HIVE_TEST_PKG \
HIVE_VERSION \
HOMEDIR \
INSTALLLOCALSAVE \
INSTALLNEWPACKAGES \
INSTALLYUMMERGE \
INSTALL_GW_IN_YROOT \
INSTALL_HIT_TEST_PACKAGES \
INSTALL_TEZ \
STACK_COMP_INSTALL_SPARK \
JOB_NAME \
KILLALLPROCESSES \
LOCAL_CONFIG_INSTALL_STRING \
LOCAL_CONFIG_PKG_NAME \
LOG_COLLECTORVERSION \
LOG_COLLECTOR_TEST_PKG \
MAPREDUSER \
NOVAVERSION \
NOVA_TEST_PKG \
NO_CERTIFICATION \
OOZIEIGORTAG \
OOZIEVERSION \
OOZIE_SERVER \
OOZIE_TEST_PKG \
OOZIE_TEST_SUITE \
PIGVERSION \
PIG_TEST_PKG \
QA_PACKAGES \
REMOVEEXISTINGDATA \
REMOVE_YARN_DATA \
REMOVERPMPACKAGES \
REMOVE_EXISTING_LOGS \
RHEL6NATIVE \
RUNKINIT \
RUNSIMPLETEST \
RUN_HIT_TESTS \
SEND_LOG_TO_STDOUT \
STARLINGVERSION \
STARTNAMENODE \
STARTYARN \
START_STEP \
TEZVERSION \
TEZ_QUEUE \
SPARKVERSION \
SPARK_HISTORY_VERSION \
SPARK_SHUFFLE_VERSION \
SPARK_QUEUE \
USE_DEFAULT_QUEUE_CONFIG \
VAIDYAVERSION \
VAIDYA_TEST_PKG \
YINSTSELFUPDATE \
ZOOKEEPERVERSION \
"

        if [[ "$HADOOP_27" == "true" ]]; then
            allfields="$allfields \
GDM_PKG_NAME \
HIT_DEPLOY \
KEEP_HIT_YROOT \
TEZ_QUEUE \
"
        fi

	for i in $allfields
	do
            x="def=false; [ -n \"\${$i}\" ] && def=\"\${$i}\"; echo YINST set $i \$def "
	    eval $x
        done
	echo $scriptline
	echo "YINST set autostartup false"
	# echo "YINST bug-product $(PRODUCT_NAME)"
	# echo "YINST bug-component General"
	echo "YINST set referencedir $HOMEDIR/hadoopqa"
	) > $outfile


}
version=''
while getopts v: name $*
do
    case $name in
    v)    version="$OPTARG";;
    ?)    printf "Usage: %s: [-v version] args\n" $0
          exit 2;;
    esac
done
shift $(($OPTIND - 1))

if [[ -z $version ]]; then
    version=0.0.0.1.$DATESTRING
fi

pkg=hadoopgridrollout

install_script=installgrid.sh

# dumpMembershipList.sh and dumpAllRoles.sh scripts is deprecated after GRIDCI-2332
# Now the cluster specific roles file is included as a part of the yinst package 
# which gets installed in the dev box.
if [[ ! -x /home/y/bin/rocl ]]; then
    yinst i rocl
fi
/home/y/bin/rocl -r %grid_re.clusters.${CLUSTER}% > ${CLUSTER}.rolelist.txt

cmd="conf/hadoop/hadoopAutomation/$install_script \"\${CLUSTER}\""
mkyicf  "$pkg"  "$pkg.yicf"  "YINST start 200  $cmd"
# echo "YINST start 100 env" >> $pkg.yicf
echo "f 0755 - - conf/hadoop/hadoopAutomation/$install_script  $install_script " >> $pkg.yicf

scripts="\
*.pl \
*.py \
*igor*.sh \
HIT-hdfsproxy-install.sh \
[0-9]*-*.sh \
cleangrid.sh \
cluster-list.sh *-rw.sh \
datanode*.sh \
namenode*.sh \
setup_nm_health_check_script.sh \
${CLUSTER}.rolelist.txt \
"

for script in $scripts; do
    echo "f 0755 - - conf/hadoop/hadoopAutomation/$script $script" >> $pkg.yicf
done

if [ -d ../fitSupport ]
then
    (cd ../fitSupport
	for script in FIT*.sh
	do
           echo "f 0755 - - conf/hadoop/hadoopAutomation/$script  ../fitSupport/$script"
	done
    )  >> $pkg.yicf
fi

# use build type test instead of release as release will require that all
# files need to be committed. This causes problem if we are debugging on
# the fly and testing without first checking in the changes.
yinst_create -t test $pkg.yicf

exit 0
