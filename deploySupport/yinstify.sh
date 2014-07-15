rm *.tgz
export debug=echo
#export DATESTRING=`date +%y%m%d%H%M`
die() {
	echo $* 1>&2
	exit 1
}

mkyicf() {
	local pkgname=$1
	local outfile=$2
	local scriptline=$3
	(
	echo PRODUCT_NAME = $pkgname
	echo SRCTOP = .
	echo VERSION = $version.$DATESTRING
	echo SHORT_DESC = Automation-task for $filename.
	echo LONG_DESC = Hadoop configuration for $pkg. Contains config-files but not .jar files.
	echo CUSTODIAN = hadoop-devel@yahoo-inc.com \ http://devel.yahoo.com/yahoo/doc/fileutils/
	echo OWNER = root
	echo PACKAGE_OS_SPECIFIC=no
        echo YINST bug-product Hadoop
        echo YINST bug-component General

	echo GROUP = root
	echo PERM = 0444
        allfields="CHECKSSHCONNECTIVITY CLUSTER BUILD_NUMBER JOB_NAME CONFIGUREJOBTRACKER CONFIGURENAMENODE ENABLE_HA HADOOP_INSTALL_STRING HADOOP_CORETREE_INSTALL_STRING HADOOP_MVN_INSTALL_STRING  HADOOP_CONFIG_INSTALL_STRING  HERRIOT_CONF_ENABLED INSTALLLOCALSAVE INSTALLNEWPACKAGES INSTALLYUMMERGE KILLALLPROCESSES REMOVE_EXISTING_LOGS REMOVEEXISTINGDATA REMOVERPMPACKAGES RUNKINIT RUNSIMPLETEST STARTYARN STARTNAMENODE YINSTSELFUPDATE HDFSUSER  MAPREDUSER PIGVERSION HDFSPROXYVERSION ZOOKEEPERVERSION DISTCPVERSION LOG_COLLECTORVERSION HIVEVERSION STARLINGVERSION HCATVERSION VAIDYAVERSION HITVERSION INSTALL_HIT_TEST_PACKAGES EXCLUDE_HIT_TESTS RUN_HIT_TESTS DATESTRING HCAT_TEST_PKG HIVE_TEST_PKG DISTCP_TEST_PKG LOG_COLLECTOR_TEST_PKG VAIDYA_TEST_PKG PIG_TEST_PKG OOZIE_TEST_PKG GDM_TEST_PKG NOVA_TEST_PKG HDFSPROXY_TEST_PKG HADOOPCORE_TEST_PKG GDMVERSION NOVAVERSION OOZIEVERSION OOZIE_SERVER OOZIE_TEST_SUITE HIT_DEPLOYMENT_TAG CREATEGSHOME LOCAL_CONFIG_INSTALL_STRING SEND_LOG_TO_STDOUT NO_CERTIFICATION QA_PACKAGES INSTALL_GW_IN_YROOT CLEANLOCALCONFIG USE_DEFAULT_QUEUE_CONFIG LOCAL_CONFIG_PKG_NAME GRIDJDK_VERSION HBASEVERSION HBASE_TEST_PKG ADMIN_HOST HBASE_SHORTCIRCUIT CREATE_NEW_CLUSTER_KEYTAB HIVE_SERVER2_VERSION HIVE_VERSION RHEL6NATIVE HCATIGORTAG HIVEIGORTAG OOZIEIGORTAG HADOOPVERSION"
	for i in $allfields
	do
            x="def=false; [ -n \"\${$i}\" ] && def=\"\${$i}\"; echo YINST set $i \$def "
	    eval $x
        done
	echo $scriptline
	echo "YINST set autostartup false"
	# echo "YINST bug-product $(PRODUCT_NAME)"
	# echo "YINST bug-component General"
	echo "YINST set referencedir /homes/hadoopqa"
	) > $outfile


}
version=0.0.0.1.$DATESTRING
while getopts v: name $*
do
    case $name in
    v)    version="$OPTARG";;
    ?)    printf "Usage: %s: [-v version] args\n" $0
          exit 2;;
    esac
done
shift $(($OPTIND - 1))


pkg=hadoopgridrollout

f=installgrid.sh

cmd="conf/hadoop/hadoopAutomation/$f \"\${CLUSTER}\""
mkyicf  "$pkg"  "$pkg.yicf"  "YINST start 200  $cmd"
# echo "YINST start 100 env" >> $pkg.yicf
echo "f 0755 - - conf/hadoop/hadoopAutomation/$f  $f " >> $pkg.yicf

for f in cleangrid.sh namenode*.sh datanode*.sh cluster-list.sh *-rw.sh   dump*.sh [0-9]*-*.sh HIT-hdfsproxy-install.sh *.pl *.py *igor*.sh
do
    echo "f 0755 - - conf/hadoop/hadoopAutomation/$f  $f" >> $pkg.yicf
done

if [ -d ../fitSupport ] 
then
    (cd ../fitSupport
	for f in FIT*.sh  
	do
           echo "f 0755 - - conf/hadoop/hadoopAutomation/$f  ../fitSupport/$f"
	done
    )  >> $pkg.yicf
fi
yinst_create -t release $pkg.yicf

exit 0
