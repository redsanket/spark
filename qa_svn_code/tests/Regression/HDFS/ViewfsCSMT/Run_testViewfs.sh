
#############################################################
## Standard library include
#############################################################
source $WORKSPACE/lib/library.sh  
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

##############################################
# to get the definition of NN0, NN2, NN3, NN4, 
##############################################
# source viewfsNNconfig.sh -- no longer valid, hardcodes namenodes 

PROG_NAME=$0
PARAM=$1
OWNER="cwchung"

###################################################################
####### env set up
###################################################################
if [ -z "$CLUSTER" ]; then
	export CLUSTER=omegam
fi

# tentatively set PATH so that 
export HADOOP_CONF_DIR=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop

PWD=`pwd`



# hadoop version output: Hadoop 0.20.202.0.1012030459
# HADOOP_GOT_VERSION=`hadoop version | head -1 | awk -F. '{print $2}'`
cat < $HADOOP_CONF_DIR/deploy.${CLUSTER}.confoptions.sh |  fgrep XXX20X
if [ $? == 0 ] ; then
	HADOOP_GOT_VERSION="20"
else
	HADOOP_GOT_VERSION="22"
fi

echo "    $PROG_NAME: Using Hadoop version - $HADOOP_GOT_VERSION"
echo "    $PROG_NAME: Using cluster - $CLUSTER"


# HADOOP_DFS_CMD is to run hadoop/hdfs fs -ls / ; while HADOOP_CMD is to run hadoop 


if [ $HADOOP_GOT_VERSION == '20' ] ; then
	export HADOOP_VERSION=20
	export HADOOP_DFS_CMD=hadoop
	export HADOOP_CMD=hadoop
	#export VIEWFS_CONF_DIR=v20_viewfs_hadoopconf_${CLUSTER}
	#export VIEWFS_CONF_DIR=v20_viewfs_both_omega_ml
	##export HDFS_CONF_DIR=v20_hdfs_hadoopconf_${CLUSTER}
	#export NN2=gsbl90470.blue.ygrid

	export VIEWFS_CONF_DIR=V20_viewfs_config
	export HDFS_CONF_DIR=V20_hdfs_config
	Q_OPTION="-Dmapred.job.queue.name=grideng"
	export HADOOP_USER_CLASSPATH_FIRST=1
	export HADOOP_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/share/hadoop-current/
	export PATH=${HADOOP_HOME}/bin:$PATH
	if [ -z "$PARAM" ]  || [ "$PARAM" != '201' ] ; then
		# export HADOOP_CLASSPATH=lib/hadoop-viewfs-0.20.202.jar
		export HADOOP_CLASSPATH=/home/y/lib/hadoop-viewfs/hadoop-viewfs-0.20.203.jar
	else
		export HADOOP_CLASSPATH=/home/y/lib/hadoop-viewfs/hadoop-viewfs-0.20.203.jar	# was .201
	fi
else
	export HADOOP_VERSION=22
	export HADOOP_DFS_CMD=hdfs
	export HADOOP_CMD=hadoop
	# export VIEWFS_CONF_DIR=v22_viewfs_hadoopconf_${CLUSTER}
	# export HDFS_CONF_DIR=v22_hdfs_hadoopconf_${CLUSTER}
	#export NN2=gsbl90772.blue.ygrid
	export VIEWFS_CONF_DIR=V22_viewfs_config
	export HDFS_CONF_DIR=V22_hdfs_config
	Q_OPTION="-Dmapred.job.queuename=grideng"
	unset HADOOP_CLASSPATH
	export HADOOP_COMMON_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/share/hadoopcommon
	export HADOOP_HDFS_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/share/hadoophdfs
	export HADOOP_MAPRED_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/share/hadoopmapred
	export PATH=${HADOOP_COMMON_HOME}/bin:${HADOOP_HDFS_HOME}/bin:${HADOOP_MAPRED_HOME}/bin:$PATH

fi
 
###################################################################
####### MD5 for wordcount input and output and global setting
###################################################################
MD5SUM_WORDCOUNT_INPUT="77f90eeb2a5f32a78225a6b66ed98feb"
MD5SUM_WORDCOUNT_OUTPUT="17dbc9e2a5b789d38db584385d82c200"
MD5SUM_WORDCOUNT_FILE_OUTPUT="32cbe843a231ba1a0b165299d58ee254"

###################################################################
####### key variables  
###################################################################

JT_OPTION="-Dmapreduce.job.hdfs-servers=hdfs://$NN0,hdfs://$NN2,hdfs://$NN3"

#export NN2=`hdft_getNN default`
# export NN2=gsbl90772	#omegal
# export NN3=gsbl90772	#omegal
# export NN0=gsbl90772	#default NN is omegam


if [ -z "$NN0" ] || [ -z "$NN2" ] || [ -z "$NN3" ] ; then
	echo "    $PROG_NAME: ERROR cannot find namenode NN0 ($NN0), NN2($NN2) or NN3($NN3). Aborted"
	echo "    $PROG_NAME: ERROR cannot find namenode NN0 ($NN0), NN2($NN2) or NN3($NN3). Aborted"
	return
	exit
fi

TMPFILE=`date '+Zcsmt_%m%d-%H%M%S'`
TEST_DATA_FILE="/homes/hdfsqa/hdfsRegressionData/helloworld.txt"
TEST_DATA_DISTCACHE_DIR="/homes/hdfsqa/hdfsRegressionData/distcache_input"


NFAIL=0
Count=0
TestId="Unk"
TestType="Unknown"

which hadoop
hadoop version
env |grep CLUSTER
env |grep HADOOP_HOME

echo "    $PROG_NAME:: HADOOP_GOT_VERSION=$HADOOP_GOT_VERSION"
echo "    $PROG_NAME:: HADOOP_CLASSPATH=$HADOOP_CLASSPATH"
echo "    $PROG_NAME:: HADOOP_CLASSPATH=$HADOOP_CLASSPATH"
echo "    $PROG_NAME:: namenode found NN2=$NN2"

function print_notes {
cat << EOF

	"Note:: testViewfs.sh - Test the use of viewfs in URI and in config file"
	"Note:: test various combintions of URI, config"
	"Note:: --config: ds.default to be hdfs://gwbl90772.blue.ygrid  vs viewfs:/// "
	"Note:: --config set ds.default to be non-final, so that it can over-ridden by -conf"
	"Note:: dfs -conf: my-viewfs-scheme-only.xml vs my-hdfs-scheme-only.xml"
	"Note:: URI: /nn2/file1, //nn2, ///nn2 - these are viewfs:" 
	"Note:: URI: hdfs:/nn2 and hdfs:/nn4 does not exist. They are mount points "
	"Note:: /nn2 is mounted to hdfs:/tmp/nn2.mt , and /nn4 is mounted to hdfs:/tmp/nn4.mt"
	"Note:: hdfs config, then viewfs config for /nn2/file1; then hdfs, viewfs config for tmp"

	# Comment on the test groups
	
	'Note:: Test Group 01-20: using hdfs as default, try to access /tmp/nn2/file1. /, ///, //gsbl90772, hdfs:/, hdfs:///, hdfs://gsbl90772 should work. All other fail'
	'Note:: Test Group 21-30: using viewfs as default, try to access /tmp/nn2/file1. All should fail, except #16 hdfs://gsbl90772/tmp/nn2/file1'
	'Note:: Test Group 41-50: using hdfs as default, try to access /nn2/file1. All should fail, except #27, #28 using viewfs:'
	'Note:: Test Group 61-70: using viewfs as default, try to access /nn2/file1. /nn2, ///nn2, viewfs:/nn2, viewfs:///nn2 should work. Other should fail.'
	'Note:: Test Group 81-90:  using viewfs as default, but use hdfs override. Should have the same behavior as in case 01-10'
	'Note:: Test Group 101-110:  basic default URI that works with hdfs_configs and viewfs_config, and the basic set up'
	'Note:: viewfs://default/nn2 is mounted to hdfs:/tmp/nn2.mt'
	'Note:: viewfs://qatest/nn4 is mounted to hdfs:/tmp/nn4.mt'

EOF
}


function test_01_CSMT3 {
	echo "    test_CSMT3: Exercise All test cases"
	echo "    test_CSMT3: Exercise All test cases"

	hdftDisplayTestGroupMsg	"Test Group 001 in using viewfs: and hdfs: URI with HDFS config files, accessing the underlying hdfs /tmp/NN.mt files"
    run_one_csmt    "POS_CSMT_01"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR  dfs -ls /tmp/nn2.mt/file1 " 		"@: HDFS case, default authority, should work"
    run_one_csmt  	"POS_CSMT_02"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ///tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"NEG_CSMT_03"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //$NN2/tmp/nn2.mt/file1" 		"@: null scheme with authority is not supported
    run_one_csmt  	"POS_CSMT_04"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:/tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"POS_CSMT_05"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1 " 		"@: HDFS case, should work"
    run_one_csmt  	"POS_CSMT_06"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN2/tmp/nn2.mt/file1 " 		"@: HDFS case, should work"
    run_one_csmt  	"NEG_CSMT_07"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:/tmp/nn2.mt/file1"  		"@: /tmp is not a mount point" 
    run_one_csmt  	"NEG_CSMT_08"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:///tmp/nn2.mt/file1"  		"@: /tmp is not a mount point"  
    run_one_csmt  	"NEG_CSMT_09"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://$NN2/tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"NEG_CSMT_10"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //tmp/nn2.mt/file1"  			"@: //tmp is not a valid authorith"  
    run_one_csmt  	"NEG_CSMT_11"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt/file1" 		"@: //@: BUG 5333373: //default/ is not in the real path of hdfs"
    run_one_csmt  	"NEG_CSMT_12"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt/file1" 	"@: //default is not in the real path of hdfs" 
    run_one_csmt  	"NEG_CSMT_13"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://default/tmp/nn2.mt/dir1" 	"@: using viewfs will not work with hdfs only config" 
    run_one_csmt  	"POS_CSMT_14"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /tmp/nn4.mt/file1" 				"@: HDFS case, defalt authority, should work"
    run_one_csmt  	"NEG_CSMT_15"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //qatest/tmp/nn4.mt/file1" 		"@: BUG 5333373: //qatest should not work."
    run_one_csmt  	"NEG_CSMT_16"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://qatest/tmp/nn4.mt/dir1" 	"@: should be viewfs://qatest, not hdfs://qatest"
    run_one_csmt  	"NEG_CSMT_17"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://qatest/tmp/nn4.mt/dir1" 	"@: using viewfs will not work with hdfs only config"
    run_one_csmt  	"NEG_CSMT_18"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 					"@: should be viewfs:/nn4"
    run_one_csmt  	"POS_CSMT_19"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/file1"  "@: use hftp:// to access a file"
    run_one_csmt  	"POS_CSMT_20"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/dir1"  "@: use hftp:// access a dir"
    run_one_csmt  	"NEG_CSMT_21"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1" 	"@: using viewfs will not work with hdfs only config"
}

function test_21_CSMT3 {
	hdftDisplayTestGroupMsg	"Test Group 021 in using viewfs and hdfs: URI with Viewfs config files, accessing the underlying hdfs /tmp/NN.mt files"
    run_one_csmt  	"NEG_CSMT_21"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR  dfs -ls /tmp/nn2.mt/file1 "			"@: viewfs cannot access to any /tmp as it is not a mount point" 
    run_one_csmt  	"NEG_CSMT_22"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls ///tmp/nn2.mt/file1"			"@: /tmp not a valid mount point"  
    run_one_csmt  	"NEG_CSMT_23"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //$NN2/tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_24"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:/tmp/nn2.mt/file1"		"@: /tmp not a valid mount point"  
    run_one_csmt  	"NEG_CSMT_25"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1 " 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"POS_CSMT_26"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://$NN2/tmp/nn2.mt/file1 " 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_27"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/tmp/nn2.mt/file1" 			"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_28"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_29"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://$NN2/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_30"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //tmp/nn2.mt/file1" 				"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_31"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_32"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_33"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls vewfs://default/tmp/nn2.mt/dir1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_34"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /tmp/nn4.mt/file1" 				"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_35"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //qatest/tmp/nn4.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_36"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://qatest/tmp/nn4.mt/dir1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_37"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://qatest/tmp/nn4.mt/dir1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_38"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 						"@: /nn4 would work, not /nn4.mt" 
    run_one_csmt  	"POS_CSMT_39"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/file1"  "@: use hftp:// to access a file"
    run_one_csmt  	"POS_CSMT_40"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/dir1"  "@: use hftp:// access a dir"
}
 
function test_41_CSMT3 {
	hdftDisplayTestGroupMsg	"Test Group 041 in using viewfs and hdfs: URI with HDFS  config files, accessing the mounted files. All negative tests"
    run_one_csmt  	"NEG_CSMT_41"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn2/file1 " 				"@: /nn2 or /nn4 are mount points, not dir. "	
    run_one_csmt  	"NEG_CSMT_42"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ///nn2/file1" 				"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_43"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //$NN2/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_44"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_45"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///nn2/file1 "			"@: /nn2 or /nn4 are mount points, not dir. " 
    run_one_csmt  	"NEG_CSMT_46"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN2/nn2/file1 " 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_47"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_48"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:///nn2/file1"			"@: /nn2 or /nn4 are mount points, not dir. " 
    run_one_csmt  	"NEG_CSMT_49"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://$NN2/nn2/file1" 	"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_50"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //nn2/file1" 				"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_51"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/nn2/file1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_52"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/nn2/file1" 	"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_53"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1" 	"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_54"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4/dir1" 					"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_55"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //qatest/nn4/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_56"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://qatest/nn4/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_57"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://qatest/nn4/dir1" 	"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_58"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 				"@: /nn2 or /nn4 are mount points, not dir. "
}
  
function test_61_CSMT3 {
	hdftDisplayTestGroupMsg	"Test Group 061 in using viewfs and hdfs: URI with Viewfs  config files, accessing the mounted files. "
    run_one_csmt  	"POS_CSMT_61"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn2/file1 "				"@: viewfs mount point" 
    run_one_csmt  	"POS_CSMT_62"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls ///nn2/file1" 				"@: /// same as /  " 
    run_one_csmt  	"POS_CSMT_63"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //$NN2/nn2/file1" 			"@: "
    run_one_csmt  	"NEG_CSMT_64"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:/nn2/file1" 			"@: hdfs:/tmp would work "
    run_one_csmt  	"NEG_CSMT_65"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:///nn2/file1 "		"@: hdfs:///tmp would work "
    run_one_csmt  	"NEG_CSMT_66"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://$NN2/nn2/file1" 	"@: hdfs://$NN2/tmp/ would work  " 
    run_one_csmt  	"POS_CSMT_67"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/nn2/file1" 		"@: /nn2 is a mounted dir"
    run_one_csmt  	"POS_CSMT_68"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///nn2/file1" 		"@: /nn2 is a mounted dir"
    run_one_csmt  	"NEG_CSMT_69"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://$NN2/nn2/file1" 	"@: /nn2 not a valid hdfs dir" 
    run_one_csmt  	"NEG_CSMT_70"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //nn2/file1" 				"@: //nn2 not a valid authority" 
    run_one_csmt  	"POS_CSMT_71"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/nn2/file1" 		"@: //default/nn2 = /nn2 " 
    run_one_csmt  	"NEG_CSMT_72"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://default/nn2/file1" 	"@: not a valid hdfs default " 
    run_one_csmt  	"POS_CSMT_73"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1"	"@: "
    run_one_csmt  	"NEG_CSMT_74"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4/dir1" 				"@: should work? XXX " 
    run_one_csmt  	"NEG_CSMT_75"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //qatest/nn4/file1" 		"@: //qatest need to be prefixed by viewfs:" 
    run_one_csmt  	"NEG_CSMT_76"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://qatest/nn4/dir1" 	"@: should be viewfs://qatest, not hdfs " 
    run_one_csmt  	"POS_CSMT_77"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://qatest/nn4/dir1" 	"@: "
    run_one_csmt  	"NEG_CSMT_78"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 				"@: /nn4.mt not a valid hdfs dir" 
} 

function test_81_CSMT3 {
	hdftDisplayTestGroupMsg	"Test Group 081 in using viewfs: and hdfs: URI with Viewfs overridden by HDFS config files, accessing the underlying hdfs /tmp/NN.mt files. Similar to CSMT_01 group"
    run_one_csmt  	"POS_CSMT_81"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /tmp/nn2.mt/file1 "	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_82"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls ///tmp/nn2.mt/file1" 		"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_83"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //$NN2/tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_84"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs:/tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_85"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs:///tmp/nn2.mt/file1 "	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_86"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://$NN2/tmp/nn2.mt/file1 "	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_87"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs:/tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_88"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs:///tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_89"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://$NN2/tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_90"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_91"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //default/tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_92"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://default/tmp/nn2.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_93"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://default/tmp/nn2.mt/dir1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_94"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /tmp/nn4.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"POS_CSMT_95"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //qatest/tmp/nn4.mt/file1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_96"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://qatest/tmp/nn4.mt/dir1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_97"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://qatest/tmp/nn4.mt/dir1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
    run_one_csmt  	"NEG_CSMT_98"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /nn4.mt/dir1" 	"@: HDFS Config takes effect. Same result as CSMT_01 to _18"
}
 
function test_101_CSMT3 {
	# basic sanity/validation check. Run this group if you want to have a quick confirmation of the viewfs config setting
	hdftDisplayTestGroupMsg	"Test Group 101 in using viewfs: and hdfs: URI with both Viewfs and HDFS config files. Basic quick test"
    run_one_csmt  	"POS_CSMT_101"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -ls /" 		"@: shows /tmp" 
    run_one_csmt  	"POS_CSMT_102"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /" 		"@: shows the mount points " 
    run_one_csmt  	"POS_CSMT_103"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn2.mt/" 	"@: viewfs:/nn2 is mounted to this directory" 
    run_one_csmt  	"POS_CSMT_104"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn4.mt/" 	"@: viewfs:/nn4 is mounted to this directory" 
    run_one_csmt  	"POS_CSMT_105"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn2" 		"@: should show the same content as POS_103" 
    run_one_csmt  	"POS_CSMT_106"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn2" 		"@: should show the same content as POS_103" 
    run_one_csmt  	"NEG_CSMT_107"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn4" 		"@: should fail as /nn4 is //qatest"
    run_one_csmt  	"POS_CSMT_108"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://default/nn2" 	"@: viewfs supports default as an authority" 
    run_one_csmt  	"POS_CSMT_109"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://qatest/nn4" 	"@: qatest is one of the configured  authority" 
    run_one_csmt  	"NEG_CSMT_110"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn4" 		"@: qatest is not default authority " 
    run_one_csmt  	"NEG_CSMT_111"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr viewfs:/nn2" 	"@: HDFS does not support viewfs:/"
    run_one_csmt  	"NEG_CSMT_112"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr viewfs:/nn4" 	"@: HDFS does not support viewfs:/"
    run_one_csmt  	"POS_CSMT_113"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs:/nn2" 	"@: viewfs:/nn2 is same as /nn2"
    run_one_csmt  	"NEG_CSMT_114"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs:/nn4" 	"@: viewfs:/nn4 is same as /nn4, which is mounted to qatest"
    run_one_csmt  	"POS_CSMT_115"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -ls hdfsRegressionData" 	"@: shows /user/hadoopqa/hdfsRegressionData, as viewfs:/user has to be  mounted properly "
    run_one_csmt  	"POS_CSMT_116"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfsRegressionData" 	"@: shows /user/hadoopqa/hdfsRegressionData, as viewfs:/user has to be mounted properly"
    run_one_csmt  	"POS_CSMT_117"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/file1"  "@: use hftp:// to access a file"
    run_one_csmt  	"POS_CSMT_118"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/dir1"  "@: use hftp:// access a dir"
    run_one_csmt  	"POS_CSMT_119"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/file1"  "@: use hftp:// to access a file"
    run_one_csmt  	"POS_CSMT_120"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/dir1"  "@: use hftp:// access a dir"
}

# exercise all file access groups
function test_01_all_CSMT3 {
	test_01_CSMT3 
#	test_21_CSMT3 
#	test_41_CSMT3 
#	test_61_CSMT3 
#	test_81_CSMT3 
#	test_101_CSMT3 
}


## Runnning various app using CSMT
function test_App_CSMT2 {

	"NEG_CSMT_501"   "$HADOOP_CMD  --config $VIEWFS_CONF_DIR/ jar"
	"Note:: viewfs://default/nn2 is mounted to hdfs:/tmp/nn2.mt"
	"Note:: viewfs://qatest/nn4 is mounted to hdfs:/tmp/nn4.mt"
}

  
#                         $1              $2                                                                                                    $3
#e.g.    run_one_csmt  	"POS_CSMT_120"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hftp://${NN2}.blue.ygrid:50070/tmp/nn2.mt/dir1"  "@: use hftp:// access a dir"
function run_one_csmt {
    TestId=$1
    TEST_CMD=$2
    TEST_NOTES=$3
    TESTCASE_DESC="$2 ## $3"
	TESTCASENAME="VFS"
    echo "###########################################"
    echo "run_one_csmt: TestId=$TestId"
    echo "run_one_csmt: TEST_CMD=$TEST_CMD"
    echo "run_one_csmt: TEST_NOTES=$TEST_NOTES"
	echo "run_one_csmt: ENV :  KRB5CCNAME=$KRB5CCNAME"
	echo "run_one_csmt: ENV :  KB Principal=`klist | fgrep 'Default principal'`"

	displayTestCaseMessage "$TestId ==== $TESTCASE_DESC"

    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi
 
	(( Count = Count + 1 ))
	echo "## [$TestId] TEST_CMD is:  $TEST_CMD "
	echo "   [Exec_notes]: $TestType Testr- $TEST_NOTES"

	# Run the command
	$TEST_CMD
	statExec=$?
	hdftMapStatus $statExec $TestType
	statExec=$?
	hdftShowExecResult $statExec $TestId "$TESTCASE_DESC"

	returnStat=$statExec
	##ZZ##hdftShowBriefTaskResult $statExec $TestId "$TEST_CMD" "$TEST_NOTES"
	hdftReturnTaskResult $statExec $TestId "$TEST_CMD" "$TEST_NOTES"
	#function junk 
	echo ""
	return $returnStat;

}



# run one test to create a file on a directory - this is to exercise the write side of the CMST
# Todo:
# make sure the directory exist
# make sure  a random file is not there  
# if exists, make sure can erase it (else fail)
# then create a file
# then remove it
# each step should work
function run_csmt_write {
    TestId=$1
    TEST_DIR=$2
    TEST_NOTES=$3
    TESTCASE_DESC="$2 -- $3"
	TESTCASENAME="VFS"
	NFAIL=0

    echo "###########################################"
    echo "TestId=$TestId"
    echo "TEST_DIR=$TEST_DIR"
    echo "TEST_NOTES=$TEST_NOTES"
	displayTestCaseMessage "$TestId ==== $TESTCASE_DESC"

    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi
 
	TEST_STAT=0
	echo "## [$TestId] TEST_CMD is:  $TEST_CMD "
	echo "   [Exec_notes]: $TestType Test- $TEST_NOTES"

	# make sure the directory exists
	$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -stat $TEST_DIR  2>&1 	> /dev/null
	statExec=$?
	hdftShowExecResult $statExec "$TestId" "existence of directory $TEST_DIR"
	if [ $statExec != 0 ] ; then
		(( NFAIL = $NAFIL +1 ))
		hdftShowExecResult $statExec $TestId "$TESTCASE_DESC - Directory $TEST_DIR does not exist. Aborted."
	fi

	# make sure the tmp file does not exist
	$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -stat $TEST_DIR/$TMPFILE  2>&1 > /dev/null
	if [ $? == 0 ] ; then
		echo "File exists: $TEST_DIR/$TMPFILE. Will remove"
		$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -rmr -skipTrash  $TEST_DIR/$TMPFILE
		$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -stat $TEST_DIR/$TMPFILE		2>&1 > /dev/null
		if [ $? == 0 ] ; then
			(( NFAIL = $NAFIL +1 ))
			hdftShowExecResult $? "$TestId" "ERROR: Cannot remove tmp file $TESTDIR/$TMPFILE"
		fi
	else
		echo "File does not exist: $TEST_DIR/$TMPFILE. OK to proceed"
	fi
		
	TCMD="$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -copyFromLocal $TEST_DATA_FILE $TEST_DIR/$TMPFILE"
	$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -copyFromLocal $TEST_DATA_FILE $TEST_DIR/$TMPFILE
	if [ $? != 0 ] ; then
		(( NFAIL = $NAFIL +1 ))
		hdftShowExecResult $? "$TestId" "ERROR: Cannot copy  tmp file to $TESTDIR/$TMPFILE. "
	else
		echo "Copy file to  $TESTDIR/$TMPFILE OK"
	fi

	$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -cat $TEST_DATA_FILE $TEST_DIR/$TMPFILE  | md5sum
	if [ $? != 0 ] ; then
		(( NFAIL = $NAFIL +1 ))
		hdftShowExecResult $? "$TestId" "ERROR: Cannot cat  file $TESTDIR/$TMPFILE. "
	else
		echo "Cat  file $TESTDIR/$TMPFILE OK"
	fi
	
	hdftReturnTaskResult $NFAIL $TestId "$TCMD" "$TEST_NOTES"
	#function junk 
	echo ""
	return $NFAIL

}


# this should be run after the write test passed.
# Todo:
# make sure the directory exist
# then create a file if not already exists (from the write test)
# then remove it
# each step should work
function run_csmt_trash {
    TestId=$1
    TEST_DIR=$2
    TEST_NOTES=$3
    TESTCASE_DESC="$2 -- $3"
	TESTCASENAME="VFS"
	NFAIL=0

    echo "###########################################"
    echo "TestId=$TestId"
    echo "TEST_DIR=$TEST_DIR"
    echo "TEST_NOTES=$TEST_NOTES"
	displayTestCaseMessage "$TestId ==== $TESTCASE_DESC"

    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi
 
	TEST_STAT=0
	echo "## [$TestId] TEST_CMD is:  $TEST_CMD "
	echo "   [Exec_notes]: $TestType Test- $TEST_NOTES"

	# make sure the directory exists
	local cmd="$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -test -d  $TEST_DIR "
	echo "    LCMD=$cmd [1]"
	eval $cmd
	statExec=$?
	hdftShowExecResult $statExec "$TestId" "existence of directory $TEST_DIR"
	if [ $statExec != 0 ] ; then
		hdftShowExecResult $statExec $TestId "$TESTCASE_DESC - Directory $TEST_DIR does not exist. Aborted."
		(( NFAIL = $NAFIL +1 ))
	fi

	# check if the tmpfile exists
	cmd="$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -test -e  $TEST_DIR/$TMPFILE "
	echo "    LCMD=$cmd [2]"
	eval $cmd
	
	# return 0 if exist. create the file if not exist
	if [ $? != 0 ] ; then
		cmd="$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -copyFromLocal $TEST_DATA_FILE $TEST_DIR/$TMPFILE"
		echo "    LCMD=$cmd [3]"
		eval $cmd
		if [ $? != 0 ] ; then
			hdftShowExecResult $? "$TestId" "ERROR: Cannot copy file from TEST_DATA_FILE $TESTDIR/$TMPFILE"
			(( NFAIL = $NAFIL +1 ))
		fi
	fi

	# now try the remove with -skipTrash option: status should return 0. This test only check status.
	TCMD="$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -rm $TEST_DIR/$TMPFILE"
	echo "    TCMD=$TCMD [4]"
	eval $TCMD
	
	statExec=$?
	if [ $statExec != 0 ] ; then
		hdftShowExecResult $statExec "$TestId" "ERROR: Cannot remove tmp file $TESTDIR/$TMPFILE"
		(( NFAIL = $NAFIL +1 ))
	else
		hdftShowExecResult $statExec "$TestId" "OK in remove tmp file $TESTDIR/$TMPFILE"
	fi
		
	
	hdftReturnTaskResult $NFAIL $TestId "$TCMD" "$TEST_NOTES"
	return $NFAIL

}


function junk {
	stat=$?
	returnStat=0
	if [ $stat == 0 ] && [ $TestType == POS ] ; then
		echo "[Pass] [exec - POS test] [$TestId] Cmd is:  $TEST_CMD  "
	fi 
	if [ $stat != 0 ] && [ $TestType == POS ] ; then
		echo "[FAIL] [exec - POS test] [$TestId] Cmd is:  $TEST_CMD  "
		returnStat=1
	fi 
	if [ $stat != 0 ] && [ $TestType == NEG ] ; then
		echo "[Pass] [exec - NEG test] [$TestId] Cmd is:  $TEST_CMD  "
	fi
	if [ $stat == 0 ] && [ $TestType == NEG ] ; then
		echo "[FAIL] [exec - NEG test] [$TestId] Cmd is:  $TEST_CMD  "
		returnStat=1
	fi
}

function test_100_basic_cases {
	echo "run_one_csmt: ENV :  KRB5CCNAME=$KRB5CCNAME"
	echo "run_one_csmt: ENV :  KB Principal=`klist | fgrep 'Default principal'`"
	echo "    Exercise the case where mounted directorie/files can be accessed ...."
	echo "    Exercise the case where mounted directorie/files can be accessed ...."
	hdftDisplayTestGroupMsg	"Basic tests 100 in viewfs file access operations"

	run_one_csmt "DUP_CSMT_100"   "$HADOOP_DFS_CMD                             dfs -ls /" 		"@: shows DFS /" 
	run_one_csmt "DUP_CSMT_101"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -ls /" 		"@: shows DFS / " 
	run_one_csmt "DUP_CSMT_102"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /" 		"@: shows the mount points " 
    run_one_csmt "DUP_CSMT_103"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn2.mt/" 	"@: viewfs:/nn2 is mounted to this directory" 
	run_one_csmt "DUP_CSMT_104"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn4.mt/" 	"@: viewfs:/nn4 is mounted to this directory" 
	run_one_csmt "DUP_CSMT_105"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn2" 		"@: should show the same content as POS_103" 
	run_one_csmt "DUP_CSMT_106"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn2" 		"@: should show the same content as POS_103" 
	run_one_csmt "DUP_CSMT_108"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://default/nn2" 	"@: viewfs supports default as an authority" 
	run_one_csmt "DUP_CSMT_109"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://qatest/nn4" 	"@: qatest is one of the configured  authority" 
	run_one_csmt "DUP_CSMT_110"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfsRegressionData" 	"@: Default home directory"
	run_one_csmt "DUP_CSMT_111"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ./hdfsRegressionData" 	"@: Default home directory"
	run_one_csmt "DUP_CSMT_112"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN2/user/hadoopqa/hdfsRegressionData" 	"@: Default home directory"
}

function test_200_bug_4129512 {
	echo "    Verify Bug:4129512 by running key tests here. "

	hdftDisplayTestGroupMsg	"Test Group in verifying or confirming BUG:4129512"
    run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1"	"@: pos - default hdfs authority"
    run_one_csmt     "NEG_CSMT_201"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt"	"@: default is not a real path of hdfs"    
	run_one_csmt     "NEG_CSMT_202"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt"	    "@: BUG 5333373: //default/ is not a real path of hdfs" 
    run_one_csmt     "POS_CSMT_203"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn2"		"@: pos - good mounted dir "
    run_one_csmt     "POS_CSMT_204"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /tmp"		"@: pos - good mounted dir"
    run_one_csmt     "POS_CSMT_205"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /"		"@: pos - show all  mounted dir"
    run_one_csmt     "POS_CSMT_206"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///"	"@: pos - good mounted dir"
    run_one_csmt     "POS_CSMT_207"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/" "@: viewfs://default/ is same as /"
    run_one_csmt     "POS_CSMT_208"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/nn2" "@: default - show nn2 "		
    run_one_csmt	 "NEG_CSMT_220"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/"	"@: BUG 5333373: the queried path is viewfs://default/default"
																									
}

function print_csmt_dir {
	if [ -d $VIEWFS_CONF_DIR ] ; then
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls    /user/
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls    ///user/
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls    viewfs:///user/
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls    viewfs:///user/hadoopqa
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls    viewfs:///tmp
	else 
		echo "ERROR: $PROG_NAME VIEWFS_CONF_DIR does not exist: $VIEWFS_CONF_DIR"
		echo "ERROR: $PROG_NAME VIEWFS_CONF_DIR does not exist: $VIEWFS_CONF_DIR"
		echo "ERROR: $PROG_NAME VIEWFS_CONF_DIR does not exist: $VIEWFS_CONF_DIR"
	fi
}


function run_wordcount_csmt {
    local TestId=$1
    local TEST_INPUT_DIR=$2
    local TEST_OUTPUT_DIR=$3
    local TEST_OPTIONS="$4 $Q_OPTION $JT_OPTION"
    local TEST_NOTES=$5
    TESTCASE_DESC="INPUT=$2; OUTPUT=$3; $4; $5"
	TESTCASENAME="VFS-WC"
    echo "###########################################"
    echo "    $PROG_NAME run_wordcount_csmt:: TestId=$TestId"
    echo "    $PROG_NAME run_wordcount_csmt:: TEST_INPUT_DIR=$TEST_INPUT_DIR"
    echo "    $PROG_NAME run_wordcount_csmt:: TEST_OUTPUT_DIR=$TEST_OUTPUT_DIR"
    echo "    $PROG_NAME run_wordcount_csmt:: TEST_OPTIONS=$TEST_OPTIONS"
    echo "    $PROG_NAME run_wordcount_csmt:: TEST_NOTES=$TEST_NOTES"
	echo "    $PROG_NAME run_wordcount csmt:: ENV :  KRB5CCNAME=$KRB5CCNAME"
	echo "    $PROG_NAME run_wordcount csmt:: ENV :  KB Principal=`klist | fgrep 'Default principal'`"

	displayTestCaseMessage "$TestId ==== $TESTCASE_DESC"

    if [[ $TestId =~ 'NEG' ]] ; then 
	TestType=NEG
    else
	TestType=POS
    fi
 
	
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -stat $TEST_OUTPUT_DIR	2>&1  > /dev/null
    if [ $? == 0 ] ; then
		echo "    $PROG_NAME: output directory exists. Remove it first: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -rmr -skipTrash $TEST_OUTPUT_DIR"
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -rmr -skipTrash $TEST_OUTPUT_DIR
 	else
		echo "    $PROG_NAME: output directory does not exist. OK to proceed with running wordcount app"
	fi

	
	TCMD="hadoop --config $VIEWFS_CONF_DIR/  jar $HADOOP_EXAMPLES_JAR  wordcount $TEST_OPTIONS $TEST_INPUT_DIR $TEST_OUTPUT_DIR"

	(( Count = Count + 1 ))
	echo "## [$TestId] TCMD is:  $TCMD "
	echo "   [Exec_notes]: $TestType Testr- $TEST_NOTES"

	# Run thec command
	echo "    Doit:: $TCMD"
	$TCMD
	statExec=$?
	hdftMapStatus $statExec $TestType
	statExec=$?
	hdftShowExecResult $statExec $TestId "$TESTCASE_DESC"
	
	Md5Output=`$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -cat $TEST_OUTPUT_DIR/part-r-00000  | md5sum | awk '{print $1}' `

        # need to deal with case where input is a file called "helloworld.txt" (instead of dir), just for case 305
        if [[ $TestId == "POS_CSMT_305" ]]; then
           echo "Input is a file, expecting helloworld.txt, set MD5 sum to match against..."
           hdftDiffMd5 "$TestId" "$MD5SUM_WORDCOUNT_FILE_OUTPUT" "$Md5Output" "$TEST_OUTPUT_DIR/part-r-00000"
        else
           hdftDiffMd5 "$TestId" "$MD5SUM_WORDCOUNT_OUTPUT" "$Md5Output" "$TEST_OUTPUT_DIR/part-r-00000"
        fi
        statData=$?

	(( retStat = $statExec + $statData ))
	#hdftShowTaskResult $statExec $TestId "$TESTCASE_DESC"
	#hdftShowBriefTaskResult $retStat $TestId "$TCMD" "$TEST_NOTES"
	hdftReturnTaskResult $retStat $TestId "$TCMD" "$TEST_NOTES"
	
	echo ""
	return $returnStat;

}

# stream commmand exercises distributed cache
# the validation requires additional steps:
# for -files and -archives: shows the file existence by ls -l 
# for -libjars: use printenv to show the jar is pointed by ENV correctly
	#   $0              $1             $2              $3                      $4   
    # run_stream_csmt  "POS_CSMT_421" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P421_stream_output"           
	# $5                                                         $6
	# "-libjars viewfs:/nn2/jar1.jar"   					"CSMT distributed cache test using streaming -libjars viewfs:/  rw jar"
function run_stream_csmt {
    local TestId=$1
    local TEST_CONFIG=$2
    local TEST_INPUT_DIR=$3
    local TEST_OUTPUT_DIR=$4
    local TEST_OPTIONS="$5  $Q_OPTION $JT_OPTION"
    local TEST_NOTES=$6
    local TEST_PARAM="$TEST_OPTIONS -input $TEST_INPUT_DIR -output $TEST_OUTPUT_DIR"
    TESTCASE_DESC="$TEST_PARAM ## $TEST_NOTES"
	TESTCASENAME="VFS_STREAM"
    echo "###########################################"
    echo "    $PROG_NAME run_stream_csmt:: TestId=$TestId"
    echo "    $PROG_NAME run_stream_csmt:: TEST_INPUT_DIR=$TEST_INPUT_DIR"
    echo "    $PROG_NAME run_stream_csmt:: TEST_OUTPUT_DIR=$TEST_OUTPUT_DIR"
    echo "    $PROG_NAME run_stream_csmt:: TEST_OPTIONS=$TEST_OPTIONS"
    echo "    $PROG_NAME run_stream_csmt:: TEST_NOTES=$TEST_NOTES"
    echo "    $PROG_NAME run_stream_csmt:: TEST_PARAM=$TEST_PARAM"
	echo "    $PROG_NAME run_wordcount csmt:: ENV :  KRB5CCNAME=$KRB5CCNAME"
	echo "    $PROG_NAME run_wordcount csmt:: ENV :  KB Principal=`klist | fgrep 'Default principal'`"

	displayTestCaseMessage "$TestId ==== $TESTCASE_DESC"

    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi

	if  [ -n "$TEST_CONFIG" ] && [ "$TEST_CONFIG" == "Hdfs" ] ; then
		TEST_CONFIG_DIR=$HDFS_CONF_DIR
	else
		TEST_CONFIG_DIR=$VIEWFS_CONF_DIR
	fi
 
	local FileUri=""
	local FileName=""
	local MAPPER_CMD="ls -l"
	if [ -n "$TEST_OPTIONS" ] ; then
		case $TEST_OPTIONS in 
		-files*)
			FileUri=`echo $TEST_OPTIONS | awk '{print $2}'`
			FileName=`basename $FileUri`
			MAPPER_CMD="ls -l"
			;;
		-libjars*)
			FileUri=`echo $TEST_OPTIONS | awk '{print $2}'`
			FileName=`basename $FileUri`
			MAPPER_CMD="printenv"
			;;
		-archives*)
			FileUri=`echo $TEST_OPTIONS | awk '{print $2}'`
			FileName=`basename $FileUri`
			MAPPER_CMD="ls -l"
			;;
		*)
			FileUri="job.jar"		# really dummy name
			FileName="job.jar"
			MAPPER_CMD="ls -l"
			;;
		esac
	fi
    echo "    $PROG_NAME run_stream_csmt:: TEST_CONFIG_DIR=$TEST_CONFIG_DIR"
    echo "    $PROG_NAME run_stream_csmt:: FileUri=$FileUri"
    echo "    $PROG_NAME run_stream_csmt:: FileName=$FileName"
    echo "    $PROG_NAME run_stream_csmt:: MAPPER_CMD=$MAPPER_CMD"
	
	$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR   dfs -stat $TEST_OUTPUT_DIR  2>&1 > /dev/null
    if [ $? == 0 ] ; then
		echo "    $PROG_NAME: output directory exists. Remove it first: $HADOOP_DFS_CMD --config $TEST_CONFIG_DIR   dfs -rmr -skipTrash $TEST_OUTPUT_DIR"
		$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR   dfs -rmr -skipTrash $TEST_OUTPUT_DIR
 	else
		echo "    $PROG_NAME: output directory does not exist. OK to proceed with running wordcount app"
	fi

	REDUCER_CMD="-reducer NONE"
	TCMD="hadoop --config $TEST_CONFIG_DIR/  jar $HADOOP_STREAMING_JAR $TEST_OPTIONS -input $TEST_INPUT_DIR -output $TEST_OUTPUT_DIR   $REDUCER_CMD "

	echo "##  [$TestId] TCMD is:  $TCMD -mapper $MAPPER_CMD "
	echo "    [Exec_notes]: $TestType Test- [$TEST_PARAM] [$TEST_NOTES]"

	# Run the command
	echo "    [$TestId Doit: $TCMD -mapper $MAPPER_CMD "
	$TCMD  -mapper "$MAPPER_CMD"
	statExec=$?
	hdftMapStatus $statExec $TestType
	statExec=$?
	hdftShowExecResult $statExec $TestId "$TEST_PARAM] [$TEST_NOTES"

	statDiff=0
	if [ -n "$MAPPER_CMD" ] && [ "$TestType" == "POS" ] ; then 
		echo "    Doit-sub: $HADOOP_DFS_CMD --config $TEST_CONFIG_DIR    dfs -stat $TEST_OUTPUT_DIR/part-00000"
		$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR    dfs -stat $TEST_OUTPUT_DIR/part-00000	2>&1 > /dev/null
		hdftShowDataResult $? $TestId "file" "Existence of file $TEST_OUTPUT_DIR/part-00000" 
		
		if [ "$MAPPER_CMD"	== "printenv" ] ; then
			# libjars need to grep mapred.jar.classpath.archives to make sure the file is in this path (but oozie can also set to mapred.jar.classpath.files)
			# mapred_job_classpath_archives=/user/hadoopqa/.staging/job_201102162041_0426/libjars/jar1.jar	
			$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -cat  $TEST_OUTPUT_DIR/part-00000 | fgrep mapred.jar.classpath 
			echo "    $HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -cat  $TEST_OUTPUT_DIR/part-00000 | fgrep mapred_job_classpath | fgrep $FileName"
			$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -cat  $TEST_OUTPUT_DIR/part-00000 | fgrep mapred_job_classpath | fgrep $FileName
			statDiff=$?
			hdftShowDataResult $statDiff $TestId "mapred_job_classpath $FileName" "Env Variable mapred_job_classpath in output file of map task printenv "
		else	
			# this is ls -l: ro_file2.txt -> /grid/0/tmp/mapred-local/taskTracker/hadoopqa/distcache/-6980783361583905401_-254456821_838513190/
			#		viewfs/user/hadoopqa/.staging/job_201102162041_0271/files/ro_file2.txt
			echo $HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -ls  $TEST_OUTPUT_DIR/part-00000 
			$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -ls  $TEST_OUTPUT_DIR/part-00000 

			echo $HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -cat  $TEST_OUTPUT_DIR/part-00000 \| fgrep -- '->'   \| fgrep $FileName
			$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR dfs -cat  $TEST_OUTPUT_DIR/part-00000 | fgrep -- '->'   | fgrep $FileName
			statDiff=$?
			hdftShowDataResult $statDiff $TestId "$FileName" "output of map task ls -l to contain the symlink to filename"
		fi
	fi
	
	(( returnStat = $statDiff + $statExec ))

	echo "    hdftReturnTaskResult" "$statExec" "$TestId" "$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR jar streaming.jar $TEST_PARAM" "$TEST_NOTES"
	hdftReturnTaskResult "$statExec" "$TestId" "$HADOOP_DFS_CMD --config $TEST_CONFIG_DIR jar streaming.jar $TEST_PARAM" "$TEST_NOTES"
	echo ""
	return $returnStat;


}

# run this after calling setup dir, to make sure the directory exist and writable.
function test_600_csmt_trash {
	hdftDisplayTestGroupMsg	"Test Group to test trash handling of  mounted directories. Assume write test group CSMT_280 already passed."
	run_csmt_trash "POS_CSMT_600"   "/nn2"  "@: trash test: rm a tmpfile from a mounted directory /nn2"
	run_csmt_trash "POS_CSMT_601"   "/nn3"  "@: trash test: rm a tmpfile from a mounted directory /nn3"
	run_csmt_trash "POS_CSMT_602"   "/tmp"  "@: trash test: rm a tmpfile from a mounted directory /tmp"
	run_csmt_trash "POS_CSMT_603"   "/user/hadoopqa"  "@: trash test: rm a tmpfile from a mounted directory /user/hadoopqa"
	run_csmt_trash "POS_CSMT_604"   "viewfs:/nn2"  "@: trash test: rm a tmpfile from a mounted directory: viewfs:/nn2"
}

# run this after calling setup dir, to make sure the directory exist and writable.
function test_280_csmt_write {
	hdftDisplayTestGroupMsg	"Test Group to test writing to mounted directories. Some are negative tests as they are not writable by hadoopqa."
	run_csmt_write "POS_CSMT_280"   "/nn2"  "@: write test: write to a mounted directory /nn2"
	run_csmt_write "POS_CSMT_281"   "/nn3"  "@: write test: write to a mounted directory /nn3"
	run_csmt_write "POS_CSMT_282"   "/tmp"  "@: write test: write to a mounted directory /tmp"
	run_csmt_write "POS_CSMT_283"   "/user/hadoopqa"  "@: write test: write to a mounted directory /user/hadoopqa"
	run_csmt_write "POS_CSMT_284"   "viewfs:/nn2"  "@: write test: write to a mounted directory: viewfs:/nn2"
	#run_csmt_write "NEG_CSMT_285"   "/mapred"  "@: write to a mounted directory /mapred owned by mapred"
	#run_csmt_write "NEG_CSMT_286"   "/mapredsystem"  "@: write to a mounted directory /mapredsystem owned by mapred"
	#run_csmt_write "NEG_CSMT_287"   "/jobtracker"  "@: write to a mounted directory /jobtracker owned by mapred"
}

# /nn2, /nn3 are mounted directories
# use /nn2 for hdfs-csmt test; 
# use /nn2 for csmt-hdfs test; 
# use /nn2 /nn3 for csmt-csmt test;  
# 16 cases
function test_300_wordcount_viewfs_basic {
	hdftDisplayTestGroupMsg	"Test Group 300 to test running of wordcount with permutation of various form of URI in Input and Output: /, viewfs:/, viewfs:///, viewfs://default"

	## INput from hdfs: & hftp (file/dir): output: using various form of csmt URI: /, viewfs:/, viewfs:///, viewfs://default/
    run_wordcount_csmt  "POS_CSMT_300"  "hdfs://$NN2/user/hadoopqa/hdfsRegressionData/wordcount_input"	"/nn2/P300_wordcount_output"        \
		 	" "   "@: pos - output to mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CSMT_301"  "hdfs://$NN2/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs:/nn2/P301_wordcount_output" \
          	" "   "@: pos - output to mounted dir with viewfs:/"
    run_wordcount_csmt  "POS_CSMT_302"  "hdfs://$NN2/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs:///nn2/P302_wordcount_output" \
        	" "   "@: pos - output to mounted dir with viewfs:///"
    run_wordcount_csmt  "POS_CSMT_303"  "hdfs://$NN2/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs://default/nn2/P303_wordcount_output"  \
			" "   "@: pos - output to viewfs://default/nn2"
    run_wordcount_csmt  "POS_CSMT_304"  "hftp://${NN2}.blue.ygrid:50070/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs://default/nn2/P304_wordcount_output"  \
			" "   "@: pos - input hftp: directory; output to viewfs://default/nn2"
   run_wordcount_csmt  "POS_CSMT_305"  "hftp://${NN2}.blue.ygrid:50070/user/hadoopqa/hdfsRegressionData/helloworld.txt"	"viewfs://default/nn2/P305_wordcount_output"  \
			" "   "@: pos - input hftp: file; output to viewfs://default/nn2"

	## Output to hdfs. Input: using various form of csmt URI: /, viewfs:/, viewfs:///, viewfs://default/. Added hftp here:
    run_wordcount_csmt  "POS_CSMT_310"  "/nn2/wordcount_input"          "hdfs://$NN2/tmp/P310_wordcount_output"  \
			" "   "@: pos - input from  mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CSMT_311"  "viewfs:/nn2/wordcount_input"   "hdfs://$NN2/tmp/P311_wordcount_output"  \
			" "   "@: pos - input from  mounted dir (/nn2) using viewfs:/"
    run_wordcount_csmt  "POS_CSMT_312"  "viewfs:///nn2/wordcount_input" "hdfs://$NN2/tmp/P312_wordcount_output"  \
			" "   "@: pos - input from  mounted dir (/nn2) using viewfs:///"
    run_wordcount_csmt  "POS_CSMT_313"  "viewfs://default/nn2/wordcount_input" "hdfs://$NN2/tmp/P313_wordcount_output"  \
			" "   "@: pos - input from  mounted dir (/nn2) using viewfs://default/"


	## both input and output: ussing various form of CMST URI: selected permutation. Go to the same mounted directory
    run_wordcount_csmt  "POS_CSMT_320"  "/nn2/wordcount_input"	"/nn2/P320_wordcount_output"                  \
			" "   "@: pos - input and output to mounted dir (/nn2) without viewfs:/"

    run_wordcount_csmt  "POS_CSMT_321"  "/nn2/wordcount_input"	"viewfs:/nn2/P321_wordcount_output"           \
			" "   "@: pos - input /nn2, output: viewfs:/nn2"

    run_wordcount_csmt  "POS_CSMT_322"  "viewfs:/nn2/wordcount_input"	"viewfs://default/nn2/P322_wordcount_output"  \
			" "   "@: pos - input /nn2, output to viewfs://default"

    run_wordcount_csmt  "POS_CSMT_323"  "viewfs:///nn2/wordcount_input"	"/nn2/P323_wordcount_output"          \
			" "   "@: pos - input from viewfs://, output to /nn2"

	## both input and output: using various form of CMST URI: selected permutation. Go to the different mounted directory
    run_wordcount_csmt  "POS_CSMT_330"  "/nn2/wordcount_input"	"/nn3/P330_wordcount_output"                  \
			" "   "@: pos - input and output to mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CSMT_331"  "/nn2/wordcount_input"	"viewfs:/nn3/P331_wordcount_output"           \
			" "   "@: pos - input /nn2, output: viewfs:/nn2"
    run_wordcount_csmt  "POS_CSMT_332"  "viewfs:/nn2/wordcount_input"	"viewfs://default/nn3/P332_wordcount_output"  \
			" "   "@: pos - input /nn2, output to viewfs://default"
    run_wordcount_csmt  "POS_CSMT_333"  "viewfs:///nn2/wordcount_input"	"/nn3/P333_wordcount_output"          \
			" "   "@: pos - input from viewfs://, output to /nn2"
}

# /nn2, /nn3 are mounted directories
# use /nn2 for hdfs-csmt test; 
# use /nn2 for csmt-hdfs test; 
# use /nn2 /nn3 for csmt-csmt test;  
# test cases with -files, -libjars, -archives option: 
# Notes: -files /xyz is referring to files on the client/Linux side (ie.. same as -files file:/xyz)

function test_400_stream_viewfs_distcache {
	hdftDisplayTestGroupMsg	"Test Group 400 to test running of streaming / distributed cache with options: -files, -archives, -libjars. Input and Output to mounted dir"


	#   $0              $1             $2              $3                      $4   						$5:options   $6:notes


	# begin test

	echo "$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR  dfs -ls /nn2/wordcount_input"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR  dfs -ls /nn2/wordcount_input

	echo "$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR  dfs -ls viewfs:/nn2/wordcount_input"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR  dfs -ls viewfs:/nn2/wordcount_input


	# Baseline case with both input and output areCSMT directories, but without -files, -libjar, or -archives
    run_stream_csmt  "POS_CSMT_400"  'Hdfs'  	"/tmp/nn2.mt/wordcount_input"	"/tmp/nn2.mt/P400_stream_output"                  \
			" "   "CSMT distributed cache test user streaming basline (no options) with input=hdfs_default output=hdfs_default mapper=ls -l and no reducer"

    run_stream_csmt  "POS_CSMT_401"  'Hdfs'  	"hdfs://$NN2/tmp/nn2.mt/wordcount_input"	"hdfs://$NN2/tmp/nn2.mt/P401_stream_output"                  \
			" "   "CSMT distributed cache test user streaming basline (no options) with input=hdfs:/   output=hdfs:/ mapper=ls -l and no reducer"

    run_stream_csmt  "POS_CSMT_402" 'Viewfs'			"/nn2/wordcount_input"	"/nn2/P402_stream_output"                  \
			" "   "CSMT distributed cache test user streaming basline (no options) with input=(viewfs:)/   output=(viewfs:)/ mapper=ls -l and no reducer"

	# -files option
    run_stream_csmt  "POS_CSMT_411" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P411_stream_output"                  \
			"-files viewfs:/nn2/file1.txt"   "CSMT distributed cache test uses  streaming -files viewfs:/rw_file input=(viewfs:)/  output=(viewfs:)/ mapper=ls -l"

    run_stream_csmt  "POS_CSMT_412" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P412_stream_output"                  \
			"-files viewfs:/nn2/dir1/ro_file2.txt"   "CSMT distributed cache test uses streaming -files viewfs:/ read-only file  options"

    run_stream_csmt  "POS_CSMT_413" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P413_stream_output"                  \
			"-files hdfs://$NN2/tmp/nn2.mt/dir1/ro_jar2.jar"   "CSMT distributed cache test uses streaming -files viewfs:/ro jar file options"

    run_stream_csmt  "POS_CSMT_414" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P414_stream_output"                  \
			"-files file://$TEST_DATA_DISTCACHE_DIR/ro_file2.txt" "CSMT distributed cache test uses streaming -files file:/// option (local Linux file)"

    run_stream_csmt  "POS_CSMT_415" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P415_stream_output"                  \
			"-files $TEST_DATA_DISTCACHE_DIR/file1.txt"   	"CSMT distributed cache test uses streaming -files Linux files"

    run_stream_csmt  "NEG_CSMT_416" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P416_stream_output"                  \
			"-files /nn2/file1.txt"   						"CSMT distr cache Negative test uses -files /nn2 Linux files non-existent"


	# -libjars option
    run_stream_csmt  "POS_CSMT_421" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P421_stream_output"                  \
			"-libjars viewfs:/nn2/jar1.jar"   					"CSMT distributed cache test using streaming -libjars viewfs:/  rw jar"

    run_stream_csmt  "POS_CSMT_422" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P422_stream_output"                  \
			"-libjars viewfs:/nn2/dir1/ro_jar2.jar" 			"CSMT distributed cache test using streaming -libjars viewfs:/ ro jar"

    run_stream_csmt  "POS_CSMT_423" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P423_stream_output"                  \
			"-libjars hdfs://$NN2/tmp/nn2.mt/dir1/ro_jar2.jar"  "CSMT distributed cache test using streaming -libjars hdfs:/ jar"

    run_stream_csmt  "POS_CSMT_424" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P424_stream_output"                  \
			"-libjars file://$TEST_DATA_DISTCACHE_DIR/dir1/rw_jar1.jar"   	"CSMT distributed cache test using streaming -libjars /nn2 rw jar"

    run_stream_csmt  "POS_CSMT_425" 'Viewfs' 		"/nn2/wordcount_input"	"/nn2/P425_stream_output"                  \
			"-libjars $TEST_DATA_DISTCACHE_DIR/ro_jar2.jar"		"CSMT distributed cache test using streaming -libjars /nn2 ro jar"


	# -archives option
    run_stream_csmt  "POS_CSMT_431"  'Viewfs'		"/nn2/wordcount_input"	"/nn2/P431_stream_output"                  \
			"-archives viewfs:/nn2/jar1.jar"   "CSMT distributed cache test using streaming with -archive option: viewfs:/nn2"

    run_stream_csmt  "POS_CSMT_432"  'Viewfs'		"/nn2/wordcount_input"	"/nn2/P432_stream_output"                  \
			"-archives viewfs:/nn2/dir1/ro_jar2.jar"   "CSMT distributed cache test using streaming with -archive option: viewfs:/nn2 read-only jar"

    run_stream_csmt  "POS_CSMT_433"  'Viewfs'		"/nn2/wordcount_input"	"/nn2/P433_stream_output"                  \
			"-archives hdfs://$NN2/tmp/nn2.mt/dir1/ro_jar2.jar"   "CSMT distributed cache test using streaming with -archive option: hdfs:/ read-only jar"

    run_stream_csmt  "POS_CSMT_434"  'Viewfs'		"/nn2/wordcount_input"	"/nn2/P434_stream_output"                  \
			"-archives file://$TEST_DATA_DISTCACHE_DIR/dir1/rw_jar1.jar"		"CSMT distributed cache test using streaming with -archive option: file:///  Linux jar"

    run_stream_csmt  "POS_CSMT_435"  'Viewfs'		"/nn2/wordcount_input"	"/nn2/P435_stream_output"                  \
			"-archives $TEST_DATA_DISTCACHE_DIR/ro_jar2.jar"		"CSMT distributed cache test using streaming with -archive option: (file://)/  Linux jar"

}


# /nn2, /nn3 are mounted directories
# use /nn2 for hdfs-csmt test; 
# use /nn2 for csmt-hdfs test; 
# use /nn2 /nn3 for csmt-csmt test;  
# test cases with -files, -libjars, -archives option

function test_350_wordcount_viewfs_distcache {

	hdftDisplayTestGroupMsg	"Test Group 350 in running wordcount with  distributed cache options: -files, -archives, -libjars. Input and Output to to same mounted dir /nn2"

	# will return error if cannot access files specified by -files, -libjars, or -archives
    run_wordcount_csmt  "POS_CSMT_350"  "/nn2/wordcount_input"	"/nn2/P350_wordcount_output"                  \
			" "   "@: pos - baseline case for wordcount test with options: input and output (/nn2) without viewfs:/"

    run_wordcount_csmt  "POS_CSMT_351"  "/nn2/wordcount_input"	"/nn2/P351_wordcount_output"                  \
			"-files file://$TEST_DATA_DISTCACHE_DIR/file1.txt"   "@: pos - using -files file:///"

    run_wordcount_csmt  "POS_CSMT_352"  "/nn2/wordcount_input"	"/nn2/P352_wordcount_output"                  \
			"-files viewfs:/nn2/file1.txt"   "@: pos - using -files viewfs:/nn2/file1 option"

    run_wordcount_csmt  "POS_CSMT_353"  "/nn2/wordcount_input"	"/nn2/P353_wordcount_output"                  \
			"-files viewfs:/nn2/dir1/ro_file2.txt"   "@: pos - using -files mounted read-only files"

    run_wordcount_csmt  "POS_CSMT_354"  "/nn2/wordcount_input"	"/nn2/P354_wordcount_output"                  \
			"-files hdfs://$NN2/tmp/nn2.mt/dir1/ro_jar2.jar"   "@: pos - using -files hdfs://read-only files"

    run_wordcount_csmt  "POS_CSMT_355"  "/nn2/wordcount_input"	"/nn2/P355_wordcount_output"                  \
			"-files $TEST_DATA_DISTCACHE_DIR/ro_jar2.jar"   "@: pos - using -files /Linux_file2 option"



    run_wordcount_csmt  "POS_CSMT_361"  "/nn2/wordcount_input"	"/nn2/P361_wordcount_output"                  \
			"-libjars file://$TEST_DATA_DISTCACHE_DIR/dir1/rw_jar1.jar"   "@: pos - using -libjars file:///"

    run_wordcount_csmt  "POS_CSMT_362"  "/nn2/wordcount_input"	"/nn2/P362_wordcount_output"                  \
			"-libjars viewfs:/nn2/jar1.jar"   "@: pos - using -libjars viewfs:/nn2/jar1.jar option"

    run_wordcount_csmt  "POS_CSMT_363"  "/nn2/wordcount_input"	"/nn2/P363_wordcount_output"                  \
			"-libjars viewfs:/nn2/dir1/ro_jar2.jar"   "@: pos - using --ibjars mounted read-only files"

    run_wordcount_csmt  "POS_CSMT_364"  "/nn2/wordcount_input"	"/nn2/P364_wordcount_output"                  \
			"-libjars hdfs://$NN2/tmp/nn2.mt/dir1/ro_jar2.jar"   "@: pos - using -libjars hdfs://read-only files"

    run_wordcount_csmt  "POS_CSMT_365"  "/nn2/wordcount_input"	"/nn2/P365_wordcount_output"                  \
			"-libjars $TEST_DATA_DISTCACHE_DIR/ro_jar2.jar"   "@: pos - using --libjars Linux /ro_jar2.jar option"



    run_wordcount_csmt  "POS_CSMT_371"  "/nn2/wordcount_input"	"/nn2/P371_wordcount_output"                  \
			"-archives file://$TEST_DATA_DISTCACHE_DIR/dir1/rw_jar1.jar"   "@: pos - using -archives Linux file:///dir1/rw_jar1.jar"

    run_wordcount_csmt  "POS_CSMT_372"  "/nn2/wordcount_input"	"/nn2/P372_wordcount_output"                  \
			"-archives viewfs:/nn2/jar1.jar"   "@: pos - using -archives viewfs:/nn2/jar1.jar option"

    run_wordcount_csmt  "POS_CSMT_373"  "/nn2/wordcount_input"	"/nn2/P373_wordcount_output"                  \
			"-archives viewfs:/nn2/dir1/ro_jar2.jar"   "@: pos - using -archives read-only /nn2/dir1/ro_jars.jar"

    run_wordcount_csmt  "POS_CSMT_374"  "/nn2/wordcount_input"	"/nn2/P374_wordcount_output"                  \
			"-archives hdfs://$NN2/tmp/nn2.mt/dir1/ro_jar2.jar"   "@: pos - using -archives hdfs://read-only files"

    run_wordcount_csmt  "POS_CSMT_375"  "/nn2/wordcount_input"	"/nn2/P375_wordcount_output"                  \
			"-archives $TEST_DATA_DISTCACHE_DIR/ro_jar2.jar"   "@: pos - using -files Linux /ro_jar1.jar"


}


function run_distcp_csmt {
    local TestId=$1
    local TEST_INPUT_DIR=$2
    local TEST_OUTPUT_DIR=$3
    local TEST_OPTIONS="$4 $Q_OPTION $JT_OPTION"
    local TEST_NOTES=$5
    TESTCASE_DESC="INPUT=$2; OUTPUT=$3; $5"
	TESTCASENAME="VFS_DISTCP"
    echo "###########################################"
    echo "    $PROG_NAME run_distcp_csmt:: TestId=$TestId"
    echo "    $PROG_NAME run_distcp_csmt:: TEST_INPUT_DIR=$TEST_INPUT_DIR"
    echo "    $PROG_NAME run_distcp_csmt:: TEST_OUTPUT_DIR=$TEST_OUTPUT_DIR"
    echo "    $PROG_NAME run_distcp_csmt:: TEST_OPTIONS=$TEST_OPTIONS"
    echo "    $PROG_NAME run_distcp_csmt:: TEST_NOTES=$TEST_NOTES"
	echo "    $PROG_NAME run_wordcount csmt:: ENV :  KRB5CCNAME=$KRB5CCNAME"
	echo "    $PROG_NAME run_wordcount csmt:: ENV :  KB Principal=`klist | fgrep 'Default principal'`"

	displayTestCaseMessage "$TestId ==== $TESTCASE_DESC"
    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi
 
	
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR  dfs -stat $TEST_OUTPUT_DIR	2>&1 > /dev/null
    if [ $? == 0 ] ; then
		echo "    $PROG_NAME: output directory exists. Remove it first: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -rmr -skipTrash $TEST_OUTPUT_DIR"
		$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -rmr -skipTrash $TEST_OUTPUT_DIR
 	else
		echo "    $PROG_NAME: output directory does not exist. OK to proceed with running wordcount app"
	fi

	
	TCMD="hadoop --config $VIEWFS_CONF_DIR/  distcp $TEST_OPTIONS $TEST_INPUT_DIR $TEST_OUTPUT_DIR"

	(( Count = Count + 1 ))
	echo "## [$TestId] TCMD is:  $TCMD "
	echo "   [Exec_notes]: $TestType Testr- $TEST_NOTES"

	# Run thec command
	echo "    Doit:: $TCMD"
	$TCMD
	statExec=$?
	hdftMapStatus $statExec $TestType
	statExec=$?
	hdftShowExecResult $statExec $TestId "$TESTCASE_DESC"

	returnStat=$statExec
	hdftReturnTaskResult $statExec $TestId "$TCMD" "$TEST_NOTES"

	return $returnStat;

}

# distcp does not seem to work
function test_800_distcp_viewfs {

	# I expect these to pass, but some how they all fail... except the first one
	# various combination of input URI
	# various combination of output URI
	# various combination of both URI
	# Note that: hftp can only be in input; not as output
	hdftDisplayTestGroupMsg	"Test Group 800 in distcp: input hdft/views; output: hdfs:/"

	run_distcp_csmt "POS_800"  "hdfs://$NN2/tmp/nn2.mt/wordcount_input"   "hdfs://$NN3/tmp/nn3.mt/P800_output"	\
					" "   	"input hdfs to output hdfs:/"
	run_distcp_csmt "POS_801"  "viewfs:/nn2/wordcount_input"     "hdfs://$NN3/tmp//nn3.mt/P801_output"		\
					" "   	"input viewfs:/ to output hdfs:/. "
	run_distcp_csmt "POS_802"  "/nn2/wordcount_input"   		"hdfs://$NN3/tmp//nn3.mt/P802_output"		\
					" "   	"input csmt /nn2 to output hdfs:/. "
	run_distcp_csmt "POS_803"  "hftp://${NN2}:50070/tmp/nn2.mt/wordcount_input" 	"hdfs://$NN3/tmp//nn3.mt/P803_output"		\
					" "   	"hftp://directory  to output hdfs:/"
# run_distcp_csmt "POS_804"  "hftp://${NN2}:50070/tmp/nn2.mt/file1.txt" 	"hdfs://$NN3/tmp//nn3.mt/P804_output"		\
# 					" "   	"hftp a file to output hdfs:/"

	hdftDisplayTestGroupMsg	"Test Group 810 in distcp: input hdft/views; output: viewfs:/nn3"
	run_distcp_csmt "POS_810"  "hdfs://$NN2/tmp/nn2.mt/wordcount_input"   "viewfs:/nn3/P810_output"		 \
					" "   	"input hdfs to output viewfs:/nn3"
	run_distcp_csmt "POS_811"  "viewfs:/nn2/wordcount_input"     "viewfs:/nn3/P811_output"		\
					" "   	"input viewfs:/ to output viewfs:/nn3."
	run_distcp_csmt "POS_812"  "/nn2/wordcount_input"   		 "viewfs:/nn3/P812_output"		\
					" "   	"input cmst /nn2 to output viewfs:/nn3."
	run_distcp_csmt "POS_813"  "hftp://${NN2}:50070/tmp/nn2.mt/wordcount_input" 	"viewfs:/nn3/P813_output"		\
					" "   	"hftp://directory  to output viewfs:/"
	run_distcp_csmt "POS_814"  "hftp://${NN2}:50070/tmp/nn2.mt/file1.txt" 	"viewfs:/nn3/P814_output"		\
					" "   	"hftp a file to output viewfs:/."

	hdftDisplayTestGroupMsg	"Test Group 820 in distcp: input hdft/views; output: csmt:/nn3"
	run_distcp_csmt "POS_820"  "hdfs://$NN2/tmp/nn2.mt/wordcount_input"   "/nn3/P820_output"		\
					" "   	"input hdfs:/  to output csmt /nn3"
	run_distcp_csmt "POS_821"  "viewfs:/nn2/wordcount_input"     "/nn3/P821_output"		\
					" "   	"input cmst /nn2 to output csmt /nn3."
	run_distcp_csmt "POS_822"  "/nn2/wordcount_input"   		 "/nn3/P822_output"		\
					" "   	"input cmst /nn2 to output csmt /nn3."
	run_distcp_csmt "POS_823"  "hftp://${NN2}:50070/tmp/nn2.mt/wordcount_input" 	"/nn3/P823_output"		\
					" "   	"hftp://directory  to output /nn2"
	run_distcp_csmt "POS_824"  "hftp://${NN2}:50070/tmp/nn2.mt/file1.txt" 	"/nn3/P824_output"		\
					" "   	"hftp a file to output /nn3."

}


function junky {
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/"

 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://xxxx/"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/"

 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  dfs -ls hdfs://xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CSMT_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"



}

function setup_dir {
	set -x
	# hadoop dfs -ls /tmp/nn2. 
	#$HADOOP_DFS_CMD dfs -rmr -skipTrash /tmp/nn2.mt /tmp/nn3.mt /tmp/nn2 /tmp/nn4.mt /tmp/nn4 /tmp/user.mt /tmp/tmp.mt  \
		#/tmp/mapred.mt /tmp/mapredsystem.mt /tmp/data.mt /nn2 /nn3
	# /tmp/nn2.mt and /tmp/nn4.mt :These two dir are  adequate for fs test. May need more for mapred app running

	PREV_MM="N/A"
	# create the directories on hdfs for the viewfs mounted directories in /tmp/nn2.mt, /tmp/nn3.mt, /tmp/nn4.mt
	for  MM  in $NN0 $NN2 $NN3 ; do 
      # no need to process if the same as immediately previous MMs
	  if [ "$MM" == "$PREV_MM" ] ; then
		echo "    MM ($MM)  is same as previous $PREV_MM. Skipped processing."
		continue
	  fi

	  $HADOOP_DFS_CMD dfs -rmr -skipTrash hdfs://$MM/tmp/user.mt hdfs://$MM/tmp/user.mt/hadoopqa hdfs://$MM/tmp/tmp.mt \
		hdfs://$MM/tmp/mapred.mt hdfs://$MM/tmp/mapredsystem.mt hdfs://$MM/tmp/data.mt

	$HADOOP_DFS_CMD dfs -mkdir -p hdfs://$MM/tmp/user.mt hdfs://$MM/tmp/user.mt/hadoopqa hdfs://$MM/tmp/tmp.mt \
		hdfs://$MM/tmp/mapred.mt hdfs://$MM/tmp/mapredsystem.mt hdfs://$MM/tmp/data.mt


	  $HADOOP_DFS_CMD dfs -stat hdfs://$MM/user/hadoopqa/hdfsRegressionData  2>&1 > /dev/null
	  if [ $? != 0 ] ; then
	  	$HADOOP_DFS_CMD dfs -copyFromLocal  /homes/hdfsqa/hdfsRegressionData hdfs://$MM/user/hadoopqa/hdfsRegressionData 
	  fi

	  $HADOOP_DFS_CMD dfs -stat hdfs://$MM//tmp/user.mt/hadoopqa/hdfsRegressionData  2>&1 > /dev/null
	  if [ $? != 0 ] ; then
	  	$HADOOP_DFS_CMD dfs -copyFromLocal  /homes/hdfsqa/hdfsRegressionData hdfs://$MM/tmp/user.mt/hadoopqa/hdfsRegressionData 
	  fi

	  for MDIR in nn2.mt nn3.mt nn4.mt user.mt/hadoopqa ; do 
	
		$HADOOP_DFS_CMD dfs -rmr -skipTrash  hdfs://$MM/tmp/$MDIR
		$HADOOP_DFS_CMD dfs -mkdir -p  hdfs://$MM/tmp/$MDIR

		# ro_file2.txt  ro_jar2.jar  rw_file1.txt  rw_jar1.jar
		$HADOOP_DFS_CMD dfs -copyFromLocal  /homes/hdfsqa/hdfsRegressionData/distcache_input/*  hdfs://$MM/tmp/$MDIR/
		$HADOOP_DFS_CMD dfs -chmod 444 hdfs://$MM/tmp/$MDIR/dir1/ro_*   hdfs://$MM/tmp/$MDIR/ro_*   
		$HADOOP_DFS_CMD dfs -copyFromLocal /homes/hdfsqa/hdfsRegressionData/wordcount_input hdfs://$MM/tmp/$MDIR

	
		echo "    Directory listing using hdfs default: $HADOOP_DFS_CMD dfs -lsr  hdfs://$MM/tmp/$MDIR"
		$HADOOP_DFS_CMD dfs -lsr  hdfs://$MM/tmp/$MDIR
		$HADOOP_DFS_CMD dfs -chmod 755 hdfs://$MM/tmp/$MDIR

	  done
	  PREV_MM=$MM

	done

	set -
	
}

# call setup_dir first, before calling this one
function setup_viewfs_dir {
	echo "    $HADOOP_DFS_CMD dfs -mkdir -p /user/hadoopqa	"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -mkdir -p /user/hadoopqa	
	$HADOOP_DFS_CMD --config $VIEWHS_CONF_DIR   dfs -mkdir -p /user/hadoopqa	

	echo "    Directory listing using viewfs:: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls /"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls /

	echo "    Directory listing using viewfs:: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls /tmp"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls /tmp

	echo "    Directory listing using viewfs:: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls /user"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -ls /user

	echo "    Directory listing using viewfs:: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -lsr /nn2"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -lsr /nn2

	echo "    Directory listing using viewfs:: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -lsr /nn3"
	 $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -lsr /nn3

	echo "    Directory listing using viewfs:: $HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -lsr /nn4"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -lsr /nn4
}

function clean_up {
	for  MM  in $NN0 $NN2 $NN3 ; do 
    	$HADOOP_DFS_CMD dfs -rm -r -skipTrash hdfs://$MM/tmp/user.mt hdfs://$MM/tmp/user.mt/hadoopqa hdfs://$MM/tmp/tmp.mt \
			hdfs://$MM/tmp/mapred.mt hdfs://$MM/tmp/mapredsystem.mt hdfs://$MM/tmp/data.mt

		$HADOOP_DFS_CMD dfs -rm -r -skipTrash  hdfs://$MM/user/hadoopqa/hdfsRegressionData
		$HADOOP_DFS_CMD dfs -rm -r -skipTrash  hdfs://$MM/tmp/*_wordcount_output
	
		for MDIR in nn2.mt nn3.mt nn4.mt; do 	
			$HADOOP_DFS_CMD dfs -rm -r -skipTrash  hdfs://$MM/tmp/$MDIR				
		done
	done
		
}

function print_env {
	echo "    $PROG_NAME Env:: CLUSTER=$CLUSTER"
	echo "    $PROG_NAME Env:: WORKSPACE=$WORKSPACE"
	echo "    $PROG_NAME Env:: HDFT_TOP_DIR=$HDFT_TOP_DIR"
	echo "    $PROG_NAME Env:: NN2=$NN2"
	echo "    $PROG_NAME Env:: HADOOP_GOT_VERSION=$HADOOP_GOT_VERSION"
	echo "    $PROG_NAME Env:: HADOOP_CLASSPATH=$HADOOP_CLASSPATH"
	echo "    $PROG_NAME Env:: HADOOP_HOME=$HADOOP_HOME"
	echo "    $PROG_NAME Env:: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
	echo "    $PROG_NAME Env:: which hadoop: = `which hadoop`"


}

date
### hdftKInitAsUser hadoopqa
SCRIPT_EXIT_CODE=0
print_env
print_notes
setKerberosTicketForUser $HADOOPQA_USER
echo "Run as KB User: "
echo "Run as KB User: "
klist |grep -i principal

setup_dir
setup_viewfs_dir

# check the basic file access cases
test_01_all_CSMT3
# run 101_CSMT3 if just want a quick test #test_101_CSMT3 
#test_101_CSMT3

# test distcp
test_800_distcp_viewfs


# basic access
test_100_basic_cases


# test the wordcount with viewfs
test_300_wordcount_viewfs_basic


test_350_wordcount_viewfs_distcache

# check the bug
 test_200_bug_4129512

# testing the write of the directory
 test_280_csmt_write

# test basic trash handling
test_600_csmt_trash 

# test distributed cache
 test_400_stream_viewfs_distcache



clean_up

print_env
date

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"

exit $SCRIPT_EXIT_CODE



cat << EOF_DONE

  The following is another test: removal of files across CSMT:
     1400  hdfs dfs -ls hdfs:///.Trash
     1401  hdfs dfs -ls .Trash
     1403  hdfs --config v22_viewfs_hadoopconf-omegab dfs -ls /nn2
     1405  hdfs --config v22_viewfs_hadoopconf-omegab dfs -put /etc/group /nn2/file2
     1407  hdfs --config v22_viewfs_hadoopconf-omegab dfs -ls /nn2/file2
	    -rw-r--r--   1 hadoopqa hdfs        806 2010-12-14 02:20 /nn2/file2
     1409  hdfs --config v22_viewfs_hadoopconf-omegab dfs -rm  /nn2/file2	# same problem -rm viewfs:/nn2/file2
	    10/12/14 02:20:43 WARN fs.Trash: Can t create trash directory: viewfs:/user/hadoopqa/.Trash/Current/nn2
	    Problem with Trash./user. Consider using -skipTrash option
		rm: Failed to move to trash: viewfs:/nn2/file2

  The following test: uses -conf to add default NN: 
  "NEG"	hdfs --config v22_viewfs_hadoopconf-omegab/ dfs -ls  /user/cwchung/test1	# does not work
  "POS"	hdfs --config v22_viewfs_hadoopconf-omegab/ dfs -conf my-hdfs-scheme-only.xml -ls  /user/cwchung/test1 # Ok


EOF_DONE
