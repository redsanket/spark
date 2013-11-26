
PROG_NAME=$0
PARAM=$1

if [ -z "$CLUSTER" ]; then
	export CLUSTER=omegam
fi


PWD=`pwd`
export WORKSPACE=$PWD/../../..
export HDFT_TOP_DIR=$PWD/../../..
export HDFT_HOME=$PWD/../../..

source ../../../hdft_include.sh

# hadoop version output: Hadoop 0.20.202.0.1012030459
HADOOP_GOT_VERSION=`hadoop version | head -1 | awk -F. '{print $2}'`
echo "    $PROG_NAME: Using Hadoop version - $HADOOP_GOT_VERSION"
echo "    $PROG_NAME: Using cluster - $CLUSTER"


# HADOOP_DFS_CMD is to run hadoop/hdfs fs -ls / ; while HADOOP_CMD is to run hadoop 

if [ $HADOOP_GOT_VERSION == '20' ] ; then
	export HADOOP_VERSION=20
	export HADOOP_DFS_CMD=hadoop
	export HADOOP_CMD=hadoop
	export VIEWFS_CONF_DIR=v20_viewfs_hadoopconf_${CLUSTER}
	export HDFS_CONF_DIR=v20_hdfs_hadoopconf_${CLUSTER}
	#export NN1=gsbl90470.blue.ygrid
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
	export VIEWFS_CONF_DIR=v22_viewfs_hadoopconf_${CLUSTER}
	export HDFS_CONF_DIR=v22_hdfs_hadoopconf_${CLUSTER}
	#export NN1=gsbl90772.blue.ygrid
	unset HADOOP_CLASSPATH
fi
 

###################################################################
####### key variables  
###################################################################
export NN1=`hdft_getNN default`

if [ -z "$NN1" ] ; then
	echo "    $PROG_NAME: ERROR cannot find namenode $NN1. Aborted"
	echo "    $PROG_NAME: ERROR cannot find namenode $NN1. Aborted"
	return
	exit
fi

TMPFILE=`date '+Zcsmt_%m%d-%H%M%S'`
TEST_DATA_FILE="/homes/hdfsqa/hdfsRegressionData/helloworld.txt"


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
echo "    $PROG_NAME:: namenode found NN1=$NN1"

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

##  "POS or NEG, followed by the actual hadoop command with all arguments, then optional comment begins with '@:"
function test_CSMT2 {
   for CMD in \
	"POS_CMST_01"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR  dfs -ls /tmp/nn2.mt/file1 " \
	"POS_CMST_02"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ///tmp/nn2.mt/file1" \
	"POS_CMST_03"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //$NN1/tmp/nn2.mt/file1" \
	"POS_CMST_04"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:/tmp/nn2.mt/file1" \
	"POS_CMST_05"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1 " \
	"POS_CMST_06"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN1/tmp/nn2.mt/file1 " \
	"NEG_CSMT_07"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:/tmp/nn2.mt/file1"  		"@: /tmp is not a mount point" \
	"NEG_CSMT_08"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:///tmp/nn2.mt/file1"  		"@: /tmp is not a mount point"  \
	"NEG_CSMT_09"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://$NN1/tmp/nn2.mt/file1" \
	"NEG_CSMT_10"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //tmp/nn2.mt/file1"  			"@: //tmp is not a valid authorith"  \
	"POS_CMST_11"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt/file1" \
	"NEG_CSMT_12"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt/file1" 	"@: expect to work, but didn;t XXX" \
	"NEG_CSMT_13"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls vewfs://default/tmp/nn2.mt/dir1" 	"@: using viewfs would work" \
	"POS_CMST_14"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /tmp/nn4.mt/file1" \
	"NEG_CSMT_15"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //qatest/tmp/nn4.mt/file1" 		"@: //qatest should not work, but it does. Why? XXX " \
	"NEG_CSMT_16"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://qatest/tmp/nn4.mt/dir1" \
	"NEG_CSMT_17"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://qatest/tmp/nn4.mt/dir1" \
	"NEG_CSMT_18"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" \
 	\
	"NEG_CSMT_21"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR  dfs -ls /tmp/nn2.mt/file1 "			"@: viewfs cannot access to any /tmp as it is not a mount point" \
	"NEG_CSMT_22"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls ///tmp/nn2.mt/file1"		"@: /tmp not a valid mount point"  \
	"NEG_CSMT_23"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //$NN1/tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" \
	"NEG_CSMT_24"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:/tmp/nn2.mt/file1"		"@: /tmp not a valid mount point"  \
	"NEG_CSMT_25"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1 " 		"@: /tmp not a valid mount point" \
	"POS_CMST_26"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://$NN1/tmp/nn2.mt/file1 " 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_27"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" \
	"NEG_CSMT_28"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_29"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://$NN1/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_30"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" \
	"NEG_CSMT_31"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_32"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_33"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls vewfs://default/tmp/nn2.mt/dir1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_34"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /tmp/nn4.mt/file1" 			"@: /tmp not a valid mount point" \
	"NEG_CSMT_35"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //qatest/tmp/nn4.mt/file1" 		"@: /tmp not a valid mount point" \
	"NEG_CSMT_36"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://qatest/tmp/nn4.mt/dir1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_37"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://qatest/tmp/nn4.mt/dir1" 	"@: /tmp not a valid mount point" \
	"NEG_CSMT_38"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 			"@: /nn4 would work, not /nn4.mt" \
 	\
	"NEG_CSMT_41"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn2/file1 " 				"@: /nn2 or /nn4 are mount points, not dir. "	\
	"NEG_CSMT_42"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ///nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_43"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //$NN1/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_44"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_45"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///nn2/file1 "			"@: /nn2 or /nn4 are mount points, not dir. " \
	"NEG_CSMT_46"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN1/nn2/file1 " 		"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_47"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_48"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:///nn2/file1"			"@: /nn2 or /nn4 are mount points, not dir. " \
	"NEG_CSMT_49"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://$NN1/nn2/file1" 		"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_50"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //nn2/file1" 				"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_51"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_52"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/nn2/file1" 		"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_53"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_54"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4/dir1" 				"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_55"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //qatest/nn4/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_56"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://qatest/nn4/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_57"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://qatest/nn4/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "\
	"NEG_CSMT_58"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 			"@: /nn2 or /nn4 are mount points, not dir. "\
 	\
	"POS_CMST_61"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn2/file1 "			"@: viewfs mount point" \
	"POS_CMST_62"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls ///nn2/file1" 			"@: /// same as /  " \
	"POS_CMST_63"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //$NN1/nn2/file1" \
	"NEG_CSMT_64"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:/nn2/file1" 			"@: hdfs:/tmp would work "\
	"NEG_CSMT_65"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:///nn2/file1 "			"@: hdfs:///tmp would work "\
	"NEG_CSMT_66"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://$NN1/nn2/file1" 		"@: hdfs://$NN1/tmp/ would work  " \
	"POS_CMST_67"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/nn2/file1" \
	"POS_CMST_68"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///nn2/file1" \
	"NEG_CSMT_69"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://$NN1/nn2/file1" 		"@: /nn2 not a valid hdfs dir" \
	"NEG_CSMT_70"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //nn2/file1" 			"@: //nn2 not a valid authority" \
	"POS_CMST_71"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/nn2/file1" 		"@: //default/nn2 = /nn2 " \
	"NEG_CSMT_72"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://default/nn2/file1" 		"@: not a valid hdfs default " \
	"POS_CMST_73"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1" \
	"NEG_CSMT_74"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4/dir1" 				"@: should work? XXX " \
	"NEG_CSMT_75"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //qatest/nn4/file1" 		"@: //qatest need to be prefixed by viewfs:" \
	"NEG_CSMT_76"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://qatest/nn4/dir1" 		"@: should be viewfs://qatest, not hdfs " \
	"POS_CMST_77"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://qatest/nn4/dir1" \
	"NEG_CSMT_78"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 			"@: /nn4.mt not a valid hdfs dir" \
	\
	"POS_CMST_81"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /tmp/nn2.mt/file1 "\
	"POS_CMST_82"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls ///tmp/nn2.mt/file1" \
	"POS_CMST_83"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //$NN1/tmp/nn2.mt/file1" \
	"POS_CMST_84"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs:/tmp/nn2.mt/file1" \
	"POS_CMST_85"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs:///tmp/nn2.mt/file1 "\
	"POS_CMST_86"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://$NN1/tmp/nn2.mt/file1 "\
	"NEG_CSMT_87"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs:/tmp/nn2.mt/file1" \
	"NEG_CSMT_88"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs:///tmp/nn2.mt/file1" \
	"NEG_CSMT_89"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://$NN1/tmp/nn2.mt/file1" \
	"NEG_CSMT_90"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //tmp/nn2.mt/file1" \
	"POS_CMST_91"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //default/tmp/nn2.mt/file1" \
	"NEG_CSMT_92"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://default/tmp/nn2.mt/file1" \
	"NEG_CSMT_93"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://default/tmp/nn2.mt/dir1" \
	"POS_CMST_94"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /tmp/nn4.mt/file1" \
	"POS_CMST_95"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //qatest/tmp/nn4.mt/file1" \
	"NEG_CSMT_96"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://qatest/tmp/nn4.mt/dir1" \
	"NEG_CSMT_97"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://qatest/tmp/nn4.mt/dir1" \
	"NEG_CSMT_98"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /nn4.mt/dir1" \
	\
	"POS_CMST_101"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -ls /" 		"@: shows /tmp" \
	"POS_CMST_102"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /" 		"@: shows the mount points " \
	"POS_CMST_103"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn2.mt/" 	"@: viewfs:/nn2 is mounted to this directory" \
	"POS_CMST_104"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn4.mt/" 	"@: viewfs:/nn4 is mounted to this directory" \
	"POS_CMST_105"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn2" 		"@: should show the same content as POS_103" \
	"POS_CMST_106"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn2" 		"@: should show the same content as POS_103" \
	"NEG_CSMT_107"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn4" 		"@: should show the same content as POS_CMST_104" \
	"POS_CMST_108"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://default/nn2" 	"@: viewfs supports default as an authority" \
	"POS_CMST_109"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://qatest/nn4" 	"@: qatest is one of the configured  authority" \
	"NEG_CSMT_110"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn4" 		"@: qatest is not default authority " \
	"NEG_CSMT_111"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr vewfs:/nn2" 	"@: need to add the /nn2 mount point defintion to make this work "\
	"NEG_CSMT_112"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr vewfs:/nn4" 	"@: need to add the /nn4 mount point defintion to make this work "\

    do
	# run the commmand if not POS or NEG 
	case "$CMD" in 
	POS_CMST_*)
		TestId=$CMD
		TestType=POS
		;;
	NEG_CSMT_*)
		TestId=$CMD
		TestType=NEG
		;;
	\@:*)		# comment
		echo "		Comment: $CMD"
		;;
	*)
		(( Count = Count + 1 ))
	 	echo "###########################################"
		echo "## [$TestId] CMD $Count is:  $CMD "

		# Run thec command
		$CMD

		stat=$?
		if [ $stat == 0 ] && [ $TestType == POS ] ; then
			echo "[Pass] [exec - POS test] [$TestId] Cmd $Count is:  $CMD  "
		fi 
		if [ $stat != 0 ] && [ $TestType == POS ] ; then
			echo "[FAIL] [exec - POS test] [$TestId] Cmd $Count is:  $CMD  "
			(( NFAIL = NFAIL + 1 ))
		fi 
		if [ $stat != 0 ] && [ $TestType == NEG ] ; then
			echo "[Pass] [exec - NEG test] [$TestId] Cmd $Count is:  $CMD  "
		fi
		if [ $stat == 0 ] && [ $TestType == NEG ] ; then
			echo "[FAIL] [exec - NEG test] [$TestId] Cmd $Count is:  $CMD  "
			(( NFAIL = NFAIL + 1 ))
		fi
		echo " "
		;;
	esac
    done

    if [ $NFAIL == 0 ] ; then
	echo "Test Summary: $Count Tests. All PASS. No Fail"
    else
	(( NPASS = Count - NFAIL ))
	echo "Test Summary: $Count Tests. $NPASS Pass. $NFAIL FAIL."
    fi
}

function test_CSMT3 {
	echo "    test_CSMT3: Exercise All test cases"
	echo "    test_CSMT3: Exercise All test cases"

    run_one_csmt    "POS_CMST_01"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR  dfs -ls /tmp/nn2.mt/file1 " 		"@: HDFS case, default authority, should work"
    run_one_csmt  	"POS_CMST_02"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ///tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"POS_CMST_03"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //$NN1/tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"POS_CMST_04"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:/tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"POS_CMST_05"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1 " 		"@: HDFS case, should work"
    run_one_csmt  	"POS_CMST_06"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN1/tmp/nn2.mt/file1 " 		"@: HDFS case, should work"
    run_one_csmt  	"NEG_CSMT_07"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:/tmp/nn2.mt/file1"  		"@: /tmp is not a mount point" 
    run_one_csmt  	"NEG_CSMT_08"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:///tmp/nn2.mt/file1"  		"@: /tmp is not a mount point"  
    run_one_csmt  	"NEG_CSMT_09"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://$NN1/tmp/nn2.mt/file1" 		"@: HDFS case, should work"
    run_one_csmt  	"NEG_CSMT_10"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //tmp/nn2.mt/file1"  			"@: //tmp is not a valid authorith"  
    run_one_csmt  	"POS_CMST_11"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt/file1" 		"@: HDFS default case, should work?"
    run_one_csmt  	"NEG_CSMT_12"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt/file1" 	"@: expect to work, but didn;t XXX" 
    run_one_csmt  	"NEG_CSMT_13"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls vewfs://default/tmp/nn2.mt/dir1" 	"@: using viewfs would work" 
    run_one_csmt  	"POS_CMST_14"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /tmp/nn4.mt/file1" 				"@: HDFS case, defalt authority, should work"
    run_one_csmt  	"NEG_CSMT_15"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //qatest/tmp/nn4.mt/file1" 		"@: //qatest should not work, but it does. Why? XXX " 
    run_one_csmt  	"NEG_CSMT_16"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://qatest/tmp/nn4.mt/dir1" 
    run_one_csmt  	"NEG_CSMT_17"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://qatest/tmp/nn4.mt/dir1" 
    run_one_csmt  	"NEG_CSMT_18"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 

    run_one_csmt  	"NEG_CSMT_21"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR  dfs -ls /tmp/nn2.mt/file1 "			"@: viewfs cannot access to any /tmp as it is not a mount point" 
    run_one_csmt  	"NEG_CSMT_22"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls ///tmp/nn2.mt/file1"		"@: /tmp not a valid mount point"  
    run_one_csmt  	"NEG_CSMT_23"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //$NN1/tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_24"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:/tmp/nn2.mt/file1"		"@: /tmp not a valid mount point"  
    run_one_csmt  	"NEG_CSMT_25"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1 " 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"POS_CMST_26"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://$NN1/tmp/nn2.mt/file1 " 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_27"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_28"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_29"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://$NN1/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_30"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //tmp/nn2.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_31"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_32"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt/file1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_33"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls vewfs://default/tmp/nn2.mt/dir1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_34"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /tmp/nn4.mt/file1" 			"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_35"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //qatest/tmp/nn4.mt/file1" 		"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_36"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://qatest/tmp/nn4.mt/dir1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_37"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://qatest/tmp/nn4.mt/dir1" 	"@: /tmp not a valid mount point" 
    run_one_csmt  	"NEG_CSMT_38"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 			"@: /nn4 would work, not /nn4.mt" 
 
    run_one_csmt  	"NEG_CSMT_41"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn2/file1 " 				"@: /nn2 or /nn4 are mount points, not dir. "	
    run_one_csmt  	"NEG_CSMT_42"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ///nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_43"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //$NN1/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_44"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_45"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///nn2/file1 "			"@: /nn2 or /nn4 are mount points, not dir. " 
    run_one_csmt  	"NEG_CSMT_46"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN1/nn2/file1 " 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_47"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_48"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs:///nn2/file1"			"@: /nn2 or /nn4 are mount points, not dir. " 
    run_one_csmt  	"NEG_CSMT_49"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://$NN1/nn2/file1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_50"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //nn2/file1" 				"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_51"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/nn2/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_52"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/nn2/file1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_53"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_54"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4/dir1" 				"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_55"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //qatest/nn4/file1" 			"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_56"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://qatest/nn4/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_57"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls viewfs://qatest/nn4/dir1" 		"@: /nn2 or /nn4 are mount points, not dir. "
    run_one_csmt  	"NEG_CSMT_58"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 			"@: /nn2 or /nn4 are mount points, not dir. "
  
    run_one_csmt  	"POS_CMST_61"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn2/file1 "			"@: viewfs mount point" 
    run_one_csmt  	"POS_CMST_62"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls ///nn2/file1" 			"@: /// same as /  " 
    run_one_csmt  	"POS_CMST_63"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //$NN1/nn2/file1" 
    run_one_csmt  	"NEG_CSMT_64"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:/nn2/file1" 			"@: hdfs:/tmp would work "
    run_one_csmt  	"NEG_CSMT_65"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs:///nn2/file1 "			"@: hdfs:///tmp would work "
    run_one_csmt  	"NEG_CSMT_66"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://$NN1/nn2/file1" 		"@: hdfs://$NN1/tmp/ would work  " 
    run_one_csmt  	"POS_CMST_67"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/nn2/file1" 
    run_one_csmt  	"POS_CMST_68"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///nn2/file1" 
    run_one_csmt  	"NEG_CSMT_69"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://$NN1/nn2/file1" 		"@: /nn2 not a valid hdfs dir" 
    run_one_csmt  	"NEG_CSMT_70"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //nn2/file1" 			"@: //nn2 not a valid authority" 
    run_one_csmt  	"POS_CMST_71"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/nn2/file1" 		"@: //default/nn2 = /nn2 " 
    run_one_csmt  	"NEG_CSMT_72"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://default/nn2/file1" 		"@: not a valid hdfs default " 
    run_one_csmt  	"POS_CMST_73"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/nn2/dir1" 
    run_one_csmt  	"NEG_CSMT_74"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4/dir1" 				"@: should work? XXX " 
    run_one_csmt  	"NEG_CSMT_75"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //qatest/nn4/file1" 		"@: //qatest need to be prefixed by viewfs:" 
    run_one_csmt  	"NEG_CSMT_76"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls hdfs://qatest/nn4/dir1" 		"@: should be viewfs://qatest, not hdfs " 
    run_one_csmt  	"POS_CMST_77"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://qatest/nn4/dir1" 
    run_one_csmt  	"NEG_CSMT_78"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn4.mt/dir1" 			"@: /nn4.mt not a valid hdfs dir" 
    
    run_one_csmt  	"POS_CMST_81"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /tmp/nn2.mt/file1 "
    run_one_csmt  	"POS_CMST_82"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls ///tmp/nn2.mt/file1" 
    run_one_csmt  	"POS_CMST_83"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //$NN1/tmp/nn2.mt/file1" 
    run_one_csmt  	"POS_CMST_84"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs:/tmp/nn2.mt/file1" 
    run_one_csmt  	"POS_CMST_85"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs:///tmp/nn2.mt/file1 "
    run_one_csmt  	"POS_CMST_86"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://$NN1/tmp/nn2.mt/file1 "
    run_one_csmt  	"NEG_CSMT_87"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs:/tmp/nn2.mt/file1" 
    run_one_csmt  	"NEG_CSMT_88"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs:///tmp/nn2.mt/file1" 
    run_one_csmt  	"NEG_CSMT_89"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://$NN1/tmp/nn2.mt/file1" 
    run_one_csmt  	"NEG_CSMT_90"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //tmp/nn2.mt/file1" 
    run_one_csmt  	"POS_CMST_91"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //default/tmp/nn2.mt/file1" 
    run_one_csmt  	"NEG_CSMT_92"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://default/tmp/nn2.mt/file1" 
    run_one_csmt  	"NEG_CSMT_93"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://default/tmp/nn2.mt/dir1" 
    run_one_csmt  	"POS_CMST_94"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /tmp/nn4.mt/file1" 
    run_one_csmt  	"POS_CMST_95"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls //qatest/tmp/nn4.mt/file1" 
    run_one_csmt  	"NEG_CSMT_96"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls hdfs://qatest/tmp/nn4.mt/dir1" 
    run_one_csmt  	"NEG_CSMT_97"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls viewfs://qatest/tmp/nn4.mt/dir1" 
    run_one_csmt  	"NEG_CSMT_98"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -conf $HDFS_CONF_DIR/my-hdfs-scheme-only.xml -ls /nn4.mt/dir1" 
   
    run_one_csmt  	"POS_CMST_101"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -ls /" 		"@: shows /tmp" 
    run_one_csmt  	"POS_CMST_102"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /" 		"@: shows the mount points " 
    run_one_csmt  	"POS_CMST_103"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn2.mt/" 	"@: viewfs:/nn2 is mounted to this directory" 
    run_one_csmt  	"POS_CMST_104"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn4.mt/" 	"@: viewfs:/nn4 is mounted to this directory" 
    run_one_csmt  	"POS_CMST_105"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn2" 		"@: should show the same content as POS_103" 
    run_one_csmt  	"POS_CMST_106"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn2" 		"@: should show the same content as POS_103" 
    run_one_csmt  	"NEG_CSMT_107"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn4" 		"@: should fail as /nn4 is //qatest"
    run_one_csmt  	"POS_CMST_108"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://default/nn2" 	"@: viewfs supports default as an authority" 
    run_one_csmt  	"POS_CMST_109"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://qatest/nn4" 	"@: qatest is one of the configured  authority" 
    run_one_csmt  	"NEG_CSMT_110"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn4" 		"@: qatest is not default authority " 
    run_one_csmt  	"NEG_CSMT_111"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr vewfs:/nn2" 	"@: need to add the /nn2 mount point defintion to make this work "
    run_one_csmt  	"NEG_CSMT_112"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr vewfs:/nn4" 	"@: need to add the /nn4 mount point defintion to make this work "

}



## Runnning various app using CSMT
function test_App_CSMT2 {

	"NEG_CSMT_501"   "$HADOOP_CMD  --config $VIEWFS_CONF_DIR/ jar"
	"Note:: viewfs://default/nn2 is mounted to hdfs:/tmp/nn2.mt"
	"Note:: viewfs://qatest/nn4 is mounted to hdfs:/tmp/nn4.mt"
}

  
function run_one_csmt {
    TestId=$1
    TEST_CMD=$2
    TEST_NOTES=$3
    TEST_OUT="$2 ## $3"
    echo "###########################################"
    echo "TestId=$TestId"
    echo "TEST_CMD=$TEST_CMD"
    echo "TEST_NOTES=$TEST_NOTES"

    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi
 
	(( Count = Count + 1 ))
	echo "## [$TestId] TEST_CMD is:  $TEST_CMD "
	echo "   [Exec_notes]: $TestType Testr- $TEST_NOTES"

	# Run thec command
	$TEST_CMD
	statExec=$?
	hdftMapStatus $statExec $TestType
	statExec=$?
	hdftShowExecResult $statExec $TestId "$TEST_OUT"

	returnStat=$statExec
	hdftShowBriefTaskResult $statExec $TestId "$TEST_OUT"
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
    TEST_OUT="$2 -- $3"
	NFAIL=0

    echo "###########################################"
    echo "TestId=$TestId"
    echo "TEST_DIR=$TEST_DIR"
    echo "TEST_NOTES=$TEST_NOTES"

    if [[ $TestId =~ 'NEG' ]] ; then 
		TestType=NEG
    else
		TestType=POS
    fi
 
	TEST_STAT=0
	echo "## [$TestId] TEST_CMD is:  $TEST_CMD "
	echo "   [Exec_notes]: $TestType Test- $TEST_NOTES"

	# make sure the directory exists
	$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -stat $TEST_DIR
	statExec=$?
	hdftShowExecResult $statExec "$TestId" "existence of directory $TEST_DIR"
	if [ $statExec != 0 ] ; then
		(( NFAIL = $NAFIL +1 ))
		hdftShowExecResult $statExec $TestId "$TEST_OUT - Directory $TEST_DIR does not exist. Aborted."
	fi

	# make sure the tmp file does not exist
	$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -stat $TEST_DIR/$TMPFILE
	if [ $? == 0 ] ; then
		echo "File exists: $TEST_DIR/$TMPFILE. Will remove"
		$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -rmr -skipTrash  $TEST_DIR/$TMPFILE
		$HADOOP_CMD --config  $VIEWFS_CONF_DIR dfs -stat $TEST_DIR/$TMPFILE
		if [ $? == 0 ] ; then
			(( NFAIL = $NAFIL +1 ))
			hdftShowExecResult $? "$TestId" "ERROR: Cannot remove tmp file $TESTDIR/$TMPFILE"
		fi
	else
		echo "File does not exist: $TEST_DIR/$TMPFILE. OK to proceed"
	fi
		
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
	
	hdftShowTaskResult $NFAIL  $TestId "$TEST_OUT"
	#function junk 
	echo ""
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

function test_basic_cases {
	echo "    Exercise the case where mounted directorie/files can be accessed ...."
	echo "    Exercise the case where mounted directorie/files can be accessed ...."
	run_one_csmt "POS_CMST_100"   "$HADOOP_DFS_CMD                             dfs -ls /" 		"@: shows DFS /" 
	run_one_csmt "POS_CMST_101"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -ls /" 		"@: shows DFS / " 
	run_one_csmt "POS_CMST_102"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /" 		"@: shows the mount points " 
    run_one_csmt "POS_CMST_103"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn2.mt/" 	"@: viewfs:/nn2 is mounted to this directory" 
	run_one_csmt "POS_CMST_104"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/   dfs -lsr /tmp/nn4.mt/" 	"@: viewfs:/nn4 is mounted to this directory" 
	run_one_csmt "POS_CMST_105"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn2" 		"@: should show the same content as POS_103" 
	run_one_csmt "POS_CMST_106"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr ///nn2" 		"@: should show the same content as POS_103" 
	run_one_csmt "NEG_CSMT_107"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr /nn4" 		"@: //qatest/nn4 is valid, but not /nn4"
	run_one_csmt "POS_CMST_108"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://default/nn2" 	"@: viewfs supports default as an authority" 
	run_one_csmt "POS_CMST_109"   "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -lsr viewfs://qatest/nn4" 	"@: qatest is one of the configured  authority" 
	run_one_csmt "POS_CMST_110"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfsRegressionData" 	"@: Default home directory"
	run_one_csmt "POS_CMST_111"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls ./hdfsRegressionData" 	"@: Default home directory"
	run_one_csmt "POS_CMST_111"   "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://$NN1/user/hadoopqa/hdfsRegressionData" 	"@: Default home directory"
}

function test_bug_4129512 {
	echo "    Verify Bug:4129512i by running key tests here. "

    run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs:///tmp/nn2.mt/file1"	"@: pos - default hdfs authority"
    run_one_csmt     "POS_CMST_201"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://default/tmp/nn2.mt"	    "@: hdfs - //default works with hdfs config"
    run_one_csmt     "POS_CMST_202"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn2.mt"	    "@: hdfs - //default works with hdfs config"
    run_one_csmt     "POS_CMST_203"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /nn2"		"@: pos - good mounted dir "
    run_one_csmt     "POS_CMST_204"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /tmp"		"@: pos - good mounted dir"
    run_one_csmt     "POS_CMST_205"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls /"		"@: pos - show all  mounted dir"
    run_one_csmt     "POS_CMST_206"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:///"	"@: pos - good mounted dir"
    run_one_csmt     "POS_CMST_207"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/" "@: viewfs://default/ is same as /"
    run_one_csmt     "POS_CMST_208"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://default/nn2" "@: default - show nn2 "
    run_one_csmt     "NEG_CSMT_220"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/"	"@: neg - //default won't work. Need viewfs:"

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
    local TEST_OPTIONS=$4
    local TEST_NOTES=$5
    local TEST_OUT="INPUT=$2; OUTPUT=$3; $5"
    echo "###########################################"
    echo "TestId=$TestId"
    echo "TEST_INPUT_DIR=$TEST_INPUT_DIR"
    echo "TEST_OUTPUT_DIR=$TEST_OUTPUT_DIR"
    echo "TEST_OPTIONS=$TEST_OPTIONS"
    echo "TEST_NOTES=$TEST_NOTES"

    if [[ $TestId =~ 'NEG' ]] ; then 
	TestType=NEG
    else
	TestType=POS
    fi
 
	
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -stat $TEST_OUTPUT_DIR
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
	hdftShowExecResult $statExec $TestId "$TEST_OUT"

	returnStat=$statExec
	hdftShowTaskResult $statExec $TestId "$TEST_OUT"
	#function junk 
	echo ""
	return $returnStat;

}

# run this after calling setup dir, to make sure the directory exist and writable.
function run_test_csmt_write {
	run_csmt_write "POS_CSMT_280"   "/nn2"  "@: write test: write to a mounted directory /nn2"
	run_csmt_write "POS_CSMT_281"   "/nn3"  "@: write test: write to a mounted directory /nn3"
	run_csmt_write "POS_CSMT_282"   "/tmp"  "@: write test: write to a mounted directory /tmp"
	run_csmt_write "POS_CSMT_282"   "/mapred"  "@: write test: write to a mounted directory /tmp"
	run_csmt_write "POS_CSMT_282"   "/mapredsystem"  "@: write test: write to a mounted directory /tmp"
	run_csmt_write "POS_CSMT_283"   "/user/hadoopqa"  "@: write test: write to a mounted directory /user/hadoopqa"
	run_csmt_write "POS_CSMT_282"   "/jobtracker"  "@: write test: write to a mounted directory /tmp"
	run_csmt_write "POS_CSMT_284"   "viewfs:/nn2"  "@: write test: write to a mounted directory: viewfs:/nn2"
}

# /nn2, /nn3 are mounted directories
# use /nn2 for hdfs-csmt test; 
# use /nn2 for csmt-hdfs test; 
# use /nn2 /nn3 for csmt-csmt test;  
# 16 cases
function test_wordcount_viewfs {
	## INput from hdfs: output: using various form of csmt URI: /, viewfs:/, viewfs:///, viewfs://default/
    run_wordcount_csmt  "POS_CMST_300"  "hdfs://$NN1/user/hadoopqa/hdfsRegressionData/wordcount_input"	"/nn2/P300_wordcount_output"        \
		 	""   "@: pos - output to mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CMST_301"  "hdfs://$NN1/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs:/nn2/P301_wordcount_output" \
          	""   "@: pos - output to mounted dir with viewfs:/"
    run_wordcount_csmt  "POS_CMST_302"  "hdfs://$NN1/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs:///nn2/P302_wordcount_output" \
        	""   "@: pos - output to mounted dir with viewfs:///"
    run_wordcount_csmt  "POS_CMST_303"  "hdfs://$NN1/user/hadoopqa/hdfsRegressionData/wordcount_input"	"viewfs://default/nn2/P303_wordcount_output"  \
			""   "@: pos - output to viewfs://default/nn2"

	## Output to hdfs. Input: using various form of csmt URI: /, viewfs:/, viewfs:///, viewfs://default/
    run_wordcount_csmt  "POS_CMST_310"  "/nn2/wordcount_input"          "hdfs://$NN1/tmp/P310_wordcount_output"  \
			""   "@: pos - input from  mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CMST_311"  "viewfs:/nn2/wordcount_input"   "hdfs://$NN1/tmp/P311_wordcount_output"  \
			""   "@: pos - input from  mounted dir (/nn2) using viewfs:/"
    run_wordcount_csmt  "POS_CMST_312"  "viewfs:///nn2/wordcount_input" "hdfs://$NN1/tmp/P312_wordcount_output"  \
			""   "@: pos - input from  mounted dir (/nn2) using viewfs:///"
    run_wordcount_csmt  "POS_CMST_313"  "viewfs://default/nn2/wordcount_input" "hdfs://$NN1/tmp/P313_wordcount_output"  \
			""   "@: pos - input from  mounted dir (/nn2) using viewfs://default/"


	## both input and output: ussing various form of CMST URI: selected permutation. Go to the same mounted directory
    run_wordcount_csmt  "POS_CMST_320"  "/nn2/wordcount_input"	"/nn2/P320_wordcount_output"                  \
			""   "@: pos - input and output to mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CMST_321"  "/nn2/wordcount_input"	"viewfs:/nn2/P321_wordcount_output"           \
			""   "@: pos - input /nn2, output: viewfs:/nn2"
    run_wordcount_csmt  "POS_CMST_322"  "viewfs:/nn2/wordcount_input"	"viewfs://default/nn2/P322_wordcount_output"  \
			""   "@: pos - input /nn2, output to viewfs://default"
    run_wordcount_csmt  "POS_CMST_323"  "viewfs:///nn2/wordcount_input"	"/nn2/P323_wordcount_output"          \
			""   "@: pos - input from viewfs://, output to /nn2"

	## both input and output: using various form of CMST URI: selected permutation. Go to the different mounted directory
    run_wordcount_csmt  "POS_CMST_330"  "/nn2/wordcount_input"	"/nn3/P330_wordcount_output"                  \
			""   "@: pos - input and output to mounted dir (/nn2) without viewfs:/"
    run_wordcount_csmt  "POS_CMST_331"  "/nn2/wordcount_input"	"viewfs:/nn3/P331_wordcount_output"           \
			""   "@: pos - input /nn2, output: viewfs:/nn2"
    run_wordcount_csmt  "POS_CMST_332"  "viewfs:/nn2/wordcount_input"	"viewfs://default/nn3/P332_wordcount_output"  \
			""   "@: pos - input /nn2, output to viewfs://default"
    run_wordcount_csmt  "POS_CMST_333"  "viewfs:///nn2/wordcount_input"	"/nn3/P333_wordcount_output"          \
			""   "@: pos - input from viewfs://, output to /nn2"
}




function junky {
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls //default/"

 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs://xxxx/"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $VIEWFS_CONF_DIR/ dfs -ls viewfs:/"

 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //default/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  dfs -ls hdfs://xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls hdfs://xxxx/tmp/nn4.mt/file1"
 run_one_csmt     "POS_CMST_200"  "$HADOOP_DFS_CMD  --config $HDFS_CONF_DIR/ dfs -ls //xxxx/tmp/nn4.mt/file1"



}

function setup_dir {
	set -x
	# hadoop dfs -ls /tmp/nn2. 
	$HADOOP_DFS_CMD dfs -rmr -skipTrash /tmp/nn2.mt /tmp/nn3.mt /tmp/nn2 /tmp/nn4.mt /tmp/nn4 /tmp/user.mt /tmp/tmp.mt  \
		/tmp/mapred.mt /tmp/mapredsystem.mt /tmp/data.mt /nn2 /nn3

	# /tmp/nn2.mt and /tmp/nn4.mt :These two dir are  adequate for fs test. May need more for mapred app running

	$HADOOP_DFS_CMD dfs -mkdir -p /tmp/nn2.mt /tmp/nn4.mt /tmp/user.mt /tmp/user.mt/hadoopqa /tmp/tmp.mt /tmp/mapred.mt /tmp/mapredsystem.mt /tmp/data.mt

	$HADOOP_DFS_CMD dfs -mkdir -p /tmp/nn2.mt/dir1
	$HADOOP_DFS_CMD dfs -put /etc/group /tmp/nn2.mt/file1
	$HADOOP_DFS_CMD dfs -put ~/testfile.txt /tmp/nn2.mt/dir1/file2

	$HADOOP_DFS_CMD dfs -copyFromLocal /homes/hdfsqa/hdfsRegressionData/wordcount_input /tmp/nn2.mt

	$HADOOP_DFS_CMD dfs -mkdir -p /tmp/nn3.mt/dir1
	$HADOOP_DFS_CMD dfs -put /etc/group /tmp/nn3.mt/file1
	$HADOOP_DFS_CMD dfs -put ~/testfile.txt /tmp/nn3.mt/dir1/file2
	$HADOOP_DFS_CMD dfs -copyFromLocal /homes/hdfsqa/hdfsRegressionData/wordcount_input /tmp/nn3.mt

	# nn4 is mounted not to default, but to //qatest/
	$HADOOP_DFS_CMD dfs -mkdir -p /tmp/nn4.mt/dir1
	$HADOOP_DFS_CMD dfs -put /etc/group /tmp/nn4.mt/file1
	$HADOOP_DFS_CMD dfs -put ~/testfile.txt /tmp/nn4.mt/dir1/file2
	$HADOOP_DFS_CMD dfs -copyFromLocal /homes/hdfsqa/hdfsRegressionData/wordcount_input /tmp/nn4.mt

	$HADOOP_DFS_CMD dfs -chmod 777 /tmp/*.mt
	set -
	
}

# call setup_dir first, before calling this one
function setup_viewfs_dir {
	echo "    $HADOOP_DFS_CMD dfs -mkdir -p /user/hadoopqa	"
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -mkdir -p /user/hadoopqa	
	$HADOOP_DFS_CMD --config $VIEWFS_CONF_DIR   dfs -mkdir -p /user/hadoopqa	
}

function print_env {
	echo "    $PROG_NAME Env:: CLUSTER=$CLUSTER"
	echo "    $PROG_NAME Env:: WORKSPACE=$WORKSPACE"
	echo "    $PROG_NAME Env:: HDFT_TOP_DIR=$HDFT_TOP_DIR"
	echo "    $PROG_NAME Env:: NN1=$NN1"
	echo "    $PROG_NAME Env:: HADOOP_GOT_VERSION=$HADOOP_GOT_VERSION"
	echo "    $PROG_NAME Env:: HADOOP_CLASSPATH=$HADOOP_CLASSPATH"
	echo "    $PROG_NAME Env:: HADOOP_HOME=$HADOOP_HOME"
	echo "    $PROG_NAME Env:: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
	echo "    $PROG_NAME Env:: which hadoop: = `which hadoop`"


}

hdftKInitAsUser hadoopqa
print_env
print_notes

setup_dir
setup_viewfs_dir

test_wordcount_viewfs

# check the bug
test_bug_4129512

# check the basic file access cases
test_basic_cases

# testing the write of the directory
run_test_csmt_write

# test the wordcount with viewfs


test_CSMT3
print_env
exit

echo "Obsolete test:  test_CSMT2"

echo "egrep 'exec|Comment|Note::' output_file to see the summary"
exit



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
