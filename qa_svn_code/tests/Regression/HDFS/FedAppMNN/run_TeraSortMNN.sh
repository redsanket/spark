
###################################################
## Exercise Terasort on multiple NNs within a cluster
## Run Teragen to generate data on NN1
## Then run Terasort to read data from NN1 to output to NN2
## Then run Teravalidate from NN2 to output to NN3
## Since all data are generated, only hdfs: or -fs will be exercised. Can't exercse hftp:// or har://
###################################################
source $WORKSPACE/lib/library.sh
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

###################################################
## SETUP & Teardown
###################################################
function setup {
	OWNER="cwchung"
	USER=$HADOOPQA_USER
	HDFS_USER=$HADOOPQA_USER
	MY_HDFS_CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR"
	MY_HADOOP_CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR"
	setKerberosTicketForUser $HADOOPQA_USER
	export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH
	umask 0002		# create 775 dir/files

	#export  COMMAND_EXIT_CODE=0
	#export TESTCASE_DESC="Terasort application read/write across multiple namenodes within one cluster"
	#export REASONS="    ####"

	#global, return to framework
	SCRIPT_EXIT_CODE=0
	NUM_TESTCASE=0
	NUM_TESTCASE_PASSED=0
	TASK_EXIT_CODE=0
	
	# module-global
	PROGNAME=`basename $0`
	DFS_COMMAND=`basename "$HADOOP_COMMON_CMD"`

	# gsbl90772.blue.ygrid.yahoo.com
	NNX=$(getDefaultNameNode)

	#get the default FS: hdfs://gsbl.90772.blue.ygrid.yahoo.com:8020
	FS=$(getDefaultFS)

	# gsbl90772
	NN0=$(hdft_getNN 0)
	NN1=$(hdft_getNN 1)
	NN2=$(hdft_getNN 2)

	JOB_OPTION="-Dmapreduce.job.hdfs-servers=hdfs://$NN0.blue.ygrid:8020,hdfs://$NN1.blue.ygrid:8020,hdfs://$NN2.blue.ygrid:8020 -Djob.mapred.queuename=grideng  -Dmapred.job.numMapTasks=100 -Dmapreduce.job.numReduceTasks=20"
	

	#dir for federation common dfs opertions on MN
	# for different URI reference
	FED_APPMNN_DEFAULT_DATA_DIR="hdfsMNNData"
	FED_APPMNN_SLASH_DATA_DIR="/user/${HDFS_USER}/${FED_APPMNN_DEFAULT_DATA_DIR}"
	FED_APPMNN_FS_DATA_DIR="${FS}${FED_APPMNN_SLASH_DATA_DIR}"
	#FED_APPMNN_DST_DIR="$TMPDIR/${FED_APPMNN_DEFAULT_DATA_DIR}"
	FED_APPMNN_SLASH_DST_DIR="/tmp/${FED_APPMNN_DEFAULT_DATA_DIR}"
	
	FED_APPMNN_EXPECTED_DIR="$JOB_SCRIPTS_DIR_NAME/Expected"
	FED_APPMNN_ARTIFACTS_DIR="$ARTIFACTS_DIR"

	FED_APPMNN_OUTDIR=/tmp/D1
	FED_TGEN_OUTDIR=$FED_APPMNN_OUTDIR/TGEN
	FED_TSORT_OUTDIR=$FED_APPMNN_OUTDIR/TSORT
	FED_TVER_OUTDIR=$FED_APPMNN_OUTDIR/TVER

	#LOCAL_TMPDIR=$TMPDIR/L1
	LOCAL_TMPDIR=/tmp/L1.${CLUSTER}.${USER}
}

function teardown {
	local NAMENODES=`getNameNodes`	# Get list of namenodes
	NAMENODES=`echo $NAMENODES | tr ';' ' '`
	for nn in $NAMENODES; do
		echo "Delete files from $nn"
		deleteHdfsDir $FED_APPMNN_OUTDIR hdfs://$nn 0
		deleteHdfsDir $FED_APPMNN_SLASH_DATA_DIR hdfs://$nn 0
		deleteHdfsDir /tmp/D1 hdfs://$nn 0				
	done
	
}

function setupData {
	echo "Nothing to do"
}



function dumpEnv {
	echo "    [$PROGNAME] Env: PROGNAME=$PROGNAME"	
	echo "    [$PROGNAME] Env: FS=$FS"	
	echo "    [$PROGNAME] Env: NNX=$NNX."	
	echo "    [$PROGNAME] Env: NN0=$NN0."	
	echo "    [$PROGNAME] Env: NN1=$NN1."	
	echo "    [$PROGNAME] Env: NN2=$NN2."	
	echo "    [$PROGNAME] Env: TESTCASE_ID=$TESTCASE_ID"	
	echo "    [$PROGNAME] Env: TESTCASE_DESC=$TESTCASE_DESC"	
	echo "    [$PROGNAME] Env: FED_APPMNN_SLASH_DATA_DIR=$FED_APPMNN_SLASH_DATA_DIR"	
	echo "    [$PROGNAME] Env: FED_APPMNN_TESTS_SRC_DIR=$FED_APPMNN_TESTS_SRC_DIR"	
	echo "    [$PROGNAME] Env: FED_APPMNN_TESTS_DST_DIR=$FED_APPMNN_TESTS_DST_DIR"	
	echo "    [$PROGNAME] Env: FED_APPMNN_EXPECTED_DIR=$FED_APPMNN_EXPECTED_DIR"	
	echo "    [$PROGNAME] Env: FED_APPMNN_ARTIFACTS_DIR=$FED_APPMNN_ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: USER=$USER"				# Linux USER
	echo "    [$PROGNAME] Env: HDFS_USER=$HDFS_USER"	# HDFS USER
	echo "    [$PROGNAME] Env: FED_APPMNN_DEFAULT_DATA_DIR=$FED_APPMNN_DEFAULT_DATA_DIR"
	echo "    [$PROGNAME] Env: FED_APPMNN_FS_DATA_DIR=$FED_APPMNN_FS_DATA_DIR"	

	echo "    [$PROGNAME] Env: WORKSPACE=$WORKSPACE"	
	echo "    [$PROGNAME] Env: TMPDIR=$TMPDIR"	
	echo "    [$PROGNAME] Env: ARTIFACTS=$ARTIFACTS"	DOOP_HDFS_CMD} 
	echo "    [$PROGNAME] Env: ARTIFACTS_DIR=$ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: JOB=$JOB"	
	echo "    [$PROGNAME] Env: JOBS_SCRIPTS_DIR=$JOBS_SCRIPTS_DIR"	
	echo "    [$PROGNAME] Env: JOB_SCRIPTS_DIR_NAME=$JOB_SCRIPTS_DIR_NAME"	
	echo "    [$PROGNAME] Env: HADOOP_COMMON_CMD=$HADOOP_COMMON_CMD"	
	echo "    [$PROGNAME] Env: which hadoop=`which hadoop`"
	
	local cmd="${MY_HDFS_CMD}  dfs -ls $FED_APPMNN_SLASH_DATA_DIR"
	echo "    $cmd"
	eval  $cmd
}


TIDENT="FED_TSORT"		# Ident of this entire set of tests FED_DFS_100, _101, ...
TCASE_TYPE="POS"		# POS or NEG. Stateful. Normally set to POS. If want to do negative test, toggle to NEG, and the toggle back to POS.

#            $1     $2              $3                                        $4                                        $5                            $6
# MAPRED_OPTION=-Dmapreduce.job.hdfs-servers=hdfs://gsbl90773.blue.ygrid.yahoo.com:8020
function run_tera {
	TID="${TIDENT}_${1}"				# testId
	MY_SIZE=$2			# size, default 1000000
	MY_TGEN_DIR=${3}_${1}		# fully qualitifed input Dir of where teragen output the data
	MY_TSORT_DIR=${4}_${1}		# fully qualified output dir of where terasort output the data
	MY_TVER_DIR=${5}_${1}		# fully qualified output dir of where teravalidate output the data
	TESTCASE_DESC=$6
	TESTCASENAME="${TID}-${TESTCASE_DESC}"

	echo "    #### [$PROGNAME - test_run_tera] param: $*"
	echo "    #### [$PROGNAME - test_run_tera] param: TID=$TID"
	echo "    #### [$PROGNAME - test_run_tera] param: MY_SIZE=$MY_SIZE"
	echo "    #### [$PROGNAME - test_run_tera] param: MY_TGEN_DIR=$MY_TGEN_DIR"
	echo "    #### [$PROGNAME - test_run_tera] param: MY_TSORT_DIR=$MY_TSORT_DIR"
	echo "    #### [$PROGNAME - test_run_tera] param: MY_TVER_DIR=$MY_TVER_DIR"
	echo "    #### [$PROGNAME - test_run_tera] param: TESTCASE_DESC=$TESTCASE_DESC"
	echo "    #### [$PROGNAME - test_run_tera] param: JOB_OPTION=$JOB_OPTION"

	displayTestCaseMessage "$TID ==== $TESTCASE_DESC"

	if [ -z "$MY_SIZE" ] || [ "$MY_SIZE" == "DEFAULT" ] ; then
		MY_SIZE=1000000
	fi	
		
	hdftRmfHdfsDirs $MY_TGEN_DIR $MY_TSORT_DIR $MY_TVER_DIR 
	if [ $? != 0 ] ;then
	    setFailCase "FAILED to reomve directories $MY_TGEN_DIR $MY_TSORT_DIR $MY_TVER_DIR. Test aborted."
	    hdftReturnTaskResult 1 $TID "Remove existing directories to run app" "terasort "
	fi

	################################################
	# Begins 

	local 	cmd="${MY_HADOOP_CMD} jar $HADOOP_EXAMPLES_JAR  teragen  $JOB_OPTION  $MY_SIZE  $MY_TGEN_DIR"
	echo "    #### TCMD1=$cmd"
	eval $cmd
	if [ $? != 0 ] ; then
	    setFailCase "FAILED in exec Teragen. [$cmd]"
	    hdftReturnTaskResult 1 $TID "$cmd" "terasort "
		return
	fi

	# return 0 if dir exists
	${MY_HDFS_CMD} dfs -test -d $MY_TGEN_DIR
	if [ $? != 0 ] ; then
	    setFailCase "FAILED to create teragen output dir. [$cmd]"
	    hdftReturnTaskResult 1  $TID "$cmd" "terasort "
		return
	else
		cmd="${MY_HDFS_CMD} dfs -lsr $MY_TGEN_DIR"
		echo "    #### UCmd1: $cmd"
		eval $cmd
	fi

	cmd="${MY_HADOOP_CMD} jar $HADOOP_EXAMPLES_JAR  terasort  $JOB_OPTION  $MY_TGEN_DIR $MY_TSORT_DIR"
	echo "    #### TCMD2= $cmd"
	eval $cmd


	if [ $? != 0 ] ; then
	    setFailCase "FAILED to exec Terasort. [$cmd]"
	    hdftReturnTaskResult 1 $TID "$cmd" "terasort "
		return
	fi

	${MY_HDFS_CMD} dfs -test -d $MY_TSORT_DIR
	if [ $? != 0 ] ; then
	    setFailCase "FAILED to create tersort output dir. [$cmd]"
	    hdftReturnTaskResult 1  $TID "$cmd" "terasort "
		return
	else
		cmd="${MY_HDFS_CMD} dfs -lsr $MY_TSORT_DIR"
		echo "    #### UCmd2: $cmd"
		eval $cmd
	fi

	cmd="${MY_HADOOP_CMD} jar $HADOOP_EXAMPLES_JAR  teravalidate  $JOB_OPTION  $MY_TSORT_DIR $MY_TVER_DIR"
	echo "    #### TCMD3= $cmd"
	eval $cmd

	if [ $? != 0 ] ; then
	    setFailCase "FAILED in Teravalidate. [$cmd]"
	    hdftReturnTaskResult 1 $TID "$cmd" "teravalidate "
		return
	fi

	${MY_HDFS_CMD} dfs -test -d $MY_TVER_DIR
	if [ $? != 0 ] ; then
	    setFailCase "FAILED to create teragen output dir. [$cmd]"
	    hdftReturnTaskResult 1  $TID "$cmd" "teravalidate"
		return
	fi

	# Error if teravalidate
	#error	misorder in part-m-00001 between 64 c7 b8 3b 3a b7 a6 c7 7c 7a and 4e 81 69 63 8f 83 bd 1b 10 52
	#error	misorder in part-m-00001 between b6 03 5f 87 3b a7 9e 3b 28 8b and 24 ea 76 a2 44 ad c1 32 71 bd
	#error	bad key partitioning:
	# file part-m-00000:end key eb 74 0e eb 5d 8f c5 10 0b a5
	# file part-m-00001:begin key 4a 35 d8 62 02 3c 15 32 04 85

	cmd="${MY_HDFS_CMD} dfs -cat $MY_TVER_DIR/part-r-0000* | grep --quiet 'error'"
	echo "    #### UCmd3: $cmd"
	eval $cmd

	# grep return 0 on finding the pattern
	if [ $? == 0 ] ; then
	    setFailCase "FAILED in Teravalidate, error generated. [$cmd]"
		hdftReturnTaskResult 1  $TID "$cmd" "Terasort "
	else
		hdftReturnTaskResult 0  $TID "$cmd" "Terasort "
	fi

}

# Note: mapred job does not seem to support -fs option yet, such as:
# hadoop jar hadoop-examples.jar  teragen  -Dmapreduce.job.hdfs-servers=hdfs://gsbl90773.blue.ygrid:8020  -fs hdfs://gsbl90773.blue.ygrid:8020  10000  /tmp/D1/TGEN_120_90773
#

# do a full combination of running Teragen, Terasort and Teravalidate on very namenode.
function do_test1 {
	hdftDisplayTestGroupMsg "Test Terasort across multiple NNs"
	#            $1     $2              $3                                        $4                                        $5                            $6 
    #            ID     number_rows    $ tgen_outdir                            $tsort_outdir                             $tver_outdir                 	$descriptions 
	run_tera 	"100"  "10000" 	hdfs://$NN0.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN1.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN0,NN1,NN2"
#return
	run_tera 	"110"  "10000" 	hdfs://$NN0.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN0,NN2,NN1"
	run_tera 	"120"  "10000" 	hdfs://$NN1.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN1,NN2,NN0"

	# now output to /user/hadoopqa/directory
	FED_APPMNN_OUTDIR=/user/$HDFS_USER/T1
    FED_TGEN_OUTDIR=$FED_APPMNN_OUTDIR/TGEN
    FED_TSORT_OUTDIR=$FED_APPMNN_OUTDIR/TSORT
    FED_TVER_OUTDIR=$FED_APPMNN_OUTDIR/TVER

	run_tera 	"130"  "100000" 	hdfs://$NN1.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN0.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN1,NN0,NN2"
	run_tera 	"140"  "100000" 	hdfs://$NN2.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN1.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN2,NN1,NN0"
	run_tera 	"150"  "100000" 	hdfs://$NN2.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN0.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN2,NN0,NN1"

return
	# enable following if really want to stress run
 	run_tera 	"160"  "100000000" 	hdfs://$NN0.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN0.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN0,NN0,NN0"
 	run_tera 	"170"  "100000000" 	hdfs://$NN1.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN1.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN1,NN1,NN1"
 	run_tera 	"180"  "100000000" 	hdfs://$NN2.blue.ygrid/$FED_TGEN_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TSORT_OUTDIR   hdfs://$NN2.blue.ygrid/$FED_TVER_OUTDIR   "NN2,NN2,NN2"
}

################################################
## MAIN
################################################
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "ENTER MY TEST"
setup
#XXX#setupData
dumpEnv
do_test1

displayTestSuiteResult

teardown
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE






