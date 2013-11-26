#!/bin/sh 

### Layout:
### 	env setting and echo. Global env has specific prefix of variables
###	test routines definition
###	acutal test

### Naming convention
### run_TestFileRdOnly.sh -> run_NameOfTest.sh then NameOfTest will be used 
##	preTestFileRdOnly	any special set up 
##	execTestFileRdOnly	acutally running the test
##	postTestFileRdOnly	post processing of dat
##	checkTestFileRdOnly	compare the data

#XXX#  if [ -z "$HDFT_RUNTEST_SH" ] ; then
	echo "Source env and library from $INCLUDE"
	source "$HDFT_TOP_DIR/hdft_include.sh"
	source "$HDFT_TOP_DIR/src/hdft_util_lib.sh"
##XXX#fi

if [ -n $HDFT_VERBOSE ] && [ $HDFT_VERBOSE == 1 ] ; then
	echo "Verbose mode set: HDFT_VERBOSE is $HDFT_VERBOSE"
	# set -x
fi



# common env for test utilities

# 4 sets of directories and files:
# dir/file for local test
# dir/files for hadoop 
# expected results in hdfsMeta
# test results in artifacts

# export HDFD_TOP_DIR="/user/hadoopqa/hdfsRegressionData" 
# export HDFL_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression/hdfsRegressionData"
# export HDMETA_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression//hdfsRegressionData/hdfsMeta"
# export LOMETA_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression//hdfsRegressionData/localMeta"

echo "HDFD_TOP_DIR=$HDFD_TOP_DIR"
echo "HDFL_TOP_DIR=$HDFL_TOP_DIR"
echo "HDFL_ARTIFACTS_TOP_DIR=$HDFL_ARTIFACTS_TOP_DIR"
echo "HDFL_EXPECTED_TOP_DIR=$HDFL_EXPECTED_TOP_DIR"

# OPS = {ls, lsr, du, dus, stat, count }
echo  "HDFT_OPS_RDDIR=$HDFT_OPS_RDDIR"
echo  "HDFT_OPS_RDFILE=$HDFT_OPS_RDFILE"

# Input parameter: $* hadoop command to run (sands hadoop dfs)
# e.g. hdftExecHadoopDfsCmd -lsr /user/hadoopqa/HDFSRegressionData/

# first one to keep track of return status from exec a job, LOCAL_DIFF_RESULT tracking the number of times output is different
LOCAL_TASKS_RESULT=0
LOCAL_TASK_RESULT=0
LOCAL_DIFF_RESULT=0
LOCAL_EXEC_RESULT=0



###########################################################################################
###########################################################################################
###########################################################################################
###########################################################################################

# nothing special to do
preTestCoreDfs() {
	echo "" > /dev/null
}
postTestCoreDfs() {
	echo "" > /dev/null
}


checkTestCoreDfs() {
	echo "" > /dev/null
}

execOneCoreDfs() {
	LOCAL_TASK_RESULT=0
	echo "##########################################"
	# CORETEST_ID=$1 ; CORETEST_SUBID=$2 ; CORETEST_OP=$3 ; CORETEST_OUTPUT_ART=$4
	# CORETEST_EXPECTED_RESULT=$5; CORETEST_INPUT_DATA=$6 ; CORETEST_OUTPUT_DATA=$7

	echo "##########################################"
	CORETEST_ID=$1 ; CORETEST_SUBID=$2 ; CORETEST_OP=$3 ; CORETEST_OUTPUT_ART=$4 
	CORETEST_EXPECTED_RESULT=$5; CORETEST_STRING=$6 ; CORETEST_INPUT_DATA=$7 ; CORETEST_OPERAND=$8

	#  default: make use of the current directory relative to job_scripts, and the operations to synthesis the output and expected file name
	#  example for lsr: jobs_scripts/Basic/run_core.sh ==> Basic/run_core/lsr.out. 
	if [ -n "$CORETEST_OUTPUT_ART"  ] && [ "$CORETEST_OUTPUT_ART" == "default" ] ; then
		TASK_OUTPUT_ART=${HDFL_ARTIFACTS_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.out
	else
		TASK_OUTPUT_ART="${HDFL_ARTIFACTS_TOP_DIR}/${CORETEST_OUTPUT_ART}"
	fi
	rm -f $TASK_OUTPUT_ART	

	if [ $CORETEST_EXPECTED_RESULT == "default" ] ; then
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.out
	else
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${CORETEST_EXPECTED_RESULT}
	fi

	# These operations requires two operands to hadoop command
	# distcp requires both operands in HDFS. The other three require one in local file, one in HDFS
	TASK_INPUT_DATA="${HDFD_TOP_DIR}/${CORETEST_INPUT_DATA}"
	TASK_OPERAND_DEST=""
	# if [ $CORETEST_OP == "CopyToLocal" ] || [ $CORETEST_OP == "CopyFromLocal" ] || [ $CORETEST_OP == "distcp" ] || [ $CORETEST_OP == "get" ] ; then
	if [ $CORETEST_OP == "copyFromLocal" ] || [ $CORETEST_OP == "distcp" ] ; then
		case $CORETEST_OP in
		copyFromLocal)
			TASK_OPERAND_SRC=$HDFL_DATA_INPUT_TOP_DIR/$CORETEST_INPUT_DATA   
			TASK_OPERAND_DEST=${HDFD_TMPWRITE_TOP_DIR}/$CORETEST_INPUT_DATA
			;;
		distcp)
			TASK_OPERAND_SRC=$TASK_INPUT_DATA
			TASK_OPERAND_DEST=${HDFD_TMPWRITE_TOP_DIR}/$CORETEST_INPUT_DATA
			;;
		*)
			;;
		esac
	fi

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		echo "   CORETEST_ID=$CORETEST_ID"
		echo "   CORETEST_SUBID=$CORETEST_SUBID"
		echo "   CORETEST_OP=$CORETEST_OP"
		echo "   CORETEST_OUTPUT_ART=$CORETEST_OUTPUT_ART"
		echo "   CORETEST_EXPECTED_RESULT=$CORETEST_EXPECTED_RESULT"
		echo "   CORETEST_INPUT_DATA=$CORETEST_INPUT_DATA"
		echo "   TASK_INPUT_DATA=$TASK_INPUT_DATA"
		echo "   TASK_OUTPUT_ART=$TASK_OUTPUT_ART"
		echo "   TASK_EXPECTED_OUTPUT_ART=$TASK_EXPECTED_OUTPUT_ART"
		echo "   TASK_OPERAND_SRC=$TASK_OPERAND_SRC"
		echo "   TASK_OPERAND_DEST=$TASK_OPERAND_DEST"
	fi

	# create the artifact directory if not exist yet
	mkdir -p `dirname $TASK_OUTPUT_ART`
	cmd="-${CORETEST_OP} ${TASK_INPUT_DATA}"

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		dumpmsg "    ------------------------"
		dumpmsg "    Exec hdfs  $CORETEST_OP  >  ${TASK_OUTPUT_ART}"
	fi

	## Some operations require different invocation, or additional operands
	LOCAL_NEED_POST_PROCESSING=1

	case "$CORETEST_OP" in 
	"copyFromLocal" | "distcp" )
		# Argument is different from other hadoop command

		LOCAL_NEED_POST_PROCESSING=1
			# First remove the existing file/directory if exist
			$HADOOP_CMD dfs  -stat   $TASK_OPERAND_DEST
			if [ $? == 0 ] ; then
				echo "    DOIT::: hdfs  -rmr   -skipTrash ${TASK_OPERAND_DEST}"
				$HADOOP_CMD  dfs -rmr  -skipTrash  "${TASK_OPERAND_DEST}"
			fi
		if [ $CORETEST_OP == "copyFromLocal" ] ; then
			LOCAL_CMD="dfs -copyFromLocal"

			echo "    ######DOIT:::: hdfs $LOCAL_CMD  $TASK_OPERAND_SRC $TASK_OPERAND_DEST"
			$HADOOP_CMD dfs -copyFromLocal  $TASK_OPERAND_SRC $TASK_OPERAND_DEST

			stat=$?

			$HADOOP_CMD dfs -lsr ${TASK_OPERAND_DEST}    > $TASK_OUTPUT_ART
			statExec=$stat
		else
			CURRENT_NN=`hdft_getNN default`
			echo "   CURRENT_NN=$CURRENT_NN"
			LOCAL_CMD="distcp -fs "hdfs://${CURRENT_NN}.blue.ygrid:8020"  -p "
			echo "    ######DOIT:::: hadoop distcp -fs "hdfs://${CURRENT_NN}:8020"  -p  $TASK_OPERAND_SRC $TASK_OPERAND_DEST"
			hadoop distcp -fs "hdfs://${CURRENT_NN}.blue.ygrid:8020"  -p  $TASK_OPERAND_SRC $TASK_OPERAND_DEST

			stat=$?

			$HADOOP_CMD dfs -lsr ${TASK_OPERAND_DEST}    > $TASK_OUTPUT_ART
			statExec=$stat
		fi

		;;

	"copyToLocal" | "get" )
		echo "    DOIT:::: hdfs dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	"fsck")
		LOCAL_NEED_POST_PROCESSING=1
		echo "    DOIT::: hdfs     $CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART} "
		$HADOOP_CMD     $CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	"rm" | "rmr")
		LOCAL_NEED_POST_PROCESSING=1
		echo "    DOIT::: hdfs  dfs   -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART} "
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA 
		stat=$?
		echo "    #########DOIT::: hdfs dfs -lsr $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -lsr $TASK_INPUT_DATA >&  ${TASK_OUTPUT_ART}
		statExec=$stat
		;;
	*)
		echo "    DOIT:::: hdfs dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	esac

	(( LOCAL_EXEC_RESULT = $LOCAL_EXEC_RESULT + $statExec ))	

	msg="${CORETEST_ID}-${CORETEST_SUBID}"
	hdftShowExecResult "$statExec" "$msg" "$CORETEST_OP"

	# skip the result file comparison if exec fails?	
	if [ "$LOCAL_NEED_POST_PROCESSING" == 1 ] ; then
		hdftFilterOutput ${TASK_OUTPUT_ART} "$CORETEST_OP"
	fi

	hdftDoDiff ${TASK_OUTPUT_ART} ${TASK_EXPECTED_OUTPUT_ART}
	statDiff=$?; (( LOCAL_DIFF_RESULT = $LOCAL_DIFF_RESULT + $statDiff ))	

	hdftShowDiffResult "$statDiff" "$msg"  "$TASK_OUTPUT_ART" "$TASK_EXPECTED_OUTPUT_ART"

	(( LOCAL_TASK_RESULT = $statExec + $statDiff ))
	hdftShowTaskResult "$LOCAL_TASK_RESULT" "$msg" "$CORETEST_OP"

	(( LOCAL_TASKS_RESULT =  $LOCAL_TASKS_RESULT + $LOCAL_TASK_RESULT ))
	return $LOCAL_TASK_RESULT
}

#############################################
### Main test driver
### For each test, pass in
### argument: test-id, sub-id, test op, output dir/file, expected dir/default , input data file/dir
###    default: expected dir/default would be the same file hierarchy as otuput dir/file
###    Input data: if dir is provided and file operation (cat, tail ) is involved, then each of the fileis in the dir is 
#############################################


execOneTestOnly() {
	execOneCoreDfs "SF010" "01"    ls              default default  "NA" hdfsTestData/basic/smallFiles 	default
}

execAllTests() {
	execOneCoreDfs "SF010" "01"    ls              default default  "NA" hdfsTestData/basic/smallFiles 	default
	statLs=$?
	execOneCoreDfs "SF010" "02"    lsr             default default  "NA" hdfsTestData/basic/           	default
	statLsr=$?
	execOneCoreDfs "SF010" "04"    get             default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	statGet=$?
	execOneCoreDfs "SF010" "05"    cat             default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	statCat=$?
	execOneCoreDfs "SF010" "06"    tail            default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	statTail=$?
	execOneCoreDfs "SF010" "10"    stat            default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	statStat=$?
	execOneCoreDfs "SF010" "11"    fsck            default default  "NA" hdfsTestData/basic/           	default
	statFsck=$?
	execOneCoreDfs "SF010" "03"    copyToLocal     default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	statCopyToLocal=$?
	execOneCoreDfs "SF010" "90"    distcp          default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	statDistcp=$?
	execOneCoreDfs "SF010" "22"    copyFromLocal   default default  "NA" hdfsTestData/basic/smallFiles	default
	statCopyFromLocal=$?
	execOneCoreDfs "SF010" "07"    rm              default default  "NA" tmpWrite/hdfsTestData/basic/smallFiles/smallRDFile755	default
	statRm=$?
	execOneCoreDfs "SF010" "08"    rmr             default default  "NA" tmpWrite/hdfsTestData/basic	default
	statRmr=$?

	hdftShowTaskResult "$statLs"                "SF030-01" "dfs ls"
	hdftShowTaskResult "$statLsr"               "SF030-02" "dfs lsr"
	hdftShowTaskResult "$statCopyToLocal"       "SF030-03" "dfs copyToLocal"
	hdftShowTaskResult "$statGet"               "SF030-04" "dfs get"
	hdftShowTaskResult "$statCat"               "SF030-05" "dfs cat"
	hdftShowTaskResult "$statTail"              "SF030-06" "dfs ls"
	hdftShowTaskResult "$statRm"                "SF030-07" "dfs ls"
	hdftShowTaskResult "$statRmr"               "SF030-08" "dfs ls"
	hdftShowTaskResult "$statCopyFromLocal"     "SF030-10" "dfs ls"
}


# handle default directory for artifacts
my_progname=$0
HDFT_JOB_DEFAULT_REL_OUTPUT_DIR="BAD_BAD"
hdftGetDefaultRelOutputDir $my_progname
LOCAL_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR

echo "    HDFT_JOB_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR; LOCAL_DEFAULT_REL_OUTPUT_DIR = $LOCAL_DEFAULT_REL_OUTPUT_DIR"
#hdftSuperuserKInit

preTestCoreDfs

#export RUN_ONE_TEST=1
if [ -n "$RUN_ONE_TEST" ] ; then
	echo "RUN ONE TEST ONLY:  execOneOnly"
	execOneTestOnly
else
	execAllTests
fi
	

postTestCoreDfs
#checkTestCoreDfs

#hdftSuperuserKDestroy

(( LOCAL_TASKS_RESULT = $LOCAL_EXEC_RESULT + $LOCAL_DIFF_RESULT ))
echo "Result of running core test is =$LOCAL_TASKS_RESULT"
echo "Result=$LOCAL_TASKS_RESULT"
# echo "Result=0"

exit  "$LOCAL_TASKS_RESULT"

