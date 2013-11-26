#!/bin/sh 

#XXX#  if [ -z "$HDFT_RUNTEST_SH" ] ; then
	echo "Source env and library from $INCLUDE"
	source "$HDFT_TOP_DIR/hdft_include.sh"
	source "$HDFT_TOP_DIR/src/hdft_util_lib.sh"
##XXX#fi

if [ -n $HDFT_VERBOSE ] && [ $HDFT_VERBOSE == 1 ] ; then
	echo "Verbose mode set: HDFT_VERBOSE is $HDFT_VERBOSE"
	# set -x
fi

###########################################################################################
###########################################################################################
#### This is basic scrip to run a hadoop job.
#### Putting this outside of the core.sh: since running a job can involve different processing and status checking
###########################################################################################
###########################################################################################


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



# nothing special to do
preTestHJob() {
	echo "" > /dev/null
}
postTestHJob() {
	echo "" > /dev/null
}


checkTestHJob() {
	echo "" > /dev/null
}

execOneHadoopJob() {
	LOCAL_TASK_RESULT=0
	echo "##########################################"
	CORETEST_ID=$1 ; CORETEST_SUBID=$2 ; CORETEST_OP=$3 ; CORETEST_OUTPUT_ART=$4
	CORETEST_EXPECTED_RESULT=$5; CORETEST_INPUT_DATA=$6 ; CORETEST_OUTPUT_DATA=$7

	#  default: make use of the current directory relative to job_scripts, and the operations to synthesis the output and expected file name
	#  example for lsr: jobs_scripts/Basic/run_core.sh ==> Basic/run_core/lsr.out. 
	if [ -n "$CORETEST_OUTPUT_ART"  ] && [ "$CORETEST_OUTPUT_ART" == "default" ] ; then
		TASK_OUTPUT_ART=${HDFL_ARTIFACTS_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.out
	else
		TASK_OUTPUT_ART="${HDFL_ARTIFACTS_TOP_DIR}/${CORETEST_OUTPUT_ART}"
	fi

	if [ $CORETEST_EXPECTED_RESULT == "default" ] ; then
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.out
	else
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${CORETEST_EXPECTED_RESULT}
	fi

	# These operations requires two operands to hadoop command
	# distcp requires both operands in HDFS. The other three require one in local file, one in HDFS
	TASK_INPUT_DATA="${HDFD_TOP_DIR}/${CORETEST_INPUT_DATA}"
	TASK_OPERAND_DEST=""

	TASK_OPERAND_SRC=$TASK_INPUT_DATA

	if [ -z "$CORETEST_OUTPUT_DATA" ] || [ "$CORETEST_OUTPUT_DATA" == "default" ] ; then
		TASK_OPERAND_DEST=${HDFD_TMPWRITE_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}-out
	else
		TASK_OPERAND_DEST=${HDFD_TMPWRITE_TOP_DIR}/$CORETEST_OUTPUT_DATA
	fi

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		echo "   CORETEST_ID=$CORETEST_ID"
		echo "   CORETEST_SUBID=$CORETEST_SUBID"
		echo "   CORETEST_OP=$CORETEST_OP"
		echo "   CORETEST_OUTPUT_ART=$CORETEST_OUTPUT_ART"
		echo "   CORETEST_EXPECTED_RESULT=$CORETEST_EXPECTED_RESULT"
		echo "   CORETEST_INPUT_DATA=$CORETEST_INPUT_DATA"
		echo "   CORETEST_OUTPUT_DATA=$CORETEST_OUTPUT_DATA"
		echo "   TASK_INPUT_DATA=$TASK_INPUT_DATA"
		echo "   TASK_OUTPUT_ART=$TASK_OUTPUT_ART"
		echo "   TASK_EXPECTED_OUTPUT_ART=$TASK_EXPECTED_OUTPUT_ART"
		echo "   TASK_OPERAND_SRC=$TASK_OPERAND_SRC"
		echo "   TASK_OPERAND_DEST=$TASK_OPERAND_DEST"
		echo "   HADOOP_EXAMPLES_JAR=$HADOOP_EXAMPLES_JAR"
	fi

	# $ Preparation: create the necessary directory, and remove the existing HDFS output dir
	# $ create the artifact directory if not exist yet
	mkdir -p `dirname $TASK_OUTPUT_ART`

	$HADOOP_CMD dfs -stat   $TASK_OPERAND_DEST
	if [ $? == 0 ] ; then
		echo "    DOIT::: hdfs dfs -rmr  ${TASK_OPERAND_DEST}"
		echo "XXXXXXXXXXXXXXXXXXXXXXXX   hdfs dfs -rmr $TASK_OPERAND_DEST  XXXXXX"
		$HADOOP_CMD dfs -rmr  "${TASK_OPERAND_DEST}"
	fi

	cmd="-${CORETEST_OP} ${TASK_INPUT_DATA}"

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		dumpmsg "    ------------------------"
		dumpmsg "    Exec hdfs dfs ${cmd}  >  "
		dumpmsg "        ${TASK_OUTPUT_ART}"
	fi

	## Some operations require different invocation, or additional operands
	LOCAL_NEED_POST_PROCESSING=1

	case "$CORETEST_OP" in 
	"wordcount" )
		echo "    DOIT: hadoop jar $HADOOP_EXAMPLES_JAR wordcount $TASK_OPERAND_SRC $TASK_OPERAND_DEST"
		hadoop jar $HADOOP_EXAMPLES_JAR wordcount $TASK_OPERAND_SRC $TASK_OPERAND_DEST
		stat=$?
		statExec=$stat
		;;

	*)
		echo "    DOIT:::: hadoop dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		hadoop dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	esac

	(( LOCAL_EXEC_RESULT = $LOCAL_EXEC_RESULT + $statExec ))	

	msg="${CORETEST_ID}-${CORETEST_SUBID}"
	hdftShowExecResult "$statExec" "$msg"  "$CORETEST_OP"

	# skip the result file comparison if exec fails?	
	$HADOOP_CMD dfs -cat $HQ/tmpWrite/Basic/run_hjob/wordcount-out/* | sort -k2 -nr | head -20
	echo "hadoop dfs -cat $TASK_OPERAND_DEST/'*' | sort -k2 -nr > ${TASK_OUTPUT_ART} "
	$HADOOP_CMD dfs -cat "$TASK_OPERAND_DEST"/'*' | sort -k2 -nr > ${TASK_OUTPUT_ART}

	if [ "$LOCAL_NEED_POST_PROCESSING" == 1 ] ; then
		hdftFilterOutput ${TASK_OUTPUT_ART}
	fi

	hdftDoDiff ${TASK_OUTPUT_ART} ${TASK_EXPECTED_OUTPUT_ART}
	statDiff=$?; (( LOCAL_DIFF_RESULT = $LOCAL_DIFF_RESULT + $statDiff ))	

	hdftShowDiffResult "$statDiff" "$msg"  "$TASK_OUTPUT_ART" "$TASK_EXPECTED_OUTPUT_ART"

	(( LOCAL_TASK_RESULT = $statExec + $statDiff ))
	hdftShowTaskResult "$LOCAL_TASK_RESULT" "$msg" "$CORETEST_OP"

	(( LOCAL_TASK_RESULT = $LOCAL_TASK_RESULT + $LOCAL_TASK_RESULT ))
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
	#                  ID     subID  test     local_outfile expected_file      hdfs input         hdfs output of the job
	execOneHadoopJob "SF010" "09"    wordcount    default default  hdfsTestData/basic/smallFiles  run_hjob/wordcount-out
}

execAllTests() {
#	execOneHadoopJob "SF010" "9"    wordcount    default default  hdfsTestData/basic/smallFiles  run_hjob/wordcount-out
	execOneHadoopJob "SF010" "09"    wordcount    default default  hdfsTestData/basic/smallFiles  default
	statWordCount=$?
	hdftShowTaskResult "$statWordCount"         "SF030-09" "mapred wordcount"
}


# handle default directory for artifacts
my_progname=$0
HDFT_JOB_DEFAULT_REL_OUTPUT_DIR="BAD_BAD"
hdftGetDefaultRelOutputDir $my_progname
LOCAL_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR

echo "    HDFT_JOB_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR; LOCAL_DEFAULT_REL_OUTPUT_DIR = $LOCAL_DEFAULT_REL_OUTPUT_DIR"

 #hdftKInit

preTestHJob

# export RUN_ONE_TEST=1
if [ -n "$RUN_ONE_TEST" ] ; then
	echo "RUN ONE TEST ONLY:  execOneOnly"
	execOneTestOnly
else
	execAllTests
fi
	
postTestHJob

#hdftKDestroy

(( LOCALY_TASKS_RESULT = $LOCAL_EXEC_RESULT + $LOCAL_DIFF_RESULT ))
echo "Result of running core test is =$LOCAL_TASKS_RESULT"
echo "Result=$LOCAL_TASKS_RESULT"


exit  "$LOCAL_TASKS_RESULT"

