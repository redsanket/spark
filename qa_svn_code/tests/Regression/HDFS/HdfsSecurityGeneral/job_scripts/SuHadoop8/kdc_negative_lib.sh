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
LOCAL_TMP_STDERR=`mktemp -p /tmp/ hdftmp_XXXXXX`
LOCAL_TASKS_RESULT=0
LOCAL_TASK_RESULT=0
LOCAL_DIFF_RESULT=0
LOCAL_EXEC_RESULT=0



###########################################################################################
###########################################################################################
###########################################################################################
###########################################################################################

# nothing special to do
preTestNegCoreDfs() {
	echo "" > /dev/null
}
postTestNegCoreDfs() {
	echo "" > /dev/null
}


checkTestNegCoreDfs() {
	echo "" > /dev/null
}

execOneNegCoreDfs() {
	echo "#==========================================================================#"
	echo "##########################################"
	CORETEST_ID=$1
	CORETEST_SUBID=$2
	CORETEST_OP=$3
	CORETEST_OUTPUT_ART=$4
	CORETEST_EXPECTED_RESULT=$5
	CORETEST_INPUT_DATA=$6
	CORETEST_SECOND_OPERAND=$7

	LOCAL_TASK_RESULT=0

	#  default: make use of the current directory relative to job_scripts, and the operations to synthesis the output and expected file name
	#  example for lsr: jobs_scripts/Basic/run_core.sh ==> Basic/run_core/lsr.out. 
	if [ -n "$CORETEST_OUTPUT_ART"  ] && [ "$CORETEST_OUTPUT_ART" == "default" ] ; then
		TASK_OUTPUT_ART=${HDFL_ARTIFACTS_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.err
	else
		TASK_OUTPUT_ART="${HDFL_ARTIFACTS_TOP_DIR}/${CORETEST_OUTPUT_ART}"
	fi

	if [ $CORETEST_EXPECTED_RESULT == "default" ] ; then
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.err
	else
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${CORETEST_EXPECTED_RESULT}
	fi

	# These operations requires two operands to hadoop command
	# distcp requires both operands in HDFS. The other three require one in local file, one in HDFS
	TASK_INPUT_DATA="${HDFD_TOP_DIR}/${CORETEST_INPUT_DATA}"
	TASK_OPERAND_DEST=""
	# if [ $CORETEST_OP == "CopyToLocal" ] || [ $CORETEST_OP == "CopyFromLocal" ] || [ $CORETEST_OP == "distcp" ] || [ $CORETEST_OP == "get" ] ; then
	if [ $CORETEST_OP == "copyFromLocal" ] || [ $CORETEST_OP == "distcp"  ] || [ $CORETEST_OP == "wordcount" ] ; then
		case $CORETEST_OP in
		"copyFromLocal" | "wordcount")
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
		echo "   LOCAL_TMP_STDERR=$LOCAL_TMP_STDERR"
		echo "   TASK_INPUT_DATA=$TASK_INPUT_DATA"
		echo "   TASK_OUTPUT_ART=$TASK_OUTPUT_ART"
		echo "   TASK_EXPECTED_OUTPUT_ART=$TASK_EXPECTED_OUTPUT_ART"
		echo "   TASK_OPERAND_SRC=$TASK_OPERAND_SRC"
		echo "   TASK_OPERAND_DEST=$TASK_OPERAND_DEST"
	fi

	# create the artifact directory if not exist yet
	rm -f $LOCAL_TMP_STDERR
	mkdir -p `dirname $TASK_OUTPUT_ART`

	cmd="-${CORETEST_OP} ${TASK_INPUT_DATA}"

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		dumpmsg "    ------------------------"
		dumpmsg "    Exec hadoop dfs ${cmd}  >  "
		dumpmsg "        ${TASK_OUTPUT_ART}"
	fi

	## Some operations require different invocation, or additional operands
	LOCAL_NEED_POST_PROCESSING=1	# error often has time

	case "$CORETEST_OP" in 
	"copyFromLocal" | "distcp" )

		# First remove the existing file/directory if exist (would fail due to no ticket)
		$HADOOP_CMD dfs -stat   $TASK_OPERAND_DEST
		if [ $? == 0 ] ; then
			echo "    DOIT::: hadoop dfs -rmr  ${TASK_OPERAND_DEST}"
			$HADOOP_CMD dfs -rmr  "${TASK_OPERAND_DEST}"
		fi
		
		$HADOOP_CMD fs -copyFromLocal  $TASK_OPERAND_SRC $TASK_OPERAND_DEST       2> ${LOCAL_TMP_STDERR}

		stat=$?

		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -lsr ${TASK_OPERAND_DEST}    > $TASK_OUTPUT_ART     2>> ${LOCAL_TMP_STDERR}
		statExec=$stat
		;;

	"distcp")
		echo "hadoop distcp -fs "${HFDT_NAMENODE}:8020"  -p   $TASK_OPERAND_SRC $TASK_OPERAND_DEST       2> ${LOCAL_TMP_STDERR}"
		hadoop distcp -fs "${HFDT_NAMENODE}:8020"  -p   $TASK_OPERAND_SRC $TASK_OPERAND_DEST       2> ${LOCAL_TMP_STDERR}
		stat=$?

		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -lsr ${TASK_OPERAND_DEST}    > $TASK_OUTPUT_ART     2>> ${LOCAL_TMP_STDERR}
		statExec=$stat
		;;

	"copyToLocal" | "get" )
		rm -f $TASK_OUTPUT_ART
		echo "    DOIT:::: hadoop dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}   2> ${LOCAL_TMP_STDERR}
		statExec=$?
		;;

	"tail" )
		rm -f $TASK_OUTPUT_ART
		echo "    DOIT:::: hadoop dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA    > ${TASK_OUTPUT_ART}   2> ${LOCAL_TMP_STDERR}
		statExec=$?
		;;
	"fsck") 
		LOCAL_NEED_POST_PROCESSING=1
		echo "    DOIT::: hadoop     $CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART} "
		$HADOOP_CMD     $CORETEST_OP $TASK_INPUT_DATA  1> ${TASK_OUTPUT_ART}  2> ${LOCAL_TMP_STDERR}
		statExec=$?
		;;
	"rm" | "rmr")
		LOCAL_NEED_POST_PROCESSING=1
		echo "    DOIT::: hadoop  dfs   -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART} "
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA                         2> ${LOCAL_TMP_STDERR}
		stat=$?
		echo "    #########DOIT::: hadoop dfs -lsr $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -lsr $TASK_INPUT_DATA           1> ${TASK_OUTPUT_ART}  2>> ${LOCAL_TMP_STDERR}
		statExec=$stat
		;;
	*)
		echo "    DOIT:::: hadoop dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA  1> ${TASK_OUTPUT_ART}  2> ${LOCAL_TMP_STDERR}
		statExec=$?
		;;
	esac
	
	#$ Negative test: reverse the stat
	echo "BEFORE CALLING hdftMapStatus $statExec 'kbneg' "
	hdftMapStatus $statExec 'kbneg'
	statExec=$?
	(( LOCAL_EXEC_RESULT = $LOCAL_EXEC_RESULT + $statExec ))	

	taskId="${CORETEST_ID}-${CORETEST_SUBID}"
	hdftShowExecResult "$statExec" "$taskId" "neg $CORETEST_OP"

	
	# Now check the error output
	echo "    Typing/cat out the file LOCAL_TMP_STDERR $LOCAL_TMP_STDERR"
	cat -n < ${LOCAL_TMP_STDERR}
	#cat < ${LOCAL_TMP_STDERR} | fgrep "Bad connection to FS. Command aborted. Exception:" | \
		#fgrep  ".security.sasl.SaslException: GSS initiate failed " | \
	##	fgrep "[Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)]"

	#fgrep -q -f $HDFT_TOP_DIR/src/etc/grep-kbsecurity.txt ${LOCAL_TMP_STDERR}
	hdftGrepPattern "kbsec" ${LOCAL_TMP_STDERR}

	statStderr=$?
	hdftShowStderrResult "$statStderr" "$taskId" "No valid credentials provided"  "neg $CORETEST_OP"

	(( LOCAL_TASK_RESULT = $statExec + $statStderr ))
	hdftShowTaskResult "$LOCAL_TASK_RESULT" "$taskId" "neg $CORETEST_OP"
	return $LOCAL_TASK_RESULT
	# this part of the test is too fragile, small change in the error output will break it
	# I removed this because the part above is enough to say that the operation was not executed
	# properly due to a security reason
	 
	#cat ${LOCAL_TMP_STDERR} >> ${TASK_OUTPUT_ART}

	#if [ "$LOCAL_NEED_POST_PROCESSING" == 1 ] ; then
	#	hdftFilterOutput ${TASK_OUTPUT_ART}
	#fi

	# Now doe the diff
	#hdftDoDiff $TASK_OUTPUT_ART $TASK_EXPECTED_OUTPUT_ART

	#statDiff=$?; 
	#hdftShowDiffResult "$statDiff" "$taskId"  "$TASK_OUTPUT_ART" "$TASK_EXPECTED_OUTPUT_ART"
	#hdftMapStatus $statDiff "diff"
	#statDiff=$?; (( LOCAL_DIFF_RESULT = $LOCAL_DIFF_RESULT + $statDiff ))	

	#(( LOCAL_TASK_RESULT = $statExec + $statDiff + $statStderr ))
	#hdftShowTaskResult "$LOCAL_TASK_RESULT" "$taskId" "neg $CORETEST_OP"

	#(( LOCAL_TASKS_RESULT = $LOCAL_TASKS_RESULT + $LOCAL_TASK_RESULT ))

	#return $LOCAL_TASK_RESULT
}


# parm: one of the three: no, expired, long-expired
hdftSetupBadKInit() {
	crentialType=$1

	echo "    Exec kdestroy to remove KB tickets"
	sleep 3
	hdftKDestroy
	klist 

	case $crednetialType in
	"no")
		return
		;;
	"expired")
		NDAY=1
		;;
	"long-expired")
		NDAY=10
		;;
	*)
		NDAY=1
	esac

	LOCAL_CUR_DATE=`date "+%s"`
	(( LOCAL_nDAY_AGO = $LOCAL_CUR_DATE - ${NDAY} * 86400 ))
	LOCAL_DATE_nDAY_AGO=`date -d@${LOCAL_nDAY_AGO} "+%Y%m%d%H%M%S" `
	echo "Current Date=`date`; epoch=$LOCAL_CUR_DATE; epoch 24 hours ago: $LOCAL_nDAY_AGO; date 24 hours ago: ${LOCAL_DATE_nDAY_AGO}"

	# get 30 seconds lease, start 24 hr ago - thus this would have been expired
	##kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab -s ${LOCAL_DATE_1DAY_AGO} -l 30s hadoopqa
	echo "    Getting Special KB Ticket: hdftKInit -s ${LOCAL_DATE_nDAY_AGO} -l 30s "
	hdftKInit -s ${LOCAL_DATE_nDAY_AGO} -l 30s 

	# echo "    Exec klist to show that KB Ticket should expire N hours ago:"
	# klist
	sleep 3


}

hdftCleanupBadKDestroy() {
	hdftKDestroy
	#hdftKInit
}

# Testing
#setupKBCredential "expired"
