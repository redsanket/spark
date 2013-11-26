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
#### This is basic scrip to run permission test, which involves both positive and negative test
####  This require the constrution of test data set in /user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest
####  NOTE: RUN AS hdfs who is superuser of the cluster
###########################################################################################
###########################################################################################

HDFD_DEFAULT_PERM_DIR=$HDFD_TOP_DIR/hdfsTestData/PermTest2/

echo "HDFD_TOP_DIR=$HDFD_TOP_DIR"
echo "HDFL_TOP_DIR=$HDFL_TOP_DIR"
echo "HDFL_ARTIFACTS_TOP_DIR=$HDFL_ARTIFACTS_TOP_DIR"
echo "HDFL_EXPECTED_TOP_DIR=$HDFL_EXPECTED_TOP_DIR"
echo "HDFD_TOP_DIR=$HDFD_TOP_DIR"
echo "HDFD_DEFAULT_PERM_DIR=$HDFD_DEFAULT_PERM_DIR"

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

TEMP_PTFILE=`mktemp -p /tmp PermTest_XXXXX`


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

#                       1       2         3              4              5      6            7                  8              9     
#                      ID    SUB_ID    TestOp        run_as         loc out expected  neg_string          local_input   hdfs_operand
#   execOnePermJob   "SF020" "01"   dir_access       hadoopqa        default default "Permission denied:"   default         default
# Negative string pattern to look for: if -z, then it is a positive test case

execOnePermJob() {
	LOCAL_TASK_RESULT=0
	echo "##########################################"
	PERMTEST_ID=$1 ; PERMTEST_SUBID=$2 ; PERMTEST_OP=$3 ; 
	PERMTEST_RUNAS=$4
	PERMTEST_OUTPUT_ART=$5 PERMTEST_EXPECTED_RESULT=$6; 
	PERMTEST_NEG_STRING=$7
	PERMTEST_INPUT_DATA=$8 ; PERMTEST_OPERAND=$9

	if [  "$PERMTEST_NEG_STRING"  == "NA" ] ; then
		TASK_TEST_TYPE="POS"
		TASK_MAP_TYPE="perm"
	else
		TASK_TEST_TYPE="MIXED"
		TASK_MAP_TYPE="permneg"
	fi

	#  default: make use of the current directory relative to job_scripts, and the operations to synthesis the output and expected file name
	#  example for lsr: jobs_scripts/Basic/run_core.sh ==> Basic/run_core/lsr.out. 
	if [ -n "$PERMTEST_OUTPUT_ART"  ] && [ "$PERMTEST_OUTPUT_ART" == "default" ] ; then
		TASK_OUTPUT_ART=${HDFL_ARTIFACTS_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${PERMTEST_RUNAS}-${PERMTEST_OP}.out
	else
		TASK_OUTPUT_ART="${HDFL_ARTIFACTS_TOP_DIR}/${PERMTEST_OUTPUT_ART}"
	fi

	if [ $PERMTEST_EXPECTED_RESULT == "default" ] ; then
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${PERMTEST_RUNAS}-${PERMTEST_OP}.out
	else
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${PERMTEST_EXPECTED_RESULT}
	fi

	if [ $PERMTEST_INPUT_DATA == "default" ] ; then
		#TASK_INPUT_DATA=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${PERMTEST_RUNAS}-${PERMTEST_OP}.out
		TASK_INPUT_DATA=helloWorld755
	else
		#TASK_INPUT_DATA=${HDFL_EXPECTED_TOP_DIR}/${PERMTEST_INPUT_DATA}
		TASK_INPUT_DATA=$PERMTEST_INPUT_DATA
	fi


	if [ $PERMTEST_OPERAND == "default" ] ; then
		#TASK_INPUT_DATA=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${PERMTEST_RUNAS}-${PERMTEST_OP}.out
		TASK_HD_OPERAND=$HDFD_DEFAULT_PERM_DIR
	else
		#TASK_INPUT_DATA=${HDFL_EXPECTED_TOP_DIR}/${PERMTEST_INPUT_DATA}
		TASK_HD_OPERAND=${HDFD_TOP_DIR}/$PERMTEST_OPERAND
	fi


	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		echo "    PERMTEST_ID=$PERMTEST_ID"
		echo "    PERMTEST_SUBID=$PERMTEST_SUBID"
		echo "    PERMTEST_OP=$PERMTEST_OP"
		echo "    PERMTEST_NEG_STRING=$PERMTEST_NEG_STRING"
		echo "    PERMTEST_OUTPUT_ART=$PERMTEST_OUTPUT_ART"
		echo "    PERMTEST_EXPECTED_RESULT=$PERMTEST_EXPECTED_RESULT"
		echo "    PERMTEST_INPUT_DATA=$PERMTEST_INPUT_DATA"
		echo "    PERMTEST_OPERAND=$PERMTEST_OPERAND"
		echo "    TASK_INPUT_DATA=$TASK_INPUT_DATA"
		echo "    TASK_OUTPUT_ART=$TASK_OUTPUT_ART"
		echo "    TASK_EXPECTED_OUTPUT_ART=$TASK_EXPECTED_OUTPUT_ART"
		echo "    TASK_HD_OPERAND=$TASK_HD_OPERAND"
		echo "    TEMP_PTFILE=$TEMP_PTFILE"
		echo "    TASK_TEST_TYPE=$TASK_TEST_TYPE"
		echo "    TASK_MAP_TYPE=$TASK_MAP_TYPE"
	fi

	# $ Preparation: create the necessary directory, and remove the existing HDFS output dir
	# $ create the artifact directory if not exist yet
	mkdir -p `dirname $TASK_OUTPUT_ART`


	## Some operations require different invocation, or additional operands
	LOCAL_NEED_POST_PROCESSING=1

	statExec=1
	case "$PERMTEST_OP" in 
	"dir_ls" )
		echo '    hdfs dfs -lsr $TASK_HD_OPERAND`    >& $TEMP_PTFILE'
		$HADOOP_CMD dfs -lsr $TASK_HD_OPERAND  >& $TEMP_PTFILE
		stat=$?
		
		# hdftMapStatus $stat "permneg" 
		hdftMapStatus $stat $TASK_MAP_TYPE
		statExec=$?

		# the write test may create temporary touchz_file files
 		grep -v touchz_file $TEMP_PTFILE > $TASK_OUTPUT_ART
		;;

	"file_read")
		FLIST=`cat < $HDFT_TOP_DIR/src/etc/perm_files.txt | sed -e "s#^#$TASK_HD_OPERAND#" `
		echo 'hdfs dfs -cat `echo $FLIST | head -60c`'
		$HADOOP_CMD dfs -cat $FLIST        2> $TASK_OUTPUT_ART     1>/dev/null
		stat=$?
		hdftMapStatus $stat $TASK_MAP_TYPE
		statExec=$?
		;;

	"file_create" | "file_write" )
		# create temp and empty file in various PermTest directory. Some of them will fail.
		#$HADOOP_CMD dfs -lsr $TASK_HD_OPERAND/*/touchz*
		

		FLIST=`cat  < $HDFT_TOP_DIR/src/etc/perm_dirs.txt | sed -e "s#^#$TASK_HD_OPERAND/#"  |sed -e 's#$#/touchz_file#' `
		$HADOOP_CMD dfs -rm -skipTrash $FLIST >& /dev/null

		# either enable this two lines, or the next 6 lines, but the count of the lines will be different below
		echo "    hdfs dfs -touchz $FLIST"
		$HADOOP_CMD dfs -touchz $FLIST >&  $TASK_OUTPUT_ART 

		# should return failed, as some directory are not writable
		statExec=$?
		hdftMapStatus $statExec permneg
		statExec=$?
		$HADOOP_CMD dfs -ls $FLIST     >>  $TASK_OUTPUT_ART 2>&1

		#CW#NLINE_PERM_DENIED=`cat  $TASK_OUTPUT_ART | fgrep 'Permission denied' | grep 'hdfsqa' | grep '7[046][046]' | wc -l`
		#CW#NLINE_IN_FILE=`cat  $TASK_OUTPUT_ART  | grep -v -f "$HDFT_TOP_DIR/src/etc/grep-v.txt" | wc -l `
		#CW#echo "    Output file has NLINE_PERM_DENIED=$NLINE_PERM_DENIED lines of Permission Denied, out of NLINE_IN_FILE=$NLINE_IN_FILE total lines"

		#CW#if [ $TASK_TEST_TYPE == "MIXED" ] ; then
			#CW# ugly way of ascertain if the run is successful
			#CW#if [ $NLINE_IN_FILE == 6 ] && [ $NLINE_PERM_DENIED == 6 ] ; then
			#CW#    statExec=0
			#CW#else
			#CW#   statExec=1
			#CW#fi
		#CW#else	# positive test case: should have no Permission Denied
			#CW#if  [ $NLINE_PERM_DENIED == 0 ] && [ $NLINE_IN_FILE == 0 ] ; then
			#CW#   statExec=0
			#CW#else
			#CW#   statExec=1
			#CW#fi
		#CW#fi
		;;
	*)
		echo "    DOIT:::: hdfs dfs -$PERMTEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$PERMTEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	esac

	(( LOCAL_EXEC_RESULT = $LOCAL_EXEC_RESULT + $statExec ))	

	msg="${PERMTEST_ID}-${PERMTEST_SUBID}"
	hdftShowExecResult "$statExec" "$msg"  "permission-$PERMTEST_OP"


	# skip the result file comparison if exec fails?	

	if [ "$LOCAL_NEED_POST_PROCESSING" == 1 ] ; then
		hdftFilterOutput ${TASK_OUTPUT_ART}
	fi

	hdftDoDiff ${TASK_OUTPUT_ART} ${TASK_EXPECTED_OUTPUT_ART}
	statDiff=$?; 
	(( LOCAL_DIFF_RESULT = $LOCAL_DIFF_RESULT + $statDiff ))	

	hdftShowDiffResult "$statDiff" "$msg"  "$TASK_OUTPUT_ART" "$TASK_EXPECTED_OUTPUT_ART"

	(( LOCAL_TASK_RESULT = $statExec + $statDiff ))
	hdftShowTaskResult "$LOCAL_TASK_RESULT" "$msg" "permission-$PERMTEST_OP"
	
	(( LOCAL_TASKS_RESULT = $LOCAL_TASKS_RESULT + $LOCAL_TASK_RESULT ))
	return $LOCAL_TASK_RESULT
}

# sample expected error msg:
# lsr: could not get get listing for 'hdfs://gsbl90772.blue.ygrid.yahoo.com:8020/user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest/hdfsqa-users-Dir-700' : 
#      Permission denied: user=hadoopqa, access=READ_EXECUTE, inode="hdfsqa-users-Dir-700":hdfsqa:users:rwx------

#############################################
### Main test driver
### For each test, pass in
### argument: test-id, sub-id, test op, output dir/file, expected dir/default , input data file/dir
###    default: expected dir/default would be the same file hierarchy as otuput dir/file
###    Input data: if dir is provided and file operation (cat, tail ) is involved, then each of the fileis in the dir is 
#############################################


execOneTestOnly() {
    case    $EFFECTIVE_KB_USER in
    "hdfs") 
	# Super user test: should be able to read/write
	echo '    execOnePermJob   "SF024" "01"   dir_ls       hdfs      default default "NA"                     default         default'
	          execOnePermJob   "SF024" "01"   dir_ls       hdfs      default default "NA"                     default         default
	;;
    "hadoopqa" | *) 
	echo '    execOnePermJob   "SF020" "01"   dir_ls       hadoopqa  default default  "Permission denied:"  default         default'
	          execOnePermJob   "SF020" "01"   dir_ls       hadoopqa  default default  "Permission denied:"  default         default
	;;
    esac
}

execAllTests() {
	# run as hadoopqa, group users on linux, but mapped to hadoopqa, group hdfs on HDFS

	#                 1       2         3         4        5      6            7                  8              9     
	#                 ID    SUB_ID    TestOp    run by    loc out expected  neg_string          local_input   hdfs_oeprand

    case    $EFFECTIVE_KB_USER in
    hdfs ) 
	# Super user test: should be able to read/write
	echo '    execOnePermJob   "SF024" "01"   dir_ls       hdfs      default default "NA"                     default         default'
	          execOnePermJob   "SF024" "01"   dir_ls       hdfs      default default "NA"                     default         default
		  statLs=$?
		  hdftShowTaskResult "$statLs"    "SF024-02" "permission ls"
		  hdftShowTaskResult "$statLs"    "SF024-03" "permission ls"

	echo '    execOnePermJob   "SF024" "04"   file_read    hdfs      default default "NA"                     default         default'
	          execOnePermJob   "SF024" "04"   file_read    hdfs      default default "NA"                     default         default
		  statRead=$?

	echo '    execOnePermJob   "SF024" "05"   file_create   hdfs      default default "NA"                     default         default'
	          execOnePermJob   "SF024" "05"   file_create   hdfs      default default "NA"                     default         default
		  statWrite=$?

		  hdftShowTaskResult "$statWrite" "SF024-06" "permission ls"
		  hdftShowTaskResult "$statWrite" "SF024-07" "permission ls"
	;;
    hadoopqa|*) 
	          execOnePermJob   "SF020" "01"   dir_ls       hadoopqa  default default  "Permission denied:"  default         default
		  statLs=$?
		  hdftShowTaskResult "$statLs"    "SF020-02" "permission ls"
		  hdftShowTaskResult "$statLs"    "SF020-03" "permission ls"

	          execOnePermJob   "SF020" "04"   file_read    hadoopqa  default default  "Permission denied:"   default         default
		  statRead=$?

	          execOnePermJob   "SF020" "05"   file_create  hadoopqa  default default  "Permission denied:"   default         default
		  statWrite=$?
		  hdftShowTaskResult "$statWrite"    "SF020-06" "permission write"
		  hdftShowTaskResult "$statWrite"    "SF020-07" "permission write"
	;;
    esac
}


# handle default directory for artifacts
my_progname=$0
HDFT_JOB_DEFAULT_REL_OUTPUT_DIR="BAD_BAD"
hdftGetDefaultRelOutputDir $my_progname
LOCAL_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR

echo "    HDFT_JOB_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR; LOCAL_DEFAULT_REL_OUTPUT_DIR = $LOCAL_DEFAULT_REL_OUTPUT_DIR"

WHOAMI=`whoami`

preTestHJob

#export RUN_ONE_TEST=1
if [ -n "$RUN_ONE_TEST" ] ; then
	echo "RUN ONE TEST ONLY:  execOneOnly"
	execOneTestOnly
else
	execAllTests
fi
	
postTestHJob

(( LOCALY_TASKS_RESULT = $LOCAL_EXEC_RESULT + $LOCAL_DIFF_RESULT ))
echo "Result of running core test is =$LOCAL_TASKS_RESULT"
echo "Result=$LOCAL_TASKS_RESULT"
# echo "Result=0"

exit  "$LOCAL_TASKS_RESULT"

