#!/bin/sh 

local_progname=$0

CUR_DIR=$HDFT_TOP_DIR/job_scripts/SuHadoop8
source "$CUR_DIR/kdc_negative_lib.sh"

my_progname=$0
HDFT_JOB_DEFAULT_REL_OUTPUT_DIR="BAD_BAD"
hdftGetDefaultRelOutputDir $my_progname
LOCAL_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR

echo "    HDFT_JOB_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR; LOCAL_DEFAULT_REL_OUTPUT_DIR = $LOCAL_DEFAULT_REL_OUTPUT_DIR"

function junkbad {

WHOAMI=`whoami`
if [ $WHOAMI == hadoopqa ]; then
	echo "ERRROR: this script cannot be run as hadoopqa as the test will interfere with KB ticket of haoodqa. It  has to be run as other user (e.g. hadoop1)."
	echo "ERRROR: this script cannot be run as hadoopqa as the test will interfere with KB ticket of haoodqa. It  has to be run as other user (e.g. hadoop1)."
	exit 1
fi
}

#############################################
### Main test driver
### For each test, pass in
### argument: test-id, sub-id, test op, output dir/file, expected dir/default , input data file/dir
###    default: expected dir/default would be the same file hierarchy as otuput dir/file
###    Input data: if dir is provided and file operation (cat, tail ) is involved, then each of the fileis in the dir is 
#############################################
execOneTestOnly() {
     #                      ID     subID  test     local_outfile expected_file      hdfs input             [hdfs output of the job]
	execOneNegCoreDfs "SF010" "12"    ls              default default  hdfsTestData/basic/smallFiles 
}

execAllTests() {
	execOneNegCoreDfs "SF010" "12"    ls              default default  hdfsTestData/basic/smallFiles 
	statNegLs=$?
	execOneNegCoreDfs "SF010" "13"    lsr             default default  hdfsTestData/basic/           
	statNegLsr=$?
	execOneNegCoreDfs "SF010" "14"    cat             default default  hdfsTestData/basic/smallFiles/smallRDFile755
	statNegCat=$?
	execOneNegCoreDfs "SF010" "15"    copyToLocal     default default  hdfsTestData/basic/smallFiles/smallRDFile755
	statNegCopyToLocal=$?
	execOneNegCoreDfs "SF010" "16"    get             default default  hdfsTestData/basic/smallFiles/smallRDFile755
	statNegGet=$?
	#execOneNegCoreDfs "SF010" "99"    wordcount       default default  hdfsTestData/basic/smallFiles/smallRDFile755
	statNegWordCount=$?

	execOneNegCoreDfs "SF030" "16"    tail            default default  hdfsTestData/basic/smallFiles/smallRDFile755

	hdftShowTaskResult "$statNegLs"            "SF030-12" 	"no tkt: dfs ls"
	hdftShowTaskResult "$statNegLsr"           "SF030-13" 	"no tkt: dfs lsr"
	hdftShowTaskResult "$statNegCat"           "SF030-14" 	"no tkt: dfs cat"
	hdftShowTaskResult "$statNegCopyToLocal"   "SF030-15" 	"no tkt: dfs copyToLocal"
	hdftShowTaskResult "$statNegGet"           "SF030-16" 	"no tkt: dfs get"
}


#####################################################################
# Execution begins here 
# No KB ticket. Pause 3 seconds for inspection
#####################################################################

hdftSetupBadKInit "no"

preTestNegCoreDfs

#export RUN_ONE_TEST=1
if [ -n "$RUN_ONE_TEST" ] ; then
	echo "RUN ONE TEST ONLY:  execOneOnly"
	execOneTestOnly
else
	execAllTests
fi
	
postTestNegCoreDfs
#checkTestNegCoreDfs

(( LOCAL_TASKS_RESULT = $LOCAL_EXEC_RESULT + $LOCAL_DIFF_RESULT ))

if [ $LOCAL_TASKS_RESULT == 0 ] ; then
	echo "PASS in runnnig $local_progname: LOCAL_TASK_RESULT=$LOCAL_TASKS_RESULT"
else
	echo "FAIL in runnnig $local_progname: LOCAL_TASK_RESULT=$LOCAL_TASKS_RESULT"
fi

echo "Result of running $local_progname =$LOCAL_TASKS_RESULT"
# echo "Result=0"

hdftCleanupBadKDestroy

exit  "$LOCAL_TASKS_RESULT"

