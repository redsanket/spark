
# this script report status back to the framework
# Paramter:

PROG_FULLNAME=$0
PROG=`basename "$PROG_FULLNAME" `
COMMAND_EXIT_CODE=$1
TESTCASENAME=$2
TESTCASE_DESC=$3
REASONS=$4

WORKSPACE=$WORKSPACE_FRAMEWORK
	echo "    ENV $PROG ENV:  WORKSPACE=$WORKSPACE"							# top directory
	echo "    ENV $PROG ENV:  SG_WORKSPACE=$SG_WORKSPACE"							# top directory
	echo "    ENV $PROG ENV:  updated WORKSPACE_FRAMEWORK=$WORKSPACE_FRAMEWORK"
	echo "    ENV $PROG ENV:  JOB_SCRIPTS_DIR_NAME=$JOB_SCRIPTS_DIR_NAME"	## full path to the parent of job-scripts/
	echo "    ENV $PROG ENV:  CLUSTER=$CLUSTER"


if [ -n "$WORKSPACE_FRAMEWORK" ]  && [ -f $WORKSPACE_FRAMEWORK/lib/library.sh ] ; then
	echo "    ENV $PROG ENV:  Found WORKSPACE_FRAMEWORK i: $WORKSPACE_FRAMEWORK/lib/library.sh"
	source $WORKSPACE_FRAMEWORK/lib/library.sh

	displayTestCaseResult  
else
	echo "    ENV $PROG ENV:  Did not find  WORKSPACE_FRAMEWORK or $WORKSPACE_FRAMEWORK/lib/library.sh"
fi


function dumpAllEnv {
#
	 echo "    ENV $PROG ENV:  Exported Env Variable pass in from the framework, defined in common_test_env.sh"
	 echo "    ENV $PROG ENV:  WORKSPACE=$WORKSPACE"							# top directory
	 echo "    ENV $PROG ENV:  WORKSPACE_FRAMEWORK=$WORKSPACE_FRAMEWORK"							# top directory
	 echo "    ENV $PROG ENV:  SG_WORKSPACE=$SG_WORKSPACE"
	 echo "    ENV $PROG ENV:  JOBS_SCRIPTS_DIR=$JOBS_SCRIPTS_DIR"
	 echo "    ENV $PROG ENV:  JOB_SCRIPTS_DIR_NAME=$JOB_SCRIPTS_DIR_NAME"	## full path to the parent of job-scripts/
	 echo "    ENV $PROG ENV:  HDFSSEC_ARTIFACTS_DIR=$HDFSSEC_ARTIFACTS_DIR"		## TODO
	 echo "    ENV $PROG ENV:  LOCAL_RESULT=$LOCAL_RESULT"
	 echo "    ENV $PROG ENV:  JOB=$JOB"			## dir name
	 echo "    ENV $PROG ENV:  STARTTIME=$STARTTIME"
	 echo "    ENV $PROG ENV:  TMPDIR=$TMPDIR"
	 echo "    ENV $PROG ENV:  HDFSSEC_ARTIFACTS_FILE=$HDFSSEC_ARTIFACTS_FILE"
	 echo "    ENV $PROG ENV:  TEST_SUMMARY_ARTIFACTS=$TEST_SUMMARY_ARTIFACTS"
	 echo "    ENV $PROG ENV:  TESTSUITE=$TESTSUITE"
	 echo "    ENV $PROG ENV:  CLUSTER=$CLUSTER"
	 echo "    ENV $PROG ENV:  ARTIFACTS=$ARTIFACTS"

	 echo "    ENV $PROG ENV:  OLD_CWD=$OLD_CWD"
	 echo "    ENV $PROG ENV:  CWD=`pwd`"
	 echo "    ENV $PROG ENV:  updated WORKSPACE=$WORKSPACE"
	 echo "    ENV $PROG ENV:  updated WORKSPACE_FRAMEWORK=$WORKSPACE_FRAMEWORK"
	 echo "    ENV $PROG ENV:  HADOOP_HOME=$HADOOP_HOME"
	 echo "    ENV $PROG ENV:  HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
	 echo "    ENV $PROG ENV:  which hadoop = `which hadoop`"

}
