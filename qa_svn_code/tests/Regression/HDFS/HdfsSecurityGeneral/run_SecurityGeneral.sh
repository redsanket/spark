
PROG_FULLNAME=$0
PROG=`basename "$PROG_FULLNAME" `

#
 echo "    ENV $PROG ENV:  Exported Env Variable pass in from the framework, defined in common_test_env.sh"
 echo "    ENV $PROG ENV:  WORKSPACE=$WORKSPACE"							# top directory
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

OLD_CWD=`pwd`
if [ -z "$JOB_SCRIPTS_DIR_NAME" ] ; then
	echo "    ERROR: $PROG_FULLNAME: JOB_SCRIPTS_DIR_NAME is not set. Aborted"
	echo "    ERROR: $PROG_FULLNAME: JOB_SCRIPTS_DIR_NAME is not set. Aborted"
	exit
fi

# SG_WORKSPACE: WORKSPACE used internally by HdfsSecurity scripts
# WORKSPACE_FRAMEWORK: WORKSPACE stashed away, provied by framework
pushd $JOB_SCRIPTS_DIR_NAME
export WORKSPACE_FRAMEWORK=$WORKSPACE
#unset WORKSPACE
#export WORKSPACE=$JOB_SCRIPTS_DIR_NAME
export SG_WORKSPACE=$JOB_SCRIPTS_DIR_NAME

 echo "    ENV $PROG ENV:  OLD_CWD=$OLD_CWD"
 echo "    ENV $PROG ENV:  CWD=`pwd`"
 echo "    ENV $PROG ENV:  updated WORKSPACE=$WORKSPACE"
 echo "    ENV $PROG ENV:  updated SG_WORKSPACE=$SG_WORKSPACE"
 echo "    ENV $PROG ENV:  updated WORKSPACE_FRAMEWORK=$WORKSPACE_FRAMEWORK"
 echo "    ENV $PROG ENV:  HADOOP_HOME=$HADOOP_HOME"
 echo "    ENV $PROG ENV:  HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
 echo "    ENV $PROG ENV:  which hadoop = `which hadoop`"


echo "#####################################################################"
echo "#####################################################################"
echo "############# Dump of Env Variables  ################################"
env | sort
echo "#####################################################################"
echo "#####################################################################"

echo "##########Need to mkdir TMPDIR or else mktemp will fail #######################"
echo "#####################################################################"
function junk100 {
echo "mkdir $TMPDIR; ls"
mkdir $TMPDIR
ls -ld $TMPDIR
}

TMPDIR_FRAMEWORK=$TMPDIR
unset TMPDIR
OWNER="cwchung"
sh ./hdfsRegression.sh

#####################################################################
### prepare to return. Restore the variable and directory context 
#####################################################################
WORKSPACE=$WORKSPACE_FRAMEWORK
TMPDIR=$TMPDIR_FRAMEWORK
popd

echo "    #### RESTORE KB STATUS"
echo "    #### RESTORE KB STATUS"
source $WORKSPACE/lib/library.sh
source $WORKSPACE/lib/user_kerb_lib.sh

# Restore the KB tickets, as some tests here will use expired KB tickets
getAllUserTickets
setKerberosTicketForUser $HADOOPQA_USER

exit

function testReturnErrorStatus {
	# Exported variable returned from each of each of test case: call displayTestCaseResult include $WORKSPACE/lib/library.sh
	source $WORKSPACE_FRAMEWORK/lib/library.sh
	TESTCASENAME="Hello"
	TESTCASE_DESC="My world "
	REASONS="Not Mentioned"
	COMMAND_EXIT_CODE=1
	displayTestCaseResult 
}


