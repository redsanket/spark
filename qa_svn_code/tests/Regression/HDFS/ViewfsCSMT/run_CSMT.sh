
#############################################################
## Standard library include
#############################################################
source $WORKSPACE/lib/library.sh  
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh


umask 0002
PROG_FULLNAME=$0
PROG=`basename "$PROG_FULLNAME" `

# Dump out the environment for debugging
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
 echo "    ENV $PROG ENV:  KRB5CCNAME=$KRB5CCNAME"
 echo "    ENV $PROG ENV:  KB Principal=`klist | fgrep 'Default principal'`"

OLD_CWD=`pwd`
if [ -z "$JOB_SCRIPTS_DIR_NAME" ] ; then
	echo "    ERROR: $PROG_FULLNAME: JOB_SCRIPTS_DIR_NAME is not set. Aborted"
	echo "    ERROR: $PROG_FULLNAME: JOB_SCRIPTS_DIR_NAME is not set. Aborted"
	exit
fi

export PATH=$HADOOP_HOME/bin:$PATH

# SG_WORKSPACE: WORKSPACE used internally by HdfsSecurity scripts
# WORKSPACE_FRAMEWORK: WORKSPACE stashed away, provied by framework
pushd $JOB_SCRIPTS_DIR_NAME
##ZZZ### export WORKSPACE_FRAMEWORK=$WORKSPACE
##ZZZ###unset WORKSPACE

#export WORKSPACE=$JOB_SCRIPTS_DIR_NAME
#ZZZ###export SG_WORKSPACE=$JOB_SCRIPTS_DIR_NAME

 echo "    ENV $PROG ENV:  OLD_CWD=$OLD_CWD"
 echo "    ENV $PROG ENV:  CWD=`pwd`"
 echo "    ENV $PROG ENV:  updated WORKSPACE=$WORKSPACE"
 echo "    ENV $PROG ENV:  updated SG_WORKSPACE=$SG_WORKSPACE"
 echo "    ENV $PROG ENV:  updated WORKSPACE_FRAMEWORK=$WORKSPACE_FRAMEWORK"
 echo "    ENV $PROG ENV:  HADOOP_HOME=$HADOOP_HOME"
 echo "    ENV $PROG ENV:  HADOOP_QA_ROOT=$HADOOP_QA_ROOT"
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

# this works for .20
function getDefaultNN20 {
	H=`grep -A 3 fs.default.name ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/core-site.xml | fgrep '<value>' | head -1 | awk -F/ '{print $3}' | awk -F. '{print $1}'`
	echo $H
}

##ZZZ### TMPDIR_FRAMEWORK=$TMPDIR
###ZZZ### unset TMPDIR

OWNER="cwchung"

if [ "$HADOOPQE_TEST_BRANCH" == "0.20" ] ; then
	MM=`getDefaultNN20`
	echo "    Default NN = $MM"

	# For V22 genViewfsConfig.sh V22 v22_viewfs_config_${CLUSTER} "${CLUSTER}"  gsbl90772 gsbl90772 gsbl90772 gsbl90772
	echo "    #### genViewfsConfig.sh V20 v20_viewfs_config_${CLUSTER} "${CLUSTER}"  $MM $MM $MM $MM"
	echo "    #### genViewfsConfig.sh V20 v20_viewfs_config_${CLUSTER} "${CLUSTER}"  $MM $MM $MM $MM"
	sleep 3
	sh genViewfsConfig.sh V20 v20_viewfs_config_${CLUSTER} "${CLUSTER}"  $MM $MM $MM $MM	

else    ## V22 branch and beyond
	MM=`hdft_getNN_V22 0`
	NN0=`hdft_getNN_V22 0`
	NN1=`hdft_getNN_V22 1`
	NN2=`hdft_getNN_V22 2`
	echo "    #### genViewfsConfig.sh V22 v22_viewfs_config_${CLUSTER} "${CLUSTER}"  $NN0 $NN1 $NN2"
	echo "    #### genViewfsConfig.sh V22 v22_viewfs_config_${CLUSTER} "${CLUSTER}"  $NN0 $NN1 $NN2"
	sleep 3
	sh genViewfsConfig.sh V22 v22_viewfs_config_${CLUSTER} "${CLUSTER}"  $NN0 $NN1 $NN2 
fi

	sleep 3
echo  "    ############################################# sh Run_testViewfs.sh"
sh Run_testViewfs.sh

#####################################################################
### prepare to return. Restore the variable and directory context 
#####################################################################
##ZZZ### WORKSPACE=$WORKSPACE_FRAMEWORK
##ZZZ### TMPDIR=$TMPDIR_FRAMEWORK
popd

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

