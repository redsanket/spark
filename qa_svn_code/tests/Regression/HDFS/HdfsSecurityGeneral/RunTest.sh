#!/bin/sh

echo "RUNTEST.SH            HDFT_TOP_DIR is $HDFT_TOP_DIR"
echo "RUNTEST.SH            HDFT_HOME is $HDFT_HOME"

source "hdft_include.sh"

echo "RUNTEST.SH            HDFT_HOME is $HDFT_HOME"

export HDFT_RUNTEST_SH=1

echo "================================================="
echo "   ENV $0: Running on cluster $CLUSTER" 
echo "   ENV WORKSPACE=$WORKSPACE"
echo "   ENV SG_WORKSPACE=$SG_WORKSPACE"
echo "   ENV WORKSPACE_FRAMEWORK=$WORKSPACE_FRAMEWORK"
echo "   ENV $0: Host name: `hostname`" echo "Current Time: `date`" "; Current user: `whoami`"
echo "   ENV $0: Current Directory: `pwd`"
echo "   ENV $0: HADOOP_HOME=$HADOOP_HOME"
echo "   ENV $0: HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME"
echo "   ENV $0: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "   ENV $0: HADOOP_VERSION=$HADOOP_VERSION"
echo "   ENV $0: HDFT_TOP_DIR=$HDFT_TOP_DIR"
echo "   ENV $0: HADOOP_CMD=$HADOOP_CMD"


# These two would be exported by yhudson. Put in here for now.
### export CLUSTER="omegaj"
### export WORKSPACE="/home/y/var/builds/workspace/HDFSRegression"

###############################################################################
#### Customized options in running this script
#### 1. Use HDFT_JOBS_TO_RUN to control which directory to run
#### 2. Then use HDFT_TASKS_TO_RUN 
#### Default is to run all HDFT_JOBS here and all tasks found in each directory
###############################################################################

#### set up HDFT_JOBS to overide which job to run
if [ -z "$HDFT_JOBS_TO_RUN" ] ; then
	##export HDFT_JOBS="ManaulInDev" 	# export HDFT_JOBS="Sanity Negative Basic Perm"
	export HDFT_JOBS="Basic" 	# export HDFT_JOBS="Sanity Negative Basic Perm"
else 
	export HDFT_JOBS=$HDFT_JOBS_TO_RUN
fi
echo "#################### HDFT_JOBS=$HDFT_JOBS"

makeArtifactsDir $HDFL_ARTIFACTS_TOP_DIR
displayEntryMessage


echo "HDFT_JOB_SCRIPTS_TOP_DIR=$HDFT_JOB_SCRIPTS_TOP_DIR"
echo "HDFL_ARTIFACTS_TOP_DIR=$HDFL_ARTIFACTS_TOP_DIR"

if [ -z "$HDFT_JOB_SCRIPTS_TOP_DIR" ] || [ -z "HDFL_ARTIFACTS_TOP_DIR" ] ; then
	echo "ERROR: env variable HDFT_JOB_SCRIPTS_TOP_DIR =[$HDFT_JOB_SCRIPTS_TOP_DIR] or HDFL_ARTIFACTS_TOP_DIR = [$HDFL_ARTIFACTS_TOP_DIR] is not set";
	exit 1
fi


#if [ -n "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
	hdftDumpEnv
#fi
echo "CLUSTER=$CLUSTER"
which hadoop
which java

# kinit before submitting the job
# kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa
hdftKInit


for HDFT_JOB in $HDFT_JOBS ; do
    export HDFT_JOB
    displayHeader "STARTING RUN of job $HDFT_JOB ..."

    export HDFT_JOB_TOP_DIR=${HDFT_JOB_SCRIPTS_TOP_DIR}/${HDFT_JOB}

    export LOCAL_RESULT=0
    export HDFSSEC_ARTIFACTS_DIR=${HDFL_ARTIFACTS_TOP_DIR}/${HDFT_JOB}
    export HDFSSEC_ARTIFACTS_FILE=${HDFSSEC_ARTIFACTS_DIR}/${HDFT_JOB}.log
    createLocalDir $HDFSSEC_ARTIFACTS_DIR

	if [ -z "$HDFT_TASKS_TO_RUN" ] ; then 
		ls ${HDFT_JOB_TOP_DIR}/Run_*.sh
		scripts=`ls ${HDFT_JOB_TOP_DIR}/Run_*.sh`
	else
		ls ${HDFT_JOB_TOP_DIR}/Run_*.sh | grep "${HDFT_TASKS_TO_RUN}" 
		scripts=`ls ${HDFT_JOB_TOP_DIR}/Run_*.sh | grep "${HDFT_TASKS_TO_RUN}" `
	fi
	
	rm -f $HDFSSEC_ARTIFACTS_FILE
        for script in $scripts; do

            echo "" | tee -a $HDFSSEC_ARTIFACTS_FILE
            echo "========= TEST:::: Executing script $script ..." | tee -a $HDFSSEC_ARTIFACTS_FILE
            sh $script 2>&1 | tee -a $HDFSSEC_ARTIFACTS_FILE

            SCRIPT_EXIT_CODE=`tail -1  $HDFSSEC_ARTIFACTS_FILE | awk -F= '{print $2}' `
	    if [ -z "$SCRIPT_EXIT_CODE" ] ; then
		SCRIPT_EXIT_CODE=1
	    fi
	    (( LOCAL_RESULT=$LOCAL_RESULT+$SCRIPT_EXIT_CODE ))
	done
	
	checkResult $LOCAL_RESULT
	(( HDFT_RESULT = $HDFT_RESULT + $LOCAL_RESULT))
done

SUMMARY_LOG=log/All-Summary.log
for HDFT_JOB in $HDFT_JOBS ; do
    HDFSSEC_ARTIFACTS_DIR=${HDFL_ARTIFACTS_TOP_DIR}/${HDFT_JOB}
    HDFSSEC_ARTIFACTS_FILE=${HDFSSEC_ARTIFACTS_DIR}/${HDFT_JOB}.log

	NTASKS=`fgrep XACT  $HDFSSEC_ARTIFACTS_FILE  | wc -l`
	NPASS=`fgrep XACT  $HDFSSEC_ARTIFACTS_FILE  | grep PASS | wc -l`
	NFAIL=`fgrep XACT  $HDFSSEC_ARTIFACTS_FILE  |  grep FAIL | wc -l`

	fgrep XACT  $HDFSSEC_ARTIFACTS_FILE  | sed -e "s#^#SUMMARY: #" | sed -e "s#\$# [$HDFT_JOB]#" 
	echo "SUMMARY of [$HDFT_JOB]: Total Tasks=$NTASKS; number of PASS: $NPASS; number of FAIL: $NFAIL" 
done

setBuildDescription


exit ${HDFT_RESULT}

