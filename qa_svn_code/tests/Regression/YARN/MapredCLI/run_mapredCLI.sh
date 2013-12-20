#!/bin/sh

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh
# . $WORKSPACE/lib/restart_CLUSTER_lib.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`
customConfDir="/homes/$USER_ID/CustomConfDir"
customSetUp="DEFAULT"

############################################
# This function takes a param $1  and passes it 
# to the list command so that it can list all jobs
# This is optional and if not passed will list only 
# the current running job
############################################
function executeJobListCommand {
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list $1 >$ARTIFACTS_DIR/joblist.out 2>&1
}

#############################################################
# This function dumps the captured log
# Takes the file content to be displayed as $1
#############################################################
function dumpCapturedOutput {
    echo "************************************************" >> $ARTIFACTS_FILE 2>&1
    echo "Output of file is ..........." >> $ARTIFACTS_FILE 2>&1
    cat $1 >> $ARTIFACTS_FILE 2>&1
    echo "end output ..............." >> $ARTIFACTS_FILE 2>&1
    echo "************************************************" >> $ARTIFACTS_FILE 2>&1
}

#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
# Params: $2 is the taskType as MAP REDUCE
# Params: $1 is the state as running or completed
#############################################################
function getAttemptIdsCountForJobId {
    local myjobId=$1
    local taskType=$2
    local state=$3
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobId $taskType $state |grep  attempt_ |wc -l
}


function test_mapredCommandWithNoArgs {
    TESTCASE_DESC="test_mapredCommandWithNoArgs"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1 

    $HADOOP_MAPRED_HOME/bin/mapred > $captureFile
    
    local usage=`cat $captureFile | grep Usage | wc -l`

    if [ $usage -ne 1 ] ; then
        echo "**********************************"
        cat $captureFile
        echo "**********************************"
        echo "The output of the command does not contain the Usage message "   >> $ARTIFACTS_FILE 2>&1 
        setFailCase "The output of the command does not contain the Usage message "    
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE     
}

function test_mapredCommandWithInvalidArgs {
    TESTCASE_DESC="test_mapredCommandWithInvalidArgs"
    TESTCASE_ID="Status01"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    COMMAND_EXIT_CODE=1
 
    $HADOOP_MAPRED_HOME/bin/mapred mradminnnnnn > $captureFile

    local invalid=`cat $captureFile | grep invalid | wc -l`

    if [ $invalid -ne 1 ] ; then
        echo "**********************************"
        cat $captureFile
        echo "**********************************"
        echo "The output of the command does not contain invalid command error text " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The output of the command does not contain invalid command error text "
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobStatusBasedOnJobId {
    TESTCASE_DESC="test_getJobStatusBasedOnJobId"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    submitSleepJob 10 10 5000 5000 1 

    local myjobId=`getJobId`
    if [ $myjobId == "0" ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile 2>&1
    
    status=`cat $captureFile | grep "Job state: " | grep -o --regexp="PREP\|RUNNING\|SUCCEEDED" `

    if [ $status == "" ] ; then
        echo "The returned job status '$status' is not what is expected (PREP|RUNNING|SUCCEEDED)" >> $ARTIFACTS_FILE 2>&1
        setFailCase "The returned job status '$status' is not what is expected (PREP|RUNNING|SUCCEEDED) "
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi

    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobStatusBasedOnCompletedJobId {
    TESTCASE_DESC="test_getJobStatusBasedOnCompletedJobId"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR \
        jar $HADOOP_MAPRED_TEST_JAR sleep \
        -Dmapreduce.job.user.name=$USER -m 10 -r 10 -mt 1000 -rt 1000 > $captureFile 2>&1 

    local myjobId=`grep -o -m 1 --regexp="job_[0-9_]*" $captureFile`
    echo "myjobId is $myjobId"

    if [ $myjobId == "" ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile

    status=`cat $captureFile | grep "Job state: " | grep -o --regexp="PREP\|RUNNING\|SUCCEEDED\|COMPLETED" `

    if [ $status != "SUCCEEDED" ] ; then
        echo "The returned job status '$status' is not what is expected (SUCCEEDED) " >> $ARTIFACTS_FILE 2>&1     
        setFailCase " The returned job status '$status' is not what is expected (SUCCEEDED) "
        windUp   
        return
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobStatusBasedOnKilledJobId {
    TESTCASE_DESC="test_getJobStatusBasedOnKilledJobId"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR \
	jar $HADOOP_MAPRED_TEST_JAR sleep \
	-Dmapreduce.job.user.name=$USER -m 10 -r 10 -mt 5000 -rt 5000 > $captureFile 2>&1 

    local myjobId=`grep -o -m 1 --regexp="job_[0-9_]*" $captureFile`

    if [ $myjobId ==  "" ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    local myresp=`killJob $myjobId`
    if [ $myresp != "Killed" ] ; then
        echo "Unable to kill the job and so failing the test case  " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Unable to kill the job and so failing the test case  "
        windUp
        return
    fi 

    sleep 10 

    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile 2>&1

    status=`cat $captureFile | grep "Job state: " | grep -o --regexp="PREP\|RUNNING\|SUCCEEDED\|COMPLETED\|KILLED" `

    if [ $status == "SUCCEEDED" ] || [ $status == "KILLED" ] ; then
	COMMAND_EXIT_CODE=0
    else
        echo "The returned job status '$status' is not what is expected (0.23 KILLED, 2.0 SUCCEEDED) " >> $ARTIFACTS_FILE 2>&1     
        setFailCase "The returned job status '$status' is not what is expected (0.23 KILLED, 2.0 SUCCEEDED) "
        windUp   
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_getJobStatusBasedOnFailedJobId {
    TESTCASE_DESC="test_getJobStatusBasedOnFailedJobId"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR \
        jar $HADOOP_MAPRED_TEST_JAR sleep \
        -Dmapreduce.job.user.name=$USER -m 10 -r 10 -mt 5000 -rt 5000 > $captureFile 2>&1

    local myjobId=`grep -o -m 1 --regexp="job_[0-9_]*" $captureFile`
    echo "myjobId is $myjobId"

    if [ $myjobId ==  0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

#   failAJob $myjobId

    attID=`echo $jobID | sed 's/job/attempt/g' | sed 's/$/_m_000000_0/g'`
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $attID

    sleep 10 

    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile 2>&1

    cat $captureFile

    status=`cat $captureFile | grep "Job state: " | grep -o --regexp="PREP\|RUNNING\|SUCCEEDED\|COMPLETED\|FAILED" `

    if [ $status != "FAILED" ] ; then
        echo "The returned job status '$status' is not what is expected (FAILED) " >> $ARTIFACTS_FILE 2>&1     
        setFailCase " The returned job status '$status' is not what is expected (FAILED) "
        windUp   
        return
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobStatusWithNoJobIdProvided {
    TESTCASE_DESC="test_getJobStatusWithNoJobIdProvided"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status > $captureFile 2>&1&

    sleep 10

    local mydata=`cat $captureFile |grep "Usage: CLI \[-status "`
    if [ "X${mydata}X" == "XX" ] ; then
        echo "The default command help is not displayed and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The default command help is not displayed and so failing the test case"
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobStatusWithInvalidJobIdProvided {
    TESTCASE_DESC="test_getJobStatusWithInvalidJobIdProvided"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local invalidId="job_000000000000_0000"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $invalidId  > $captureFile 2>&1&

    sleep 10

    local mydata=`cat $captureFile |grep " $invalidId is not properly formed"`
    if [ "X${mydata}X" == "XX" ] ; then
        dumpCapturedOutput $captureFile
        echo "The invalid jobid msg  is not displayed and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The invalid jobid msg is not displayed and so failing the test case"
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_KillJobValidJobIdInRunningState {
    TESTCASE_DESC="test_KillJobValidJobIdInRunningState"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    submitSleepJob 10 10 5000 5000 1
    local myjobId=`getJobId`

    if [ $myjobId == 0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    local myresp=`killJob $myjobId`

    if [ $myresp != "Killed" ] ; then
        echo "The job was not killed succesfully and so markin the testcase failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job was not killed succesfully and so markin the testcase failed "
        windUp
        return
    else
        echo "Job was killed successfully and so passing the tescase "
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_KillJobIdAlreadyKilledJob {
    TESTCASE_DESC="test_KillJobIdAlreadyKilledJob"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    submitSleepJob 10 10 5000 5000 1

    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi      

    local myresp=`killJob $myjobId`
    if [ $myresp != "Killed" ] ; then
        echo "Expected Killed message and instead got $myresp as output " >> $ARTIFACTS_FILE 2>&1
        echo "The job has not been killed successfully and so marking the test case as failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job has not been killed successfully and so marking the test case as failed " 
        windUp
        return
    fi

    killJob $myjobId $captureFile
    sleep 10
    local myresp1=`cat $captureFile |grep "Unknown job" |wc -l`

    if [ $myresp1 -gt 0 ] ; then
        echo "Got the expected exception when trying to kill a job that is already killed  and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        dumpCapturedOutput $captureFile
        setFailCase " Did not get the expected exception when trying to kill a job that is already killed and so failing the test case "
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE    
    # TODO  validate the job failure .. TO BE IMPLEMENTED
}  

function test_KillInvalidJobId {
    # submit an kill job command for an invalid job id 
    # expect an java.lang.IllegalArgumentException 
    TESTCASE_DESC="test_KillInvalidJobId"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local myresp1=0
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    killJob "aaaaaaa" $captureFile
    sleep 10
    myresp1=`cat $captureFile |grep "IllegalArgumentException" |wc -l`
    if [ $myresp1 -gt 0 ] ; then
        echo "Got the expected exception when trying to kill a job by providing invalid job id   and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Output of killing a job by passing invalid job id is **************************"
        cat $captureFile
        echo "End of log output ***********************************"   
        setFailCase " Did not get the expected exception when trying to kill a job by providing invalid id and so failing the test case "
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_KillCommandWithNoJobId {
    # submit an kill job command with no job id 
    # expect an usage message 
    TESTCASE_DESC="test_KillInvalidJobId"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local myresp1=0
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"
    killJob  $captureFile
    sleep 10
    myresp1=`cat $captureFile |grep "Usage: CLI"`
    if [ $myresp1 -gt 0 ] ; then
        echo "Got the expected exception when trying to kill a job and not  providing  job id   and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        dumpCapturedOutput $captureFile
        echo "The output of captured command is **************************"
        cat $captureFile
        echo "End of output capture *****************************"
        setFailCase " Did not get the expected exception when trying to kill a job and not providing job id and so failing the test case "
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_KillJobIdAlreadyFailedJob {
    TESTCASE_DESC="test_KillJobIdAlreadyFailedJob"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    submitSleepJob 10 10 5000 5000 1

    sleep 10 

    local myjobId=`getJobId`

    if [ $myjobId == 0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    failAJob $myjobId
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile 2>&1

    if [ `cat $captureFile | grep "FAILED" | wc -l` -eq 0 ] ; then
        echo " Unable to fail the job and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Unable to fail the job and so failing the test case "
        windUp
        return
    fi

    killJob $myjobId $captureFile
    sleep 10

    local myresp1=`cat $captureFile |grep "Killed " |wc -l`

    if [ $myresp1 -gt 0 ] ; then
        echo "Got the expected exception when trying to kill a job that is already killed  and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        dumpCapturedOutput $captureFile
        setFailCase " Did not get the expected exception when trying to kill a job that is already killed and so failing the test case "
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_KillJobIdAlreadyCompletedJob {
    TESTCASE_DESC="test_KillJobIdAlreadyCompletedJob"
    TESTCASE_ID="Status01"
    COMMAND_EXIT_CODE=1
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    submitSleepJob 1 1 10 10 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    for (( i=0; i < 15; i++ )); do
        checkForJobStatus $myjobId $captureFile
        local myresp=`cat $captureFile |grep "Job Complete" |wc -l`
        if [ $myresp -gt 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    echo "Output capture of CLI is as follows " >> $ARTIFACTS_FILE 2>&1
    cat $captureFile  >> $ARTIFACTS_FILE 2>&1  
    killJob $myjobId $captureFile 
    # checkForJobStatus $myjobId $captureFile
    sleep 10
    local myresp1=`cat $captureFile |grep "Killed "|wc -l`
    if [ $myresp1 -gt 0 ] ; then
        echo "Got the expected exception when trying to kill a job that is already killed  and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        dumpCapturedOutput $captureFile
        echo "Output capture of CLI is as follows " >> $ARTIFACTS_FILE 2>&1
        cat $captureFile  >> $ARTIFACTS_FILE 2>&1
        setFailCase " Did not get the expected exception when trying to kill a job that is already killed and so failing the test case "
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobListWithNoRunningJobs {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithNoRunningJobs"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    executeJobListCommand
    if [ `cat $ARTIFACTS_DIR/joblist.out |grep "jobs:0" |wc -l ` -eq 0 ] ; then
        dumpCapturedOutput "$ARTIFACTS_DIR/joblist.out"
        echo "Requesting job -lis when there are no jobs running does not return jobs:0 and so failing the testcase " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Requesting job -lis when there are no jobs running does not return jobs:0 and so failing the testcase"
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobListWithOneRunningJob {
  # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithOneRunningJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    submitSleepJob 10 10 5000 5000 1
    sleep 5 
    executeJobListCommand
    cat $ARTIFACTS_DIR/joblist.out |grep "jobs:1"
    if [ $? -ne 0 ] ; then
        echo "Start of command line output capture ************************************"
	echo ""
        cat $ARTIFACTS_DIR/joblist.out
	echo ""
        echo "End of command line output capture *************************************"
        echo "Requesting job -lis when there is one job running does not return jobs:1 and so failing the testcase " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Requesting job -lis when there is one  jobs running does not return jobs:1 and so failing the testcase"
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_getJobListWithOneKilledJob {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithOneRunningJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    submitSleepJob 10 10 5000 5000 1
    sleep 5
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    killAJob $myjobId
    sleep 10
    executeJobListCommand "all"
    local jobstatus=`cat $ARTIFACTS_DIR/joblist.out |grep  $myjobId |awk -F" " '{print $2}'`
    if [ $jobstatus != "KILLED" ] ; then
        echo "The status of the killed job is not returned correctly as expected  for job -list all and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        setFailCase "The status of the killed job is not returned correctly as expected  for job -list all and so failing the test case"
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobListAllForCompletedJobs  {
    # test to get the list of all job ids processed by the cluster
    # Trigger a job and get the job id
    # Verify that the job has completed 
    # now do a job -list all and make sure that the completed job is listed 
    TESTCASE_DESC="test_getJobListAllForCompletedJobs"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    COMMAND_EXIT_CODE=1
    sleep 5
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo "The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    # TODO  verify that the job is completed
    sleep 10
    executeJobListCommand "all"
    local jobstatus=`cat $ARTIFACTS_DIR/joblist.out |grep  $myjobId  |grep  $myjobId |awk -F" " '{print $2}'`
    if [ $jobstatus != "SUCCESSFULL" ] ; then
        echo "The returned status of the killed job '$jobstatus' is not what is expected (SUCCESSFUL)" >> $ARTIFACTS_FILE 2>&1
        setFailCase "The returned status of the killed job '$jobstatus' is not what is expected (SUCCESSFUL)"
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#setUpTestEnv capacity-scheduler_MaxResourcesForDefaultQueue.xml
#sleep 300

test_getJobListWithNoRunningJobs
test_getJobListWithOneRunningJob
test_mapredCommandWithNoArgs
test_mapredCommandWithInvalidArgs       
test_getJobStatusBasedOnJobId
test_getJobStatusBasedOnCompletedJobId
test_getJobStatusWithNoJobIdProvided
test_getJobStatusWithInvalidJobIdProvided
test_KillJobValidJobIdInRunningState
test_KillInvalidJobId
#test_KillCommandWithNoJobId
test_KillJobIdAlreadyKilledJob
test_KillJobIdAlreadyFailedJob
test_KillJobIdAlreadyCompletedJob
test_getJobStatusBasedOnKilledJobId
# test_getJobStatusBasedOnFailedJobId

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
