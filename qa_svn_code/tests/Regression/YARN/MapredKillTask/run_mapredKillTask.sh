#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/restart_cluster_lib.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`

customConfDir="/homes/$USER_ID/CustomConfDir"
customNMConfDir="/homes/$USER_ID/CustomNMConfDir"
customSetUp="DEFAULT"
#customSetUp="CUSTOM"

function test_killRunningTask {
    # This test case is to test killing of running task
    TESTCASE_DESC="test_killRunningTask"  
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_killRunningTask.out"
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_00000_0/g')       
    killTask $myAttemptId1 >$captureFile
    if [ `cat $captureFile |grep $myAttemptId1 |grep "Killed task" |wc -l` -gt 0 ] ; then
        echo " Got the killed task message as expected and so passing the test case " 
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the killed error message and so failing the test case "
        setFailCase "Did not get the killed error message and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE      
}

function test_killTaskOfAlreadyKilledJob {
    # This test case is to test killing of Killed Job
    TESTCASE_DESC="test_killTaskOfAlreadyKilledJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_killTaskOfAlreadyKilledJob.out"
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    local myresp=`killJob $myjobId`
    if [ $myresp != "Killed" ] ; then
        echo " Unable to kill the job and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Unable to kill the job and so failing the test case "
        windUp
        retun
    fi
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_00000_0/g')
    killTask $myAttemptId1 > $captureFile 2>&1
    sleep 50
    #killTask $myAttemptId1 >$captureFile
    if [ `cat $captureFile |grep $myAttemptId1 |grep "Killed task" |wc -l` -gt 0 ] ; then
        echo " Got the killed task message as expected and so passing the test case " 
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the killed error message and so failing the test case "
        setFailCase "Did not get the killed error message and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_killTaskOfAlreadyFailedJob {
    # This test case is to test kill attempt id of Failed Job
    TESTCASE_DESC="test_killTaskOfAlreadyFailedJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_killTaskOfAlreadyFailedJob.out"

    # Timing Issue #1: 
    # Potential timing issue if job does not actually start before getJobId runs
    submitSleepJob 10 10 200000 200000 1
    for FILENAME in `cat $FILES`; do
        echo "get job id from $FILENAME"
        # local myjobId=`getJobId $FILENAME`
        local myjobId=`getJobId`
        echo "my job id = $myjobId"
    done    

    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi

    # Timing Issue #2
    # Job may have completed before attempting to fail it. Need to make sure the
    # sleep time is long enough.
    echo "Fail job $myjobId"
    failAJob $myjobId
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile 2>&1
    echo "fail task output ="
    cat $captureFile 
    if [ `cat $captureFile |grep "FAILED" |wc -l` -eq 0 ] ; then
        echo " Unable to fail the job and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Unable to fail the job and so failing the test case "
        windUp
        return
    fi

    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_00000_0/g')
    echo "kill task of already failed job: on attempt $myAttemptId1"
    #killTask $myAttemptId1
    #sleep 50
    killTask $myAttemptId1 > $captureFile 2>&1
    echo "kill task output ="
    cat $captureFile 
    # E.g. Killed task attempt_1351251311000_0003_m_00000_0
    local EXP_STR1="Killed task attempt"
    local EXP_STR2="Error from remote end: Invalid operation on completed job"
    if [ `cat $captureFile | grep $myAttemptId1 | grep "$EXP_STR1" | wc -l` -gt 0 ] ; then
        set -x
        echo " Got the killed task message as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        set +x
        COMMAND_EXIT_CODE=0
    elif  [ `cat $captureFile | grep "$EXP_STR2" | wc -l` -gt 0 ] ; then
        set -x
        echo " Got the message that the kill operation is not valid because the job has moved to the history server" >> $ARTIFACTS_FILE 2>&1
        set +x
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the killed error message and so failing the test case "
        setFailCase "Did not get the killed error message and so failing the test case "
        windUp
        return
    fi
    echo "COMMAND EXIT CODE=$COMMAND_EXIT_CODE"
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_killTaskOfAlreadyCompletedJob {
    # This test case is to test kill attempt id of Completed Job
    TESTCASE_DESC="test_killTaskOfAlreadyCompletedJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_killTaskOfAlreadyCompletedJob.out"
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    for (( i=0; i < 15; i++ )); do
        $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list all |grep $myjobId > $captureFile 2>&1
        if [ `cat $captureFile |grep "SUCCEEDED" |wc -l` -ne 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_00000_0/g')

    killTask $myAttemptId1 > $captureFile 2>&1
    # May need to clean this up once bug 5366200 and what to expect is known
    if [ `cat $captureFile | grep "XXYYXXYY" |wc -l` -gt 0 ] ; then
        echo " Got the expected message and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the expected message and so failing the test case "
        setFailCase "Did not get the expected message and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


# This does not seem necessary to override the default configuration
# setUpTestEnv "capacity-scheduler.xml"

test_killRunningTask
test_killTaskOfAlreadyKilledJob
test_killTaskOfAlreadyFailedJob
test_killTaskOfAlreadyCompletedJob

# resetYarnEnv

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
