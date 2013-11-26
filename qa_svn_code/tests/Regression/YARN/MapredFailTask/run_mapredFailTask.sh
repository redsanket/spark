#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/yarn_library.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`

customConfDir="/homes/$USER_ID/CustomConfDir"
customNMConfDir="/homes/$USER_ID/CustomNMConfDir"
customSetUp="DEFAULT"
#customSetUp="CUSTOM"

function test_failRunningTask {
    # This test case is to test failing of running task
    TESTCASE_DESC="test_failRunningTask"  
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_failRunningTask.out"
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')      
    failGivenAttemptId $myAttemptId1 >$captureFile
    if [ `cat $captureFile |grep $myAttemptId1 |grep "Killed task" |wc -l` -gt 0 ] ; then
        echo " Got the killed task message as expected and so passing the test case " 
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the killed task message and so failing the test case "
        setFailCase "Did not get the killed task message and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE      
}

function test_failTaskOfAlreadyKilledJob {
# This test case is to test failing of task of Killed Job
    TESTCASE_DESC="test_failTaskOfAlreadyKilledJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_failTaskOfAlreadyKilledJob.out"
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
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
    failGivenAttemptId $myAttemptId1 >$captureFile 2>&1
    sleep 50
    if [ `cat $captureFile |grep "Killed task attempt" | grep "by failing it" | wc -l` -gt 0 ] ; then
        echo " Got the task Killed message and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the task Killed message and so failing the test case "
        setFailCase "Did not get the task Killed message and so failing the test case  "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_failTaskOfAlreadyFailedJob {
# This test case is to test kill attempt id of Failed Job
    TESTCASE_DESC="test_failTaskOfAlreadyFailedJob bug 4573467 filed "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_failTaskOfAlreadyFailedJob.out"
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`

    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    failAJob $myjobId
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -status $myjobId > $captureFile 2>&1

    if [ `cat $captureFile |grep "FAILED" | wc -l` -eq 0 ] ; then
        echo " Unable to fail the job and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Unable to fail the job and so failing the test case "
        windUp
        return
    fi

    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
    failGivenAttemptId $myAttemptId1 > $captureFile 2>&1

    # will need to fixup grep patterns when the bug is fixed and real fail message is seen
    if [ `cat $captureFile | grep "Failed task attempt" | wc -l` -gt 0 ] ; then
        echo " Got the task failed message and passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the task failed message and so failing the test case "
        setFailCase "Did not get the task failed message and so failing the test case  "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_failTaskOfAlreadyCompletedJob {
    # This test case is to test kill attempt id of Completed Job
    TESTCASE_DESC="test_failTaskOfAlreadyCompletedJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="${ARTIFACTS_DIR}/test_failTaskOfAlreadyCompletedJob.out"
    submitSleepJob 10 10 25000 25000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    for (( i=0; i < 20; i++ )); do
        $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list all |grep $myjobId > $captureFile 2>&1
        if [ `cat $captureFile |grep "SUCCEEDED" |wc -l` -ne 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
    failGivenAttemptId $myAttemptId1 > $captureFile 2>&1
    if [ `cat $captureFile | grep "XXYYXXYY" |wc -l` -gt 0 ] ; then
        echo " Got the expected message, passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Did not get the expected message, failing the test case "
        setFailCase "Did not get the expected message, failing the test case"
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


setUpTestEnv "capacity-scheduler.xml"
sleep 20
test_failRunningTask
test_failTaskOfAlreadyKilledJob
test_failTaskOfAlreadyFailedJob
test_failTaskOfAlreadyCompletedJob
resetYarnEnv

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
