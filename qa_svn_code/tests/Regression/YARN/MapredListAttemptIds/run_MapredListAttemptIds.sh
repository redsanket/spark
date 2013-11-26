#!/bin/bash

# set -x

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/restart_CLUSTER_lib.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`

function dumpCLIOutput {
    local myfile=$1
    echo " Capture of CLIoutput ******************************************" >> $ARTIFACTS_FILE 2>&1
    cat $myfile >> $ARTIFACTS_FILE 2>&1
    echo "end capture output ********************************************" >> $ARTIFACTS_FILE 2>&1
}

function test_listAttemptIdsWithOnlyValidJobId {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyValidJobId"  TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId  >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with only JobId  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with only JobId "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_listAttemptIdsForValidJobIdTaskTypeTaskState {
    TESTCASE_DESC="test_listAttemptIdsForValidJobIdTaskTypeTaskState  bug 4608365"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 10 10 500000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "MAP" "running"`
    sleep 20
    if [ $attemptCount -ne 10 ] ; then
        dumpCLIOutput "/tmp/t5.out"
        echo " The attempt Id count returned is not as expected which is 10 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 10 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithNoArguments {
    TESTCASE_DESC="test_listAttemptIdsWithNoArguments"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    getAttemptIdsCountForJobId >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with no options " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with no options "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithMissingJobId {
    TESTCASE_DESC="test_listAttemptIdsWithMissingJobId"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    getAttemptIdsCountForJobId "" "MAP" "running" >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with missiong jobid " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with missing jobid "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithOnlyTaskType {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyTaskType"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    getAttemptIdsCountForJobId "MAP" >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with only task type  " >> $ARTIFACTS_FILE 2>&1   
        setFailCase " Default help message is missing for listAttemptid command with only task type "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithOnlyTaskState {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyTaskState"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    getAttemptIdsCountForJobId "running" >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with only task state  " >> $ARTIFACTS_FILE 2>&1  
        setFailCase " Default help message is missing for listAttemptid command with only task state "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithOnlyValidJobId {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyValidJobId"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId  >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with only JobId  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with only JobId "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithOnlyValidJobIdAndValidTaskType {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyValidJobIdAndValidTaskType"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId "MAP"  >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with only JobId  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with only JobId "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithOnlyValidJobIdAndInValidTaskType {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyValidJobIdAndInValidTaskType"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId "MAPP" "running" > /tmp/t5.out 2>&1
    cat /tmp/t5.out | grep "Usage: CLI"
    if [ $? -ne 0 ] ; then
        echo " Default help message is missing for listAttemptid command with invalid task type  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with invalid task type "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithOnlyValidJobIdValidTaskTypeAndInvalidTaskState {
    TESTCASE_DESC="test_listAttemptIdsWithOnlyValidJobIdValidTaskTypeAndInvalidTaskState bug 4608365"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    # TO DO : This test case fails and need to file a bug later 
    submitSleepJob 10 10 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId "MAP" "runninggggg"  >/tmp/t5.out 2>&1
    cat /tmp/t5.out |grep "java.lang.IllegalArgumentException"
    if [ $? -ne 0 ] ; then
        dumpCLIOutput "/tmp/t5.out"                     
        echo " Default help message is missing for listAttemptid command with only JobId  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Default help message is missing for listAttemptid command with only JobId "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithNoRunningMaps {
    TESTCASE_DESC="test_listAttemptIdsWithNoRunningMaps Bug 4608365"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    #  TO DO : This test case fails and need to file a bug later 
    submitSleepJob 1 10 1 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId "MAP" "running"
    local linecount=`cat /tmp/t5.out | wc -l`
    if [ $linecount -ne 16 ] ; then
        echo " Verifying the response line count when requesting attempt ids for  maps when there are no maps running  " >> $ARTIFACTS_FILE 2>&1  
        setFailCase " Verifying the response line count when requesting attempt ids for  maps when there are no maps running  "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_listAttemptIdsWithNoCompletedMapsAndTaskStatusCompleted {
  # Test to request attemptids for map with state as completed when there are no map tasks that have completed 
    TESTCASE_DESC="test_listAttemptIdsWithNoCompletedMapsAndTaskStatusCompleted Bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    # TO DO : This test case fails and need to file a bug later 
    submitSleepJob 1 1 50000 50000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId "MAP" "completed"
    local linecount=`cat /tmp/t5.out | wc -l`
    if [ $linecount -ne 16 ] ; then
        echo " Verifying the response line count when requesting attempt ids for  maps when there are no maps running  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Verifying the response line count when requesting attempt ids for  maps when there are no maps running  "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsForReduceTaskAndTaskStateAsRunning {
    # Test listAttemptId for reduce tasks that is are currently in running state
    TESTCASE_DESC="test_listAttemptIdsForReduceTaskAndTaskStateAsRunning bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 1 10 1 70000 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    sleep 20
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "REDUCE" "running"`
    if [ $attemptCount -ne 10 ] ; then
        echo " The attempt Id count returned is not as expected which is 10 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 10 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsCompleted {
    # Test listAttemptId for reduce tasks that is are currently in completed state
    TESTCASE_DESC="test_listAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsCompleted Bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t30.out"
    submitSleepJob 1 10 1 700 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    sleep 20
    checkForJobStatus $myjobId $captureFile
    for (( i=0; i < 5; i++ )); do
        local myresp=`cat $captureFile |grep "Job Complete" |wc -l`
        if [ $myresp -gt 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "REDUCE" "completed"`
    if [ $attemptCount -ne 10 ] ; then
        echo " The attempt Id count returned is not as expected which is 10 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 10 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_listAttemptIdsForFailedJobsForMapTaskAndTaskStateAsCompleted {
    # Test listAttemptId for map tasks that is are currently in completed state
    TESTCASE_DESC="test_listAttemptIdsForFailedJobsForMapTaskAndTaskStateAsCompleted Bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t30.out"
    submitSleepJob 10 10 10000 700 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    sleep 20
    failAJob $myjobId
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -status $myjobId >/tmp/t6.out 2>&1
    for (( i=0; i < 5; i++ )); do
        local myresp=`cat /tmp/t6.out |grep "Job state" |cut -d ':' -f2`
        if [ ! "X${myresp}X" == "XX" ] ; then
            break
        else
            sleep 10
        fi
    done
    if [ $myresp != "FAILED" ] ; then
        echo " The status of the job expected is not returned when queried for job status and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The status of the job expected is not returned when queried for job status and so failing the test case"
        windUp
        return
    fi
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "MAP" "completed"`
    if [ $attemptCount -ne 10 ] ; then
        echo " The attempt Id count returned is not as expected which is 10 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 10 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsCompleted {
    # Test listAttemptId for reduce tasks that is are currently in completed state
    TESTCASE_DESC="test_listAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsCompleted Bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t30.out"
    submitSleepJob 10 10 10000 700 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    sleep 20
    checkForJobStatus $myjobId $captureFile
    for (( i=0; i < 5; i++ )); do
        local myresp=`cat $captureFile |grep "Job Complete" |wc -l`
        if [ $myresp -gt 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "MAP" "completed"`
    if [ $attemptCount -ne 10 ] ; then
        echo " The attempt Id count returned is not as expected which is 10 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 10 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsRunning {
    # Test listAttemptId for reduce tasks that is are currently in completed state
    TESTCASE_DESC="test_listAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsRunning Bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t30.out"
    submitSleepJobToQueue "default" $captureFile 1 10 10000 7000
    local myjobId=`getJobId $captureFile`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    sleep 20
    checkForJobStatus $myjobId $captureFile
    for (( i=0; i < 15; i++ )); do
        local myresp=`cat $captureFile |grep -a "Job complete" |wc -l`
        if [ $myresp -gt 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "REDUCE" "running"`
    if [ $attemptCount -ne 0 ] ; then
        echo " The attempt Id count returned is not as expected which is 0 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 0 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsRunning {
    # Test listAttemptId for reduce tasks that is are currently in completed state
    TESTCASE_DESC="test_listAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsRunning Bug 4608365 "
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t30.out"
    submitSleepJobToQueue "default" $captureFile 10 10 10000 7000
    local myjobId=`getJobId $captureFile`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    sleep 20
    checkForJobStatus $myjobId $captureFile
    for (( i=0; i < 15; i++ )); do
        local myresp=`cat $captureFile |grep -a "Job complete" |wc -l`
        if [ $myresp -gt 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    local attemptCount=`getAttemptIdsCountForJobId $myjobId "MAP" "running"`
    if [ $attemptCount -ne 0 ] ; then
        echo " The attempt Id count returned is not as expected which is 0 ( based on the job submitted " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The attempt Id count returned is not as expected which is 0 ( based on the job submitted )"
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_listAttemptIdsWithNoRunningReduceTasksAndTaskStatusRunning {
    # Test to request attemptids for reduce task with state as running when there are no reduce tasks that are running 
    TESTCASE_DESC="test_listAttemptIdsWithNoRunningReduceTasksAndTaskStatusRunning Bug 4608365"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    # TO DO : This test case fails and need to file a bug later 
    submitSleepJob 10 0 50000 0 1
    local myjobId=`getJobId`
    if [ $myjobId == 0 ] ; then
        echo " The job id returned is $myjobId which is invalid and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase " The job id returned is $myjobId which is invalid and so failing the test case "
        windUp
        return
    fi
    getAttemptIdsCountForJobId $myjobId "REDUCE" "running"
    local linecount=`cat /tmp/t5.out | wc -l`
    if [ $linecount -ne 16 ] ; then
        echo " Verifying the response line count when requesting attempt ids for  reduce when there are no reduce running  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Verifying the response line count when requesting attempt ids for  reduce when there are no reduce running  "
        windUp
        return
    fi
    killJob $myjobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


# Tests have been commented due to Bug 4608365

test_listAttemptIdsWithNoArguments 
test_listAttemptIdsWithMissingJobId 
test_listAttemptIdsWithOnlyTaskType 
test_listAttemptIdsWithOnlyTaskState 
test_listAttemptIdsWithOnlyValidJobId 
test_listAttemptIdsWithOnlyValidJobIdAndValidTaskType 
test_listAttemptIdsWithOnlyValidJobIdAndInValidTaskType 

#Commented due to bugs
#test_listAttemptIdsForReduceTaskAndTaskStateAsRunning
#test_listAttemptIdsWithOnlyValidJobIdValidTaskTypeAndInvalidTaskState 
#test_listAttemptIdsWithNoRunningMaps 
#test_listAttemptIdsWithNoCompletedMapsAndTaskStatusCompleted
#test_listAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsCompleted 
#test_listAttemptIdsForValidJobIdTaskTypeTaskState 
#test_listAttemptIdsForFailedJobsForMapTaskAndTaskStateAsCompleted 
#test_listAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsCompleted 
#test_listAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsRunning 
#test_listAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsRunning 
#test_listAttemptIdsWithNoRunningReduceTasksAndTaskStatusRunning 

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
