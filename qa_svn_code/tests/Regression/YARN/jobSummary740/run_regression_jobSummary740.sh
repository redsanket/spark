#!/bin/bash 

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh

#################################################################
#This script checks the job summary related scenarios 
# to make sure the job summary records all the data correctly
#################################################################

# Setting Owner of this TestSuite
# OWNER="vmotilal"

function checkInJobSummaryLog {
    local myjobId=$1
    local expected=$2
    local times=$3
    
    waitForJobHistFile $myjobId
    
    resp=`$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R /mapred/history/done | grep $myjobId |  grep -v conf | grep $expected`
    echo "resp is $resp"

    if [ -z "$resp" ] ; then
      return 1
    fi
    return 0
}



function testSuccessJobStatusInJobSummaryLog {
    COMMAND_EXIT_CODE=0
    TESTCASE_DESC="Test the status of the job executed in the job summary log"
    TESTCASE_ID="testSucessJobStatusInJobSummaryLog"      

    # submit a job and wait for it to complete
    # verify that the job is complete
    # Now get the jobsummary.log file and search for the jobId
    # search for the status as success

    triggerSleepJob 1 1 1 1 1
    sleep 20
    jobId=`getJobId`
    echo "Job id is $jobId "

    # Now that you have the jobid now check the summary of the job in jobsummary.log
    checkInJobSummaryLog $jobId "SUCCEEDED"

    if [ $? -eq 0 ] ; then
        echo " Found the string "
    else
        echo " Did not find the expected status and so failing  the test case "
        setFailCase " Did not find the expected status and so failing  the test case "
        windUp  
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE    
}       

function testQueueInfoInJobSummaryLog {
    COMMAND_EXIT_CODE=0
    TESTCASE_DESC="Test the status of the job executed in the job summary log"
    TESTCASE_ID="testSucessJobStatusInJobSummaryLog"

    # submit a job and wait for it to complete
    # verify that the job is complete
    # Now get the jobsummary.log file and search for the jobId
    # search for the status as success
    queue="-Dmapred.job.queue.name=grideng" 
    triggerSleepJob 1 1 1 1 1 $queue
    sleep 20
    jobId=`getJobId`
    echo "Job id is $jobId "

    checkInJobSummaryLog $jobId "grideng" 
    if [ $? -eq 0 ] ; then
        echo " Found the string "
    else
        echo " Did not find the expected status and so failing  the test case "
        setFailCase " Did not find the expected status and so failing  the test case "
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testKilledJobStatusInJobSummaryLog {
    COMMAND_EXIT_CODE=0
    TESTCASE_DESC="Test the Killed status of the job executed in the job summary log"
    TESTCASE_ID="testKilledJobStatusInJobSummaryLog"

    # submit a job and wait to get the job id
    # Kill the job via command line arg
    # Now get the jobsummary.log file and search for the jobId
    # search for the status as success
    triggerSleepJob 10 1 5000 1 1
    sleep 10
    jobId=`getJobId`
    echo "Job id is $jobId "

    # Now that I have the job id, I will kill the job and later check for status
    killJob $jobId

    # Now that you have the jobid now check the summary of the job in jobsummary.log
    checkInJobSummaryLog $jobId "KILLED"
    if [ $? -eq 0 ] ; then
        echo " Found the string "
    else
        echo " Did not find the expected status and so failing  the test case "
        setFailCase " Did not find the expected status and so failing  the test case "
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testFailedJobStatusInJobSummaryLog {
    COMMAND_EXIT_CODE=0
    TESTCASE_DESC="Test the failed status of the job executed in the job summary log"
    TESTCASE_ID="testFailedJobStatusInJobSummaryLog"

    # submit a job and wait  to get the job id
    # Fail the job via command line arg
    # Now get the jobsummary.log file and search for the jobId
    # search for the status as success
    createFile $ARTIFACTS_DIR/AttemptIdFile
    getJobTrackerHost
    if [  "$JOBTRACKER" == "None" ] ; then
        setFailCase " Unable to get the Job tracker Host name and so failing "
        windUp
        return
    fi

    # If the task sleep time (msec) is too short, the fail task attempts
    # (hadoop job -fail-task <task-id>) may fail, then causing the test to fail.
    # Increased sleep time from 50,000 to 100,000 ms.
    triggerSleepJob 10 1 100000 1 1
    sleep 10
    myjobId=`getJobId`
    echo "Job id is $myjobId "

    checkForNewAttemptIds $myjobId
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are attempts ids proceeding to kill them "
        failAttempts
        failAttemptsStatus=$?
        if [ $failAttemptsStatus -ne 0 ] ; then
           echo "Kill failed: The job could not be failed successfully and unable to proceed with the tests "
           setFailCase "Kill failed: The job could not be failed successfully and unable to proceed with the tests "  
           windUp
           return
        fi
        checkForNewAttemptIds $myjobId
    done

    checkForJobStatus $myjobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "  
        windUp
        return
    fi 

    checkInJobSummaryLog $myjobId "FAILED"
    if [ $? -eq 0 ] ; then
        echo " Found the string "
    else
        echo " Did not find the expected status and so failing the test case "
        setFailCase " Did not find the expected status and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


testSuccessJobStatusInJobSummaryLog
testKilledJobStatusInJobSummaryLog
testQueueInfoInJobSummaryLog
testFailedJobStatusInJobSummaryLog

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
