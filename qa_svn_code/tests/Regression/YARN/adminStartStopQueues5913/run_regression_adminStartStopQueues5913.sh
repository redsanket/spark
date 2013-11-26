#!/bin/bash
# set -x

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"

export USER_ID=`whoami`
customConfDir="/homes/hadoopqa/tmp/CustomConfDir"
customNMConfDir="/homes/hadoopqa/tmp/CustomNMConfDir"
customSetUp="DEFAULT"

mkdir $customConfDir
mkdir $customNMConfDir

function testJobSubmissionToQueueWhichIsStoppedLeafNode {
    TESTCASE_DESC="testJobSubmissionToQueueWhichIsStoppedLeafNode"
    TESTCASE_ID="Status01"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"
    local stoppedQueue="c2"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    # The following is to set up the env to point to custom conf dir
    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"
    sleep 20
    submitSleepJobToQueue $stoppedQueue $captureFile
    sleep 10
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "org.apache.hadoop.security.AccessControlException: Queue root.c.c2 is STOPPED" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break           
        fi
    done    

    if [ $myexpected -eq 0 ] ; then     
        echo " Did not get the Queue stopped message when submitting to a stopped queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the Queue stopped message when submitting to a stopped queue  and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testJobSubmissionToQueueWhichHasInvalidQueueState {
    TESTCASE_DESC="testJobSubmissionToQueueWhichHasInvalidQueueState"
    TESTCASE_ID="Status01"
    local stoppedQueue="c2"
    local checkpoint="FAIL"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    # The following is to set up the env to point to custom conf dir
    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"
    submitSleepJobToQueue $stoppedQueue $captureFile
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "org.apache.hadoop.security.AccessControlException: Queue root.c.c2 is STOPPED" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the Queue stopped message when submitting to a stopped queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        checkpoint="FAIL"
    else
        checkpoint="PASS"
    fi
    # update an invalid queue state and do a node refresh
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/capacity-scheduler_c2InvalidState.xml $customConfDir/capacity-scheduler.xml
    # set the user as hdfs
    setKerberosTicketForUser $HDFSQA_USER
    $HADOOP_YARN_HOME/bin/yarn rmadmin -refreshQueues
    setKerberosTicketForUser $HADOOPQA_USER
    submitSleepJobToQueue $stoppedQueue $captureFile
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "org.apache.hadoop.security.AccessControlException: Queue root.c.c2 is STOPPED" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the Queue stopped message when submitting to a stopped queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        checkpoint="FAIL"
    else
        checkpoint="PASS"
    fi
    if [ $checkpoint == "FAIL" ] ; then
        echo " One of the checks failed in validating invalid queue state and so marking the test case failed "
        setFailCase "One of the checks failed in validating invalid queue state  and so marking the test case failed "          
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testJobSubmissionToQueueWhichIsNotLeafNode {
    TESTCASE_DESC="testJobSubmissionToQueueWhichIsNotLeafNode"
    TESTCASE_ID="Status01"
    local stoppedQueue="a"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"
    submitSleepJobToQueue $stoppedQueue $captureFile
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "java.io.IOException" |grep "non-leaf queue" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the non leaf queue message when submitting to a non leaf queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the non leaf queue message when submitting to a non leaf queue  and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testJobSubmissionToQueueWhichIsNotDefined {
    TESTCASE_DESC="testJobSubmissionToQueueWhichIsNotDefined"
    TESTCASE_ID="Status01"
    local undefinedQueue="d"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    # The following is to set up the env to point to custom conf dir
    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"  
    submitSleepJobToQueue $undefinedQueue $captureFile
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "java.io.IOException" |grep "unknown queue" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the unknown queue message when submitting to a queue  that does not exist and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the unknown queue message when submitting to a queue that does not exist  and so failing the test case "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testJobSubmissionToQueueWhoseParentNodeIsStopped {
    TESTCASE_DESC="testJobSubmissionToQueueWhoseParentNodeIsStopped"
    TESTCASE_ID="Status01"
    local stoppedQueue="a"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    # The following is to set up the env to point to custom conf dir
    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"
    submitSleepJobToQueue $stoppedQueue $captureFile
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "java.io.IOException" |grep "non-leaf queue" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the AccessControlException when submitting to a queue whose parent is stopped  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the AccessControlException when submitting to a queue whose parent is stopped  and so failing the test case  " 
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testJobSubmissionToQueueWhenRootIsStopped {
    TESTCASE_DESC="testJobSubmissionToQueueWhenRootIsStopped"
    TESTCASE_ID="Status01"
    local stoppedQueue="a"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    customSetUp="DEFAULT"
    setUpTestEnv "capacity-scheduler_RootQueueStopped.xml"

    # setting it to default again for the next test to change the state
    customSetUp="DEFAULT"
    submitSleepJobToQueue $stoppedQueue $captureFile

    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile | grep "failed" | grep "non-leaf queue" | wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the AccessControlException when submitting to a queue whose parent is stopped  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the AccessControlException when submitting to a queue whose parent is stopped  and so failing the test case  "
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testJobSubmissionToRunningLeafQueue {
    TESTCASE_DESC="testJobSubmissionToRunningLeafQueue"
    TESTCASE_ID="Status01"
    local myQueue="c1"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    # The following is to set up the env to point to custom conf dir
    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"
    sleep 15
    submitSleepJobToQueue $myQueue $captureFile
    for (( i=0; i < 10; i++ )); do
        local myexpected=`cat $captureFile |grep "completed successfully" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case  "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function  testChangingQueueStatusAndRestartingRm {
    # Modify queue C from running to stopped
    # Move the capacityscheduler.xml that is modified locally on to RM
    # Restart RM and NM
    # Submit to the queue c2
    # Expect the job to fail with appropriate queue stopped message 
    TESTCASE_DESC="testChangingQueueStatusAndRestartingRm"
    TESTCASE_ID="Status01"
    local myQueue="c2"      
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1  

    #local rmHost=`getResourceManagerHost`
    #copyConfigs $customConfDir $rmHost
    #putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/capacity-scheduler_modified.xml $customConfDir/capacity-scheduler.xml                        
    #setUpRMWithConfig $rmHost $customConfDir
    setUpTestEnv "capacity-scheduler_modified.xml"
	 
    sleep 10
    submitSleepJobToQueue $myQueue $captureFile
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile | grep "completed successfully" | wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case  "
        windUp
        return
    fi    

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_QueueStatusChangeAndRefreshQueues {
    # Modify queue B from running to stopped
    # Move the capacityscheduler.xml to the RM machine
    # Do a refreshQueues
    # Now submit to the queue that is modified and the new configs should be picked up
    TESTCASE_DESC="testChangingQueueStatusAndRestartingRm"
    TESTCASE_ID="Status01"
    local myQueue="b"      
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1  

    # setting the env to point to custom conf dir 
    setUpTestEnv "capacity-scheduler_c2QueueStopped.xml"
    local rmHost=`getResourceManagerHost`
    copyConfigs $customConfDir $rmHost
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/capacity-scheduler_refreshQueues.xml $customConfDir/capacity-scheduler.xml  
    # set the user as hdfs
    setKerberosTicketForUser $HDFSQA_USER
    $HADOOP_YARN_HOME/bin/yarn rmadmin -refreshQueues
    setKerberosTicketForUser $HADOOPQA_USER
    submitSleepJobToQueue $myQueue $captureFile

    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile |grep "org.apache.hadoop.security.AccessControlException: Queue root.b is STOPPED" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1  
        setFailCase "Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case  "      windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}         

function test_QueueStatusChangeAndRefreshQueuesOnJobsRunningInThatQueue {
    # Move a capacityscheduler.xml with has all queues running
    # Submit a long running job to  queue c2
    # Now move capacityscheudler.xml that has queuec2 as stopped
    # Do a refreshQueues
    # Submit a new job to this queue c2 and no new jobs should be accepted
    # Verify that the old job submitted completes successfully
    TESTCASE_DESC="test_QueueStatusChangeAndRefreshQueuesOnJobsRunningInThatQueue"
    TESTCASE_ID="Status01"
    local myQueue="c2" 
    # this is a flag to validate multiple checks. Any checkpoint failure will mark the test as failed
    local mycheck="FAIL"
    local captureFile="${ARTIFACTS_DIR}/${TESTCASE_DESC}.out"
    local captureFile2="${ARTIFACTS_DIR}/${TESTCASE_DESC}2.out"

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1

    customSetUp="DEFAULT"
    setUpTestEnv "capacity-scheduler_AllQueuesRunning.xml"
    echo " Sleeping after setting the env with the config location "
    # setting it again to default so that next test case will set it to custom the way they want it
    customSetUp="DEFAULT"
    sleep 20
    # setKerberosTicketForUser $HADOOPQA_USER
    submitSleepJobToQueue $myQueue $captureFile 10 10 50000 50000

    local myjobId1=`getJobId $captureFile`
    sleep 30
    local rmHost=`getResourceManagerHost`
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/capacity-scheduler_c2QueueStopped.xml $customConfDir/capacity-scheduler.xml                  
    # set the user as hdfs
    setKerberosTicketForUser $HDFSQA_USER
    $HADOOP_YARN_HOME/bin/yarn rmadmin -refreshQueues
    setKerberosTicketForUser $HADOOPQA_USER       
    submitSleepJobToQueue $myQueue $captureFile2
    for (( i=0; i < 5; i++ )); do
        local myexpected=`cat $captureFile2 |grep "org.apache.hadoop.security.AccessControlException: Queue root.c.c2 is STOPPED" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 10
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the access control exception  when submitting to a running leaf queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        mycheck="FAIL"
    else
        mycheck="PASS"
    fi
    # Checking the status of the job that was submitted before the change to the queue
    for (( i=0; i < 25; i++ )); do
        local myexpected=`cat $captureFile |grep "completed successfully" |wc -l`
        if [ $myexpected -eq 0 ] ; then
            sleep 20
        else
            break
        fi
    done

    if [ $myexpected -eq 0 ] ; then
        echo " Did not get the Job Complete msg  when submitting to a running leaf queue  and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        mycheck="FAIL"
    else
        mycheck="PASS"
    fi
    if [ $mycheck == "FAIL" ] ; then
        setFailCase " One or more checks failed when testing the state of submitted jobs when the queues states are changed "
        windUp
        return 
    fi              
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

testJobSubmissionToQueueWhichIsStoppedLeafNode
testJobSubmissionToQueueWhichIsNotLeafNode
testJobSubmissionToQueueWhichIsNotDefined
testJobSubmissionToQueueWhoseParentNodeIsStopped
testJobSubmissionToRunningLeafQueue
# testChangingQueueStatusAndRestartingRm
test_QueueStatusChangeAndRefreshQueues
# test_QueueStatusChangeAndRefreshQueuesOnJobsRunningInThatQueue
# testJobSubmissionToQueueWhenRootIsStopped
# testJobSubmissionToQueueWhichHasInvalidQueueState

resetYarnEnv

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
