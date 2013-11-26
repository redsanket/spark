#!/bin/bash

set -x

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/restart_CLUSTER_lib.sh


# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`

############################################
# This function takes a param $1  and passes it 
# to the list command so that it can list all jobs
# This is optional and if not passed will list only 
# the current running job
############################################
function executeJobListCommand {
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list $1 >/tmp/joblist.out 2>&1
}

function test_getJobListWithNoRunningJobs {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithNoRunningJobs"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    setKerberosTicketForUser $HADOOPQA_USER
    executeJobListCommand
    cat /tmp/joblist.out |grep "jobs:0"
    if [ $? -ne 0 ] ; then
        echo "Requesting job -lis when there are no jobs running does not return jobs:0 and so failing the testcase " >> $ARTIFACTS_FILE 2>&1   
        setFailCase "Requesting job -lis when there are no jobs running does not return jobs:0 and so failing the testcase"
        windUp
        return
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
    submitSleepJob 10 10 5000 5000 1
    sleep 5
    executeJobListCommand
    cat /tmp/joblist.out |grep "jobs:1"
    if [ $? -ne 0 ] ; then
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


function test_getJobListWithCompletedJob {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithCompletedJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t25.out"
    #submitSleepJob 10 10 5000 5000 1 $captureFile
    submitSleepJobToQueue "a" $captureFile 10 10 5000 5000 
    sleep 15
    local myjobId=`getJobId $captureFile`   
    for  (( i=0; i < 5; i++ )); do
        local myresp=`cat $captureFile |grep "Job complete" |grep $myjobId |wc -l`
        if [ $myresp -eq 1 ] ; then
            break
        else
            sleep 10 
        fi
    done
    executeJobListCommand
    cat /tmp/joblist.out |grep "jobs:0"
    if [ $? -ne 0 ] ; then
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

function test_getJobListWithKilledJob {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithKilledJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t25.out"
    #submitSleepJob 10 10 5000 5000 1 $captureFile
    submitSleepJobToQueue "a" $captureFile 10 10 50000 50000
    sleep 15
    local myjobId=`getJobId $captureFile`
    local myresp=`killJob $myjobId`
    if [ $myresp == "Killed" ] ; then
        echo " Job has been killed successfullly " >> $ARTIFACTS_FILE 2>&1
    fi               
    sleep 10
    executeJobListCommand
    cat /tmp/joblist.out |grep "jobs:0"
    if [ $? -ne 0 ] ; then
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

function test_getJobListWithFailedJob {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListWithKilledJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t25.out"
    #submitSleepJob 10 10 500000 500000 1 $captureFile
    submitSleepJobToQueue "a" $captureFile 10 10 500000 500000
    sleep 15
    local myjobId=`getJobId $captureFile`
    failAJob $myjobId
    sleep 10
    executeJobListCommand
    cat /tmp/joblist.out |grep "jobs:0"
    if [ $? -ne 0 ] ; then
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

function test_getJobListMetaData {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListMetaData"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    submitSleepJob 10 10 5000 5000 1
    sleep 5
    executeJobListCommand
    #cat /tmp/joblist.out |grep "jobs:1"
    local state=`cat /tmp/joblist.out |tail -1 |awk -F' ' '{print $2}'`
    local userName=`cat /tmp/joblist.out |tail -1 |awk -F' ' '{print $4}'`
    local myQueue=`cat /tmp/joblist.out |tail -1 |awk -F' ' '{print $5}'`
    
    if [ $state != "RUNNING" ] || [ $userName != $USER_ID ] || [ $myQueue != "default" ] ; then
        echo " One of the checks for state, username, queue name did not match the expected and returned $state, $userName, $myQueue resp and so failing " >> $ARTIFACTS_FILE 2>&1
        setFailCase " One of the checks for state, username, queue name did not match the expected and returned $state, $userName, $myQueue resp and so failing "
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_getJobListAll {
    # Test to request job -list with there are no running jobs 
    TESTCASE_DESC="test_getJobListMetaData"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t25.out"
    #submitSleepJob 10 10 500000 500000 1 $captureFile
    submitSleepJobToQueue "default" $captureFile 10 10 500000 500000
    sleep 15
    local myFailJobId=`getJobId $captureFile`
    failAJob $myFailJobId
    sleep 10      
    submitSleepJobToQueue "default" $captureFile 10 10 500000 500000
    sleep 15
    local myKillJobId=`getJobId $captureFile`
    local myresp=`killJob $myKillJobId`
    if [ $myresp == "Killed" ] ; then
        echo " Job has been killed successfullly " >> $ARTIFACTS_FILE 2>&1
    fi
    sleep 10              
    submitSleepJobToQueue "default" $captureFile 10 10 500000 500000
    sleep 15
    local myRunningJobId=`getJobId $captureFile`
    executeJobListCommand all
    cat /tmp/joblist.out
    if [ `cat /tmp/joblist.out |grep $myKillJobId |awk -F' ' '{print $2}'` != "KILLED" ] || [ `cat /tmp/joblist.out |grep $myRunningJobId |awk -F' ' '{print $2}'` != "RUNNING" ] ; then
        echo " One of the checks for Failed, Running or Killed job is not recorded in the job -list all command and so failing the same.  " >> $ARTIFACTS_FILE 2>&1
        setFailCase " One of the checks for Failed, Running or Killed job is not recorded in the job -list all command and so failing the same.  "
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE 

}               

function test_getJobListWithPendingJobs {
    # Test to request job -list with there are pending jobs 
    TESTCASE_DESC="test_getJobListWithPendingJobs"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFileSeed="/tmp/t25.out"
    for  (( i=0; i < 10; i++ )); do
        local captureFile=$captureFileSeed$i    
        submitSleepJobToQueue "default" $captureFile 10 10 500000 500000 >> $ARTIFACTS_FILE 2>&1&
        sleep 15
        echo " ######################################"
        cat $captureFile
        echo " ######################################"
        local myLongSleepJobId=`getJobId $captureFile`  
        echo " >>>>>>>>>>>>>>>>>>>>>>>>>>>> job id created is $myLongSleepJobId "       
        jobIds[$i]=$myLongSleepJobId            
        executeJobListCommand all
        if [ `cat /tmp/joblist.out |grep $myLongSleepJobId |awk -F' ' '{print $2}'` == "PREP" ] ; then
            echo " Successfully set the job in pending state and so passing the test case "  
            break
        fi
    done
    if [ `cat /tmp/joblist.out |grep $myLongSleepJobId |awk -F' ' '{print $2}'` != "PREP" ] ; then  
        echo " Unable to set the job in prep state and so failing the tests.  " >> $ARTIFACTS_FILE 2>&1
        setFailCase "   "   
        windUp
        return
    else
        COMMAND_EXIT_CODE=0
    fi
    echo " List of job ids created are as follows "
    for myjobid in ${jobIds[@]}; do
        killJob $myjobid
    done     

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}                       


test_getJobListWithNoRunningJobs
test_getJobListWithOneRunningJob
test_getJobListWithCompletedJob
test_getJobListWithKilledJob
test_getJobListWithFailedJob
test_getJobListMetaData
test_getJobListAll
test_getJobListWithPendingJobs

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
