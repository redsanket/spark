#!/bin/bash

set -x

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/restart_CLUSTER_lib.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`

customConfDir="/homes/$USER_ID/CustomConfDir"
customSetUp="DEFAULT"
#customSetUp="CUSTOM"

function setUpQueueCustomEnv {
    local myrmHost=$1                       
    copyConfigs $customConfDir $myrmHost
    putFileToRemoteServer $myrmHost ${JOB_SCRIPTS_DIR_NAME}/data/capacity-scheduler_c2QueueStopped.xml $customConfDir/capacity-scheduler.xml
    
    setUpRMandNMWithConfig $myrmHost "" $nmHost $customConfDir 
    
    export HADOOP_CONF_DIR=$customConfDir
    customSetUp="CUSTOM"
}


##########################################
# This function triggers queue command 
# takes the command like list, info or showacls as $2
# takes the arg showjobs as $3
# takes the file to capture the output as $1
##########################################
function triggerQueueCommand {
    local captureFile=$1
    local mycommand=$2
    local myargs=$3
    local showjobs=$4
    if [ ! -z $showjobs ] ; then
        $HADOOP_MAPRED_HOME/bin/mapred queue -$mycommand $myargs -$showjobs > $captureFile 2>&1&
    else
        $HADOOP_MAPRED_HOME/bin/mapred queue -$mycommand $myargs > $captureFile 2>&1&
    fi
    sleep 5
    echo " Capture of the output of queue command " >> $ARTIFACTS_FILE 2>&1 
    cat $captureFile >> $ARTIFACTS_FILE 2>&1        
}

function test_QueueWithNoOptions {
    # This test verifies that the queue interface with no params returns a help message
    TESTCASE_DESC="test_QueueWithNoOptions"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    local captureFile="/tmp/t30.out"
    local expectedData=17
    COMMAND_EXIT_CODE=1
    triggerQueueCommand $captureFile
    if [ `cat $captureFile |wc -l` -eq $expectedData ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo " Did not get the expected line count and so failing the test case" 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_QueueWithInvalidOptions {
    # This test verifies that the queue interface with no params returns a help message
    TESTCASE_DESC="test_QueueWithInvalidOptions"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t30.out" 
    local expectedData=17
    triggerQueueCommand $captureFile "qqqqqqqqqqqqqq"
    if [ `cat $captureFile |wc -l` -eq $expectedData ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo " Did not get the expected line count and so failing the test case" 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_QueueWithListOption {
    # This test verifies that the queue interface with list option
    TESTCASE_DESC="test_QueueWithListOption"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t30.out"
    local expectedData=17
    
    local rmHost=`getResourceManagerHost`
    setUpQueueCustomEnv $rmHost
              
    sleep 10
    triggerQueueCommand $captureFile "list"
    if [ `cat $captureFile |wc -l` -eq $expectedData ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo "The bug 4592142 is returning only the first level queues and not the rest and failing test case " 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_QueueWithInfoOptionWithNoQueueName {
    # This test verifies that the queue interface with info option
    TESTCASE_DESC="test_QueueWithInfoOptionWithNoQueueName"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t31.out"
    local expectedData=17
    local rmHost=`getResourceManagerHost`
    if [ $customSetUp == "DEFAULT" ] ; then
        setUpQueueCustomEnv $rmHost
    fi
    sleep 10
    triggerQueueCommand $captureFile "info"
    if [ `cat $captureFile |wc -l` -eq $expectedData ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo "The bug 4592142 is returning only the first level queues and not the rest and failing test case " 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_QueueWithInfoOptionWithQueueName {
    # This test verifies that the queue interface with info option
    TESTCASE_DESC="test_QueueWithInfoOptionWithQueueName Bug 4592168"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t30.out"
    local expectedData=17
    local queueName="c2"
    local rmHost=`getResourceManagerHost`
    if [ $customSetUp == "DEFAULT" ] ; then
        setUpQueueCustomEnv $rmHost
    fi
    sleep 10
    triggerQueueCommand $captureFile "info" $queueName
    if [ `cat $captureFile |wc -l` -eq $expectedData ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo "The bug 4592168 failing test case " 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_QueueWithInfoOptionWithInvalidQueueName {
    # This test verifies that the queue interface with info option
    TESTCASE_DESC="test_QueueWithInfoOptionWithInvalidQueueName"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t30.out"
    local expectedData=1
    local queueName="c5"
    local rmHost=`getResourceManagerHost`
    if [ $customSetUp == "DEFAULT" ] ; then
        setUpQueueCustomEnv $rmHost
    fi
    sleep 10
    triggerQueueCommand $captureFile "info" $queueName
    if [ `cat $captureFile |grep "java.io.IOException: Unknown queue" |wc -l` -eq $expectedData ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo "The check for Invalid queue message failed and so marking the test case " 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_QueueWithInfoOptionWithQueueStopped {
    # This test verifies that the queue interface with info option
    TESTCASE_DESC="test_QueueWithInfoOptionWithQueueName"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t30.out"
    local expectedData=17
    local queueName="c2"
    local rmHost=`getResourceManagerHost`
    if [ $customSetUp == "DEFAULT" ] ; then
        setUpQueueCustomEnv $rmHost
    fi
    sleep 10
    triggerQueueCommand $captureFile "info" $queueName
    local myresp=`cat $captureFile |grep "Scheduling Info" |grep Q_STOPPED |wc -l`
    if [ $myresp -eq 1 ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo "The bug 4592168 failing test case " 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_QueueWithShowJobsInfoForQueueRunningJob {
    # This test verifies the showJobs option for queues that has jobs in running state
    TESTCASE_DESC="test_QueueWithShowJobsInfoForQueueRunningJob"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    COMMAND_EXIT_CODE=1
    local captureFile="/tmp/t30.out"
    local captureFile1="/tmp/t31.out"
    local expectedData=17
    local queueName="b"
    local showjobs="showJobs"
    local rmHost=`getResourceManagerHost`
    if [ $customSetUp == "DEFAULT" ] ; then
        setUpQueueCustomEnv $rmHost
    fi
    sleep 10
    # Submit a job to the queue 
    submitSleepJobToQueue "b" $captureFile 10 10 50000 50000
    sleep 10
    local myjobId=`getJobId $captureFile`
    triggerQueueCommand $captureFile1 "info" $queueName $showjobs
    local myresp=`cat $captureFile1 |grep $myjobId |wc -l`
    if [ $myresp -eq 1 ] ; then
        echo " The line count is as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        COMMAND_EXIT_CODE=0
    else
        echo "Capture of command line output ***********************"
        cat $captureFile
        echo " End of commandline output **************************"
        echo " Did not get the appid of the running job in the showjobs  failing test case " 
        setFailCase "Did not get the expected line count and so failing the test case" >> $ARTIFACTS_FILE 2>&1
        windUp
        return
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


test_QueueWithNoOptions
#test_QueueWithInvalidOptions
#test_QueueWithListOption
#test_QueueWithInfoOptionWithNoQueueName
#test_QueueWithInfoOptionWithQueueName
#test_QueueWithInfoOptionWithQueueStopped
#test_QueueWithInfoOptionWithInvalidQueueName
#test_QueueWithShowJobsInfoForQueueRunningJob

resetYarnEnv

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
