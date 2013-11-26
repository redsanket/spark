#!/bin/bash  
set -x

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh
#export CLUSTER=wilma
#export HADOOP_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoop-current
#export HADOOP_COMMON_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoop-current
#export HADOOP_CONF_DIR=/grid/0/gs/gridre/yroot.$CLUSTER/conf/hadoop

export COMMAND_EXIT_CODE=0
export SCRIPT_EXIT_CODE=0
export TESTCASE_ID=10
export TESTCASE_DESC="None"
export JOBTRACKER="None"
export USER_ID=`whoami`

#config file location for JT 
myconfDir=/homes/$USER_ID/customconf
customSetUp="default"

function moveRequiredFiles {
    echo " Moving both the jmxterm and Fetchmetrics info to hadoop homes dir "
    cp /$JOB_SCRIPTS_DIR_NAME/data/jmxterm-1.0-SNAPSHOT-uber.jar /homes/hadoopqa
    cp /$JOB_SCRIPTS_DIR_NAME/data/FetchMetricsInfo.sh /homes/hadoopqa
}


function setMetricsTestEnv {
    local myconfDir=$1
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    copyJTConfigs $myconfDir $JOBTRACKER
    # Modify the jmx metrics refresh intreval
    sed -i 's/*.period=10/*.period=1/g' $myconfDir/hadoop-metrics2.properties
    setUpJTWithConfig $JOBTRACKER $myconfDir
    if [ $? -eq 1 ] ; then
        setFailCase "Unable to set the env pointing to the custom location and so failing the test case"
    else 
        customSetUp="custom"
    fi
}

#################################################################
# This function takes the Jobid as $1 and kills  the job as part of clean up 
#################################################################

function windUpAndKillJob {
    local myJobId=$1
    # displayTestCaseResult
    if [ ! -z $myJobId ] ; then
        echo " Proceeding to kill the job $myJobId "
        killAJob $myJobId
    fi
}


#################################################################
# This funciton gets the process id of the Job tracker 
# takes the jobtracker host as $1
#################################################################
function getJTProcessId {
    local jtHost=$1
    # sudo su mapred -c "ssh $jtHost ps -ef |grep mapred |grep jobtracker" |awk -F' ' '{print $2}'
    # ssh -i  /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$jtHost exec ps -ef |grep mapred |grep jobtracker |awk -F' ' '{print $2}'
    ssh -i  /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$jtHost exec ps -ef |grep mapred |grep "proc_jobtracker" |awk -F' ' '{print $2}' |head -1
    return $?
}

#################################################################
# This function updates the mapred site.xml with the new location for the mapred.exclude file
#################################################################
function updateJTConfigWithNewExcludeLocation {
    local newLocation=$1
    echo " The new location is $newLocation "   
    modifyValueOfAField $ARTIFACTS_DIR/mapred-site.xml mapred.hosts.exclude $newLocation
}

#################################################################
# This function returns the metrics info as requested
# $1 is the JT host
# $2 is teh PID of the job tracker
# $3 is the value of the key interested in
#################################################################
function getJTMetricsInfo {
    local myJTHost=$1
    local myPID=$2
    local myparam=$3
    local mytimes=$4
    local myexpected=$5
    if [ -z $mytimes ] ; then
        mytimes=0
    fi
    for (( k=0 ; k <= $mytimes; k++ )); do
        ssh -i  /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$myJTHost exec /homes/mapred/FetchMetricsInfo.sh $myPID $myparam  >$ARTIFACTS_DIR/jmxmetrix.log       
        sleep 10
        if [ ! -z $myexpected ] ; then
            echo " Since I have the expected data...looping till I get the expected data $mytimes "
            local mydata=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
            if [ $myexpected -eq $mydata ] ; then
                break
            fi
        fi
        echo " Looping in to verify the metrics info $k times "
    done
    echo "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
    cat $ARTIFACTS_DIR/jmxmetrix.log
    echo "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
}


function failAMapTask {
    local myjobId=$1
    local myTaskId=$(echo $myjobId|sed 's/job/task/g'|sed 's/$/_m_000000/g')
    local myattemptId0=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_0/g')
    local myattemptId1=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_1/g')
    local myattemptId2=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_2/g')
    local myattemptId3=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_3/g')
    echo " my jod id = $myjobId"
    echo "my taskid = $myTaskId"
    echo "my attemptid0 is $myattemptId0 "
    echo "my attemptid1 is $myattemptId1 "
    echo "my attemptid2 is $myattemptId2 "
    echo "my attemptid3 is $myattemptId3 "

    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId0  
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId1  
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId2  
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId3  
}


function clearCustomSetting {
    # resetting the env to clear the  decommissioning
    resetEnv $myconfDir
    echo " Stopping the task trackers using $HADOOP_CONF_DIR"
    resetTaskTrackers stop
    sleep 10
    echo " Start the task trackers using $HADOOP_CONF_DIR"
    resetTaskTrackers start
    resetJobTracker stop
    sleep 10
    resetJobTracker start
    sleep 10
}       

function failAReduceTask {
    local myjobId=$1
    local myTaskId=$(echo $myjobId|sed 's/job/task/g'|sed 's/$/_r_000000/g')
    local myattemptId0=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_0/g')
    local myattemptId1=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_1/g')
    local myattemptId2=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_2/g')
    local myattemptId3=$(echo $myTaskId|sed 's/task/attempt/g'|sed 's/$/_3/g')
    echo " my jod id = $myjobId"
    echo "my taskid = $myTaskId"
    echo "my attemptid0 is $myattemptId0 "
    echo "my attemptid1 is $myattemptId1 "
    echo "my attemptid2 is $myattemptId2 "
    echo "my attemptid3 is $myattemptId3 "

    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId0  
    sleep 10
    date    
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId1  
    sleep 10
    date
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId2  
    sleep 4
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task $myattemptId3  
}

################################################################
#Function excludes the tasktrackers
#if there is no input, it takes the first 4 tasktrakers and excludes them
# if $1 is provided with a task tracker host 
# it just excludes them
# This method depends on  custom configuration of JT
################################################################
function excludeTaskTrackers {
    local myTTHost=$1       
    if [ -z $myTTHost ] ; then
        echo " The host passed to excludeTaskTrackers is empty and so going to exclude top 4 hosts "    
    fi
    echo " The task tracker to exculde is $myTTHost"
    # if this function gets a host entering the same to TT_Hosts file 
    # if not getting the first 4 entries from getActiveTaskTrackers  and entering in TT_Hosts file
    if [ -z $myTTHost ] ; then
        getActiveTaskTrackers|awk '{if (NR <= 4 ) {print}}'> $ARTIFACTS_DIR/TT_hosts
    else
        echo $myTTHost > $ARTIFACTS_DIR/TT_hosts
    fi
    echo " No of trackers excluded is  as follows "
    cat $ARTIFACTS_DIR/TT_hosts
    # Getting JobTracker hostname
    getJobTracker
    echo $JOBTRACKER
    getFileFromRemoteServer $JOBTRACKER $myconfDir/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    updateJTConfigWithNewExcludeLocation $myconfDir/mapred.exclude
    # Moving the mapred file to JT host
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/mapred-site.xml $myconfDir/mapred-site.xml
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/TT_hosts $myconfDir/mapred.exclude
    # Do a refresh as hdfs user
    setKerberosTicketForUser $HDFS_USER
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR mradmin -refreshNodes
    setKerberosTicketForUser $HADOOPQA_USER
    sleep 10        
}

function testJobs_FailedMetrics {
    echo "*************************************************************"
    TESTCASE_DESC="testJobs_FailedMetrics"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of jobs_failed in JT using Jmxterm  "
    echo " 2. Trigger a failed job "
    echo " 3. Verify that the count gets increased to one  "
    echo "*************************************************************"
    local mymetricsKey="jobs_failed"
    local initialCount=0
    local finalCount=0
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi    
    if [ -z $initialCount ]; then
        echo " The intial count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    local  myprocessId=`getJTProcessId $JOBTRACKER` 
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey         
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`   
    echo " Initial count of  jobs_failed is  $initialCount "
    triggerSleepJob 1 1 100000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    checkForNewAttemptIds $myJobId
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to fail them "
        failAttempts
        checkForNewAttemptIds $myJobId
    done
    checkForJobStatus $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "   
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "
        windUp  
        return
    fi      
    sleep 10        
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi      
    echo " The final count of jobs_failed is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of $mymetricsKey  was accounted for and test case passed " 
    else
        echo " The count of failed job was not accounted for and so failing test case"
        setFailCase " The count of failed job was not accounted for and so failing test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE 
}

function testJobs_KilledMetrics {
    echo "*************************************************************"
    TESTCASE_DESC="testJobs_KilledMetrics "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of jobs_killed in JT using Jmxterm  "
    echo " 2. Trigger a killed job "
    echo " 3. Verify that the count gets increased to one  "
    echo "*************************************************************"
    local mymetricsKey="jobs_killed"
    local initialCount=0
    local finalCount=0
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`   
    if [ -z $initialCount ]; then
        echo " The intial count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi      
    echo " Initial count of  jobs_failed is  $initialCount "
    triggerSleepJob 1 1 100000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    killAJob $myJobId
    sleep 10
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The intial count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of jobs_failed is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of jobs_killed  was  accounted for and test case passed " 
    else
        echo " The count of $mymetricsKey  was not accounted for and so failing test case"
        setFailCase " The count of $mymetricsKey was not accounted for and so failing test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testJobs_RunningMetrics {
    echo "*************************************************************"
    TESTCASE_DESC="testJobs_RunningMetrics"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of jobs_running in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the running job count gets increased to one while the job is running "
    echo "*************************************************************"
    local mymetricsKey="jobs_running"
    local initialCount=0
    local finalCount=0
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey  
    initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The intial count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  jobs_running is  $initialCount "
    triggerSleepJob 10 1 70000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    sleep 12
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of jobs_failed is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of running job was not accounted for and so failing test case"
        setFailCase " The count of running job was not accounted for and so failing test case"
        windUp
        return    
    fi

    for (( i=0; i < 10; i++ )); do
        checkForJobCompletion $myJobId
        sleep 20
        if [ $? -eq 0 ] ; then
            echo "Confirmed that the job has completed "
            break
        fi
    done
    if [ $? -eq 1 ] ; then
        echo " Did not find the completion string to confirm that the job has completed and so failing"
        setFailCase " Did not find the completion string to confirm that the job has completed and so failing"
        return
    fi
    sleep 10
    
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`           
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion  count to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The job_running has been reset to $finalCountAfterJobCompletion "
    else
        echo " job_running has not been reset  and so failing the test case"
        setFailCase " job_running has not been reset  and so failing the test case"
        windUp
        return
    fi 
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testMaps_RunningMetricsForSuccessfulJobs {
    echo "*************************************************************"
    TESTCASE_DESC="testMaps_RunningMetricsForSuccessfulJobs"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of running_maps in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the running maps count gets increased  while the job is running "
    echo "*************************************************************"
    local mymetricsKey="running_maps"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep running_maps |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`  
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi 
    echo " Initial count of  running_maps is  $initialCount "
    triggerSleepJob 2 1 15000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep running_maps |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of running_maps is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of running maps was not accounted for and so failing test case"
        setFailCase " The count of running maps was not accounted for and so failing test case"
        windUp
        return
    fi
    for (( i=0; i < 10; i++ )); do
        checkForJobCompletion $myJobId
        sleep 20
        if [ $? -eq 0 ] ; then
            echo "Confirmed that the job has completed "
            break
        fi
    done
    if [ $? -eq 1 ] ; then
        echo " Did not find the completion string to confirm that the job has completed and so failing"
        setFailCase " Did not find the completion string to confirm that the job has completed and so failing"
        return
    fi
    sleep 10
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion  count to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion  count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The job_running has been reset to $finalCountAfterJobCompletion "
    else
        echo " running_maps has not been reset  and so failing the test case"
        setFailCase " running_maps has not been reset  and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testMaps_RunningMetricsForFailedJobsByFailingAttempts {
    echo "*************************************************************"
    TESTCASE_DESC="testMaps_RunningMetricsForFailedJobsByFailingAttempts"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of running_maps in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the running maps count gets increased  while the job is running and reduces for failed jobs "
    echo "*************************************************************"
    local mymetricsKey="running_maps"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep running_maps |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  running_maps is  $initialCount "
    triggerSleepJob 20 1 150000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    sleep 10
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of running_maps is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of running maps was not accounted for and so failing test case"
        setFailCase " The count of running maps was not accounted for and so failing test case"
        windUp
        return
    fi
    
    checkForNewAttemptIds $myJobId
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to fail them "
        failAttempts
        checkForNewAttemptIds $myJobId
    done
    checkForJobStatus $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "   
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "
        windUp
        return
    fi
    sleep 10
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The running_maps has been reset to $finalCountAfterJobCompletion "
    else
        echo " running_maps has not been reset  and so failing the test case"
        setFailCase " running_maps has not been reset  and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testReduce_RunningMetricsForSuccessfulJobs {
    echo "*************************************************************"
    TESTCASE_DESC=" testReduce_RunningMetricsForSuccessfulJobs"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of running_reduce in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the running reduce count gets increased  while the job is running "
    echo "*************************************************************"
    local mymetricsKey="running_reduces"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  running_reduces is  $initialCount "
    triggerSleepJob 1 5 1000 15000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of running_reduces is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of running reduces was not accounted for and so failing test case"
        setFailCase " The count of running reduces was not accounted for and so failing test case"
        windUp
        return
    fi
    for (( i=0; i < 10; i++ )); do
        checkForJobCompletion $myJobId
        sleep 20
        if [ $? -eq 0 ] ; then
            echo "Confirmed that the job has completed "
            break
        fi
    done
    if [ $? -eq 1 ] ; then
        echo " Did not find the completion string to confirm that the job has completed and so failing"
        setFailCase " Did not find the completion string to confirm that the job has completed and so failing"
        return
    fi
    sleep 35
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0  
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion  to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The running_reduces has been reset to $finalCountAfterJobCompletion "
    else
        echo " running_reduces has not been reset  and so failing the test case"
        setFailCase " running_reduces has not been reset  and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testReduce_RunningMetricsForFailedJobsByFailingAttempts {
    echo "*************************************************************"
    TESTCASE_DESC="testReduce_RunningMetricsForFailedJobsFailingAttempts"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 3. Verify that the running reduce count gets increased  while the job is running and reduces for failed jobs "
    echo "*************************************************************"
    local mymetricsKey="running_reduces"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  running_reduces is  $initialCount "
    triggerSleepJob 1 5 1000 15000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of running_reduces is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of running reduces was not accounted for and so failing test case"
        setFailCase " The count of running reduces was not accounted for and so failing test case"
        windUp
        return
    fi

    checkForNewAttemptIds $myJobId "reduce"
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to fail them "
        failAttempts
        checkForNewAttemptIds $myJobId "reduce"
    done
    checkForJobStatus $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "   
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "
        windUp
        return
    fi

    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The running_reduces has been reset to $finalCountAfterJobCompletion "
    else
        echo " running_reduces has not been reset  and so failing the test case"
        setFailCase " running_reduces has not been reset  and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testMaps_RunningMetricsForKilledJobsByKillingTasks {
    echo "*************************************************************"
    TESTCASE_DESC=" testMaps_RunningMetricsForKilledJobsByKillingTasks"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of running_maps in JT using Jmxterm  "
    echo " 2. Trigger a  job and kill the same "
    echo " 3. Verify that the running maps count gets increased  while the job is running "
    echo "*************************************************************"
    local mymetricsKey="running_maps"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  running_maps is  $initialCount "
    triggerSleepJob 10 1 20000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey  |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of running_maps is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of running_maps  was accounted for and test case passed " 
    else
        echo " The count of running maps was not accounted for and so failing test case"
        setFailCase " The count of running maps was not accounted for and so failing test case"
        windUp
        return
    fi
    #killAJob $myJobId 
    checkForNewAttemptIds $myJobId "reduce"
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to fail them "
        failAttempts
        checkForNewAttemptIds $myJobId 
    done 
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion  count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The running_maps has been reset to $finalCountAfterJobCompletion "
    else
        echo " running_maps has not been reset  and so failing the test case"
        setFailCase " running_maps has not been reset  and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testReduces_RunningMetricsForKilledJobsByKillingTasks {
    echo "*************************************************************"
    TESTCASE_DESC="testReduces_RunningMetricsForKilledJobsByKillingTasks"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of running_maps in JT using Jmxterm  "
    echo " 2. Trigger a  job and kill the same "
    echo " 3. Verify that the running maps count gets increased  while the job is running "
    echo "*************************************************************"
    local mymetricsKey="running_reduces"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  running_reduces is  $initialCount "
    triggerSleepJob 1 5 1000 10000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of running_reduces is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of running_reduces was accounted for and test case passed " 
    else
        echo " The count of running_reduces was not accounted for and so failing test case"
        setFailCase " The count of running_reduces was not accounted for and so failing test case"
        windUp
        return
    fi
    checkForNewAttemptIds $myJobId "reduce"
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to fail them "
        failAttempts
        checkForNewAttemptIds $myJobId "reduce"
    done
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 0 
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    echo " The value of the $mymetricsKey after the job completion is  $finalCountAfterJobCompletion "      
    
    if [ $finalCountAfterJobCompletion -eq 0 ] ; then
        echo " The running_reduces has been reset to $finalCountAfterJobCompletion "
    else
        echo " running_reduces has not been reset  and so failing the test case"
        setFailCase " running_reduces has not been reset  and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testMaps_FailedMetricsForFailedJobsByFailingMaps {
    echo "*************************************************************"
    TESTCASE_DESC="testMaps_FailedMetricsForFailedJobsByFailingMaps"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of maps_failed in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the running maps count gets increased  while the job is running and reduces for failed jobs "
    echo "*************************************************************"
    local mymetricsKey="maps_failed"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  $mymetricsKey is  $initialCount "
    triggerSleepJob 10 1 15000 1000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    checkForNewAttemptIds $myJobId
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to fail them "
        failAttempts
        checkForNewAttemptIds $myJobId
    done
    sleep 15        
    checkForJobStatus $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "   
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "
        windUp
        return
    fi

    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -gt $initialCount ] ; then
        echo " The $mymetricsKey has moved from $initialCount to   $finalCountAfterJobCompletion "
    else
        echo " $mymetricsKey has not increased after failing the map tasks and so failing the test case"
        setFailCase "$mymetricsKey has not increased after failing the map tasks and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testReduces_FailedMetricsForFailedJobs {
    echo "*************************************************************"
    TESTCASE_DESC="testReduces_FailedMetricsForFailedJobs"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of reduces_failed in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the reduces_failed count gets increased  while the job is running and reduces for failed jobs "
    echo "*************************************************************"
    local mymetricsKey="reduces_failed"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the jobs_failed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  $mymetricsKey is  $initialCount "
    #triggerSleepJob 1 10 15000 15000 1
    triggerSleepJob 1 1 1000  15000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    # failAReduceTask $myJobId
    # return
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "

    checkForNewAttemptIds $myJobId "reduce"
    echo " &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
    cat $ARTIFACTS_DIR/AttemptIdFile
    echo " &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to kill them "
        failAttempts
        checkForNewAttemptIds $myJobId "reduce"
        echo " &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
        cat $ARTIFACTS_DIR/AttemptIdFile
        echo " &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
    done
    checkForJobStatus $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "   
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "
        windUp
        return
    fi
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local finalCountAfterJobCompletion=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCountAfterJobCompletion ]; then
        echo " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        setFailCase " The finalCountAfterJobCompletion to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $finalCountAfterJobCompletion -gt $initialCount ] ; then
        echo " The $mymetricsKey has moved from $initialCount to   $finalCountAfterJobCompletion "
    else
        echo " $mymetricsKey has not increased after failing the map tasks and so failing the test case"
        setFailCase "$mymetricsKey has not increased after failing the map tasks and so failing the test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testHeartBeatMetrics1680 { 
    echo "*************************************************************"
    TESTCASE_DESC="testHeartBeatMetrics "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the heartbeat processed by JT using Jmxterm  "
    echo " 2. Sleep for few seconds "
    echo " 3. Verify that the count gets increased   "
    echo "*************************************************************"
    local mymetricsKey="heartbeats"
    local initialCount=0
    local finalCount=0
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the metrics processed
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The intial count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  heartbeat processed  is  $initialCount "
    sleep 20
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The intial count to be returned from jmx term is empty and so failing "
        setFailCase " The intial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of jobs_failed is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of metrics  was  accounted for and test case passed " 
    else
        echo " The count of $mymetricsKey  was not accounted for and so failing test case"
        setFailCase " The count of $mymetricsKey was not accounted for and so failing test case"  
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testOccupiedMapSlotsMetricsForSuccessfulJobs {
    echo "*************************************************************"
    TESTCASE_DESC="testOccupiedMapSlotsMetricsForSuccessfulJobs "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of occupiedMapSlots  in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the occupiedMapsSlots count gets increased  while the job is running "
    echo "*************************************************************"
    local mymetricsKey="occupied_map_slots"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  occupied_map_slots is  $initialCount "
    triggerSleepJob 20 20 15000 15000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    for (( i=0 ; i < 3; i++ )); do
        getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 
        local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
        if [ $finalCount -gt $initialCount ] ; then
            echo "Got the final count greater than initial count for $mymetricsKey "
            break
        fi
    done
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of occupied_map_slots is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of occupied_map_slots  was not accounted for and so failing test case"
        setFailCase " The count of occupied_map_slots was not accounted for and so failing test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testOccupiedReduceSlotsMetricsForSuccessfulJobs {
    echo "*************************************************************"
    TESTCASE_DESC="testOccupiedReduceSlotsMetricsForSuccessfulJobs "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the count of occupiedReduceSlots  in JT using Jmxterm  "
    echo " 2. Trigger a  job "
    echo " 3. Verify that the occupiedreducesSlots count gets increased  while the job is running "
    echo "*************************************************************"
    local mymetricsKey="occupied_reduce_slots"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  occupied_reduce_slots is  $initialCount "
    triggerSleepJob 20 20 15000 15000 1
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    for (( i=0 ; i < 3; i++ )); do
        getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
        local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
        if [ $finalCount -gt $initialCount ] ; then
            echo "Got the final count greater than initial count for $mymetricsKey "
            break
        fi
    done
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " The final count of occupied_reduce_slots is $finalCount "
    if [ $finalCount -gt $initialCount ] ; then
        echo " The count of job was accounted for and test case passed " 
    else
        echo " The count of occupied_reduce_slots  was not accounted for and so failing test case"
        setFailCase " The count of occupied_reduce_slots was not accounted for and so failing test case"
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testTrackersDecommissionedMetrics {
    echo "*************************************************************"
    TESTCASE_DESC="testTrackersDecommissionedMetrics "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Exclude certain nodes in the cluster by adding them to mapred.exclude file"
    echo "2.  fetch the trackers_decommissioned "   
    echo "3. verify that the decommissioned count matches the exclude count "
    echo "*************************************************************"
    local mymetricsKey="trackers_decommissioned"
    # local mymetricsKey="trackers_blacklisted"
    local myexcludedNodes=4
    # Check if the env is pointing to customconfig   as expected
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    echo " Getting the initial count of decommissioned nodes via jmx metrics"
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        clearCustomSetting
        return
    fi
    echo " Initial count of  decommissioned metrics is  $initialCount "
    # Calling the exclude tasktracker method without passing the TT host 
    excludeTaskTrackers
    #Checking the WebUI for the count
    wget --quiet "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jt_content
    local expExcludedNodes=`grep -A 1 "excluded" $ARTIFACTS_DIR/jt_content |awk -F'excluded' '{print $2}' |sed "s/[^0-9]//g"`
    echo " The expected excluded Nodes count is  $myexcludedNodes "
    echo " The new excluded Nodes count is  $expExcludedNodes "
    echo " Now checking the metrix for decommision count "
    for (( i=0 ; i < 3; i++ )); do
        getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
        local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
        if [ $finalCount -gt $initialCount ] ; then
            echo "Got the final count greater than initial count for $mymetricsKey "
            break
        fi
    done
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        clearCustomSetting
        return
    fi
    echo " The final count of trackers_decommissioned is $finalCount "
    if [ $finalCount -eq $expExcludedNodes ] ; then
        echo " The count of decommisioned nodes matched the UI count  test case passed " 
    else
        echo " The count of trackers_decommissioned did not match the UI count and so failing test case"
        setFailCase " The count of trackers_decommissioned did not match the UI count and so failing test case"
        windUp
        clearCustomSetting
        return
    fi
    # clearing the custom entries in the mapred.exclude file and resetting the env
    clearCustomSetting
    local myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    echo " Getting the clear count of decommissioned nodes  after reset of mapred.exclude via jmx metrics"
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey       
    local clearCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`       
    if [ -z $clearCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        clearCustomSetting
        return
    fi
    echo " The clear count of trackers_decommissioned is $clearCount "
    
    if [ $clearCount -eq $initialCount ] ; then
        echo " The count of decommisioned nodes after reset of exclude nodes   was accounted for and test case passed " 
    else
        echo " The count of trackers_decommissioned  was not accounted for and so failing test case"
        setFailCase " The count of trackers_decommissioned was not accounted for and so failing test case"
        windUp
        clearCustomSetting
        return
    fi 
    # clearin the env after the test
    clearCustomSetting
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testTrackersCountMetrics {
    echo "**********************************************************"
    TESTCASE_DESC="testTrackersCountMetrics "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1.Fetch the count of tasktrackers via command line utility"
    echo " 2. Using jmxterm get the count of active task trackers"  
    echo " 3. Compare both the results "
    echo "**********************************************************"
    if [ $customSetUp == "default" ] ; then
        setMetricsTestEnv $myconfDir
    fi
    local mymetricsKey="trackers"
        #local myactiveTTCount=`getActiveTaskTrackers |wc -l`
    local myactiveTTCount=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -list-active-trackers |wc -l`
    echo " The count of active trackers is $myactiveTTCount "
    getJobTrackerHost
    # restartJobTracker $JOBTRACKER   
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    echo " Getting the initial count of nodes via jmx metrics"
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local metrixCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $metrixCount ]; then
        echo " The metrix count to be returned from jmx term is empty and so failing "
        setFailCase " The metrix count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    if [ $metrixCount -eq $myactiveTTCount ] ; then
        echo " The count of task tracker node matched the count via  command line and test case passed " 
    else
        echo " The count of task tracker node is $metrixCount and did not match the command line $myactiveTTCount and the test case failed"
        setFailCase " The count of trackers_decommissioned was not accounted for and so failing test case"
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}               

function testOccupiedMapSlotsMetricsAfterDecommissioningTT {
    echo "**********************************************************"
    TESTCASE_DESC="testOccupiedMapSlotsMetricsAfterDecommissioningTT "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Submit a job that occupies x number of slots "
    echo " 2. Get the count of Occupied MapSlots "
    echo " 3. Now get the task tracker that is executing the task for this job"
    echo " 4. Add this host to mapred.exclude file  and do a node refresh"
    echo " 5. Verify that the occupied map slots count is reduced "
    local mymetricsKey="occupied_map_slots"
    # Check if the env is pointing to customconfig   as expected
    if [ $customSetUp == "default" ] ; then
        setTestEnv $myconfDir
    fi
    triggerSleepJob 20 20 100000 100000 1
    getJobTrackerHost
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        clearCustomSetting
        return
    fi
    echo " The job id got is $myJobId " 
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        clearCustomSetting
        return
    fi
    echo " Initial count of  occupied_map_slots  is  $initialCount "
    echo " The job id that is being used is $myJobId "      
    #myattemptId=`getAttemptIdsForJobId $myJobId`
    myattemptId=`$HADOOP_HOME/bin/hadoop job -list-attempt-ids $myJobId map running | awk 'NR==1 {print}'`
    echo $myattemptId
    sleep 20
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id "
        dumpmsg "*** $TESTCASE_ID test case failed"
        setFailCase "Unable to get the attempt id and so failing " 
        windUp
        clearCustomSetting
        return  
    fi
    echo " The attempt id interested is $myattemptId"
    local taskTracker=`getTTHostForAttemptId $myattemptId`
    excludeTaskTrackers $taskTracker        
    # Checking the WebUI for the count
    wget --quiet "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jt_content
    local expExcludedNodes=`grep -A 1 "excluded" $ARTIFACTS_DIR/jt_content |awk -F'excluded' '{print $2}' |sed "s/[^0-9]//g"`
    if [ $expExcludedNodes -eq 1 ] ; then
        echo " The task trakcer has been successfully excluded $taskTracker "
    else
        echo " The  count  of excluded nodes is not accounted  for in the UI and so failing "
        setFailCase " The  count  of excluded nodes is not accounted for in the UI and the value obtained is $expExcludedNodes so failing "
        windUp
        clearCustomSetting
        return            
    fi
    # get the initial count of the occupied_map_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        clearCustomSetting
        return
    fi
    echo " final count of  occupied_map_slots  is  $finalCount "
    if [ $finalCount -le $initialCount ] ; then
        echo " The final count of occupied_map_slots is less than the initial count after decommissioning of tracker  and test case passed "
    else
        echo " The count of occupied_map_slots was not lesser after decommisioning of one task tracker and markting the testcase as failed "
        setFailCase " The count of occupied_map_slots was not lesser after decommisioning of one task tracker and markting the testcase as failed "
        windUp
        clearCustomSetting
        return
    fi
    clearCustomSetting
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testOccupiedReduceSlotsMetricsAfterSuspendingTask {
    echo "**********************************************************"
    TESTCASE_DESC="testOccupiedReduceSlotsMetricsAfterSuspendingTask "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Submit a job that occupies x number of slots "
    echo " 2. Get the count of Occupied RedceSlots "
    echo " 3. Now get the task tracker that is executing the task for this job"
    echo " 4. Add this host to mapred.exclude file  and do a node refresh"
    echo " 5. Verify that the occupied map slots count is reduced "
    echo "**********************************************************"
    local mymetricsKey="occupied_reduce_slots"
    triggerSleepJob 1 1 10 100000 1
    sleep 10
    getJobTrackerHost
    sleep 20
    myJobId=`getJobId`
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        clearCustomSetting
        return
    fi
    echo " The job id got is $myJobId " 
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_reduce_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local initialCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $initialCount ]; then
        echo " The initial count to be returned from jmx term is empty and so failing "
        setFailCase " The initial count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " Initial count of  occupied_reduce_slots  is  $initialCount "   
    myattemptId=`getAttemptIdsForJobId $myJobId "reduce"`
    echo " The attempt id to work with is  $myattemptId"
    sleep 20
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id "
        dumpmsg "*** $TESTCASE_ID test case failed"
        setFailCase "Unable to get the attempt id and so failing " 
        windUp
        clearCustomSetting
        return
    fi
    local taskTracker=`getTTHostForAttemptId $myattemptId "REDUCE"`
    local myPID=`ssh $taskTracker ps -ef |grep hadoopqa |grep attempt_ |awk '{if (NR == 1) {print $2}}'`
    echo " The process id of the task interested is $myPID "
    echo " Proceeding to pause the process "
    ssh $taskTracker kill -SIGSTOP $myPID
    echo " Done suspending the task "
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey
    local finalCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $finalCount ]; then
        echo " The final count to be returned from jmx term is empty and so failing "
        setFailCase " The final count to be returned from jmx term is empty and so failing "
        windUp
        return
    fi
    echo " final count of  occupied_reduce_slots  is  $finalCount " 
    windUpAndKillJob $myJobId
}

function testOccupiedMapSlotsMetricsForHiRamJobs {
    echo "**********************************************************"
    TESTCASE_DESC="testOccupiedMapSlotsMetricsForHiRamJobs"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the memory info for map tasks from the mapred-site.xml"
    echo " 2. submit a job with map memory set to 2x of the alloted memory"
    echo " 3. While the job is running, get the maps_launched and occupied_map_slots"
    echo " 4. The occupoed_map_slots should show count of 2 "
    echo "**********************************************************"
    local mymetricsKey="occupied_map_slots"
    ssh $JOBTRACKER "echo `getValueFromField $HADOOP_CONF_DIR/mapred-site.xml mapred.cluster.map.memory.mb` " > ${ARTIFACTS_DIR}/memorydata
    # getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml     
    local mapMemory=`cat ${ARTIFACTS_DIR}/memorydata`
    echo " The data in mapred-site is $mapMemory " 
    local doubleMapMemory=$(($mapMemory*2))
    echo $doubleMapMemory
        #Submit a sleep job
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-examples-*.jar sleep -Dmapred.job.map.memory.mb=$doubleMapMemory -m 1 -r 1 -mt 20000 -rt 1000 > $ARTIFACTS_DIR/sleep.log 2>&1&  
    sleep 15
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_map_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 2
    local myCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $myCount ]; then
        echo " The count to be returned from jmx term for $mymetricsKey  is empty and so failing "
        setFailCase " The count to be returned from jmx term for $mymetricsKey  is empty and so failing "
        windUp
        return
    fi
    echo " count of  $mymetricsKey  is  $myCount "
    if [ $myCount -ne 2 ] ; then
        echo " The metrics info for $mymetricsKey returned $myCount instead of 2 and so failing "
        setFailCase " The metrics info for $mymetricsKey returned $myCount instead of 2 and so failing "
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE    
}



function testOccupiedReduceSlotsMetricsForHiRamJobs {
    echo "**********************************************************"
    TESTCASE_DESC="testOccupiedReduceSlotsMetricsForHiRamJobs"
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Get the memory info for reduce tasks from the mapred-site.xml"
    echo " 2. submit a job with reduce memory set to 2x of the alloted memory"
    echo " 3. While the job is running, get the reduce_launched and occupied_reduce_slots"
    echo " 4. The occupoed_reduce_slots should show count of 2 "
    echo "**********************************************************"
    local mymetricsKey="occupied_reduce_slots"
    ssh $JOBTRACKER "echo `getValueFromField $HADOOP_CONF_DIR/mapred-site.xml mapred.cluster.reduce.memory.mb` " > ${ARTIFACTS_DIR}/memorydata
    # getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml     
    local reduceMemory=`cat ${ARTIFACTS_DIR}/memorydata`
    echo " The data in mapred-site is $reduceMemory " 
    local doubleReduceMemory=$(($reduceMemory*2))
    echo $doubleReduceMemory
    #Submit a sleep job
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-examples-*.jar sleep -Dmapred.job.reduce.memory.mb=$doubleReduceMemory -m 1 -r 1 -mt 1000 -rt 30000 > $ARTIFACTS_DIR/sleep.log 2>&1&
    sleep 20
    myprocessId=`getJTProcessId $JOBTRACKER`
    # get the initial count of the occupied_reduce_slots
    getJTMetricsInfo $JOBTRACKER $myprocessId $mymetricsKey 10 2
    local myCount=`cat $ARTIFACTS_DIR/jmxmetrix.log |grep $mymetricsKey |tail -1 | cut -d '=' -f2 |cut -d ';' -f1`
    if [ -z $myCount ]; then
        echo " The count to be returned from jmx term for $mymetricsKey  is empty and so failing "
        setFailCase " The count to be returned from jmx term for $mymetricsKey  is empty and so failing "
        windUp
        return
    fi
    echo " count of  $mymetricsKey  is  $myCount "
    if [ $myCount -ne 2 ] ; then
        echo " The metrics info for $mymetricsKey returned $myCount instead of 2 and so failing "
        setFailCase " The metrics info for $mymetricsKey returned $myCount instead of 2 and so failing "
        windUp
        return
    fi
    windUpAndKillJob $myJobId
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


moveRequiredFiles
testJobs_FailedMetrics
testJobs_KilledMetrics
testJobs_RunningMetrics
testMaps_RunningMetricsForSuccessfulJobs
testMaps_RunningMetricsForFailedJobsByFailingAttempts
testReduce_RunningMetricsForSuccessfulJobs
#testReduce_RunningMetricsForFailedJobsByFailingAttempts
testMaps_RunningMetricsForKilledJobsByKillingTasks
testReduces_RunningMetricsForKilledJobsByKillingTasks
testMaps_FailedMetricsForFailedJobsByFailingMaps
testReduces_FailedMetricsForFailedJobs
testHeartBeatMetrics1680
testOccupiedMapSlotsMetricsForSuccessfulJobs
testOccupiedReduceSlotsMetricsForSuccessfulJobs
#testTrackersDecommissionedMetrics
#testTrackersCountMetrics
#testOccupiedMapSlotsMetricsAfterDecommissioningTT
#testOccupiedReduceSlotsMetricsAfterSuspendingTask
testOccupiedMapSlotsMetricsForHiRamJobs
testOccupiedReduceSlotsMetricsForHiRamJobs

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE

