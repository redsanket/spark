#!/bin/bash  
set -x
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/restart_cluster_lib.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"
export USER_ID=`whoami`

TTLogPath="${HADOOP_LOG_DIR}/mapred/hadoop-mapred-tasktracker-"
UserLogs="${HADOOP_LOG_DIR}/mapred/userlogs"
FailDisk="/grid/2"

function getLogFileDir {
    cat $HADOOP_CONF_DIR/taskcontroller.cfg |grep hadoop.log.dir |cut -d'=' -f2
    return $?
}


function getARandomTTHost {
    TTHOST=`getActiveTaskTrackers |head -1`
    echo " The TThost used for this test is $TTHOST " >> $ARTIFACTS_FILE 2>&1
}


function resetDisk {
    local myaction=$1 
    # local FailDisk=$2
    echo "mounting a drive " >> $ARTIFACTS_FILE 2>&1
    if [ $myaction == "mount" ] ; then
        runasroot "$TTHOST" "mount" "$FailDisk "
    elif [ $myaction == "readonly" ] ; then
        runasroot "$TTHOST" "chmod" "755 $FailDisk "              
    fi      
    echo "restarting task tracker after mounting the disk " >> $ARTIFACTS_FILE 2>&1
    resetNode $TTHOST tasktracker stop
    sleep 10
    resetNode $TTHOST tasktracker start
    sleep 10
}

function UpdateOfGoodLocalDirs {
    local diskFailType=$1
    # local FailDisk=$2
    getARandomTTHost
    if [ "X""$TTHOST""X" == "XX" ] ; then
        echo " The TThost is empty and cannot proceed with the tests ">> $ARTIFACTS_FILE 2>&1
        setFailCase " The TThost is empty and cannot proceed with the tests "
        windUp
        return
    fi
    TTLOG="${TTLogPath}${TTHOST}.log"
    echo "Stopping task tracker " >> $ARTIFACTS_FILE 2>&1
    #stopTaskTracker $TTHOST
    resetNode $TTHOST tasktracker stop
    sleep 10
    resetNode $TTHOST tasktracker start
    sleep 10
    echo " The entries for Good mapred local directories in the log before unmounting are as follows " >> $ARTIFACTS_FILE 2>&1
    ssh $TTHOST cat $TTLOG |grep "Good mapred local directories" |grep mapred-local >> $ARTIFACTS_FILE 2>&1
    local initialCount=`ssh $TTHOST cat $TTLOG |grep "Good mapred local directories" |grep mapred-local |wc -l`
    echo " The initial count of mapred-local in log file is $initialCount " >> $ARTIFACTS_FILE 2>&1
    # Navigate to TT and umount a drive
    echo "Unmounting a drive " >> $ARTIFACTS_FILE 2>&1
    if [ $diskFailType == "umount" ] ; then 
        runasroot "$TTHOST" "umount" "-l $FailDisk"
    elif [ $diskFailType == "readonly" ] ; then
        runasroot "$TTHOST" "chmod" "000 $FailDisk"
    fi
    # Wait for 60 seconds
    sleep 100
    echo " Fetching the logs again to check the entry " >> $ARTIFACTS_FILE 2>&1
    ssh $TTHOST cat $TTLOG |grep "Good mapred local directories" |grep mapred-local >> $ARTIFACTS_FILE 2>&1
    # Now in the logs look for mapred-local count   
    local finalCount=`ssh $TTHOST cat $TTLOG |grep "Good mapred local directories" |grep mapred-local |wc -l`
    echo " The final count of  mapred-local in the log file is $finalCount " >> $ARTIFACTS_FILE 2>&1
    if [ $finalCount -le $initialCount ] ; then
        echo "Final count of mapred-local occurence is the same as the initial count and so failing the testcase" >> $ARTIFACTS_FILE 2>&1
        setFailCase "Final count of mapred-local occurence is the same as the initial count and so failing the testcase"
        resetDisk $diskFailType
        return 1
    fi
    local mydata=`ssh $TTHOST cat $TTLOG |grep "Good mapred local directories" |grep mapred-local |tail -1 |grep "/grid/0"`
    local myentry=`echo $mydata |grep "$FailDisk" `
    if [ !  "X""$myentry""X" == "XX" ] ; then
        echo "The entry $FailDisk is found in the log file and so marking the test as failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The entry $FailDisk is  found in the log file and so marking the test as failed "
        resetDisk $diskFailType
        return 1
    else
        echo " Looking  for $FailDisk returned empty and so passing the test case " >> $ARTIFACTS_FILE 2>&1
        resetDisk $diskFailType
    fi
    return 0
} 



function test_UpdateOfGoodLocalDirsByMakingDiskReadOnly {
    TESTCASE_DESC="test_UpdateOfGoodLocalDirsByMakingDiskReadOnly"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    UpdateOfGoodLocalDirs "readonly" 
    resetDisk "readonly"
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_UpdateOfGoodLocalDirsByUnMountingDisk {
    TESTCASE_DESC="test_UpdateOfGoodLocalDirsByUnmountingDisk"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    UpdateOfGoodLocalDirs "umount" 
    
    resetDisk "mount" 
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}



function test_UserLogsAttemptIdAttributes {
    # Submit a job and get the job id
    # Construct the attempt id 
    # Search in hadoop.log.dir folder for attempt id
    # Check that the attemptid folder is a symlink
    # Check the ownership of the files created
    TESTCASE_DESC="test_UserLogsForAttemptsAreSymlinked"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    if [ "X""$TTHOST""X" -eq "XX" ] ; then
        echo " The TThost is empty and cannot proceed with the tests ">> $ARTIFACTS_FILE 2>&1
        setFailCase
        windUp
        return
    fi
    TTLOG="${TTLogPath}${TTHOST}.log"
    echo "Submitting a sleep job" >> $ARTIFACTS_FILE 2>&1
    # triggerSleepJob 3 3
    triggerSleepJob 2 2 10000 10000 1
    # $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-examples-*.jar sleep -m 3 -r 3 >> $ARTIFACTS_FILE 2>&1&
    for (( i=0; i < 5; i++ )); do
        echo " Looping $i times to get the job id " >> $ARTIFACTS_FILE 2>&1
        local myJobId=`getJobId`
        if [ ! $myJobId == "JobId" ] ; then
            break
        else
            sleep 5
        fi
    done    
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed "
        windUp
        return
    fi      
    local myattemptid=$(echo $myJobId|sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')        
    echo "The attempt id generated is  $myattemptid " >> $ARTIFACTS_FILE 2>&1
    for (( i=0; i < 5; i++ )); do
        echo "Looping in to get the TTHost $i times " >> $ARTIFACTS_FILE 2>&1
        local myTTHost=`getTTHostForAttemptId $myattemptid`
        if [ ! -z $myTTHost ] ; then
            break
        else
            sleep 5
        fi
    done
    echo " The TT host that is executing the task is $myTTHost " >> $ARTIFACTS_FILE 2>&1
    local mylogfileDir=`getLogFileDir`
    local myuserDir=`ssh $myTTHost ls -l $mylogfileDir/userlogs/$myJobId/$myattemptid`    
    echo " All the info about the attempt id file is ..  $myuserDir " >> $ARTIFACTS_FILE 2>&1
    local myowner=`echo $myuserDir |awk -F" " '{print $3}'`
    local mygroup=`echo $myuserDir |awk -F" " '{print $4}'`
    local myfileSize=`echo $myuserDir |awk -F" " '{print $5}'`
    # Checking if the attemptid file is a symlink
    local isSymLink=`ssh $myTTHost readlink $mylogfileDir/userlogs/$myJobId/$myattemptid` 
    if [ -z $isSymLink ] ; then
        echo " The attemptid file is not a symlink since the check returned empty " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The attemptid file is not a symlink since the check returned empty "
    else
        echo " The attemptid file is a symlink  and the check returned $isSymLink " >> $ARTIFACTS_FILE 2>&1
    fi
    # Checking  the owner of the attempid file
    if [ ! $myowner == $USER_ID ]   ; then
        echo "The ownership of the symlink is not as expected and is $myowner instead of $USER_ID" >> $ARTIFACTS_FILE 2>&1
        setFailCase "The ownership of the symlink is not as expected and is $myowner instead of $USER_ID"
    else
        echo " the ownership check of the attribute file is as expected ">> $ARTIFACTS_FILE 2>&1
    fi
    # Checking the group of the attemptid file
    if [ ! $mygroup == "hadoop" ] ; then
        echo "The group of the symlink is not as expected and is $mygroup instead of hadoop" >> $ARTIFACTS_FILE 2>&1 
        setFailCase "The group  of the symlink is not as expected and is $mygroup instead of hadoop"
    else 
        echo " the group check of the attribute file is as expected ">> $ARTIFACTS_FILE 2>&1 
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE 
}


function test_AttemptIdMarkedForCleanup {
    # Trigger a job
    # Fail a task
    # get the attemptid 
    # navigate to the TT and check the folder
    TESTCASE_DESC="test_AttemptIdMarkedForCleanup"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    if [ "X""$TTHOST""X" -eq "XX" ] ; then
        echo " The TThost is empty and cannot proceed with the tests ">> $ARTIFACTS_FILE 2>&1
        setFailCase " The TThost is empty and cannot proceed with the tests "
        windUp
        return
    fi
    TTLOG="${TTLogPath}${TTHOST}.log" 
    echo "Submitting a sleep job" >> $ARTIFACTS_FILE 2>&1
    # triggerSleepJob 3 3
    triggerSleepJob 2 2 80000 10000 1
    # $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-examples-*.jar sleep -m 3 -r 3 >> $ARTIFACTS_FILE 2>&1&
    for (( i=0; i < 5; i++ )); do
        echo " Looping $i times to get the job id " >> $ARTIFACTS_FILE 2>&1
        local myJobId=`getJobId`
        if [ ! $myJobId == "JobId" ] ; then
            break
        else
            sleep 5
        fi
    done
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed "
        windUp
        return
    fi
    local myattemptid=$(echo $myJobId|sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
    echo "The attempt id generated is  $myattemptid " >> $ARTIFACTS_FILE 2>&1
    for (( i=0; i < 5; i++ )); do
        echo "Looping in to get the TTHost $i times " >> $ARTIFACTS_FILE 2>&1
        local myTTHost=`getTTHostForAttemptId $myattemptid`
        if [ ! -z $myTTHost ] ; then
            break
        else
            sleep 5
        fi
    done
    echo " The TT host that is executing the task is $myTTHost " >> $ARTIFACTS_FILE 2>&1  
    # Trigger fail task 
    failJob $myattemptid
    # Navigate to the tasktracker log file to check if the attempt id is marked for cleanup
    local TTLocation=$UserLogs"/$myJobId/$myattemptid.cleanup"
    echo " The log location  is $TTLocation "
    if [ !  -e ` ssh $myTTHost $TTLocation` ] ; then
        echo " Cleanup file for attempid does not exist and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Cleanup file for attemptid does not exisit and so failing the test case  "
    fi
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE    
}


function test_UserLogsSizeLimit {
    # Get the log file size from mapred-site.xml
    # Trigger a job that will populate stderr and stdout files
    # Get the size of the stderr and stdout files 
    # verify that they are close to what is defined in mapred-site.xml
    TESTCASE_DESC="test_UserLogsSizeLimit"
    TESTCASE_ID="Status01"
    INDATA="data10.txt"
    # The value of the max log file set in the mapred-site.xml        
    LOGLIMIT=`getValueFromField $HADOOP_CONF_DIR/mapred-site.xml mapreduce.cluster.map.userlog.retain-size`
    echo " The logfile limit set in the mapred site file is $LOGLIMIT "
    return

    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    if [ "X""$TTHOST""X" -eq "XX" ] ; then
        echo " The TThost is empty and cannot proceed with the tests ">> $ARTIFACTS_FILE 2>&1
        setFailCase " The TThost is empty and cannot proceed with the tests "
        windUp
        return
    fi
    # Cleaning up the log file
    >/tmp/vinod9988
    TTLOG="${TTLogPath}${TTHOST}.log"
    putLocalToHdfs ${JOB_SCRIPTS_DIR_NAME}/data/data10.txt data10.txt
    deleteHdfsDir /tmp/out101       
    echo "Submitting a custom  job that will populate th logs " >> $ARTIFACTS_FILE 2>&1
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar ${JOB_SCRIPTS_DIR_NAME}/data/PopulateUserLogs1.jar org.apache.hadoop.examples.PopulateUserLogs /user/${USER_ID}/data10.txt /tmp/out101 >>/tmp/vinod9988 2>&1&
    sleep 55 
    cat /tmp/vinod9988 >> $ARTIFACTS_FILE 2>&1      
    cat /tmp/vinod9988 |grep job_ |awk 'BEGIN {FS="complete:"}{print $2}'
    local myJobId=`cat /tmp/vinod9988 |grep job_ |awk 'BEGIN {FS="complete:"}{print $2}'` 
    
    # Get the job id of the job submitted
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed "
        windUp
        return
    fi
    local myattemptid=$(echo $myJobId|sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
    echo "The attempt id generated is  $myattemptid " >> $ARTIFACTS_FILE 2>&1
    for (( i=0; i < 5; i++ )); do
        echo "Looping in to get the TTHost $i times " >> $ARTIFACTS_FILE 2>&1
        local myTTHost=`getTTHostForAttemptId $myattemptid`
        if [ ! -z $myTTHost ] ; then
            break
        else
            sleep 5
        fi
    done
    myJobId=`echo $myJobId`
    TTLocation=$UserLogs"/$myJobId/$myattemptid"
    # Navigate to the TThost and look at the size of the stderr and stdout log files
    local   stderrsize=`ssh $myTTHost ls -l $TTLocation/stderr |awk -F" " '{print $5}'`
    local stdoutsize=`ssh $myTTHost ls -l $TTLocation/stdout |awk -F" " '{print $5}'`

    if [ $stderrsize -gt $LOGLIMIT ] || [ $stdoutsize -gt 102500 ] ; then
        echo " The size of the stderr and stdout files are not within the limits and so failing the test" >> $ARTIFACTS_FILE 2>&1
        setFailCase "The size of the stderr and stdout files are not within the limits and so failing the test  "
    fi    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_NoUserLogSymlinksToUnmountedDrive {
    # Restart a task tracker to clean User logs
    # Fail a drive in the TT
    # Submit a job with lot of maps
    # Wait for the job to complete
    # Go to the task tracker and check all the attemptid symlinks
    # Verify that they point to the failed drive
    TESTCASE_DESC="test_NoUserLogSymlinksToUnmountedDrive"
    TESTCASE_ID="Status01"
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    getARandomTTHost
    if [ "X""$TTHOST""X" -eq "XX" ] ; then
        echo " The TThost is empty and cannot proceed with the tests ">> $ARTIFACTS_FILE 2>&1
        setFailCase
        windUp
        return
    fi
    TTLOG="${TTLogPath}${TTHOST}.log" 
    echo "Stopping task tracker " >> $ARTIFACTS_FILE 2>&1
    #stopTaskTracker $TTHOST
    resetNode $TTHOST tasktracker stop
    sleep 10
    resetNode $TTHOST tasktracker start 
    sleep 10      
    # Navigate to TT and umount a drive
    echo "Unmounting a drive " >> $ARTIFACTS_FILE 2>&1
    runasroot "$TTHOST" "umount" "-l $FailDisk"
    # Wait for 60 seconds
    sleep 100
    # trigger a sleep job with log 
    triggerSleepJob 150 150 1 1 1
    for (( i=0; i < 5; i++ )); do
        echo " Looping $i times to get the job id " >> $ARTIFACTS_FILE 2>&1
        local myJobId=`getJobId`
        if [ ! $myJobId == "JobId" ] ; then
            break
        else
            sleep 5
        fi
    done
    validateJobId $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The job id obtained is $myJobId Since it did not meet the pattern marking the test as failed "
        resetDisk "mount"
        windUp
        return
    fi
    for (( i=0; i < 15; i++ )); do
        echo " Looping $i times to check for job completion ">> $ARTIFACTS_FILE 2>&1
        checkForJobCompletion $myJobId
        if [ $? -eq 0 ] ; then
            break
        else
            sleep 10
        fi
    done
    # Navigate to the TT and check the logs
    echo " The TT host that is executing the task is $myTTHost " >> $ARTIFACTS_FILE 2>&1
    local mylogfileDir=`getLogFileDir`
    ssh $TTHOST ls -l $mylogfileDir/userlogs/$myJobId     >/tmp/vinod9876
    sleep 5
    echo " The list of attemptids folders created in the user logs " >> $ARTIFACTS_FILE 2>&1        
    cat /tmp/vinod9876 >> $ARTIFACTS_FILE 2>&1
    local tasksInNode=`cat /tmp/vinod9876 |grep attempt_ |wc -l`
    if [ $tasksInNode -eq 0 ] ; then
        echo "Since there are no attempt ids created in this node and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "Since there are no attempt ids created in this node and so failing the test case "
        resetDisk "mount"
        windUp
        return
    fi               
    local symLinkToFailDisk=`cat /tmp/vinod9876 |grep "$FailDisk" |wc -l`
    if [ $symLinkToFailDisk -ne 0 ] ; then
        echo " Looks like an attemptid is refering to the failed disk $symLinkToFailDisk " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Looks like an attemptid is refering to the failed disk $symLinkToFailDisk "
        resetDisk "mount"
        windUp
        return
    fi

    echo "mounting a drive " >> $ARTIFACTS_FILE 2>&1
    runasroot "$TTHOST" "mount" "$FailDisk "
    echo "restarting task tracker after mounting the disk " >> $ARTIFACTS_FILE 2>&1
    resetNode $TTHOST tasktracker stop
    sleep 10
    resetNode $TTHOST tasktracker start
    sleep 10
    displayTestCaseResult
    return $COMMAND_EXIT_CODE                                     
}       


getARandomTTHost

test_UpdateOfGoodLocalDirsByUnMountingDisk
test_UpdateOfGoodLocalDirsByMakingDiskReadOnly
test_UserLogsAttemptIdAttributes
test_AttemptIdMarkedForCleanup
test_UserLogsSizeLimit
test_NoUserLogSymlinksToUnmountedDrive

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
