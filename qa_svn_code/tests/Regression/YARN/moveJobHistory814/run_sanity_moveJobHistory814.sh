#!/bin/bash  
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
#################################################################
#This test case tests the exclude node functionality
#Get the list of active trackers and add a few to mapred.exclude file 
#Now refresh nodes from gateway and go to the jobtracker UI to see the effect

#################################################################

# Setting Owner of this TestSuite
OWNER="vmotilal"

function setUp {
    createFile $ARTIFACTS_DIR/AttemptIdFile

}

function modifyValueOfField {
    echo "Modify field $2 with new value $3 in file $1"
    sed -i "s,`xmllint $1 | grep $2 -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1`,$3," $1
}

function updateJobHistoryLocation {
    echo " updating the location in the mapred-site.xml since its not set "
    echo " the value passed to function update Job history location is  $1"
    modifyValueOfField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location $1
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
    startJobTracker $JOBTRACKER
    sleep 15
    startJobTracker $JOBTRACKER
    sleep 15
    return 0
}

function killAJob {
    resp=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $1 | grep Killed `
    if [ -z  "$resp" ] ; then
        echo " Job id $1 has not been killed "
    else
        echo " job id $1 has been killed successfully "
    fi      
}

function testMoveSuccessJobHistoryToLocation {
    echo "*************************************************************"
    TESTCASE_DESC="Move the Job history details to the predetermined location"
    TESTCASE_ID="move01" 
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Verify that the mapred.job.tracker.history.completed.location in mapred-site.xml in JT is set "
    echo " 2. Once the path is set,store the path "
    echo " 3. Submit a job and get the job id "
    echo " 4. Wait for the job to finish "
    echo " 5. Now based on the location in the mapred site, confirm if the file is moved "
    echo "*************************************************************"
    getJobTracker
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    # updateJobHistoryLocation  /mapred/history/done

    triggerSleepJob 10 1 1000 1000 1
    sleep 20
    myJobId=`getJobId`
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp  
        return
    fi      
    echo " My job id is $myJobId "
    getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    location=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location`
    # location="mapred/history/done"  
    if [ -z "$location" ] ; then
        echo " The location is not set for the job history to be moved to "
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
        echo " Could not get the confimration that the job has completed and marking the test case as failed "
        setFailCase " Could not get the confimration that the job has completed and marking the test case as failed "
        windUp  
        return 
    fi                    
    moved1=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  dfs -lsr $location | grep $myJobId |grep Sleep`
    # moved2=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  dfs -lsr $location | grep $myJobId |grep xml`
    # if [ -z "$moved1"  || -z "$moved2" ] ; then
    if [ -z "$moved1" ] ; then
        echo "Looks like the job history file or job conf file  has not been moved "
        setFailCase "Looks like the job history file or job conf file  has not been moved "
        windUp
        return
    else
        echo " The job history files for completed jobs have been moved to hdfs "
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function testMoveKilledJobHistoryLocation {
    echo "*************************************************************"
    TESTCASE_DESC="Move the Killed Job history details to the predetermined location"
    TESTCASE_ID="move02"
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Verify that the mapred.job.tracker.history.completed.location in mapred-site.xml in JT is set "
    echo " 2. Once the path is set,store the path "
    echo " 3. Submit a job and get the job id "
    echo " 4. Kill the job "
    echo " 5. Now based on the location in the mapred site, confirm if the file is moved "
    echo "*************************************************************"
    getJobTracker
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi

    triggerSleepJob 10 1 1000 1000 1
    sleep 20
    myJobId=`getJobId`
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp  
        return
    fi
    echo " My job id is $myJobId "
    getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    location=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location`
    if [ -z "$location" ] ; then
        echo " The location is not set for the job history to be moved to "
        updateJobHistoryLocation  /mapred/history/done
        getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
        location=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location`
    fi
    killAJob $myJobId
    sleep 20
    local moved1=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  dfs -lsr $location | grep $myJobId |grep Sleep`      
    if [ -z "$moved1" ] ; then
        echo "Looks like the job history file or job conf file  has not been moved "
        setFailCase "Looks like the job history file or job conf file  has not been moved "
        windUp 
        return
    else
        echo " The job history files for completed jobs have been moved to hdfs "
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE 
}


function testMoveFailedJobHistoryLocation {
    echo "*************************************************************"
    TESTCASE_DESC="Move the failed Job history details to the predetermined location"
    TESTCASE_ID="move03"
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Verify that the mapred.job.tracker.history.completed.location in mapred-site.xml in JT is set "
    echo " 2. Once the path is set,store the path "
    echo " 3. Submit a job and get the job id "
    echo " 4. Fail the job "
    echo " 5. Now based on the location in the mapred site, confirm if the file is moved "
    echo "*************************************************************"
    setUp 
    getJobTracker
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp 
        return
    fi

    triggerSleepJob 10 1 20000 20000 1
    sleep 20
    myJobId=`getJobId`
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    echo " My job id is $myJobId "
    getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    location=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location`
    if [ -z "$location" ] ; then
        echo " The location is not set for the job history to be moved to "
        updateJobHistoryLocation  /mapred/history/done
        getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
        location=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location`
    fi
    checkForNewAttemptIds $myJobId  
    while [ $attemptIdCount -ne 0 ]; do
        echo " Since there are  attempts ids  proceeding to kill them "
        failAttempts
        checkForNewAttemptIds $myJobId  
    done
    checkForJobStatus $myJobId
    if [ $? -eq 1 ] ; then
        echo " The job could not be failed successfully and unable to proceed with the tests "
        setFailCase " The job could not be failed successfully and unable to proceed with the tests "
        # windUp  
        return
    fi
    local moved1=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  dfs -lsr $location | grep $myJobId |grep Sleep`
    if [ -z "$moved1" ] ; then
        echo "Looks like the job history file or job conf file  has not been moved "
        setFailCase "Looks like the job history file or job conf file  has not been moved "
        windUp 
        return
    else
        echo " The job history files for completed jobs have been moved to hdfs "
        COMMAND_EXIT_CODE=0
    fi            
    displayTestCaseResult
    return $COMMAND_EXIT_CODE    
}       


function testCreateFolderAndMoveJobHistory {
    echo "*************************************************************"
    TESTCASE_DESC="Move the Job history details to the predetermined location by creating the folder if its not available"
    TESTCASE_ID="move04" 
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Generate a ramdom folder name and use it to store the job history files "
    echo " 1. Verify that the mapred.job.tracker.history.completed.location in mapred-site.xml in JT is set "
    echo " 2. Once the path is set,store the path "
    echo " 3. Submit a job and get the job id "
    echo " 4. Wait for the job to finish "
    echo " 5. Now based on the location in the mapred site, confirm if the file is moved "
    echo "*************************************************************"
    displayTestCaseMessage $TESTCASE_DESC
    getJobTracker
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase "Job tracker host is empty and cannot proceed further and so failing"
        return
    fi
    #updateJobHistoryLocation  /mapred/history/done$RANDOM
    if [ $? -ne 0 ] ; then
        echo " Looks like the update job history location did not go well "
        setFailCase "Looks like the update job history location did not go well and so failing "        
        return
    fi

    triggerSleepJob 10 1 1000 1000 1
    sleep 20
    myJobId=`getJobId`
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        setFailCase "Unable to get the job id and so failing "
        return
    fi
    echo " My job id is $myJobId "
    getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    location=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.job.tracker.history.completed.location`
    echo " The location of the file saved is  $location "
    if [ -z "$location" ] ; then
        echo " The location is not set for the job history to be moved to "
        setFailCase " The location is not set for the job history to be moved to "
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
    moved1=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  dfs -lsr $location | grep $myJobId |grep Sleep`
    # moved2=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  dfs -lsr $location | grep $myJobId |grep xml`
    # if [ -z "$moved1"  || -z "$moved2" ] ; then
    if [ -z "$moved1" ] ; then
        echo "Looks like the job history file or job conf file  has not been moved "
        setFailCase "Looks like the job history file or job conf file  has not been moved "
        return
    else
        echo " The job history files for completed jobs have been moved to hdfs "
        COMMAND_EXIT_CODE=0
        return
    fi
    # updateJobHistoryLocation  /mapred/history/done
    if [ $? -ne 0 ] ; then
        echo " Looks like the update job history location did not go well "
        setFailCase "Looks like the update job history location did not go well and so failing "
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


testMoveFailedJobHistoryLocation
testMoveSuccessJobHistoryToLocation
testMoveKilledJobHistoryLocation
#testCreateFolderAndMoveJobHistory

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
