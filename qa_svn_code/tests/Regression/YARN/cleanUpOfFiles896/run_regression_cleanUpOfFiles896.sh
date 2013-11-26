#!/bin/bash
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh

# Setting Owner of this TestSuite
OWNER="vmotilal"

############################################################
# This function inserts the file into hdfs 
# takes the filename as input $1
############################################################
function insertFileIntoHdfs {
    local myfile=$1     
    # local myfile="CreateFile2.sh"   
    # Move the the shellscript that is used to create a file on to dfs
    curUser=`whoami`
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -cat /user/$curUser/$myfile | grep "File does not exist"
    if [ $? -eq 1 ] ; then
        echo " The file does not already exist and so we are good to proceed to insert the file "
        $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -put $JOB_SCRIPTS_DIR_NAME/data/$myfile /user/$curUser/$myfile >> $ARTIFACTS_FILE 2>&1 &                      
        #putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/CreateFile.sh /user/$curUser/CreateFile.sh
    fi    
}

function cleanUp {
    #Delete the file added to hdfs  
    curUser=`whoami` 
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -rm /user/$curUser/CreateFile.sh
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -rm /user/$curUser/CreateFile2.sh
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -rmr /user/$curUser/out1
}


###########################################
# Function to get  the localDirInfo from mapred-site.xml
# takes Jobtracker host as $1 
###########################################
function getLocalDirInfo {
    ssh $1  "echo `getValueFromField ${HADOOP_CONF_DIR}/mapred-site.xml mapred.local.dir`"
    return $?
}


#########################################################################
# This function checks to see if the file is created on the tasktracker for the given location
# Takes local dir path as $1, user as $2, job id as $3, attemptid as $4 and file to look for as $5
function checkJobCreatedTempFileInTT {
    local mylogfile=$1
    local myuser=$2
    local currJobId=$3
    local currAttemptId=$4
    local myfile=$5
    local myTTHost=$6
    ssh $myTTHost ls $mylogfile/taskTracker/$myuser/jobcache/$currJobId/$currAttemptId/work/$myfile |wc -l
    # ssh $taskTracker ls /grid/0/tmp/mapred-local/taskTracker/hadoopqa/jobcache/$myJobId/$attemptId/work/FileCreatedByJob.log`       
    return $?
}

function testCleanUpOfFilesAfterJobCompletion {
    TESTCASE_DESC="testCleanUpOfFilesAfterJobCompletion"
    TESTCASE_ID="cleanup01"
    displayTestCaseMessage $TESTCASE_DESC
    local fileCreated="FileCreatedByJob.log"
    local fileUsedByStreaming="CreateFile.sh"
    insertFileIntoHdfs $fileUsedByStreaming
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    echo " Submitting a streaming job that will create a file "
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "CreateFile.sh" -output out1 -mapper "CreateFile.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/CreateFile.sh >> $ARTIFACTS_FILE 2>&1 &
    sleep 20
    # The $JOBID param will be populated with getJobId method       
    for (( i=0; i < 5; i++ )); do
        myJobId=`getJobId`
        validateJobId $myJobId
        if [ $? -eq 0 ] ; then
            echo " Got the job id successfully $myJobId " 
            break
        fi
        echo " Still have not got the job id  for the job submitted and so looping again"
        sleep 10
    done
    
    # get the attempt id by calling the job utility with
    attemptId=`getAttemptIdsForJobId $myJobId`
    validateAttemptId $attemptId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    taskTracker=`getTTHostForAttemptId $attemptId`
    echo " Task Tracker running the map task is $taskTracker"
    sleep 20
    logFileList=`getLocalDirInfo $JOBTRACKER`
    #logFile list has multiple paths and need to parse the same
    logFile1=`echo $logFileList | cut -d "," -f1`
    logFile2=`echo $logFileList | cut -d "," -f2`
    logFile3=`echo $logFileList | cut -d "," -f3`
    logFile4=`echo $logFileList | cut -d "," -f4`
    fileCheckResponse1=`checkJobCreatedTempFileInTT $logFile1 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse2=`checkJobCreatedTempFileInTT $logFile2 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse3=`checkJobCreatedTempFileInTT $logFile3 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse4=`checkJobCreatedTempFileInTT $logFile4 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    if [ $fileCheckResponse1 -eq 1 ] ; then
        echo " The temp file has been created in $logFile1 location"
        createdPath=$logFile1
    elif [ $fileCheckResponse2 -eq 1 ] ; then
        echo " The temp file has been created in $logFile2 location"
        createdPath=$logFile2           
    elif [ $fileCheckResponse3 -eq 1 ] ; then
        echo " The temp file has been created in $logFile3 location"
        createdPath=$logFile3
    elif [ $fileCheckResponse4 -eq 1 ] ; then
        echo " The temp file has been created in $logFile4 location"
        createdPath=$logFile4
    fi
    if [ -z "$createdPath" ] ; then
        echo " Did not find the file created and so test case failed"
        setFailCase " The job did not create the expected file in the temp path and so cannot proceed with testing "
        windUp
        return
    else
        echo " created path value is $createdPath "
    fi
    for (( i=0; i < 10; i++ )); do
        checkForJobCompletion $myJobId
        if [ $? -eq 0 ] ; then
            echo "Confirmed that the job has completed "
            break
        fi
        sleep 20
    done  
    if [ $? -eq 1 ] ; then
        echo " Could not get the confimration that the job has completed and marking the test case as failed "
        setFailCase " Could not get the confimration that the job has completed and marking the test case as failed "
        windUp
        return
    fi  
    # Now check for the file to be cleared off  after the job is complete
    checkJobCreatedTempFileInTT $createdPath $curUser $myJobId $attemptId $fileCreated $taskTracker     
    if [ $? -ne 0 ] ; then
        echo " The test case  to check the files cleared after completion of jobs failed "
        setFailCase " The file created by the job still exisits even after the job is successfully completed"
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testCleanUpOfFilesAfterKilledJob {
    TESTCASE_DESC="testCleanUpOfFilesAfterKilledJob"
    TESTCASE_ID="cleanup02"
    displayTestCaseMessage $TESTCASE_DESC
    local fileCreated="FileCreatedByJob.log"
    local fileUsedByStreaming="CreateFile.sh"
    insertFileIntoHdfs $fileUsedByStreaming
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    echo " Submitting a streaming job that will create a file "
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "CreateFile.sh" -output out1 -mapper "CreateFile.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/CreateFile.sh >> $ARTIFACTS_FILE 2>&1 &
    sleep 20
    # The $JOBID param will be populated with getJobId method 

    for (( i=0; i < 5; i++ )); do
        myJobId=`getJobId`
        validateJobId $myJobId
        if [ $? -eq 0 ] ; then
            echo " Got the job id successfully $myJobId "
            break
        fi
        echo " Still have not got the job id  for the job submitted and so looping again"
        sleep 10
    done  
    # get the attempt id by calling the job utility with
    attemptId=`getAttemptIdsForJobId $myJobId`
    validateAttemptId $attemptId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    taskTracker=`getTTHostForAttemptId $attemptId`
    echo " Task Tracker running the map task is $taskTracker"
    sleep 20
    logFileList=`getLocalDirInfo $JOBTRACKER`
    #logFile list has multiple paths and need to parse the same
    logFile1=`echo $logFileList | cut -d "," -f1`
    logFile2=`echo $logFileList | cut -d "," -f2`
    logFile3=`echo $logFileList | cut -d "," -f3`
    logFile4=`echo $logFileList | cut -d "," -f4`
    fileCheckResponse1=`checkJobCreatedTempFileInTT $logFile1 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse2=`checkJobCreatedTempFileInTT $logFile2 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse3=`checkJobCreatedTempFileInTT $logFile3 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse4=`checkJobCreatedTempFileInTT $logFile4 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    if [ $fileCheckResponse1 -eq 1 ] ; then
        echo " The temp file has been created in $logFile1 location"
        createdPath=$logFile1
    elif [ $fileCheckResponse2 -eq 1 ] ; then
        echo " The temp file has been created in $logFile2 location"
        createdPath=$logFile2
    elif [ $fileCheckResponse3 -eq 1 ] ; then
        echo " The temp file has been created in $logFile3 location"
        createdPath=$logFile3
    elif [ $fileCheckResponse4 -eq 1 ] ; then
        echo " The temp file has been created in $logFile4 location"
        createdPath=$logFile4
    fi
    if [ -z "$createdPath" ] ; then
        echo " Did not find the file created and so test case failed"
        windUp
        return
    else
        echo " created path value is $createdPath "
    fi

    # Now kill the job 
    killJob $myJobId

    # Now check for the file to be cleared off  after the job is killed
    checkJobCreatedTempFileInTT $createdPath $curUser $myJobId $attemptId $fileCreated $taskTracker
    if [ $? -ne 0 ] ; then
        echo " The test case  to check the files cleared after killing of jobs failed "
        setFailCase " The file created by the job still exisits even after the job is successfully killed"
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testCleanUpOfFilesAfterFailedJob {
    TESTCASE_DESC="testCleanUpOfFilesAfterFailedJob"
    TESTCASE_ID="cleanup03"
    displayTestCaseMessage $TESTCASE_DESC
    local fileCreated="FileCreatedByJob.log"
    local fileUsedByStreaming="CreateFile.sh"
    insertFileIntoHdfs $fileUsedByStreaming  
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    echo " Submitting a streaming job that will create a file "
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "CreateFile.sh" -output out1 -mapper "CreateFile.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/CreateFile.sh >> $ARTIFACTS_FILE 2>&1 &
    sleep 20
    # The $JOBID param will be populated with getJobId method 

    for (( i=0; i < 5; i++ )); do
        myJobId=`getJobId`
        validateJobId $myJobId
        if [ $? -eq 0 ] ; then
            echo " Got the job id successfully $myJobId "
            break
        fi
        echo " Still have not got the job id  for the job submitted and so looping again"
        sleep 10
    done  

    # get the attempt id by calling the job utility with
    attemptId=`getAttemptIdsForJobId $myJobId`
    validateAttemptId $attemptId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    taskTracker=`getTTHostForAttemptId $attemptId`
    echo " Task Tracker running the map task is $taskTracker"
    sleep 20
    logFileList=`getLocalDirInfo $JOBTRACKER`
    #logFile list has multiple paths and need to parse the same
    logFile1=`echo $logFileList | cut -d "," -f1`
    logFile2=`echo $logFileList | cut -d "," -f2`
    logFile3=`echo $logFileList | cut -d "," -f3`
    logFile4=`echo $logFileList | cut -d "," -f4`
    fileCheckResponse1=`checkJobCreatedTempFileInTT $logFile1 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse2=`checkJobCreatedTempFileInTT $logFile2 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse3=`checkJobCreatedTempFileInTT $logFile3 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse4=`checkJobCreatedTempFileInTT $logFile4 $curUser $myJobId $attemptId $fileCreated $taskTracker` 
    if [ $fileCheckResponse1 -eq 1 ] ; then
        echo " The temp file has been created in $logFile1 location"
        createdPath=$logFile1
    elif [ $fileCheckResponse2 -eq 1 ] ; then
        echo " The temp file has been created in $logFile2 location"
        createdPath=$logFile2
    elif [ $fileCheckResponse3 -eq 1 ] ; then
        echo " The temp file has been created in $logFile3 location"
        createdPath=$logFile3
    elif [ $fileCheckResponse4 -eq 1 ] ; then
        echo " The temp file has been created in $logFile4 location"
        createdPath=$logFile4
    fi
    if [ -z "$createdPath" ] ; then
        echo " Did not find the file created and so test case failed"
        windUp
        return
    else
        echo " created path value is $createdPath "
    fi

    # Now fail the job 
    getAttemptIdsForJobIdAndStoreInFile $myJobId
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
        windUp
        return
    fi

    # Now check for the file to be cleared off  after the job is failed
    checkJobCreatedTempFileInTT $createdPath $curUser $myJobId $attemptId $fileCreated $taskTracker
    if [ $? -ne 0 ] ; then
        echo " The test case  to check the files cleared after killing of jobs failed "
        setFailCase " The file created by the job still exisits even after the job is successfully killed"
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testCleanUpOfFilesAfterJobCompletionForFilesWithSymLink {
    TESTCASE_DESC="testCleanUpOfFilesAfterJobCompletionForFilesWithSymLink "
    TESTCASE_ID="cleanup01"
    displayTestCaseMessage $TESTCASE_DESC
    local  fileCreated="mysymlink.txt"
    local fileUsedByStreaming="CreateFile2.sh"
    insertFileIntoHdfs $fileUsedByStreaming
    sleep 15 
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi

    echo $JOB_SCRIPTS_DIR_NAME
    echo $ARTIFACTS_FILE        
    echo " Submitting a streaming job that will create a file "
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "CreateFile2.sh" -output out1 -mapper "CreateFile2.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/CreateFile2.sh >> $ARTIFACTS_FILE 2>&1 &
    sleep 15
    # The $JOBID param will be populated with getJobId method 

    for (( i=0; i < 5; i++ )); do
        myJobId=`getJobId`
        validateJobId $myJobId
        if [ $? -eq 0 ] ; then
            echo " Got the job id successfully $myJobId "
            break
        fi
        echo " Still have not got the job id  for the job submitted and so looping again"
        sleep 10
    done  

    # get the attempt id by calling the job utility with
    attemptId=`getAttemptIdsForJobId $myJobId`
    validateAttemptId $attemptId
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to get the attempt id  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        windUp
        return
    fi
    taskTracker=`getTTHostForAttemptId $attemptId`
    echo " Task Tracker running the map task is $taskTracker"
    sleep 40
    logFileList=`getLocalDirInfo $JOBTRACKER`
    #logFile list has multiple paths and need to parse the same
    logFile1=`echo $logFileList | cut -d "," -f1`
    logFile2=`echo $logFileList | cut -d "," -f2`
    logFile3=`echo $logFileList | cut -d "," -f3`
    logFile4=`echo $logFileList | cut -d "," -f4`
    fileCheckResponse1=`checkJobCreatedTempFileInTT $logFile1 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse2=`checkJobCreatedTempFileInTT $logFile2 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse3=`checkJobCreatedTempFileInTT $logFile3 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    fileCheckResponse4=`checkJobCreatedTempFileInTT $logFile4 $curUser $myJobId $attemptId $fileCreated $taskTracker`
    if [ $fileCheckResponse1 -eq 1 ] ; then
        echo " The temp file has been created in $logFile1 location"
        createdPath=$logFile1
    elif [ $fileCheckResponse2 -eq 1 ] ; then
        echo " The temp file has been created in $logFile2 location"
        createdPath=$logFile2
    elif [ $fileCheckResponse3 -eq 1 ] ; then
        echo " The temp file has been created in $logFile3 location"
        createdPath=$logFile3
    elif [ $fileCheckResponse4 -eq 1 ] ; then
        echo " The temp file has been created in $logFile4 location"
        createdPath=$logFile4
    fi
    if [ -z "$createdPath" ] ; then
        echo " Did not find the file created and so test case failed"
        setFailCase " The job did not create the expected file in the temp path and so cannot proceed with testing "
        windUp
        return
    else
        echo " created path value is $createdPath "
    fi
    for (( i=0; i < 10; i++ )); do
        checkForJobCompletion $myJobId
        if [ $? -eq 0 ] ; then
            echo "Confirmed that the job has completed "
            break
        fi
        sleep 20
    done
    if [ $? -eq 1 ] ; then
        echo " Could not get the confimration that the job has completed and marking the test case as failed "
        setFailCase " Could not get the confimration that the job has completed and marking the test case as failed "
        windUp
        return
    fi

    # Now check for the file to be cleared off  after the job is complete
    checkJobCreatedTempFileInTT $createdPath $curUser $myJobId $attemptId $fileCreated $taskTracker
    if [ $? -ne 0 ] ; then
        echo " The test case  to check the files cleared after completion of jobs failed "
        setFailCase " The file created by the job still exisits even after the job is successfully completed"
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


cleanUp 
testCleanUpOfFilesAfterJobCompletion
cleanUp
testCleanUpOfFilesAfterKilledJob
cleanUp
testCleanUpOfFilesAfterFailedJob
cleanUp
testCleanUpOfFilesAfterJobCompletionForFilesWithSymLink

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
