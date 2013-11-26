#!/bin/bash   
set -x

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
#export CLUSTER=wilma
#export HADOOP_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoop-current
#export HADOOP_CONF_DIR=/grid/0/gs/gridre/yroot.$CLUSTER/conf/hadoop
#export ARTIFACTS_DIR=/home/y/var/builds/workspace/Vinod-Test/artifacts/ldLibraryPath5980

export COMMAND_EXIT_CODE=0
export SCRIPT_EXIT_CODE=0
export TESTCASE_DESC="None"
export JOBTRACKER="None"
export USER_ID=`whoami`
export CHECKED=0
export REASONS=""
export IN_ACTIVE_TRACKERS=1
export IN_BLACKLISTED=0
WAIT_TIME=120

function cleanUpDataOnHdfs {
    deleteHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    deleteHdfsDir out1 >> $ARTIFACTS_FILE 2>&1
}               

###############################################################
# Function to check map processes that exceeds the memory limits  log process tree when killed
###############################################################
function testLoggingOfProcessTreeForMapTasksExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="testLoggingOfProcessTreeForMapTasksExceedMemoryLimit"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper highRAM which spawns subshells in tasks that exceeded memory limit"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running highRAM"
    echo "5. kill the process "
    echo "6. verify that the process tree is logged "
    echo "*************************************************************"
    echo " Executing testLoggingOFProcessTreeForMapTasksExceedMemoryLimit  " >> $ARTIFACTS_FILE 2>&1 
    #curUser=`whoami`
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    getJobTrackerHost       

    echo "Submit streaming job ..."
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAMSleep" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/highRAMSleep >> $ARTIFACTS_FILE 2>&1 &
    jobID=`waitForJobId $WAIT_TIME`
    echo "Job ID: $jobID" >> $ARTIFACTS_FILE 2>&1
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after $WAIT_TIME secs"
        windUp
        return
    fi

    attemptID1=`waitForAttemptIdsForJobId $jobID MAP $WAIT_TIME`;
    echo " The attempt id is $attemptID1" >> $ARTIFACTS_FILE 2>&1
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank after $WAIT_TIME secs"
        windUp
        return
    fi

    taskTracker=`waitForTTHostForAttemptId $attemptID1 MAP $WAIT_TIME`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get task tracker from attempt $attemptID1 after $WAIT_TIME secs"
        windUp
        return
    fi
    echo "Task tracker running map task: $taskTracker" >> $ARTIFACTS_FILE 2>&1

    ssh $taskTracker ps -efj | grep $attemptID1 > $ARTIFACTS_DIR/processinfo                        
    # Wait for the task got killed and cleaned up
    sleep 200 

    echo "Tasks should be killed due to exceeding memory limits" >> $ARTIFACTS_FILE 2>&1
    #echo "Clean up by killing the job"
    echo "  Killing the job $jobID " >> $ARTIFACTS_FILE 2>&1
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID

    # clean up output file
    $HADOOP_HOME/bin/hadoop dfs -rmr out1
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log | grep $attemptID1 | grep -2 process-tree |grep java > $ARTIFACTS_DIR/error
    sleep 5
    echo " The output of error file " >> $ARTIFACTS_FILE 2>&1
    cat $ARTIFACTS_DIR/error >> $ARTIFACTS_FILE 2>&1
    local processPID=`cat $ARTIFACTS_DIR/processinfo | awk 'NR==1 {print}' |awk '{print $2}'`
    local processPPID=`cat $ARTIFACTS_DIR/processinfo | awk 'NR==1 {print}' |awk '{print $3}'`
    local processPGRPID=`cat $ARTIFACTS_DIR/processinfo | awk 'NR==1 {print}' |awk '{print $4}'`
    echo " The info from the process is  $processPID -- $processPPID ---   $processPGRPID  " >> $ARTIFACTS_FILE 2>&1
    local processPID1=`cat $ARTIFACTS_DIR/error | awk '{print $2}'`
    local processPPID1=`cat $ARTIFACTS_DIR/error |awk '{print $3}'`
    local processPGRPID1=`cat $ARTIFACTS_DIR/error |awk '{print $4}'`
    echo " The info from log file is  $processPID1 --  $processPPID1 -- $processPGRPID1  "
    if [ $processPID -ne $processPID1 ] ; then
        setFailCase " The process id of the killed task is not logged in the log file and so failing "
        windUp
        return
    fi
    if [ $processPPID -ne $processPPID1 ] ; then
        setFailCase " The parent process id PPID of the killed task is not logged in the log file and so failing "          
        windUp
        return
    fi
    if [ $processPGRPID -ne $processPGRPID1 ] ; then 
        setFailCase " The processgroud id PGRP id  of the killed task is not logged in the log file and so failing "          
        windUp
        return
    fi              
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


###############################################################
# Function to check map processes that exceeds the memory limits  log process tree when killed
###############################################################
function testLoggingOfProcessTreeForReduceTasksExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="testLoggingOfProcessTreeForReduceTasksExceedMemoryLimit"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using reducer highRAM which spawns subshells in tasks that exceeded memory limit"
    echo "3. Find out which TTs are running reducer tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running highRAM"
    echo "5. kill the process "
    echo "6. verify that the process tree is logged "
    echo "*************************************************************"
    echo " Executing testLoggingOFProcessTreeForReduceTasksExceedMemoryLimit  " >> $ARTIFACTS_FILE 2>&1
    #curUser=`whoami`
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    getJobTrackerHost       

    echo "Submit streaming job ..."
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "env" -reducer "highRAMSleep" -file $JOB_SCRIPTS_DIR_NAME/data/highRAMSleep >> $ARTIFACTS_FILE 2>&1 &
    jobID=`waitForJobId $WAIT_TIME`
    echo "Job ID: $jobID" >> $ARTIFACTS_FILE 2>&1
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after $WAIT_TIME secs"
        windUp
        return
    fi

    attemptID1=`waitForAttemptIdsForJobId $jobID REDUCE $WAIT_TIME`;
    echo " The attempt id is $attemptID1" >> $ARTIFACTS_FILE 2>&1
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank after $WAIT_TIME secs"
        windUp
        return
    fi

    taskTracker=`waitForTTHostForAttemptId $attemptID1 REDUCE $WAIT_TIME`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get task tracker from attempt $attemptID1 after $WAIT_TIME secs"
        windUp
        return
    fi
    echo "Task tracker running map task: $taskTracker" >> $ARTIFACTS_FILE 2>&1

    # Wait for the task got killed and cleaned up
    ssh $taskTracker ps -efj | grep $attemptID1 > $ARTIFACTS_DIR/processinfo
    sleep 200

    echo "Tasks should be killed due to exceeding memory limits" >> $ARTIFACTS_FILE 2>&1
    #echo "Clean up by killing the job"
    echo "  Killing the job $jobID " >> $ARTIFACTS_FILE 2>&1
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID

    # clean up output file
    $HADOOP_HOME/bin/hadoop dfs -rmr out1
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log | grep $attemptID1 | grep -2 process-tree |grep java > $ARTIFACTS_DIR/error
    sleep 5
    echo " The output of error file " >> $ARTIFACTS_FILE 2>&1
    cat $ARTIFACTS_DIR/error >> $ARTIFACTS_FILE 2>&1
    local processPID=`cat $ARTIFACTS_DIR/processinfo | awk 'NR==1 {print}' |awk '{print $2}'`
    local processPPID=`cat $ARTIFACTS_DIR/processinfo | awk 'NR==1 {print}' |awk '{print $3}'`
    local processPGRPID=`cat $ARTIFACTS_DIR/processinfo | awk 'NR==1 {print}' |awk '{print $4}'`
    echo " The info from the process is  $processPID -- $processPPID ---   $processPGRPID  " >> $ARTIFACTS_FILE 2>&1
    local processPID1=`cat $ARTIFACTS_DIR/error | awk '{print $2}'`
    local processPPID1=`cat $ARTIFACTS_DIR/error |awk '{print $3}'`
    local processPGRPID1=`cat $ARTIFACTS_DIR/error |awk '{print $4}'`
    echo " The info from log file is  $processPID1 --  $processPPID1 -- $processPGRPID1  "
    if [ $processPID -ne $processPID1 ] ; then
        setFailCase " The process id of the killed task is not logged in the log file and so failing "
        windUp
        return
    fi
    if [ $processPPID -ne $processPPID1 ] ; then
        setFailCase " The parent process id PPID of the killed task is not logged in the log file and so failing "
        windUp
        return
    fi
    if [ $processPGRPID -ne $processPGRPID1 ] ; then
        setFailCase " The processgroud id PGRP id  of the killed task is not logged in the log file and so failing "
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

cleanUpDataOnHdfs
testLoggingOfProcessTreeForMapTasksExceedMemoryLimit

cleanUpDataOnHdfs
testLoggingOfProcessTreeForReduceTasksExceedMemoryLimit

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE

