#!/bin/bash

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="bui"

export USER_ID=`whoami`
export CHECKED=0
export IN_ACTIVE_TRACKERS=1
export IN_BLACKLISTED=0

######################################################################################
# This function returns the JOBID of the task submitted based on the output returns.
# The function retries 5 times and waits for 10 secs in between.
# Params: $1 is the output
######################################################################################
function getJobIds {
    for i in 0 1 2 3 4; do
        joblist=`${HADOOP_COMMON_HOME}/bin/hadoop job -list`
        JOBIDS=( `echo "$joblist" | grep job_ | awk '{print $1}'` )
        if (( ${#JOBIDS[*]} < $1 )) ; then
            echo "Waiting for job Ids ..."
            sleep 10
        else
            break
        fi
    done
}

#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
#############################################################
function getAttemptIdsForJobId_MapTask {
    for i in 0 1 2 3 4 5 6 7 8 9; do
        ATTEMPTIDS=( `${HADOOP_COMMON_HOME}/bin/hadoop job -list-attempt-ids $1 map running` )
        if (( ${#ATTEMPTIDS[*]} -eq 0 )); then
            echo "Waiting for attempt IDs ..."
            sleep 2
        else
            break
        fi
    done
}

function getAttemptIdsForJobId_ReduceTask {
    for i in 0 1 2 3 4 5 6 7 8 9; do
        ATTEMPTIDS=( `${HADOOP_COMMON_HOME}/bin/hadoop job -list-attempt-ids $1 reduce running` )
        if (( ${#ATTEMPTIDS[*]} -eq 0 )); then
            echo "Waiting for attempt IDs ..."
            sleep 2
        else
            break
        fi
    done
}

##############################################################
# Function to get the JOBID of the task submitted
##############################################################
function getJobId {
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -list | tail -1 | cut -f 1
    return $?
}

##############################################################
# Function to get the tasktracker host for a given attemptid
# Params: $1 is the attempID
##############################################################
function getTTHostForAttemptId {
    getJobTracker
    # ssh into the jobtracker host and get the TT host that this attempt is being executed
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log |grep $1 |grep MAP | tail -1 |awk -F'tracker_' '{print $2}' | cut -d ':' -f1
    return $?
}

###############################################################
# Function to check child processes of the killed job are cleaned up 
###############################################################
function runAwayProcesses_killJobAndChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Kill the streaming job using JobID"
    echo "6. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    echo "Creating Hdfs dir streaming-${USER_ID}"
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system after creating HdfsDir"
    $HADOOP_COMMON_HOME/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -ls 
    echo "Copy input.txt file to Hdfs"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    $HADOOP_COMMON_HOME/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -ls /user/hadoopqa/streaming-${USER_ID}/
        # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    sleep 20

    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Cannot get jobID for the streaming job after 20 secs"
    fi
    
    getAttemptIdsForJobId_MapTask $jobID    
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID"
    if [ -z $attemptID ] ; then
        setFailCase "Attempt ID is blank"
    fi

    sleep 15
    taskTracker=`getTTHostForAttemptId $attemptID`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID"
    fi

    echo "Task tracker running map task: $taskTracker"
    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep subshell"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $taskTracker"
    echo "$subshell_before"
    child_pid=`ssh $taskTracker ps -efj | grep -v grep | grep subshell | awk '{print $2}'` 
    echo "Processes running subshell.sh script (process id):"
    echo "$child_pid"
    
    sleep 20
    echo "Kill the job"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID

    sleep 20
    ssh $taskTracker "ps -ef | grep -v grep | grep subshell | awk '{print $2}'" > childfile 
    echo "Child processes `cat childfile` "
    if [ -s childfile ] ; then 
        setFailCase "Child process subshell is still running on $taskTracker after job is killed"
    fi
    
    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#################################################################
# Function to check child processes of the killed task are cleaned up
#################################################################
function runAwayProcesses_killTaskAndChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="runAwayProcesses_killTaskAndChildProcess"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Kill one of the tasks using task attempID"
    echo "6. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    sleep 20

    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 15 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    echo "$attemptID1"
    echo "And ..."
    echo "$attemptID2"
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi

    sleep 20
    getJobTracker
    taskTracker=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID"
    fi
    echo "Task tracker running map task: $taskTracker"
    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep subshell"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $taskTracker"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pid=`ssh $taskTracker ps -efj | grep -v grep | grep subshell | awk '{print $2}'`
    echo "Processes running subshell.sh (process ids):"
    echo "$child_pid"

    sleep 20
    echo "Kill all tasks"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill-task $attemptID1
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill-task $attemptID2

    sleep 30
    # Verify that no more processes running subshell.sh
    ssh $taskTracker "ps -efj | grep -v grep | grep $attemptID1 | grep subshell | awk '{print $2}'" > childfile
    echo "Child processes: `cat childfile` "
    if [ -s childfile ] ; then
        setFailCase "Child process subshell is still running on $taskTracker after job is killed"
    fi
    ssh $taskTracker "ps -efj | grep -v grep | grep $attemptID2 | grep subshell | awk '{print $2}'" > childfile
    echo "Child processes: `cat childfile` "
    if [ -s childfile ] ; then
        setFailCase "Child process subshell is still running on $taskTracker after job is killed"
    fi

    #echo "Clean up by killing the job"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID

    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#######################################################
# Function to check child processes of the suspended job are killed
#######################################################
function runAwayProcesses_SuspendTaskAndKillChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="runAwayProcesses_SuspendTaskAndKillChildProcess"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Get the process ID of the task attempt"
    echo "6. Kill the process ID of the task attempt"
    echo "7. Wait for response time timed out, ie. 600sec"
    echo "8. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20
    echo "ProcessID of the parent process (streaming job): $processID"
    jobID=`getJobId`
    echo "Job ID: $jobID"
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after 15 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    attemptID3=${ATTEMPTIDS[2]}
    echo "$attemptID1"
    echo "$attemptID2"
    echo "$attemptID3"
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z $attemptID2 ] ; then
        setFailCase "Attempt ID2 is blank"
    fi

    sleep 20
    getJobTracker
    taskTracker=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID1"
    fi
    echo "Task tracker running map task: $taskTracker"

    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep subshell"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $taskTracker"
    echo "$subshell_before"
    # Get process IDs of the task attempts
    local i=0
    PID_TTs=( `echo "$subshell_before" | awk '{print $4}'` )
    echo "Parent process IDs: ${PID_TTs[*]}"
    while [ $i -lt $num_subshell_before ] ; do
        echo "PPID of TT process: ${PID_TTs[$i]}"
        # Suspend the task process IDs
        echo "Killing the task by PPIDs"
        ssh $taskTracker kill -SIGSTOP ${PID_TTs[$i]}
        (( i = $i + 1 ))
        sleep 10
    done

    echo "Wait for timed out for 10 mins"
    sleep 620
    # Verify that no more processes running subshell.sh
    ssh $taskTracker "ps -efj | grep -v grep | grep subshell" > childfile
    echo "Child processes: `cat childfile` "
    if [ -s childfile ] ; then
        setFailCase "Child process subshell is still running on $taskTracker after job is killed"
    fi

    #echo "Clean up by killing the job"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID
    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check child processes of a failed task are cleaned up
###########################################
function runAwayProcesses_FailTaskAndKillChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="runAwayProcesses_FailTaskAndKillChildProcess"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Kill one of the tasks using task attempID"
    echo "6. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20
    echo "ProcessID of the parent process (streaming job): $processID"
    jobID=`getJobId`
    echo "Job ID: $jobID"
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after 15 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    echo "$attemptID1"
    echo "And ..."
    echo "$attemptID2"
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z $attemptID2 ] ; then
        setFailCase "Attempt ID2 is blank"
    fi

    sleep 20
    getJobTracker
    taskTracker=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get task tracker from attempt $attemptID1"
    fi
    echo "Task tracker running map task: $taskTracker"

    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep subshell"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $taskTracker"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pid=`ssh $taskTracker ps -efj | grep -v grep | grep subshell | awk '{print $2}'`
    echo "Processes running subshell.sh (process ids):"
    echo "$child_pid"
    sleep 20

    echo "Fail all tasks 4 times"
    for i in 0 1 2 3 ; do
        $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -fail-task "`echo "$attemptID1" | sed "s/.$/${i}/"`"
        sleep 10
    done

    sleep 10
    # Verify that no more processes running subshell.sh
    ssh $taskTracker "ps -ef | grep -v grep | grep $attemptID1 | grep subshell | awk '{print $2}'" > childfile
    echo "Child processes: `cat childfile` "
    if [ -s childfile ] ; then
        setFailCase "Child process subshell is still running on $taskTracker after job is killed"
    fi

    #Clean up by killing the job, just in case
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID

    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check child processes of a failed task are cleaned up
###########################################
function runAwayProcesses_ChangeOwnerAndKillTask {
    echo "*************************************************************"
    TESTCASE_DESC="runAwayProcesses_ChangeOwnerAndKillTask"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Get the pid file in the TT directory and change ownership to somebody else (other than current logged in user)"
    echo "6. Kill one of the tasks using task attempID"
    echo "7. Verify exception thrown LinuxTaskController: IOException in killing task"
    echo "8. Login to the TT and verify all child processes hung"
    echo "9. Clean up the job by killing it"
    echo "*************************************************************"

    #curUser=`whoami`
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20
    echo "ProcessID of the parent process (streaming job): $processID"
    jobID=`getJobId`
    echo "Job ID: $jobID"
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after 15 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    echo "$attemptID1"
    echo "And ..."
    echo "$attemptID2"
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z $attemptID2 ] ; then
        setFailCase "Attempt ID2 is blank"
    fi

    sleep 20
    getJobTracker
    taskTracker=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get task tracker from attempt $attemptID1"
    fi
    echo "Task tracker running map task: $taskTracker"

    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep $attemptID1"`
    echo "$subshell_before"
    # Get process IDs of the task attempts
    TT1=`echo "$subshell_before" | awk '{print $11}' | head -1`
    TT2=`echo "$subshell_before" | awk '{print $11}' | tail -1`
    echo "file of TT process: $TT1"
    echo "file of TT process: $TT2"

    # Change ownership of pid file
    ssh $taskTracker "chown bui $TT1"

    sleep 90
    # Kill all the tasks
    echo "Kill all tasks"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill-task $attemptID1
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill-task $attemptID2
    sleep 10

    # Verify exception thrown

    #Clean up by killing the job
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID

    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###############################################################
# Function to check child processes that exceeds the memory limits after the job completed are cleaned up
###############################################################
function runAwayProcesses_ChildProcessExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="runAwayProcesses_ChildProcessExceedMemoryLimit"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper highRAM which spawns subshells in tasks that exceeded memory limit"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running highRAM"
    echo "5. Let the streaming job completed"
    echo "6. Login to the TT and verify there are no more processes running highRAM"
    echo "*************************************************************"

    #curUser=`whoami`
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAM" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/highRAM >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20
    echo "ProcessID of the parent process (streaming job): $processID"
    jobID=`getJobId`
    echo "Job ID: $jobID"
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after 15 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    echo "$attemptID1"
    echo "And ..."
    echo "$attemptID2"
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z $attemptID2 ] ; then
        setFailCase "Attempt ID2 is blank"
    fi

    sleep 20
    getJobTracker
    taskTracker=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get task tracker from attempt $attemptID1"
    fi
    echo "Task tracker running map task: $taskTracker"

    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep highRAM"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $taskTracker"
    echo "$subshell_before"
    child_pid=`ssh $taskTracker ps -efj | grep -v grep | grep highRAM | awk '{print $2}'`
    echo "Processes running highRAM script (process id):"
    echo "$child_pid"

    # Wait for the task got killed and cleaned up
    sleep 120

    echo "Tasks should be killed due to exceeding memory limits"
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log | grep $attemptID1 | grep "beyond memory-limits" > error
    cat error
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log | grep $attemptID2 | grep "beyond memory-limits" > error
    cat error
    ssh $taskTracker "ps -efj | grep -v grep | grep highRAM | awk '{print $2}'" > childfile
    echo "Child processes `cat childfile` "
    if [ -s childfile ] ; then
        setFailCase "Child process highRAM is still running on $taskTracker after job is killed"
    fi

    #echo "Clean up by killing the job"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID
    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


###########################################
# Function to kill the job that has child processes exceeding memory litmit
###########################################
function runAwayProcesses_KillJobAndChildProcessExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="runAwayProcesses_KillJobAndChildProcessExceedMemoryLimit"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper highRAM which spawns subshells in tasks that exceeded memory limit"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running highRAM"
    echo "5. Kill the streaming job and wait for the clean up"
    echo "6. Login to the TT and verify there are no more processes running highRAM"
    echo "*************************************************************"

    #curUser=`whoami`
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

    echo "Submit streaming job ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAM" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/highRAM >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 15
    echo "ProcessID of the parent process (streaming job): $processID"
    jobID=`getJobId`
    echo "Job ID: $jobID"
    if [ -z $jobID -o $jobID == "JobId" ] ; then
        setFailCase "Streaming job is not started after 15 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "$attemptID1"
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi

    sleep 20
    getJobTracker
    taskTracker=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker ] ; then
        setFailCase "Cannot get task tracker from attempt $attemptID1"
    fi
    echo "Task tracker running map task: $taskTracker"

    subshell_before=`ssh $taskTracker "ps -efj | grep -v grep | grep highRAM"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $taskTracker"
    echo "$subshell_before"
    child_pid=`ssh $taskTracker ps -efj | grep -v grep | grep highRAM | awk '{print $2}'`
    echo "Processes running highRAM script (process id):"
    echo "$child_pid"

    # Kill the job
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -kill $jobID
    sleep 40

    ssh $taskTracker "ps -efj | grep -v grep | grep highRAM | awk '{print $2}'" > childfile
    echo "Child processes `cat childfile` "
    if [ -s childfile ] ; then
        setFailCase "Child process highRAM is still running on $taskTracker after job is killed"
    fi
    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#########################################
# Main program
#########################################
runAwayProcesses_killJobAndChildProcess
runAwayProcesses_killTaskAndChildProcess
runAwayProcesses_SuspendTaskAndKillChildProcess
runAwayProcesses_FailTaskAndKillChildProcess
runAwayProcesses_ChildProcessExceedMemoryLimit
runAwayProcesses_KillJobAndChildProcessExceedMemoryLimit

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
