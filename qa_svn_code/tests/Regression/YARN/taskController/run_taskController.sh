#!/bin/bash

. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

# Setting Owner of this TestSuite
OWNER="bui"

export USER_ID=`whoami`
export CHECKED=0
export IN_ACTIVE_TRACKERS=1
export IN_BLACKLISTED=0

##################################################
# This function stops jobtracker
##################################################
function stopJobTracker {
    ssh ${USER_ID}@$1 /usr/local/bin/yinst stop -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER jobtracker >> $ARTIFACTS_FILE 2>&1
}

##################################################
# This function start jobtracker
##################################################
function startJobTracker {
    ssh ${USER_ID}@$1 /usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER jobtracker >> $ARTIFACTS_FILE 2>&1
}

###########################################
# Function to setup new mapred-site.xml config file at new location in TT and restart TT
###########################################
function setupNewMapredConfigFile {
    getJobTracker
    echo "Job tracker: $JOBTRACKER on $CLUSTER cluster"
    # Setup new location on JT
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/taskController_conf/"
    if [ -z "`ssh ${USER_ID}@$JOBTRACKER ls -d $NEW_CONFIG_LOCATION`" ] ; then
        ssh ${USER_ID}@$JOBTRACKER mkdir "$NEW_CONFIG_LOCATION"
        ssh ${USER_ID}@$JOBTRACKER chmod -R 755 "$NEW_CONFIG_LOCATION"
    fi
    # Copy all the conf files to new location in JT host
    ssh ${USER_ID}@"$JOBTRACKER" cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@"$JOBTRACKER" ls -l "$NEW_CONFIG_LOCATION"

    # Remove the value of mapreduce.tasktracker.taskcontroller in mapred-site.xml 
    filename="mapred-site.xml"
    echo "Change mapreduce.tasktracker.task-controller to DefaultTaskController"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $JOBTRACKER "${NEW_CONFIG_LOCATION}$filename" "mapred.task.tracker.task-controller" "org.apache.hadoop.mapred.DefaultTaskController" 
    ssh ${USER_ID}@$JOBTRACKER "cat ${NEW_CONFIG_LOCATION}$filename"

    # Restart JT or refreshNode
    resetJobTracker stop ${NEW_CONFIG_LOCATION}
    #stopJobTracker $JOBTRACKER
    sleep 10
    resetJobTracker start ${NEW_CONFIG_LOCATION}
    #startJobTracker $JOBTRACKER
    sleep 20
    echo "New HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "ps aux | grep -v grep | grep jobtracker | sed \"s/classpath /@/\" | cut -d '@' -f2 | cut -d ':' -f1"
}

#################################################
# Function to reset JT to default
#################################################
function resetJTToDefault {
    getJobTracker
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/taskController_conf/"
    ssh ${USER_ID}@$JOBTRACKER rm -r $NEW_CONFIG_LOCATION
    resetJobTracker stop
    #stopJobTracker $JOBTRACKER
    sleep 10
    #startJobTracker $JOBTRACKER
    resetJobTracker start
    sleep 10
    echo "Default HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "ps aux | grep -v grep | grep jobtracker | sed \"s/classpath /@/\" | cut -d '@' -f2 | cut -d ':' -f1"
}

function getJobIds {
    for i in 0 1 2 3 4; do
        joblist=`$HADOOP_COMMON_HOME/bin/hadoop job -list`
        JOBIDS=( `echo "$joblist" | grep job_ | awk '{print $1}'` )
        if (( ${#JOBIDS[*]} < $1 )) ; then
            echo "Waiting for job Ids ..."
            sleep 10
        else
            break
        fi
    done
    return $?
}

function killAJob {
    if [ -n "$1" ] ; then
        $HADOOP_COMMON_HOME/bin/hadoop job -kill $1
    else
        echo "Empty JobID"
    fi
}

function getOnwerOfJob {
    $HADOOP_COMMON_HOME/bin/hadoop job -list | grep $1 | awk '{print $4}'
}

function cleanUpTmpDirs {
    local admin_host="adm102.blue.ygrid.yahoo.com"
    task_trackers=`getActiveTaskTrackers`
    tt_hosts=`echo $task_trackers | tr -d '\n\'`
    if [ -z "$tt_hosts" ] ; then
        setFailCase "Cannot get Active tracker list"
    fi
    echo "List of active Task Trackers: $tt_hosts"
    for tt_host in $tt_hosts; do
        ssh $admin_host "pdsh -R exec -w $tt_host "rm -r ${HADOOP_QA_ROOT}/tmp/mapred-local/taskTracker/hadoopqa"
                ssh $admin_host "pdsh -R exec -w $tt_host "rm -r /grid/1/tmp/mapred-local/taskTracker/hadoopqa"
        ssh $admin_host "pdsh -R exec -w $tt_host "rm -r /grid/2/tmp/mapred-local/taskTracker/hadoopqa"
                ssh $admin_host "pdsh -R exec -w $tt_host "rm -r /grid/3/tmp/mapred-local/taskTracker/hadoopqa"
    done             
}       

function copyNewMapredFiletoAllTTNodes {
    task_trackers=`getActiveTaskTrackers`
    tt_hosts=`echo $task_trackers | tr -d '\n\'`
    if [ -z "$tt_hosts" ] ; then
        setFailCase "Cannot get Active tracker list"
    fi
    echo "List of active Task Trackers: $tt_hosts"
    for tt_host in $tt_hosts; do
        if [ -z "`ssh $tt_host \"ls -d $NEW_CONFIG_LOCATION\"`" ] ; then
            ssh $tt_host mkdir "$NEW_CONFIG_LOCATION"
            ssh $tt_host chmod -R 755 "$NEW_CONFIG_LOCATION"
        fi
        # Copy all the conf files to new location all hosts
        ssh $tt_host cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
        ssh $tt_host ls -l "$NEW_CONFIG_LOCATION"

        # Remove the value of mapreduce.tasktracker.taskcontroller in mapred-site.xml 
        filename="mapred-site.xml"
        echo "Change mapreduce.tasktracker.taskcontroller to DefaultTaskController"
        scpFileFromRemoteHost_ModifyAValueAndSendBack $tt_host ${NEW_CONFIG_LOCATION}$filename mapreduce.tasktracker.taskcontroller "org.apache.hadoop.mapred.DefaultTaskController"  
    done             
}

#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
#############################################################
function getAttemptIdsForJobId {
    for i in 0 1 2 3 4; do
        ATTEMPTIDS=( `${HADOOP_COMMON_HOME}/bin/hadoop job -list-attempt-ids $1 $2 running` )
        if (( ${#ATTEMPTIDS[*]} == 0 )); then
            echo "Waiting for attempt IDs ..."
            sleep 2
        else
            break
        fi
    done
    #$HADOOP_COMMON_HOME/bin/hadoop job -list-attempt-ids $1 map running | sed 'N; s/\n[       ]*/ /'
    #return $?
}

##############################################################
# Function to get the tasktracker host for a given attemptid
# Params: $1 is the attempID
##############################################################
function getTTHostForAttemptId {
    getJobTracker
    # ssh into the jobtracker host and get the TT host that this attempt is being executed
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log |grep $1 |grep MAP | tail -1 | awk -F'tracker_' '{print $2}' | cut -d ':' -f1
    return $?
}

###########################################
# Function to check the owner of the initialized tasks and job 
###########################################
function taskController_checkOwnerOfJobAndTasks {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a job by user hadoop3" 
    echo "2. Submit a job by user hadoop4" 
    echo "4. Submit a job by user hadoopqa" 
    echo "5. Verify the jobs are owned by the correct users"
    echo "6. Get the task trackers of job 1 and verify the task logs on all TTs are owned by the correct user"
    echo "*************************************************************"
    getJobTracker
    echo "Running on Job Tracker $JOBTRACKER in $CLUSTER cluster"
    echo "Generate a Kerberos ticket for hadoop3"
    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a normal job as hadoop3 user ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -m 2  -r 2 -mt 20000 -rt 10000 2>&1 &
    sleep 2

    echo "Generate a Kerberos ticket for hadoop4"
    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit a normal job as hadoop4 user ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -m 2 -r 2 -mt 20000 -rt 10000 2>&1 &
    sleep 2

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a normal job as hadoopqa user ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -m 2  -r 2 -mt 20000 -rt 10000 2>&1 &
    sleep 2

    getJobIds 3 
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the first sleep job"
    fi

    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID for the second sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID for the third sleep job"
    fi
    echo "JOBIDS: $jobID1 $jobID2 $jobID3"
    $HADOOP_COMMON_HOME/bin/hadoop job -list
    echo "Owner of $jobID1:"
    getOnwerOfJob $jobID1
    # Check owner for jobs
    if [ "$(getOnwerOfJob $jobID1)" != "${HADOOP3_USER}" ] ; then
        setFailCase "Owner of job $jobID1 launched by ${HADOOP3_USER} is not owned by ${HADOOP3_USER}"
    fi
    if [ "$(getOnwerOfJob $jobID2)" != "${HADOOP4_USER}" ] ; then
        setFailCase "Owner of job $jobID2 launched by ${HADOOP4_USER} is not owned by ${HADOOP4_USER}"
    fi
    if [ "$(getOnwerOfJob $jobID3)" != "${HADOOPQA_USER}" ] ; then
        setFailCase "Owner of job $jobID3 launched by ${HADOOPQA_USER} is not owned by ${HADOOPQA_USER}"
    fi
    
    # Check owner for tasks
    getAttemptIdsForJobId $jobID1 map
    echo "AttemptIDs for job $jobID1: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    if [ -z $attemptID1 ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z $attemptID2 ] ; then
        setFailCase "Attempt ID2 is blank"
    fi
    echo "Attempt ID got from $jobID1 : $attemptID1"
    echo "Attempt ID got from $jobID1 : $attemptID2"
    taskTracker1=$(getTTHostForAttemptId "$attemptID1")
    taskTracker2=$(getTTHostForAttemptId "$attemptID2")
    echo "Task tracker 1: $taskTracker1"
    echo "Task tracker 2: $taskTracker2"
    if [ -z $taskTracker1 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID1"
    fi
    if [ -z $taskTracker2 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID2"
    fi
    echo "Check task Tracker: $taskTracker1 log"
    ssh $taskTracker1 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker1}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${HADOOP3_USER}/jobcache/${jobID1}/${attemptID1}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID1 is not written to correct log path for user ${HADOOP3_USER}"
    fi
    echo "Check task Tracker: $taskTracker2 log"
    ssh $taskTracker2 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker2}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${HADOOP3_USER}/jobcache/${jobID1}/${attemptID2}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID2 is not written to correct log path for user ${HADOOP3_USER}"
    fi

    $HADOOP_COMMON_HOME/bin/hadoop job -list
    getAttemptIdsForJobId $jobID2 map
    echo "AttemptIDs for job $jobID2: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    if [ -z "$attemptID1" ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z "$attemptID2" ] ; then
        setFailCase "Attempt ID2 is blank"
    fi
    echo "Attempt ID got from $jobID2 : $attemptID1"
    echo "Attempt ID got from $jobID2 : $attemptID2"
    taskTracker1=$(getTTHostForAttemptId $attemptID1)
    taskTracker2=$(getTTHostForAttemptId $attemptID2)
    if [ -z $taskTracker1 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID1"
    fi
    if [ -z $taskTracker2 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID2"
    fi
    echo "Check task Tracker: $taskTracker1 log"
    ssh $taskTracker1 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker1}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${HADOOP4_USER}/jobcache/${jobID2}/${attemptID1}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID1 is not written to correct log path for user ${HADOOP4_USER}"
    fi
    echo "Check task Tracker: $taskTracker2 log"
    ssh $taskTracker2 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker2}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${HADOOP4_USER}/jobcache/${jobID2}/${attemptID2}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID2 is not written to correct log path for user ${HADOOP4_USER}"
    fi

    $HADOOP_COMMON_HOME/bin/hadoop job -list
    getAttemptIdsForJobId $jobID3 map
    echo "AttemptIDs for job $jobID3: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    attemptID2=${ATTEMPTIDS[1]}
    if [ -z "$attemptID1" ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z "$attemptID2" ] ; then
        setFailCase "Attempt ID2 is blank"
    fi
    echo "Attempt ID got from $jobID3 : $attemptID1"
    echo "Attempt ID got from $jobID3 : $attemptID2"
    taskTracker1=$(getTTHostForAttemptId $attemptID1)
    taskTracker2=$(getTTHostForAttemptId $attemptID2)
    if [ -z $taskTracker1 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID1"
    fi
    if [ -z $taskTracker2 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID2"
    fi
    echo "Check task Tracker: $taskTracker1 log"
    ssh $taskTracker1 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker1}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${HADOOPQA_USER}/jobcache/${jobID3}/${attemptID1}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID1 is not written to correct log path for user ${HADOOPQA_USER}"
    fi
    echo "Check task Tracker: $taskTracker2 log"
    ssh $taskTracker2 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker2}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${HADOOPQA_USER}/jobcache/${jobID3}/${attemptID2}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID2 is not written to correct log path for user ${HADOOPQA_USER}"
    fi

    # Don't need to wait for the job to complete; so kill them
    killAJob $jobID1
    sleep 5
    killAJob $jobID2
    sleep 5
    killAJob $jobID3
    sleep 5

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check the owner of the data output written out by a job
###########################################
function taskController_correctOwnerOfDataOutput {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job to write data output to dfs"
    echo "3. Login to the datanodes and verify output directories are owned by the correct owner"
    echo "*************************************************************"
    echo "Creating Hdfs dir streaming-${USER_ID}"
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system after creating HdfsDir"
    $HADOOP_COMMON_HOME/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Copy input.txt file to Hdfs"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    echo "File system"
    $HADOOP_COMMON_HOME/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -ls /user/hadoopqa/streaming-${USER_ID}/

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output "/user/${USER_ID}/streaming-${USER_ID}/streaming$TESTCASE_DESC.out" -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1" 
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output "/user/${USER_ID}/streaming-${USER_ID}/streaming$TESTCASE_DESC.out" -mapper "subshell.sh" -reducer "NONE" -file $JOB_SCRIPTS_DIR_NAME/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the streaming job"
    fi

    sleep 30
    echo "Checking ownership of output file"
    $HADOOP_COMMON_HOME/bin/hadoop dfs -ls "/user/${USER_ID}/streaming-${USER_ID}/streaming$TESTCASE_DESC.out"
    $HADOOP_COMMON_HOME/bin/hadoop dfs -ls "/user/${USER_ID}/streaming-${USER_ID}" | grep "streaming$TESTCASE_DESC.out" | awk '{print $3}'

    # Clean up the job
    killAJob $jobID1
    sleep 5      

    # clean up output file
    $HADOOP_COMMON_HOME/bin/hadoop dfs -rmr "/user/${USER_ID}/streaming-${USER_ID}/streaming$TESTCASE_DESC.out"

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to kill a job and check the owner of the killed job
###########################################
function taskController_OwnerOfKilledJobAndTasks {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a job by user hadoop3" 
    echo "2. Submit a job by user hadoopqa" 
    echo "3. Kill the jobs and verify the terminating tasks are owned by the correct users"
    echo "*************************************************************"
    getJobTracker
    echo "Running on Job Tracker $JOBTRACKER in $CLUSTER cluster"
    
    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a normal job as hadoop3 user ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -m 20  -r 10 -mt 10000 -rt 10000 2>&1 &
    sleep 5
    
    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a normal job as hadoopqa user ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -m 20  -r 10 -mt 10000 -rt 10000 2>&1 &
    sleep 5

    getJobIds 2
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the first sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID for the second sleep job"
    fi
    echo "JOBIDS: $jobID1 $jobID2"
    $HADOOP_COMMON_HOME/bin/hadoop job -list
    
    # Check owner for tasks
    attemptIDs=$(getAttemptIdsForJobId ${jobID1})
    echo "Attempts for $jobID1: $attemptIDs"
    attemptID1=`echo "$attemptIDs" | awk '{print $1}'`
    attemptID2=`echo "$attemptIDs" | awk '{print $2}'`
    $HADOOP_COMMON_HOME/bin/hadoop job -list
    echo "Attempt ID got from $jobID1 : $attemptID1"
    echo "Attempt ID got from $jobID1 : $attemptID2"
    if [ -z "$attemptID1" ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z "$attemptID2" ] ; then
        setFailCase "Attempt ID2 is blank"
    fi
    taskTracker1=$(getTTHostForAttemptId $attemptID1)
    taskTracker2=$(getTTHostForAttemptId $attemptID2)
    if [ -z $taskTracker1 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID1"
    fi
    if [ -z $taskTracker2 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID2"
    fi
    
    # Kill the jobs
    killAJob $jobID1
    killAJob $jobID2
    sleep 10
    # Verify the terminating tasks owner
    ssh $JOBTRACKER tail -50 $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-${JOBTRACKER}.log

    displayTestCaseResult
    return $COMMAND_EXIT_CODE  
}

###########################################
# Function to remove value of taskcontroller in mapred-site.xml and verify the owner of jobs are mapred
###########################################
function taskController_OwnerOfJobAndTasksWithEmptyValueTaskController {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy files from $HADOOP_CONF_DIR to new location"
    echo "2. Remove mapreduce.tasktracker.taskcontroller in mapred-site.xml" 
    echo "3. Restart Job Tracker with new config dir" 
    echo "4. Submit a job by user hadoopqa" 
    echo "5. Verify the jobs are owned by mapred"
    echo "6. Get the task trackers of the job and verify the task logs on all TTs are owned by mapred user"
    echo "*************************************************************"
    getJobTracker
    echo "Running on Job Tracker $JOBTRACKER in $CLUSTER cluster"
    getKerberosTicketForUser ${MAPRED_USER}
    setKerberosTicketForUser ${MAPRED_USER}
    cleanUpTmpDirs

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    setupNewMapredConfigFile
    copyNewMapredFiletoAllTTNodes
    echo "Submit a normal job as hadoopqa user ..."
    $HADOOP_COMMON_HOME/bin/hadoop --config $NEW_CONFIG_LOCATION jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -m 20  -r 10 -mt 10000 -rt 10000 2>&1 &
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the first sleep job"
    fi
    echo "JOBIDS: $jobID1"
    $HADOOP_COMMON_HOME/bin/hadoop job -list

    # Check owner for the job
    if [ "$(getOnwerOfJob $jobID1)" != "${MAPRED_USER}" ] ; then
        setFailCase "Owner of job $jobID1 is not owned by ${MAPRED_USER}"
    fi

    # Check owner for tasks
    attemptIDs=( $(getAttemptIdsForJobId ${jobID1}) )
    echo ${attemptIDs[*]}
    attemptID1=${attemptIDs[0]}
    attemptID2=${attemptIDs[1]}
    echo "Attempt ID got from $jobID1 : $attemptID1"
    echo "Attempt ID got from $jobID1 : $attemptID2"
    if [ -z "$attemptID1" ] ; then
        setFailCase "Attempt ID1 is blank"
    fi
    if [ -z "$attemptID2" ] ; then
        setFailCase "Attempt ID2 is blank"
    fi
    taskTracker1=$(getTTHostForAttemptId $attemptID1)
    taskTracker2=$(getTTHostForAttemptId $attemptID2)
    if [ -z $taskTracker1 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID1"
    fi
    if [ -z $taskTracker2 ] ; then
        setFailCase "Cannot get Task Tracker for this attempt $attemptID2"
    fi
    echo "Check task Tracker: $taskTracker1 log"
    ssh $taskTracker1 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker1}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${MAPRED_USER}/jobcache/${jobID1}/${attemptID1}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID1 is not written to correct log path of user ${MAPRED_USER}"
    fi

    echo "Check task Tracker: $taskTracker2 log"
    ssh $taskTracker2 "cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-tasktracker-${taskTracker2}.log | grep \"/tmp/mapred-local/ttprivate/taskTracker/${MAPRED_USER}/jobcache/${jobID1}/${attemptID2}/taskjvm.sh\""
    if [ -z "$?" ] ; then
        setFailCase "Task attempt $attemptID2 is not written to correct log path of user ${MAPRED_USER}"
    fi

    # Don't need to wait for the job to complete; so kill them
    killAJob $jobID1
    sleep 5
    
    #resetJTToDefault
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##########################################
# Main function
##########################################
# MAPREDUCE:899
taskController_checkOwnerOfJobAndTasks
taskController_correctOwnerOfDataOutput
#taskController_OwnerOfKilledJobAndTasks
#taskController_OwnerOfJobAndTasksWithEmptyValueTaskController

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
