#!/bin/bash

. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh

# Setting Owner of this TestSuite
OWNER="bui"
export USER_ID=`whoami`
export FAILEDDISK_HOST=""

function validateStreamingOutput {
    REASONS=`diff ${WORKSPACE}/data/expectedOutput "$1"`
    if [ "X${REASONS}X" == "XX" ];then
        return 0
    else
        return 1
    fi
}

###############################################################
# Function to inject a disk failure 
# Params: $1 is the hostname, $2 is the disk to be umounted
###############################################################
function injectDiskFailure {
    runasroot $1 $2 "$3"
    ssh $1 df -l
}

function ReMountDisk {
    runasroot $1 $2 "$3"
    ssh $1 df -l
}

######################################################################################
# This function returns the JOBIDs of the running jobs
# The function retries 5 times and waits for 10 secs in between.
# Params: $1 is the number of running jobs
######################################################################################
function getJobIds {
    for i in 0 1 2 3 4; do
        joblist=`${HADOOP_COMMON_CMD} job -list`
        JOBIDS=( `echo "$joblist" | grep job_ | awk '{print $1}'` )
        if (( ${#JOBIDS[*]} < $1 )) ; then
            echo "Waiting for job Ids ..."
            sleep 5
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
    for i in 0 1 2 3 4; do
        ATTEMPTIDS=( `${HADOOP_COMMON_CMD} job -list-attempt-ids $1 map running` )
        if (( ${#ATTEMPTIDS[*]} < 1 )) ; then
            echo "Waiting for Attempt Ids ..."
            sleep 3
        else
            break
        fi
    done
}

function getAttemptIdsForJobId_ReduceTask {
    for i in 0 1 2 3 4; do
        ATTEMPTIDS=( `${HADOOP_COMMON_CMD} job -list-attempt-ids $1 reduce running` )
        if (( ${#ATTEMPTIDS[*]} < 1 )) ; then
            echo "Waiting for Attempt Ids ..."
            sleep 3
        else
            break
        fi
    done
}

##############################################################
# Function to get the tasktracker host for a given attemptid
# Params: $1 is the attempID
##############################################################
function getTTHostForAttemptId {
    getJobTracker
    # ssh into the jobtracker host and get the TT host that this attempt is being executed
    ssh $JOBTRACKER cat $HADOOP_LOG_DIR/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log |grep $1 |grep MAP | tail -1 |awk -F'tracker_' '{print $2}' | cut -d ':' -f1
}

function getTTHostsForAttemptIds {
    for (( i=0 ; i<${#ATTEMPTIDS[*]} ; i++ )); do
        attemptID=${ATTEMPTIDS[$i]}
        echo "Attempt ID: $attemptID"
        taskTracker=`getTTHostForAttemptId $attemptID`
        echo "Task Tracker: $taskTracker"
        for disk in 1 2 3; do
            ssh $taskTracker ls -l /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID}/
            if [ $? == 0 ] ; then
                echo "Disk $disk running the task"
            fi
        done
    done    
}

function captureClusterInfo {
    # Need to be hdfs to run fsck command
    getKerberosTicketForUser ${HDFS_USER}
    setKerberosTicketForUser ${HDFS_USER}
    # Save the Cluster info to Artifact file
    ${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR fsck / -files -blocks -locations >> ${ARTIFACTS}/FSCK_${CLUSTER}
    # Change back to hadoopqa 
    setKerberosTicketForUser ${HADOOPQA_USER}
}

#############################################################
# Function to clean up all the hosts (re-mount all the disks
#############################################################
function cleanUp {
    HOSTS=( `echo "$FAILEDDISK_HOST"` )
    for (( i=0 ; i < ${#HOSTS[*]} ; i++ )); do
        host=`echo ${HOSTS[$i]} | cut -d ':' -f1`
        echo "Host: $host"
        disk=`echo ${HOSTS[$i]} | cut -d ':' -f2`
        echo "Disk failed: $disk"
        echo "Re-mounted $disk on $host"
        ReMountDisk $host mount "$disk"
    done
    stopCluster
    startCluster
    ${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode wait
}

function runNormalJob {
    echo "Start a normal job ..."
    ${HADOOP_COMMON_CMD} jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -Dmapred.job.queue.name=default -m 20 -r 20 -mt 50000 -rt 50000 2>&1 &
    processID=$!
    echo "ProcessID: $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

    sleep 5
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else  
        echo "Task tracker running map task: $taskTracker1"
        echo "Fail disk1 on $taskTracker1"
        injectDiskFailure $taskTracker1 umount "-l /grid/1"
        FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/1"
    fi
    wait $processID
    result=$(${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR job -status $jobID)
    echo $result | grep "$jobID map() completion: 1.0 reduce() completion: 1.0" 
    if [ -z "$?" ] ; then
        setFailCase "Normal Job not completed sucessfully"
    fi
}

function runHighRAMJob {
    echo "Start a high RAM job ..."
    $HADOOP_COMMON_CMD jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -Dmapred.job.map.memory.mb=3072 -Dmapred.job.reduce.memory.mb=8192 -m 20 -r 20 -mt 50000 -rt 50000 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

    sleep 5
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        echo "Fail disk2 on $taskTracker1"
        injectDiskFailure $taskTracker1 umount "-l /grid/2"
        FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/2"
    fi
    wait $processID
    result=$(${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR job -status $jobID)
    echo $result | grep "$jobID map() completion: 1.0 reduce() completion: 1.0" 
    if [ -z "$?" ] ; then
        setFailCase "HighRAM Job not completed sucessfully"
    fi
}

function runMultipleJobs {
    echo "Start the first normal job ..."
    ${HADOOP_COMMON_CMD} jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -Dmapred.job.queue.name=default -m 20 -r 20 -mt 50000 -rt 50000 2>&1 &
    processID1=$!
    echo "ProcessID: $processID1"

    echo "Start the first high RAM job ..."
    $HADOOP_COMMON_CMD jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -Dmapred.job.map.memory.mb=3072 -Dmapred.job.reduce.memory.mb=8192 -m 20 -r 20 -mt 50000 -rt 50000 2>&1 &
    processID2=$!
    echo "ProcessID: $processID2"

    echo "Start the second normal job ..."
    ${HADOOP_COMMON_CMD} jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -Dmapred.job.queue.name=default -m 20 -r 20 -mt 50000 -rt 50000 2>&1 &
    processID3=$!
    echo "ProcessID: $processID3"

    echo "Start the second high RAM job ..."
    $HADOOP_COMMON_CMD jar $HADOOP_COMMON_HOME/hadoop-examples.jar sleep -Dmapred.job.map.memory.mb=3072 -Dmapred.job.reduce.memory.mb=8192 -m 20 -r 20 -mt 50000 -rt 50000 2>&1 &
    processID4=$!
    echo "ProcessID: $processID4"

    sleep 5
    getJobIds 4 
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        echo "Cannot get jobID1 for the job after 20 secs"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        echo "Cannot get jobID2 for the job after 20 secs"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        echo "Cannot get jobID3 for the job after 20 secs"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        echo "Cannot get jobID4 for the job after 20 secs"
    fi

    getAttemptIdsForJobId_MapTask $jobID1
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID1 got from $jobID1 : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        echo "Fail disk1 on $taskTracker1"
        injectDiskFailure $taskTracker1 umount "-l /grid/1"
        FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/1"
    fi

    sleep 5
    getAttemptIdsForJobId_MapTask $jobID2
    attemptID2=${ATTEMPTIDS[0]}
    echo "Attempt ID2 got from $jobID2 : $attemptID2"
    if [ -z $attemptID2 ] ; then
        echo "Attempt ID2 is blank"
    fi

    taskTracker2=`getTTHostForAttemptId $attemptID2`
    if [ -z $taskTracker2 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID2"
    else
        echo "Task tracker running map task: $taskTracker2"
        echo "Fail disk2 on $taskTracker2"
        injectDiskFailure $taskTracker2 umount "-l /grid/2"
        FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker2:/grid/2"
    fi

    getAttemptIdsForJobId_MapTask $jobID3
    attemptID3=${ATTEMPTIDS[0]}
    echo "Attempt ID3 got from $jobID3 : $attemptID3"
    if [ -z $attemptID3 ] ; then
        echo "Attempt ID3 is blank"
    fi

    taskTracker3=`getTTHostForAttemptId $attemptID3`
    if [ -z $taskTracker3 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID3"
    else
        echo "Task tracker running map task: $taskTracker3"
        echo "Fail disk3 on $taskTracker3"
        injectDiskFailure $taskTracker3 umount "-l /grid/3"
        FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker3:/grid/3"
    fi

    sleep 5
    getAttemptIdsForJobId_MapTask $jobID4
    attemptID4=${ATTEMPTIDS[0]}
    echo "Attempt ID4 got from $jobID4 : $attemptID4"
    if [ -z $attemptID4 ] ; then
        echo "Attempt ID4 is blank"
    fi

    taskTracker4=`getTTHostForAttemptId $attemptID4`
    if [ -z $taskTracker4 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID4"
    else
        echo "Task tracker running map task: $taskTracker4"
        echo "Fail disk1 on $taskTracker4"
        injectDiskFailure $taskTracker4 umount "-l /grid/1"
        FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker4:/grid/1"
    fi

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    result=$(${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR job -status $jobID1)
    echo $result | grep "$jobID1 map() completion: 1.0 reduce() completion: 1.0"
    if [ -z "$?" ] ; then
        setFailCase "Normal Job not completed sucessfully"
    fi
    result=$(${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR job -status $jobID2)
    echo $result | grep "$jobID2 map() completion: 1.0 reduce() completion: 1.0"
    if [ -z "$?" ] ; then
        setFailCase "HighRAM Job not completed sucessfully"
    fi
    result=$(${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR job -status $jobID3)
    echo $result | grep "$jobID3 map() completion: 1.0 reduce() completion: 1.0"
    if [ -z "$?" ] ; then
        setFailCase "Normal Job not completed sucessfully"
    fi
    result=$(${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR job -status $jobID4)
    echo $result | grep "$jobID4 map() completion: 1.0 reduce() completion: 1.0"
    if [ -z "$?" ] ; then
        setFailCase "HighRAM Job not completed sucessfully"
    fi
}

function runStreamingJob_cacheArchive {
    TESTID=`echo $FUNCNAME`
    echo "Creating Hdfs dir streaming-${TESTID}"
    createHdfsDir streaming-${TESTID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTID}"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Copy all data files to Hdfs"
    putLocalToHdfs $PWD/data/streaming/cacheinput.txt streaming-${TESTID}/cacheinput.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $PWD/data/streaming/cachedir.jar  streaming-${TESTID}/cachedir.jar >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
        # clean up output file
    $HADOOP_HDFS_CMD dfs -rmr "streaming-${TESTID}/streaming-${TESTID}outDir"

    echo "Submit streaming job ..."
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -input "streaming-${TESTID}/cacheinput.txt" -mapper "xargs cat" -reducer "cat" -output "streaming-${TESTID}/streaming-${TESTID}outDir" -cacheArchive "$NAMENODE/user/$USER_ID/streaming-${TESTID}/cachedir.jar#testlink" -jobconf mapred.map.tasks=20 -jobconf mapred.reduce.tasks=20 -jobconf mapreduce.job.acl-view-job=* 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi
    
    sleep 5
    getAttemptIdsForJobId_MapTask $jobID    
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else 
        echo "Task tracker running map task: $taskTracker1"
        for disk in 1 2 3; do
            echo "Checking $taskTracker1 log for Creating symlink message and verify it's not on the failed disk"
            ssh $taskTracker1 cat /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID1}/syslog | grep "TaskRunner: Creating symlink:" 
            if [ "$?" -eq 0 ] ; then
                echo "Symlink created on disk $disk"
                injectDiskFailure $taskTracker1 umount "-l /grid/$disk"
                FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/$disk"
            fi
        done
    fi
    
    wait $processID
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTID}/streaming-${TESTID}outDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched" 
    fi
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/"
    rm ${ARTIFACTS}/actualOutput
}

function runStreamingJob_archives {
    TESTID=`echo $FUNCNAME`
    echo "Creating Hdfs dir streaming-${TESTID}"
    createHdfsDir streaming-${TESTID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTID}"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Copy all data files to Hdfs"
    putLocalToHdfs $WORKSPACE/data/streaming/cacheinput.txt streaming-${TESTID}/cacheinput.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $WORKSPACE/data/streaming/cachedir.jar  streaming-${TESTID}/cachedir.jar >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
   # clean up output file
    $HADOOP_HDFS_CMD dfs -rmr "streaming-${TESTID}/streaming-${TESTID}outDir"

    echo "Submit streaming job ..."
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -archives "$NAMENODE/user/$USER_ID/streaming-${TESTID}/cachedir.jar#testlink" -input "streaming-${TESTID}/cacheinput.txt" -mapper "xargs cat" -reducer "cat" -output "streaming-${TESTID}/streaming-${TESTID}outDir" -jobconf mapred.map.tasks=20 -jobconf mapred.reduce.tasks=20 -jobconf mapreduce.job.acl-view-job=* 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

    sleep 5
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        for disk in 1 2 3; do
            echo "Checking $taskTracker1 log for Creating symlink message and verify it's not on the failed disk"
            ssh $taskTracker1 cat /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID1}/syslog | grep "TaskRunner: Creating symlink:"
            if [ "$?" -eq 0 ] ; then
                echo "Symlink created on disk $disk"
                injectDiskFailure $taskTracker1 umount "-l /grid/$disk"
                FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/$disk"
            fi
        done
    fi
    wait $processID
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTID}/streaming-${TESTID}outDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/"
    rm ${ARTIFACTS}/actualOutput
}

function runStreamingJob_files {
    TESTID=`echo $FUNCNAME`
    echo "Creating Hdfs dir streaming-${TESTID}"
    createHdfsDir streaming-${TESTID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTID}"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Copy all data files to Hdfs"
    putLocalToHdfs ${WORKSPACE}/data/streaming/input_files.txt streaming-${TESTID}/input_files.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/file1.txt  streaming-${TESTID}/file1.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/file2.txt  streaming-${TESTID}/file2.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/inputDir  streaming-${TESTID}/inputDir >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
   # clean up output file
    $HADOOP_HDFS_CMD dfs -rmr "streaming-${TESTID}/streaming-${TESTID}outDir"

    echo "Submit streaming job ..."
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -files "$NAMENODE/user/$USER_ID/streaming-$TESTID/file1.txt#testlink1,$NAMENODE/user/$USER_ID/streaming-$TESTID/file2.txt#testlink2,$NAMENODE/user/$USER_ID/streaming-$TESTID/inputDir#testlink" -input "streaming-${TESTID}/input_files.txt" -mapper "xargs cat" -reducer "cat" -output "streaming-${TESTID}/streaming-${TESTID}outDir" -jobconf mapreduce.job.acl-view-job=* 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

   #sleep 5
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        for disk in 1 2 3; do
            echo "Checking $taskTracker1 log for Creating symlink message and verify it's not on the failed disk"
            ssh $taskTracker1 cat /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID1}/syslog | grep "TaskRunner: Creating symlink:"
            if [ "$?" -eq 0 ] ; then
                echo "Symlink created on disk $disk"
                injectDiskFailure $taskTracker1 umount "-l /grid/$disk"
                FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/$disk"
            fi
        done
    fi
    wait $processID
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTID}/streaming-${TESTID}outDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    echo "Expected:"
    cat "${WORKSPACE}/data/streaming/expectedOutput_files"
    echo "Actual:"
    cat ${ARTIFACTS}/actualOutput
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput_files "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/"
    rm ${ARTIFACTS}/actualOutput
}

function runStreamingJob_cacheFile {
    TESTID=`echo $FUNCNAME`
    echo "Creating Hdfs dir streaming-${TESTID}"
    createHdfsDir streaming-${TESTID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTID}"
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Copy all data files to Hdfs"
    putLocalToHdfs ${WORKSPACE}/data/streaming/input_files.txt streaming-${TESTID}/input_files.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/file1.txt  streaming-${TESTID}/file1.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/file2.txt  streaming-${TESTID}/file2.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/inputDir  streaming-${TESTID}/inputDir >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
   # clean up output file
    $HADOOP_HDFS_CMD dfs -rmr "streaming-${TESTID}/streaming-${TESTID}outDir"

    echo "Submit streaming job ..."
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -cacheFile "$NAMENODE/user/$USER_ID/streaming-$TESTID/file1.txt#testlink1,$NAMENODE/user/$USER_ID/streaming-$TESTID/file2.txt#testlink2,$NAMENODE/user/$USER_ID/streaming-$TESTID/inputDir#testlink" -input "streaming-${TESTID}/input_files.txt" -mapper "xargs cat" -reducer "cat" -output "streaming-${TESTID}/streaming-${TESTID}outDir" -jobconf mapreduce.job.acl-view-job=* 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

   #sleep 5
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    if [ ${#ATTEMPTIDS[*]} == 0 ] ; then
        getAttemptIdsForJobId_ReduceTask $jobID
    fi
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        for disk in 1 2 3; do
            echo "Checking $taskTracker1 log for Creating symlink message and verify it's not on the failed disk"
            ssh $taskTracker1 cat /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID1}/syslog | grep "TaskRunner: Creating symlink:"
            if [ "$?" -eq 0 ] ; then
                echo "Symlink created on disk $disk"
                injectDiskFailure $taskTracker1 umount "-l /grid/$disk"
                FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/$disk"
            fi
        done
    fi
    wait $processID
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTID}/streaming-${TESTID}outDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    echo "Expected:"
    cat "${WORKSPACE}/data/streaming/expectedOutput_files"
    echo "Actual:"
    cat ${ARTIFACTS}/actualOutput
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput_files "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/"
    rm ${ARTIFACTS}/actualOutput
}

function runStreamingJob_libjar {
    TESTID=`echo $FUNCNAME`
    echo "Creating Hdfs dir streaming-${TESTID}"
    createHdfsDir streaming-${TESTID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTID}"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Copy all data files to Hdfs"
    putLocalToHdfs ${WORKSPACE}/data/streaming/file1.txt streaming-${TESTID}/file1.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/file.jar  streaming-${TESTID}/file.jar >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
   # clean up output file
    $HADOOP_HDFS_CMD dfs -rmr "streaming-${TESTID}/streaming-${TESTID}outDir"

    echo "Submit streaming job ..."
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -libjars  ${WORKSPACE}/data/streaming/file.jar -input "streaming-${TESTID}/file1.txt" -mapper "mapper.sh" -reducer "NONE" -output "streaming-${TESTID}/streaming-${TESTID}outDir" -jobconf mapred.map.tasks=20 -jobconf mapred.reduce.tasks=20 -jobconf mapreduce.job.acl-view-job=* -file $WORKSPACE/data/streaming/mapper.sh 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

   #sleep 5
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    if [ ${#ATTEMPTIDS[*]} == 0 ] ; then 
        getAttemptIdsForJobId_ReduceTask $jobID
    fi
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        for disk in 1 2 3; do
            echo "Checking $taskTracker1 log for Creating symlink message and verify it's not on the failed disk"
            ssh $taskTracker1 cat /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID1}/syslog | grep "TaskRunner: Creating symlink:"
            if [ "$?" -eq 0 ] ; then
                echo "Symlink created on disk $disk"
                injectDiskFailure $taskTracker1 umount "-l /grid/$disk"
                FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/$disk"
            fi
        done
    fi
    wait $processID

    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTID}/
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTID}/streaming-${TESTID}outDir/* | grep file.jar
    if [ -z "$?" ];then
        setFailCase "libjar is not passed to the classpath"
    fi
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/"
}

function runStreamJob_MapperReducerMakeDFSCalls {
    TESTID=`echo $FUNCNAME`
    echo "Creating Hdfs dir streaming-${TESTID}"
    createHdfsDir streaming-${TESTID} >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTID}"
    createHdfsDir streaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1  
    putLocalToHdfs $WORKSPACE/data/input.txt streaming-${TESTID}/input.txt >> $ARTIFACTS_FILE 2>&1
        # clean up output file
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/streaming-${TESTID}.out"

    echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "streaming-${TESTID}/input.txt" -mapper "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -reducer "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -output "streaming${TESTCID}outDir" -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapreduce.job.acl-view-job=*"
    echo "OUTPUT:"   
    ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR -input "streaming-${TESTID}/input.txt" -mapper "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR dfs -help" -reducer "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR dfs -help" -output "streaming-${TESTID}/streaming-${TESTID}outDir" -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapreduce.job.acl-view-job=* 2>&1 &

    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

   #sleep 3
    getAttemptIdsForJobId_MapTask $jobID
    echo "AttemptIDs: ${ATTEMPTIDS[*]}"
    echo "Number of attempts: ${#ATTEMPTIDS[*]}"
    if [ ${#ATTEMPTIDS[*]} == 0 ] ; then
        getAttemptIdsForJobId_ReduceTask $jobID
    fi
    attemptID1=${ATTEMPTIDS[0]}
    echo "Attempt ID got from $jobID : $attemptID1"
    if [ -z $attemptID1 ] ; then
        echo "Attempt ID1 is blank"
    fi

    taskTracker1=`getTTHostForAttemptId $attemptID1`
    if [ -z $taskTracker1 ] ; then
        echo "Cannot get Task Tracker for this attempt $attemptID1"
    else
        echo "Task tracker running map task: $taskTracker1"
        for disk in 1 2 3; do
            echo "Checking $taskTracker1 log for Creating symlink message and verify it's not on the failed disk"
            ssh $taskTracker1 cat /grid/$disk/tmp/mapred-local/userlogs/${jobID}/${attemptID1}/syslog | grep "Creating symlink:"
            if [ "$?" -eq 0 ] ; then
                echo "Symlink created on disk $disk"
                injectDiskFailure $taskTracker1 umount "-l /grid/$disk"
                FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/$disk"
            fi
        done
    fi
    wait $processID
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -help > ${ARTIFACTS}/expectedOutput
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTID}/streaming-${TESTID}outDir/* > ${ARTIFACTS}/actualOutput
    REASONS=`diff --ignore-space-change "${ARTIFACTS}/expectedOutput" "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    $HADOOP_COMMON_CMD dfs -rmr "streaming-${TESTID}/"
    rm ${ARTIFACTS}/actualOutput
    rm ${ARTIFACTS}/expectedOutput
}

function runPipeJob {
    TESTID=`echo $FUNCNAME`
    createHdfsDir pipes-${TESTID} 2>&1
    echo "Files system pipes-${TESTID}"
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls

    echo "Copying HADOOP_HOME to $ARTIFACTS/hadoop"
    cp -R $HADOOP_HOME/ $ARTIFACTS/hadoop  2>&1

    cd $ARTIFACTS/hadoop
    export ANT_HOME=/homes/hadoopqa/tools/ant/latest/

    echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/pipes/"
    chmod -R 755 $ARTIFACTS/hadoop/src/c++/pipes/
    echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/utils/"
    chmod -R 755 $ARTIFACTS/hadoop/src/c++/utils/
    echo "Changing permissions of $ARTIFACTS/hadoop/src/examples/pipes/"
    chmod -R 755 $ARTIFACTS/hadoop/src/examples/pipes/

    echo "Compiling the examples"
    echo "$ANT_HOME/bin/ant -Dcompile.c++=yes examples"
    $ANT_HOME/bin/ant -Dcompile.c++=yes examples
    if [[ $? -eq 0 ]] ; then
        putLocalToHdfs $ARTIFACTS/hadoop/build/c++-examples/Linux-i386-32/bin/ pipes-${TESTID} 2>&1
        putLocalToHdfs $WORKSPACE/data/pipe/input.txt pipes-${TESTID}/input.txt 2>&1
        $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls
        echo "Change directory:"
        cd - 2>&1
        echo "PIPES COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  pipes -conf  $WORKSPACE/data/pipe/word.xml -input pipes-${TESTID}/input.txt -output pipes-$TESTID/outputDir -jobconf mapred.job.name="pipesTest-$TESTID" -jobconf mapreduce.job.acl-view-job=*"
        echo "OUTPUT:"
        $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  pipes -conf $WORKSPACE/data/pipe/word.xml -input pipes-${TESTID}/input.txt -output pipes-$TESTID/outputDir -jobconf mapred.job.name="pipesTest-$TESTID" -jobconf mapreduce.job.acl-view-job=* 2>&1 &
        processID=$!
        echo "ProcessID of the parent process (streaming job): $processID"
        getJobIds 1
        jobID=${JOBIDS[0]}
        if [ -z "$jobID" ] ; then
            echo "Cannot get jobID for the streaming job after 20 secs"
        fi

        getAttemptIdsForJobId_MapTask $jobID
        echo "AttemptIDs: ${ATTEMPTIDS[*]}"
        echo "Number of attempts: ${#ATTEMPTIDS[*]}"
        attemptID1=${ATTEMPTIDS[0]}
        echo "Attempt ID got from $jobID : $attemptID1"
        if [ -z $attemptID1 ] ; then
            echo "Attempt ID1 is blank"
        fi

        taskTracker1=`getTTHostForAttemptId $attemptID1`
        if [ -z $taskTracker1 ] ; then
            echo "Cannot get Task Tracker for this attempt $attemptID1"
        else
            echo "Task tracker running map task: $taskTracker1"
            echo "Fail disk3 on $taskTracker1"
            injectDiskFailure $taskTracker1 umount "-l /grid/3"
            FAILEDDISK_HOST="$FAILEDDISK_HOST $taskTracker1:/grid/3"
        fi
        wait $processID
        
        $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -cat pipes-${TESTID}/outputDir/* > ${ARTIFACTS}/actualOutput
        REASONS=`diff ${WORKSPACE}/data/pipe/expectedOutput ${ARTIFACTS}/actualOutput`
        if [ -n "${REASONS}" ];then
            setFailCase "Output not matched"
        fi
        rm ${ARTIFACTS}/actualOutput
        $HADOOP_COMMON_CMD dfs -rmr "pipes-${TESTID}/"
    fi
}


##############################################################
# Function to inject a disk failure when running a normal job
##############################################################
function test_DFIP_NormalJob {
    runNormalJob 
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

##############################################################
# Function to inject a disk failure when running a streaming job
##############################################################
function test_DFIP_HighRAMJob {
    runHighRAMJob
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

##############################################################
# Function to inject a disk failure when running a streaming job
##############################################################
function test_DFIP_MultipleJobs {
    runMultipleJobs
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

##############################################################
# Function to inject a disk failure when running a streaming job
##############################################################
function test_DFIP_StreamingJob_cacheArchive {
    runStreamingJob_cacheArchive
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

function test_DFIP_StreamingJob_archives {
    runStreamingJob_archives
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

function test_DFIP_StreamingJob_files {
    runStreamingJob_files
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

function test_DFIP_StreamingJob_cacheFile {
    runStreamingJob_cacheFile
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_DFIP_StreamingJob_libjar {
    runStreamingJob_libjar
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}

function test_DFIP_StreamingJob_mapperReducerMakesDFSCalls {
    runStreamJob_MapperReducerMakeDFSCalls
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##############################################################
# Function to inject a disk failure when running a pipe job
##############################################################
function test_DFIP_PipeJob {
    runPipeJob
    displayTestCaseResult 
    return $COMMAND_EXIT_CODE
}


##############################################################
# Main Function
##############################################################
getJobTracker
getFileSytem
echo "Before Running the test"  >> ${ARTIFACTS}/FSCK_${CLUSTER}
echo "#############################" >> ${ARTIFACTS}/FSCK_${CLUSTER}
captureClusterInfo

test_DFIP_NormalJob
test_DFIP_HighRAMJob
test_DFIP_MultipleJobs
test_DFIP_StreamingJob_cacheArchive
test_DFIP_StreamingJob_archives
test_DFIP_StreamingJob_files
test_DFIP_StreamingJob_cacheFile
test_DFIP_StreamingJob_libjar
test_DFIP_StreamingJob_mapperReducerMakesDFSCalls
test_DFIP_PipeJob

echo "After Running the test"  >> ${ARTIFACTS}/FSCK_${CLUSTER}
echo "#############################" >> ${ARTIFACTS}/FSCK_${CLUSTER}
captureClusterInfo
cleanUp

echo "After Restart the cluster"  >> ${ARTIFACTS}/FSCK_${CLUSTER}
echo "#############################" >> ${ARTIFACTS}/FSCK_${CLUSTER}
captureClusterInfo

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE

