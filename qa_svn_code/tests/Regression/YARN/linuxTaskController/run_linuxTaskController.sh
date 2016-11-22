#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/yarn_library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

# Setting Owner of this TestSuite
OWNER="bui"

export USER_ID=`whoami`

function restartCluster_newConfig {
    if [ ! -z $1 ] ; then
        new_conf=$1
    else
        new_conf=''
    fi

    getJobTracker
    resetJobTracker stop $new_conf
    resetTaskTrackers stop $new_conf
    sleep 10
    resetJobTracker start $new_conf
    resetTaskTrackers start $newconf
    sleep 10
}

############################################
# Function to setup queue and user limit
# Param: $1: capacity-scheduler.xml data file to use
############################################
function setupNewLinuxTaskControllerConfFile  {
    getJobTracker
    echo "Job tracker: $JOBTRACKER on $CLUSTER cluster"
    # Setup new location on RM
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/linuxTaskController_conf/"
    if [ -z `ssh ${USER_ID}@$JOBTRACKER ls -d "$NEW_CONFIG_LOCATION"` ] ; then
        ssh ${USER_ID}@$JOBTRACKER mkdir "$NEW_CONFIG_LOCATION"
    fi

    # Copy all the conf files to new location in RM host
    ssh ${USER_ID}@$JOBTRACKER cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER chmod -R 755 "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER ls -l "$NEW_CONFIG_LOCATION"

    echo "Restart RM and NM"
    restartCluster_newConfig "$NEW_CONFIG_LOCATION"
    echo "New HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "${HADOOP_COMMON_CMD} --config $NEW_CONFIG_LOCATION classpath | cut -d ':' -f1"
}

############################################
# Function to setup queue and user limit
# Param: $1: capacity-scheduler.xml data file to use
############################################
function setupInvalidTaskControllerConfFile  {
    getJobTracker
    echo "Job tracker: $JOBTRACKER on $CLUSTER cluster"
    # Setup new location on RM
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/linuxTaskController_conf/"
    if [ -z `ssh ${USER_ID}@$JOBTRACKER ls -d "$NEW_CONFIG_LOCATION"` ] ; then
        ssh ${USER_ID}@$JOBTRACKER mkdir "$NEW_CONFIG_LOCATION"
    fi

    # Copy all the conf files to new location in RM host
    ssh ${USER_ID}@$JOBTRACKER cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER chmod -R 755 "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER ls -l "$NEW_CONFIG_LOCATION"

    # Remove the value of mapreduce.tasktracker.taskcontroller in mapred-site.xml 
    cp ${WORKSPACE}/data/invalid_taskcontroller.cfg $NEW_CONFIG_LOCATION/taskcontroller.cfg
    ssh ${USER_ID}@$JOBTRACKER "cat ${NEW_CONFIG_LOCATION}/taskcontroller.cfg"

    echo "Restart RM and NM"
    restartCluster_newConfig "$NEW_CONFIG_LOCATION"
    echo "New HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "${HADOOP_COMMON_CMD} --config $NEW_CONFIG_LOCATION classpath | cut -d ':' -f1"
}

#################################################
# Function to reset JT to default
#################################################
function resetJTToDefault {
    getJobTracker
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/taskController_conf/"
    ssh ${USER_ID}@$JOBTRACKER rm -r $NEW_CONFIG_LOCATION
    resetJobTracker stop
    sleep 10
    resetJobTracker start
    sleep 10
    echo "Default HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "ps aux | grep -v grep | grep jobtracker | sed \"s/classpath /@/\" | cut -d '@' -f2 | cut -d ':' -f1"
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

##########################################
# $1: AM, $2: ID of the job
##########################################
function getNMfromAttemptID {
    ssh $1 cat "/home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${1}.log" | grep "Assigned container" | grep "container$2" | grep -o "on host [A-Za-z0-9\.]*" | awk '{print $3}' | awk 'NR==1 {print}'
}

###########################################
# Function to check the owner of the initialized tasks and job
###########################################
function taskController_checkOwnerOfJobAndTasks_mrJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a normal sleep job by user hadoop3"
    echo "2. Verify the jobs are owned by the correct users"
    echo "3. Get the task trackers of the job and verify the task logs on all TTs are owned by the correct user"
    echo "4. Verify the job completed sucessfully"
    echo "5. After the job ends, there are no child processes running on the cluster"
    echo "6. After the job ends, the temp directories under mapred.local.dir/taskTracker/jobcache/ are all deleted." 
    echo "*************************************************************"
    getJobTracker
    echo "Running on Job Tracker $JOBTRACKER in $CLUSTER cluster"
    echo "Generate a Kerberos ticket for hadoop3"
    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}

    echo "Submit a sleep job as $USER_ID user ..."
    SubmitASleepJob 10 10 50000 50000 default 2>&1 &
    processID=$!
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Cannot get Job ID for the first sleep job"
    fi
    echo "JOBIDS: $jobID"
    listJobs

    # Check owner for jobs
    echo "Owner of $jobID: ` getOnwerOfJob $jobID`"
    if [ "$(getOnwerOfJob $jobID)" != "${USER_ID}" ] ; then
        setFailCase "Owner of job $jobID launched by ${USER_ID} is not owned by ${USER_ID}"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"

    for i in 0 1 2 ; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID1"
                echo "Attempt ID for map of $jobID : $attemptID2"
                NM_mapHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_mapHost"
            fi
            echo "Checking number of REDUCE tasks running for job $jobID"
            getAttemptIdsForJobId $jobID REDUCE running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running reduce tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of reduce tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for reduce of $jobID : $attemptID1"
                echo "Attempt ID for reduce of $jobID : $attemptID2"
                NM_reduceHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_reduceHost"
            fi
            if [ -n "$AM_hostname" ] ; then
                echo "Checking $AM_hostname log for creation of usercache and container"
                ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application$ID" | grep "/tmp/mapred-local/usercache/${USER_ID}/appcache/"
                if [ -z "$?" ] ; then 
                    ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}"
                    setFailCase "Not correct user in creation of usercache"
                fi
            fi
        fi
        sleep 10
    done

    wait $processID

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}

    sleep 10

    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job is not running sucessfully"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function taskController_checkOwnerOfJobAndTasks_cacheArchiveStreamingJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a streaming job by user hadoop3"
    echo "2. Verify the jobs are owned by the correct users"
    echo "3. Get the task trackers of the job and verify the task logs on all TTs are owned by the correct user"
    echo "4. Verify the job completed sucessfully"
    echo "5. After the job ends, there are no child processes running on the cluster"
    echo "6. After the job ends, the temp directories under mapreduce.cluster.local.dir/taskTracker/jobcache/ are all deleted."
    echo "*************************************************************"
    NAMENODE=$(getNN1)
    echo "Name Node: $NAMENODE"
    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}
    echo "Creating Hdfs dir streaming-${TESTCASE_DESC}"
    createHdfsDir streaming-${TESTCASE_DESC}  >> $ARTIFACTS_FILE 2>&1
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls /user/$USER_ID/streaming-${TESTCASE_DESC}/

    echo "Copy all data files to Hdfs"
    putLocalToHdfs  $WORKSPACE/data/streaming/cachedir.jar /user/$USER_ID/streaming-${TESTCASE_DESC}/cachedir.jar >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $WORKSPACE/data/streaming/cacheinput.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/cacheinput.txt >> $ARTIFACTS_FILE 2>&1

    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTCASE_DESC}/
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/OutDir"

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -Dmapreduce.job.queuename=default -input streaming-${TESTCASE_DESC}/cacheinput.txt -mapper '/usr/bin/xargs cat' -reducer 'cat' -output streaming-${TESTCASE_DESC}/OutDir -cacheArchive hdfs://${NAMENODE}/user/$USER_ID/streaming-${TESTCASE_DESC}/cachedir.jar#testlink 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -Dmapreduce.job.queuename=default -input "streaming-${TESTCASE_DESC}/cacheinput.txt" -mapper "/usr/bin/xargs cat" -reducer "cat" -output "streaming-${TESTCASE_DESC}/OutDir" -cacheArchive "hdfs://${NAMENODE}/user/$USER_ID/streaming-${TESTCASE_DESC}/cachedir.jar#testlink" 2>&1 &
    processID=$!
    sleep 10

    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi
    listJobs

    # Check owner for jobs
    echo "Owner of $jobID: ` getOnwerOfJob $jobID`"
    if [ "$(getOnwerOfJob $jobID)" != "${USER_ID}" ] ; then
        setFailCase "Owner of job $jobID launched by ${USER_ID} is not owned by ${USER_ID}"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"

    for i in 0 1 2 ; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                echo "Attempt ID for map of $jobID : $attemptID1"
                NM_mapHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_mapHost"
            fi
            echo "Checking number of REDUCE tasks running for job $jobID"
            getAttemptIdsForJobId $jobID REDUCE running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running reduce tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of reduce tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                echo "Attempt ID for reduce of $jobID : $attemptID1"
                NM_reduceHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_reduceHost"
            fi
            if [ -n "$AM_hostname" ] ; then
                echo "Checking $AM_hostname log for creation of usercache and container"
                ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}" | grep "/tmp/mapred-local/usercache/${USER_ID}/appcache/"
                if [ -z "$?" ] ; then
                    ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}"
                    setFailCase "Not correct user in creation of usercache"
                fi
            fi
        fi
        echo "Sleeping ..."
        sleep 2
    done

    wait $processID
    echo "Checking ownership of output file"
    $HADOOP_HDFS_CMD dfs -ls "streaming-${TESTCASE_DESC}/OutDir"
    #$HADOOP_HDFS_CMD dfs -ls "streaming-${TESTCASE_DESC}/OutDir" | grep "streaming-${TESTCASE_DESC}.out" | awk '{print $3}'

    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTCASE_DESC}/OutDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    echo "Expected output: "
    cat ${WORKSPACE}/data/streaming/expectedOutput_files
    echo "Actual output:"
    cat ${ARTIFACTS}/actualOutput

    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput_files "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    sleep 10

    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job is not running sucessfully"
    fi

    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/"
    rm ${ARTIFACTS}/actualOutput

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function taskController_checkOwnerOfJobAndTasks_cacheFileStreamingJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a streaming job by user hadoop3"
    echo "2. Verify the jobs are owned by the correct users"
    echo "3. Get the task trackers of the job and verify the task logs on all TTs are owned by the correct user"
    echo "4. Verify the job completed sucessfully"
    echo "5. After the job ends, there are no child processes running on the cluster"
    echo "6. After the job ends, the temp directories under mapreduce.cluster.local.dir/taskTracker/jobcache/ are all deleted."
    echo "*************************************************************"
    NAMENODE=$(getNN1)
    echo "Name Node: $NAMENODE"

    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}

    echo "Creating Hdfs dir streaming-${TESTCASE_DESC}"
    createHdfsDir streaming-${TESTCASE_DESC}/ >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTCASE_DESC}"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls /user/${USER_ID}/streaming-${TESTCASE_DESC}/
    echo "Copy all data files to Hdfs"
    putLocalToHdfs ${WORKSPACE}/data/streaming/input.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/input.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/cache.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/cache.txt >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls /user/$USER_ID/streaming-${TESTCASE_DESC}/
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/OutDir"

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.queuename=default -input streaming-${TESTCASE_DESC}/input.txt -mapper '/usr/bin/xargs cat' -reducer cat -output streaming-${TESTCASE_DESC}/OutDir -cacheFile hdfs://${NAMENODE}/user/${USER_ID}/streaming-$TESTCASE_DESC/cache.txt#testlink 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.queuename=default -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -input "streaming-${TESTCASE_DESC}/input.txt" -mapper '/usr/bin/xargs cat' -reducer 'cat' -output "streaming-${TESTCASE_DESC}/OutDir" -cacheFile "hdfs://${NAMENODE}/user/${USER_ID}/streaming-$TESTCASE_DESC/cache.txt#testlink" 2>&1 &
    processID=$!
    sleep 10

    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi
    listJobs

    # Check owner for jobs
    echo "Owner of $jobID: ` getOnwerOfJob $jobID`"
    if [ "$(getOnwerOfJob $jobID)" != "${USER_ID}" ] ; then
        setFailCase "Owner of job $jobID launched by ${USER_ID} is not owned by ${USER_ID}"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"

    for i in 0 1 2 ; do
        listJobs
        if [ -n "`isJobRunning $jobID`" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID1"
                echo "Attempt ID for map of $jobID : $attemptID2"
                NM_mapHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_mapHost"
            fi
            echo "Checking number of REDUCE tasks running for job $jobID"
            getAttemptIdsForJobId $jobID REDUCE running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running reduce tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of reduce tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for reduce of $jobID : $attemptID1"
                echo "Attempt ID for reduce of $jobID : $attemptID2"
                NM_reduceHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_reduceHost"
            fi
            if [ -n "$AM_hostname" ] ; then
                echo "Checking $AM_hostname log for creation of usercache and container"
                ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}" | grep "/grid/*/tmp/mapred-local/usercache/${USER_ID}/appcache/"
                if [ -z "$?" ] ; then
                    ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}" 
                    setFailCase "Not correct user in creation of usercache"
                fi
            fi
        fi
        sleep 10
    done

    wait $processID
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTCASE_DESC}/OutDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    echo "Expected output: "
    cat ${WORKSPACE}/data/streaming/expectedOutput
    echo "Actual output:"
    cat ${ARTIFACTS}/actualOutput
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    sleep 10

    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job is not running sucessfully"
    fi

    $HADOOP_HDFS_CMD dfs -rm -R "Streaming/streaming-${TESTCASE_DESC}/"
    rm ${ARTIFACTS}/actualOutput

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function taskController_checkOwnerOfJobAndTasks_fileStreamingJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a streaming job by user hadoop3"
    echo "2. Verify the jobs are owned by the correct users"
    echo "3. Get the task trackers of the job and verify the task logs on all TTs are owned by the correct user"
    echo "4. Verify the job completed sucessfully"
    echo "5. After the job ends, there are no child processes running on the cluster"
    echo "6. After the job ends, the temp directories under mapreduce.cluster.local.dir/taskTracker/jobcache/ are all deleted."
    echo "*************************************************************"
    NAMENODE=$(getNN1)
    echo "Name Node: $NAMENODE"

    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}

    echo "Creating Hdfs dir streaming-${TESTCASE_DESC}"
    createHdfsDir streaming-${TESTCASE_DESC}/ >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTCASE_DESC}"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls /user/${USER_ID}/streaming-${TESTCASE_DESC}/
    echo "Copy all data files to Hdfs"
    putLocalToHdfs ${WORKSPACE}/data/streaming/input.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/input.txt >> $ARTIFACTS_FILE 2>&1
    #putLocalToHdfs ${WORKSPACE}/data/streaming/cache.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/cache.txt >> $ARTIFACTS_FILE 2>&1
    #putLocalToHdfs ${WORKSPACE}/data/streaming/cache2.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/cache2.txt >> $ARTIFACTS_FILE 2>&1

    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTCASE_DESC}/
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/outDir"

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -files file://${WORKSPACE}/data/streaming/cache.txt#testlink -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -Dmapreduce.job.queuename=default -input streaming-${TESTCASE_DESC}/input.txt -mapper '/usr/bin/xargs cat' -reducer cat -output streaming-${TESTCASE_DESC}/OutDir 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -files "file://${WORKSPACE}/data/streaming/cache.txt#testlink" -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -Dmapreduce.job.queuename=default -input "streaming-${TESTCASE_DESC}/input.txt" -mapper '/usr/bin/xargs cat' -reducer 'cat' -output "streaming-${TESTCASE_DESC}/OutDir" 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi

    # Check owner for jobs
    echo "Owner of $jobID: ` getOnwerOfJob $jobID`"
    if [ "$(getOnwerOfJob $jobID)" != "${USER_ID}" ] ; then
        setFailCase "Owner of job $jobID launched by ${USER_ID} is not owned by ${USER_ID}"
    fi

    listJobs
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"

    for i in 0 1 2 ; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID1"
                echo "Attempt ID for map of $jobID : $attemptID2"
                NM_mapHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_mapHost"
            fi
            echo "Checking number of REDUCE tasks running for job $jobID"
            getAttemptIdsForJobId $jobID REDUCE running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running reduce tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of reduce tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for reduce of $jobID : $attemptID1"
                echo "Attempt ID for reduce of $jobID : $attemptID2"
                NM_reduceHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_reduceHost"
            fi
            if [ -n "$AM_hostname" ] ; then
                echo "Checking $AM_hostname log for creation of usercache and container"
                ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}" | grep "/tmp/mapred-local/usercache/${USER_ID}/appcache/"
                if [ -z "$?" ] ; then
                    ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}" 
                    setFailCase "Not correct user in creation of usercache"
                fi
            fi
        fi
        sleep 10
    done

    wait $processID
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTCASE_DESC}/OutDir/
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTCASE_DESC}/OutDir/* >  ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    echo "Expected:"
    cat "${WORKSPACE}/data/streaming/expectedOutput"
    echo "Actual:"
    cat ${ARTIFACTS}/actualOutput
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi

    sleep 10

    local return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Streaming job is not running sucessfully"
    fi

    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/"
    rm ${ARTIFACTS}/actualOutput

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function taskController_checkOwnerOfJobAndTasks_libjarStreamingJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a streaming job by user hadoop3"
    echo "2. Verify the jobs are owned by the correct users"
    echo "3. Get the task trackers of the job and verify the task logs on all TTs are owned by the correct user"
    echo "4. Verify the job completed sucessfully"
    echo "5. After the job ends, there are no child processes running on the cluster"
    echo "6. After the job ends, the temp directories under mapreduce.cluster.local.dir/taskTracker/jobcache/ are all deleted."
    echo "*************************************************************"
    NAMENODE=$(getNN1)
    echo "Name Node: $NAMENODE"

    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}
    echo "Creating Hdfs dir streaming-${TESTCASE_DESC}"
    createHdfsDir streaming-${TESTCASE_DESC}  >> $ARTIFACTS_FILE 2>&1
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls /user/$USER_ID/streaming-${TESTCASE_DESC}/

    echo "Copy all data files to Hdfs"
    putLocalToHdfs  $WORKSPACE/data/streaming/cachedir.jar /user/$USER_ID/streaming-${TESTCASE_DESC}/cachedir.jar >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $WORKSPACE/data/streaming/cacheinput.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/cacheinput.txt >> $ARTIFACTS_FILE 2>&1

    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTCASE_DESC}/
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/OutDir"

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -libjars ${WORKSPACE}/data/streaming/cachedir.jar -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -Dmapreduce.job.queuename=default -input streaming-${TESTCASE_DESC}/cacheinput.txt -mapper 'mapper.sh' -reducer cat -output streaming-${TESTCASE_DESC}/OutDir -file $WORKSPACE/data/streaming/mapper.sh 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -libjars "${WORKSPACE}/data/streaming/cachedir.jar" -Dmapreduce.job.maps=10 -Dmapreduce.job.reduces=10 -Dmapreduce.job.queuename=default -input "streaming-${TESTCASE_DESC}/cacheinput.txt" -mapper 'mapper.sh' -reducer cat -output "streaming-${TESTCASE_DESC}/OutDir" -file "$WORKSPACE/data/streaming/mapper.sh" 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"
    sleep 20

    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 20 secs"
    fi
    # Check owner for jobs
    echo "Owner of $jobID: ` getOnwerOfJob $jobID`"
    if [ "$(getOnwerOfJob $jobID)" != "${USER_ID}" ] ; then
        setFailCase "Owner of job $jobID launched by ${USER_ID} is not owned by ${USER_ID}"
    fi

    listJobs
    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"

    for i in 0 1 2 3; do
        ${HADOOP_MAPRED_CMD} job -status $jobID
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID1"
                echo "Attempt ID for map of $jobID : $attemptID2"
                NM_mapHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_mapHost"
            fi
            echo "Checking number of REDUCE tasks running for job $jobID"
            getAttemptIdsForJobId $jobID REDUCE running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running reduce tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of reduce tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for reduce of $jobID : $attemptID1"
                echo "Attempt ID for reduce of $jobID : $attemptID2"
                NM_reduceHost=`getActiveTaskTrackers |awk 'NR==1 {print}'`
                echo "NM Host: $NM_reduceHost"
            fi
            if [ -n "$AM_hostname" ] ; then
                echo "Checking $AM_hostname log for creation of usercache and container"
                ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application${ID}" | grep "/tmp/mapred-local/usercache/${USER_ID}/appcache/"
                if [ -z "$?" ] ; then
                    setFailCase "Not correct user in creation of usercache"
                fi
            fi
        fi
        sleep 10
    done

    echo "Wait for sreaming job completion"
    wait $processID
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls streaming-${TESTCASE_DESC}/
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTCASE_DESC}/OutDir/* | grep cachedir.jar
    if [ -z "$?" ];then
        setFailCase "libjar is not passed to the classpath"
    fi
    sleep 10

    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Job $jobID is not running sucessfully"
    fi

    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/"

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function taskController_checkOwnerOfJobAndTasks_PipeJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a streaming job by user hadoop3"
    echo "2. Verify the jobs are owned by the correct users"
    echo "3. Get the task trackers of the job and verify the task logs on all TTs are owned by the correct user"
    echo "4. Verify the job completed sucessfully"
    echo "5. After the job ends, there are no child processes running on the cluster"
    echo "6. After the job ends, the temp directories under mapreduce.cluster.local.dir/taskTracker/jobcache/ are all deleted."
    echo "*************************************************************"

    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}

    createHdfsDir pipes-runPipes 2>&1
    putLocalToHdfs $WORKSPACE/data/pipe/wordcount-simple pipes-runPipes/wordcount-simple 2>&1
    putLocalToHdfs $WORKSPACE/data/pipe/input.txt pipes-runPipes/input.txt 2>&1
    echo "Files system pipes-runPipes"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls
    echo "Change directory:"
    cd - >> $ARTIFACTS_FILE 2>&1

    $HADOOP_HDFS_CMD dfs -rm -R "pipes-runPipes/outputDir"

    echo "PIPES COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR -Dmapreduce.job.queuename=default pipes -conf $WORKSPACE/data/pipe/word.xml -input pipes-runPipes/input.txt -output pipes-runPipes/outputDir 2>&1 &"
    echo "OUTPUT:"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR pipes -conf "$WORKSPACE/data/pipe/word.xml" -input "pipes-runPipes/input.txt" -output "pipes-runPipes/outputDir" 2>&1 &
    processID=$!
    echo "ProcessID of the parent process (streaming job): $processID"

    sleep 10
    getJobIds 1
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        echo "Cannot get jobID for the streaming job after 10 secs"
    fi

    # Check owner for jobs
    echo "Owner of $jobID: ` getOnwerOfJob $jobID`"
    if [ "$(getOnwerOfJob $jobID)" != "${USER_ID}" ] ; then
        setFailCase "Owner of job $jobID launched by ${USER_ID} is not owned by ${USER_ID}"
    fi

    listJobs
    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"

    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"

    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                echo "Attempt ID for map of $jobID : $attemptID1"
                NM_mapHost=`getNMfromAttemptID $AM_hostname $ID`
                echo "NM Host: $NM_mapHost"
            fi
            echo "Checking number of REDUCE tasks running for job $jobID"
            getAttemptIdsForJobId $jobID REDUCE running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running reduce tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of reduce tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                echo "Attempt ID for reduce of $jobID : $attemptID1"
                NM_reduceHost=`getNMfromAttemptID $AM_hostname $ID`
                echo "NM Host: $NM_reduceHost"
            fi
            if [ -n "$AM_hostname" ] ; then
                echo "Checking $AM_hostname log for creation of usercache and container"
                ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application$ID" | grep "/tmp/mapred-local/usercache/${USER_ID}/appcache/"
                if [ -z "$?" ] ; then
                    ssh ${AM_hostname} cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep "launchContainer" | grep "application$ID"
                    setFailCase "Not correct user in creation of usercache"
                fi
            fi
        fi
        sleep 5
    done

    wait $processID
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat pipes-runPipes/outputDir/* > ${ARTIFACTS}/actualOutput
    echo "Actual output:"
    cat ${ARTIFACTS}/actualOutput
    echo "Expect output:"
    cat ${WORKSPACE}/data/pipe/expectedOutput
    REASONS=`diff --ignore-all-space ${WORKSPACE}/data/pipe/expectedOutput ${ARTIFACTS}/actualOutput`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi
    sleep 10

    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job is not running sucessfully"
    fi
    $HADOOP_HDFS_CMD dfs -rm -R "pipes-runPipes/"
    rm ${ARTIFACTS}/actualOutput
    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to launch a job with invalid taskcontroller.cfg file
###########################################
function taskController_invalidTaskControllerConfigFile {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Setup a new configuration path and copy an invalid taskcontroller.cfg (with invalid mapreduce.cluster.local.dir"
    echo "1. Submit a normal sleep job"
    echo "2. Verify the job failed with exit code 2"
    echo "*************************************************************"
    getJobTracker
    echo "Running on Job Tracker $JOBTRACKER in $CLUSTER cluster"
    setupInvalidTaskControllerConfFile

    echo "Submit a sleep job ..."
    SubmitASleepJob 10 10 30000 30000 default 2>&1
    echo "Return code: $?"

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Cannot get Job ID for the first sleep job"
    fi
    echo "JOBIDS: $jobID"
    listJobs
    restartCluster_newConfig

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to launch a job with invalid user
###########################################
function taskController_invalidJobOwner {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a normal sleep job by an invalid user"
    echo "2. Verify the job failed with exit code 2"
    echo "*************************************************************"
    getJobTracker
    echo "Running on Job Tracker $JOBTRACKER in $CLUSTER cluster"
    getKerberosTicketForUser ${$MAPRED_USER}
    setKerberosTicketForUser ${$MAPRED_USER}

    echo "Submit a sleep job as $USER_ID user ..."
    echo "${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.map.memory.mb=1024 -Dmapreduce.reduce.memory.mb=1024 -Dmapreduce.job.queuename=default -m 10 -r 10 -mt 500 -rt 500"
    output=$( ${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.map.memory.mb=1024 -Dmapreduce.reduce.memory.mb=1024 -Dmapreduce.job.queuename=default -m 10 -r 10 -mt 500 -rt 500 )
    echo "$output"

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    return=$(${HADOOP_MAPRED_CMD} job -list all | grep job_ | grep ${USER_ID} | awk '{print $2}')
    if [ "$return" != "FAILED" ] ; then
        echo "Actual return: $return"
        setFailCase "Job launched by invalid user $USER_ID is not failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#################################################################
# Function to check child processes of the killed task are cleaned up
#################################################################
function taskController_killTaskAndCheckChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Kill one of the tasks using task attempID"
    echo "6. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"


    USER_ID=${HADOOPQA_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20

    listJobs
    getJobIds 1
    jobID=${JOBIDS[0]}
    echo "Job ID: $jobID"
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 20 secs"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"
    local NMHost
    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID=${ATTEMPTIDS[0]}
                echo "Attempt ID for map of $jobID : $attemptID"
                NMHost=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID}: $NMHost"
            fi
        fi
        sleep 10
    done

    subshell_before=`ssh "$NMHost" "ps -efj | grep -v grep | grep subshell"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $NMHost"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pid=`ssh "$NMHost" ps -efj | grep -v grep | grep subshell | awk '{print $2}'`
    echo "Processes running subshell.sh (process ids):"
    echo "$child_pid"

    sleep 20
    echo "Kill task attempt $attemptID"
    $HADOOP_MAPRED_CMD job -kill-task $attemptID

    sleep 30
    echo "Verify that no more processes running subshell.sh"
    ssh $NMHost "ps -efj | grep -v grep | grep $attemptID | grep subshell"
    ssh $NMHost "ps -efj | grep -v grep | grep $attemptID | grep subshell | awk '{print $2}'" > ${ARTIFACTS}/childfile
    echo "Child processes: `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        setFailCase "Child process subshell is still running on $NMHost after job is killed"
    else
        echo "No more child processes running subshell.sh"
    fi
    #echo "Clean up by killing the job"
    $HADOOP_MAPRED_CMD job -kill $jobID

    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#################################################################
# Function to check child processes of the suspended task are cleaned up
#################################################################
function taskController_suspendedTaskAndCheckChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC=""`echo $FUNCNAME`""
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Suspend one of the tasks using task attempID"
    echo "6. Wait for response time timed out, ie. 600sec"
    echo "7. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20

    listJobs
    getJobIds 1
    jobID=${JOBIDS[0]}
    echo "Job ID: $jobID"
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 20 secs"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"
    local NMHost
    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID=${ATTEMPTIDS[0]}
                echo "Attempt ID for map of $jobID : $attemptID"
                NMHost=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID}: $NMHost"
                if [ -n "$NMHost" ]; then
                    break
                fi
            fi
        fi
        sleep 10
    done

    subshell_before=`ssh "$NMHost" "ps -efj | grep -v grep | grep subshell"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $NMHost"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pid=`ssh "$NMHost" ps -efj | grep -v grep | grep subshell | awk '{print $2}'`
    echo "Processes running subshell.sh (process ids):"
    echo "$child_pid"
    # Get process IDs of the task attempts
    PID_1=`echo "$child_pid" | head -1`
    PID_2=`echo "$child_pid" | tail -1`

    # Suspend the task process IDs
    ssh $NMHost kill -s SIGSTOP $PID_1
    ssh $NMHost kill -s SIGSTOP $PID_2
    echo "Wait for timed out for 10 mins"
    sleep 620

    echo "Verify that no more processes running subshell.sh"
    ssh $NMHost "ps -efj | grep -v grep | grep $attemptID | grep subshell"
    ssh $NMHost "ps -efj | grep -v grep | grep $attemptID | grep subshell | awk '{print $2}'" > ${ARTIFACTS}/childfile
    echo "Child processes: `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        setFailCase "Child process subshell is still running on $NMHost after job is killed"
    else
        echo "No more child processes running subshell.sh"
    fi
    #echo "Clean up by killing the job"
    $HADOOP_MAPRED_CMD job -kill $jobID

    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#################################################################
# Function to check child processes of the failed task are cleaned up
#################################################################
function taskController_failTaskAndCheckChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which TTs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the TT and verify there are processes running subshell.sh"
    echo "5. Fail one of the tasks using task attempID"
    echo "6. Login to the TT and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20

    listJobs
    getJobIds 1
    jobID=${JOBIDS[0]}
    echo "Job ID: $jobID"
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 20 secs"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"
    local NMHost
    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID"
                NMHost=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID}: $NMHost"
                if [ -n "$NMHost" ]; then
                    break
                fi
            fi
        fi
        sleep 10
    done

    containerID=`ssh $NMHost ps -efj | grep -v grep | grep $attemptID | sed 's/container/@/' | cut -d '@' -f2 | awk '{print $1}'`
    subshell_before=`ssh $NMHost ps -efj | grep -v grep | grep subshell | grep "container${containerID}"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $NMHost"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pid=`ssh "$NMHost" ps -efj | grep -v grep | grep subshell | grep "container${containerID}" | awk '{print $2}'`
    echo "Processes running subshell.sh (process ids):"
    echo "$child_pid"

    echo "Fail all the task for $attemptID"
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $attemptID
    sleep 5

    echo "Verify that no more processes of that $attemptID running subshell.sh"
    ssh $NMHost ps -efj | grep -v grep | grep subshell | grep "container${containerID}" | awk '{print $2}' > ${ARTIFACTS}/childfile
    echo "Child processes: `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        # sporadic failures. need to investigate.
        setFailCase "Child process subshell is still running on $NMHost after job is killed"
    else
        echo "No more child processes of that $attemptID running subshell.sh"
    fi
    echo "Clean up by killing the job "
#    echo "$HADOOP_MAPRED_CMD job -kill $jobID"
    $HADOOP_MAPRED_CMD job -kill $jobID

    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#################################################################
# Function to check child processes of the killed job are cleaned up
#################################################################
function taskController_killJobAndCheckChildProcess {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper subshell.sh which spawns subshells in tasks"
    echo "3. Find out which NMs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the NM and verify there are processes running subshell.sh"
    echo "5. Kill the job"
    echo "6. Login to the NM and verify there are no more processes running subshell.sh"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "subshell.sh" -reducer "NONE" -file ${WORKSPACE}/data/subshell.sh >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20

    listJobs
    getJobIds 1
    jobID=${JOBIDS[0]}
    echo "Job ID: $jobID"
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 20 secs"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"
    local NMHost
    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID"
                NMHost=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID}: $NMHost"
                if [ -n "$NMHost" ]; then
                    break
                fi
            fi
        fi
        sleep 10
    done

    containerID=`ssh $NMHost ps -efj | grep -v grep | grep $attemptID | sed 's/container/@/' | cut -d '@' -f2 | awk '{print $1}'`
    subshell_before=`ssh $NMHost ps -efj | grep -v grep | grep subshell | grep "container${containerID}"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $NMHost"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pid=`ssh "$NMHost" ps -efj | grep -v grep | grep subshell | grep "container${containerID}" | awk '{print $2}'`
    echo "Processes running subshell.sh (process ids):"
    echo "$child_pid"

    echo "Kill the $jobID"
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $jobID
    sleep 5

    echo "Verify that no more child processes of $jobID running subshell.sh"
    ssh $NMHost ps -efj | grep -v grep | grep subshell | grep "container${containerID}" 
    ssh $NMHost ps -efj | grep -v grep | grep subshell | awk '{print $2}' > ${ARTIFACTS}/childfile
    echo "Child processes: `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        setFailCase "Child process subshell is still running on $NMHost after job is killed"
    else
        echo "No more child processes of $jobID running subshell.sh"
    fi

    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


###############################################################
# Function to check child processes that exceeds the memory limits after the job completed are cleaned up
###############################################################
function taskController_ChildProcessExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper highRAM which spawns subshells in tasks that exceeded memory limit"
    echo "3. Find out which NMs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the NM and verify there are processes running highRAM"
    echo "5. Let the streaming job completed"
    echo "6. Login to the NM and verify that the highRAM processed got killed and there are no more processes running highRAM"
    echo "*************************************************************"

    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAM" -reducer "NONE" -file ${WORKSPACE}/data/highRAM >> $ARTIFACTS_FILE 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAM" -reducer "NONE" -file ${WORKSPACE}/data/highRAM >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20

    listJobs
    getJobIds 1
    jobID=${JOBIDS[0]}
    echo "Job ID: $jobID"
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 20 secs"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"
    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"
    local NMHost
    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID1, $attemptID2"
                NMHost1=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID1}: $NMHost1"
                if [ -n "$NMHost1" ]; then
                    break
                fi
                NMHost2=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID2}: $NMHost2"
                if [ -n "$NMHost2" ]; then
                    break
                fi
            fi
        fi
        sleep 10
    done

    containerID1=`ssh $NMHost1 ps -efj | grep -v grep | grep $attemptID1 | sed 's/container/@/' | cut -d '@' -f2 | awk '{print $1}'`
    subshell_before1=`ssh $NMHost1 ps -efj | grep -v grep | grep highRAM | grep "container${containerID1}"`
    num_subshell_before1=`echo "$subshell_before1" | wc -l`
    echo "There are $num_subshell_before1 child processes running on $NMHost1"
    echo "$subshell_before1"
    # running ps and got pid; second collumn is the PID
    child_pids=`ssh "$NMHost1" ps -efj | grep -v grep | grep highRAM | grep "container${containerID1}" | awk '{print $2}'`
    echo "Processes running highRAM (process ids):"
    echo "$child_pids"

    echo "Wait for streaming job completed"
    wait $processID

    echo "Verify that no more child processes of $jobID running highRAM"
    ssh $NMHost1 ps -efj | grep -v grep | grep highRAM | grep "container${containerID1}"
    ssh $NMHost1 ps -efj | grep -v grep | grep highRAM | awk '{print $2}' > ${ARTIFACTS}/childfile
    echo "Child processes: `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        setFailCase "Child process subshell is still running on $NMHost1 after job is completed"
    else
        echo "No more child processes of $jobID running highRAM"
    fi
    ssh $NMHost2 ps -efj | grep -v grep | grep highRAM | grep "container${containerID2}"
    ssh $NMHost2 ps -efj | grep -v grep | grep highRAM | awk '{print $2}' > ${ARTIFACTS}/childfile
    echo "Child processes: `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        setFailCase "Child process subshell is still running on $NMHost2 after job is completed"
    else
        echo "No more child processes of $jobID running highRAM"
    fi

    echo "Tasks should be killed due to exceeding memory limits"
    ssh $AM_hostname cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep $attemptID1 | grep "beyond memory-limits" > error
    cat error
    ssh $AM_hostname cat /home/gs/var/log/mapredqa/yarn-mapredqa-resourcemanager-${AM_hostname}.log | grep $attemptID2 | grep "beyond memory-limits" > error
    cat error

    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to kill the job that has child processes exceeding memory litmit and check for processes cleaned up
###########################################
function test_runAwayProcesses_KillJobAndChildProcessExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Copy a file from local over to hdfs to use for stream job"
    echo "2. Run a streaming job using mapper highRAM which spawns subshells in tasks that exceeded memory limit"
    echo "3. Find out which NMs are running mapper tasks by using attempID and job log in Job Tracker"
    echo "4. Login to the NM and verify there are processes running highRAM"
    echo "5. Kill the streaming job and wait for the clean up"
    echo "6. Login to the NM and verify there are no more processes running highRAM"
    echo "*************************************************************"
    createHdfsDir streaming-${USER_ID} >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/input.txt streaming-${USER_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAM" -reducer "NONE" -file ${WORKSPACE}/data/highRAM >> $ARTIFACTS_FILE 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.queuename=default -input "/user/${USER_ID}/streaming-${USER_ID}/input.txt" -output out1 -mapper "highRAM" -reducer "NONE" -file ${WORKSPACE}/data/highRAM >> $ARTIFACTS_FILE 2>&1 &
    processID=$!
    sleep 20

    listJobs
    getJobIds 1
    jobID=${JOBIDS[0]}
    echo "Job ID: $jobID"
    if [ -z "$jobID" ] ; then
        setFailCase "Streaming job is not started after 20 secs"
    fi

    ID=`echo $jobID | sed 's/job_/_/'`
    echo "ID: $ID"

    AM_hostname=$(getAMHost $jobID)
    echo "AM hostname: $AM_hostname"
    local NMHost
    for i in 0 1 2; do
        isJobRunning $jobID
        if [ -n "$?" ] ; then
            echo "Checking number of MAP tasks running for job $jobID"
            getAttemptIdsForJobId $jobID MAP running
            if [ "${#ATTEMPTIDS[*]}" -ne 0 ] ; then
                echo "AttemptIDs for running map tasks: ${ATTEMPTIDS[*]}"
                echo "****** Number of map tasks running: ${#ATTEMPTIDS[*]}"
                attemptID1=${ATTEMPTIDS[0]}
                attemptID2=${ATTEMPTIDS[1]}
                echo "Attempt ID for map of $jobID : $attemptID1, $attemptID2"
                NMHost1=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID1}: $NMHost1"
                if [ -n "$NMHost1" ]; then
                    break
                fi
                NMHost2=`getNMfromAttemptID $AM_hostname $ID`
                echo "NMHost running map ${attemptID2}: $NMHost2"
                if [ -n "$NMHost2" ]; then
                    break
                fi
            fi
        fi
        sleep 10
    done

    containerID1=`ssh $NMHost1 ps -efj | grep -v grep | grep $attemptID1 | sed 's/container/@/' | cut -d '@' -f2 | awk '{print $1}'`
    subshell_before=`ssh $NMHost1 ps -efj | grep -v grep | grep highRAM | grep "container${containerID1}"`
    num_subshell_before=`echo "$subshell_before" | wc -l`
    echo "There are $num_subshell_before child processes running on $NMHost1"
    echo "$subshell_before"
    # running ps and got pid; second collumn is the PID
    child_pids=`ssh "$NMHost1" ps -efj | grep -v grep | grep highRAM | grep "container${containerID1}" | awk '{print $2}'`
    echo "Processes running highRAM (process ids):"
    echo "$child_pids"

    # Kill the job
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $jobID
    sleep 20

    ssh $NMHost1 "ps -efj | grep -v grep | grep highRAM | awk '{print $2}'" > ${ARTIFACTS}/childfile
    echo "Child processes `cat ${ARTIFACTS}/childfile` "
    if [ -s ${ARTIFACTS}/childfile ] ; then
        setFailCase "Child process highRAM is still running on $taskTracker after job is killed"
    fi
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R out1

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function runStreaming {
    TESTCASE_DESC="cacheFile"
    NAMENODE=$(getNN1)
    echo "Name Node: $NAMENODE"
    USER_ID=${HADOOP3_USER}
    getKerberosTicketForUser ${USER_ID}
    setKerberosTicketForUser ${USER_ID}
    echo "Creating Hdfs dir streaming-${TESTCASE_DESC}"
    createHdfsDir streaming-${TESTCASE_DESC}/ >> $ARTIFACTS_FILE 2>&1
    echo "Files system streaming-${TESTCASE_DESC}"
    $HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR} dfs -ls /user/${USER_ID}/streaming-${TESTCASE_DESC}/
    echo "Copy all data files to Hdfs"
    putLocalToHdfs ${WORKSPACE}/data/streaming/input.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/input.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${WORKSPACE}/data/streaming/cache.txt /user/$USER_ID/streaming-${TESTCASE_DESC}/cache.txt >> $ARTIFACTS_FILE 2>&1
    echo "Checking input file existed in HDFS"
    ${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} dfs -ls /user/$USER_ID/streaming-${TESTCASE_DESC}/
    # clean up output file
    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/OutDir"

    echo "Submit streaming job ..."
    echo "$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.queuename=default -input streaming-${TESTCASE_DESC}/input.txt -mapper '/usr/bin/xargs cat' -reducer cat -output streaming-${TESTCASE_DESC}/OutDir -cacheFile hdfs://${NAMENODE}/user/${USER_ID}/streaming-$TESTCASE_DESC/cache.txt#testlink 2>&1 &"
    $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.queuename=default -input "streaming-${TESTCASE_DESC}/input.txt" -mapper '/usr/bin/xargs cat' -reducer 'cat' -output "streaming-${TESTCASE_DESC}/OutDir" -files "hdfs://${NAMENODE}/user/${USER_ID}/streaming-$TESTCASE_DESC/cache.txt#testlink" 2>&1 &
    processID=$!
    sleep 10

    wait $processID
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat streaming-${TESTCASE_DESC}/OutDir/* > ${ARTIFACTS}/actualOutput
    echo "Comparing outputs"
    echo "Expected output: "
    cat ${WORKSPACE}/data/streaming/expectedOutput
    echo "Actual output:"
    cat ${ARTIFACTS}/actualOutput
    REASONS=`diff ${WORKSPACE}/data/streaming/expectedOutput "${ARTIFACTS}/actualOutput"`
    if [ -n "${REASONS}" ];then
        setFailCase "Output not matched"
    fi

    $HADOOP_HDFS_CMD dfs -rm -R "streaming-${TESTCASE_DESC}/"
    cp ${ARTIFACTS}/actualOutput .
    rm ${ARTIFACTS}/actualOutput

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


##################################################
# Main function
##################################################
setupNewLinuxTaskControllerConfFile
taskController_checkOwnerOfJobAndTasks_mrJob
taskController_checkOwnerOfJobAndTasks_cacheArchiveStreamingJob
taskController_checkOwnerOfJobAndTasks_cacheFileStreamingJob
taskController_checkOwnerOfJobAndTasks_fileStreamingJob
taskController_checkOwnerOfJobAndTasks_libjarStreamingJob
### taskController_checkOwnerOfJobAndTasks_PipeJob
#taskController_invalidTaskControllerConfigFile
taskController_killTaskAndCheckChildProcess
taskController_suspendedTaskAndCheckChildProcess

# 5976065 - Fix sporadic failure for YARN test taskController_failTaskAndCheckChildProcess 
# taskController_failTaskAndCheckChildProcess

taskController_killJobAndCheckChildProcess

# 5976065 - Fix sporadic failure for YARN test taskController_failTaskAndCheckChildProcess 
# taskController_ChildProcessExceedMemoryLimit

test_runAwayProcesses_KillJobAndChildProcessExceedMemoryLimit
taskController_invalidJobOwner
restartCluster_newConfig

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
