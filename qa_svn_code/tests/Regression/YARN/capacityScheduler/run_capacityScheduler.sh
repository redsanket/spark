#!/bin/bash
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/yarn_library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

# Setting Owner of this TestSuite
OWNER="bui"

export USER_ID=`whoami`
export JOBIDS=""
export NODES_ON_CLUSTER=""

# Setup new location on RM
NEW_CONFIG_LOCATION="/homes/${USER_ID}/capacityScheduler_conf/"

function restartCluster_newConfig {
    if [ -n "$1" ] ; then
        new_conf="$1"
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

##############################################
# This function is used to query number of (MAP/REDUCE) tasks in a state (RUNNING/COMPLETED/PENDING)
# $1: jobID, $2: MAP/REDUCE, $3: running/completed/pending
##############################################
function getNumTasksForJobId {
    ${HADOOP_MAPRED_CMD} job -list-attempt-ids $1 $2 $3 | grep attempt_ | wc -l
}

function getTTHostForJobId {
    ${HADOOP_MAPRED_CMD} job -list | grep job_ | awk '{print $7}' | cut -d ':' -f2 | cut -d '/' -f3
    return $?
}

function killAllRunningJobs {
    JOBIDS=( `${HADOOP_MAPRED_CMD} job -libjars $YARN_CLIENT_JAR -list | grep job_ | awk '{print $1}'` )
    echo "Killing running jobs: ${JOBIDS[*]}"
    num=${#JOBIDS[*]}
    i=0
    while [ "$i" -lt "$num" ]; do
        ${HADOOP_MAPRED_CMD} --config ${HADOOP_CONF_DIR} job -libjars $YARN_CLIENT_JAR -kill "${JOBIDS[$i]}"
        sleep 5
        (( i = $i + 1 ))
    done
}

##############################################################
# Function to get total Queue Capacity of a cluster
# It's calculated based on memory allocated for number of NM (each NM has 10 GB of memory)
##############################################################
function getTotalClusterCapacity {
    set -x
    getNodesOnCluster
    MBPERNODE=$(readAFieldInFileFromRemoteHost $JOBTRACKER "${NEW_CONFIG_LOCATION}/yarn-site.xml" "yarn.nodemanager.resource.memory-mb")
    TOTAL_CLUSTER_CAPACITY=$(( $(( ${NODES_ON_CLUSTER} * ${MBPERNODE} )) / 1024 ))
    set +x
}

function getQueueCapacity { 
    set -x
    getTotalClusterCapacity
    getJobTracker
    queueSetting=$(readAFieldInFileFromRemoteHost $JOBTRACKER "${NEW_CONFIG_LOCATION}/capacity-scheduler.xml" "yarn.scheduler.capacity.root.$1.capacity")
    QueueCapacity=$(( $(( ${queueSetting} * ${TOTAL_CLUSTER_CAPACITY} )) / 100 ))
    set +x
}

function setUserLimit {
    getJobTracker
    scpFileFromRemoteHost_ModifyAValueAndSendBack $JOBTRACKER $3 capacity-scheduler.xml "yarn.scheduler.capacity.root.$1.minimum-user-limit-percent" $2
}

function getUserLimit {
    userSetting=$(readAFieldInFileFromRemoteHost $JOBTRACKER "${NEW_CONFIG_LOCATION}/capacity-scheduler.xml" "yarn.scheduler.capacity.root.$1.minimum-user-limit-percent")
    getQueueCapacity $1
    UserLimit=$(( $(( ${QueueCapacity} * ${userSetting} )) / 100 ))
    echo "$UserLimit"
}

################################################################
# Function to add a property (name and value) to a config file
# Params: $1: xml config file name; $2: name of the property; $3: value of the property
################################################################
function addPropertyToConfigXML {
    #read the file name with full path
    local file="$1"
    local propName="$2"
    local propValue="$3"

    #create the property text, make sure the / are escaped
    propText="<property>\n<name>$propName<\/name>\n<value>$propValue<\/value>\n<\/property>\n"

    #check to make sure if the property exists or not
    xmllint $1 | grep $2

    #get the status of the previous command
    status="$?"

    #if status=0, that means property exists
    if [ "$status" -eq 0 ];then
        #then call the available method to update the property
        modifyValueOfAField $file $propName $propValue
    else
        #add the property to the file
        endText="<\/configuration>"
        #add the text using sed at the end of the file
        sed -i "s|$endText|$propText$endText|" $file
    fi
}

############################################
# Function to setup queue and user limit 
# Param: $1: capacity-scheduler.xml data file to use
############################################
function setupNewCapacitySchedulerConfFile  {
    getJobTracker
    echo "Job tracker: $JOBTRACKER on $CLUSTER cluster"
    if [ -z `ssh ${USER_ID}@$JOBTRACKER ls -d "$NEW_CONFIG_LOCATION"` ] ; then
        ssh ${USER_ID}@$JOBTRACKER mkdir "$NEW_CONFIG_LOCATION"
    fi

    # Copy all the conf files to new location in RM host
    ssh ${USER_ID}@$JOBTRACKER cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER chmod -R 755 "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER ls -l "$NEW_CONFIG_LOCATION"
    scp $1 ${USER_ID}@$JOBTRACKER:$NEW_CONFIG_LOCATION/capacity-scheduler.xml
    ssh ${USER_ID}@$JOBTRACKER "cat $NEW_CONFIG_LOCATION/capacity-scheduler.xml"

    echo "Restart RM and NM"
    restartCluster_newConfig "$NEW_CONFIG_LOCATION" 
    echo "New HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "${HADOOP_COMMON_CMD} --config $NEW_CONFIG_LOCATION classpath | cut -d ':' -f1"
}

############################################
# Function to setup queue and user limit
# Param: $1: capacity-scheduler.xml data file to use
############################################
function setupMetricsConfig {
    getJobTracker
    echo "Job tracker: $JOBTRACKER on $CLUSTER cluster"
    if [ -z `ssh ${USER_ID}@$JOBTRACKER ls -d "$NEW_CONFIG_LOCATION"` ] ; then
        ssh ${USER_ID}@$JOBTRACKER mkdir "$NEW_CONFIG_LOCATION"
    fi

    # Copy all the conf files to new location in RM host
    ssh ${USER_ID}@$JOBTRACKER cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER chmod -R 755 "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@$JOBTRACKER ls -l "$NEW_CONFIG_LOCATION"
    setMetricsTestData
    echo "Restart RM and NM"
    restartCluster_newConfig "$NEW_CONFIG_LOCATION"
    echo "New HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "${HADOOP_COMMON_CMD} --config $NEW_CONFIG_LOCATION classpath | cut -d ':' -f1"
    echo "ResourceManager processID: `getRMProcessId`"
}

function setMetricsTestData {
    # Modify the jmx metrics refresh intreval
    ssh ${USER_ID}@$JOBTRACKER "sed -i 's/*.period=10/*.period=1/g' ${NEW_CONFIG_LOCATION}/hadoop-metrics2.properties"
    echo "New $NEW_CONFIG_LOCATION/hadoop-metrics2.properties"
    ssh ${USER_ID}@$JOBTRACKER "cat $NEW_CONFIG_LOCATION/hadoop-metrics2.properties | grep period="
    # Modify mapreduce.job.reduce.slowstart.completedmaps value to 1 in mapred-site.xml file 
    scpFileFromRemoteHost_ModifyAValueAndSendBack $JOBTRACKER ${NEW_CONFIG_LOCATION}/mapred-site.xml "mapreduce.job.reduce.slowstart.completedmaps" "1.0"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $JOBTRACKER ${NEW_CONFIG_LOCATION}/mapred-site.xml "yarn.mapreduce.job.reduce.rampup.limit" "0"
    ssh ${USER_ID}@$JOBTRACKER "cat $NEW_CONFIG_LOCATION/mapred-site.xml | grep -A2 mapreduce.job.reduce.slowstart.completedmaps"
    ssh ${USER_ID}@$JOBTRACKER "cat $NEW_CONFIG_LOCATION/mapred-site.xml | grep -A2 yarn.mapreduce.job.reduce.rampup.limit"
    echo " Moving both the jmxterm and Fetchmetrics info to hadoop homes dir /homes/hadoopqa/"
    cp /$WORKSPACE/data/metrics/jmxterm-1.0-SNAPSHOT-uber.jar /homes/hadoopqa/
    cp /$WORKSPACE/data/metrics/FetchMetricsInfo.sh /homes/hadoopqa/
}

function getRMProcessId {
    getJobTracker
    ssh -i /home/y/var/pubkeys/mapredqa/.ssh/id_dsa mapredqa@$JOBTRACKER exec jps | grep ResourceManager | awk '{print $1}'
    return $?
}

#################################################################
# This function returns the metrics info as requested
# Params: $1: the JT, $2: PID of JT, $3: the value of the key interested in
#################################################################
function getJTMetricsInfo {
    local myHost=$1
    local myPID=$2
    local myParam=$3
    local myQueue=$4
    ssh -i /home/y/var/pubkeys/mapredqa/.ssh/id_dsa mapredqa@$myHost "sh /homes/hadoopqa/FetchMetricsInfo.sh $myPID $myParam $myQueue" > $ARTIFACTS_DIR/jmxmetrix.log
    cat $ARTIFACTS_DIR/jmxmetrix.log | cut -d '=' -f2 | cut -d ';' -f1|awk '{print $1}'
}

function getNumSlots {
    local allocatedGB=$1
    local allocatedContainers=$2
    #echo "AllocatedGB: $allocatedGB"
    #echo "AllocatedContainers: $allocatedContainers"
    # Number of slots = Allocated GB - 2 (for AM)/AllocatedContainers - 1 (for AM)
    AGB=$(( $allocatedGB - 2 ))
    ACT=$(( $allocatedContainers - 1 )) 
    #echo "AGB: $AGB"
    #echo "ACT: $ACT"
    if [ "$AGB" -gt 0 -a "$ACT" -gt 0 ]; then
        numSlot=$(( $AGB/$ACT ))
    else
        numSlot=0
    fi
    echo $numSlot
}

###########################################
# Function to check user limit (100%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit100_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a normal job which have number of map and reduce tasks equal 2 times queue capacity"
    echo "3. Verify the number of tasks run is not exceeded queue limit"
    echo "4. Verify the normal job can run sucessfully"
    echo "*************************************************************"

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "User limit on default queue: $default_ul"
    echo "Submit a normal job:"
    SubmitASleepJob $(( $default_qc * 2 )) $(( $default_qc * 2 )) 20000 20000 default 2>&1 &
    processID=$!
    sleep 15

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID"

    for i in 0 1 2 3 4 5 6 7 8 ; do
        listJobs
        if [ -n "`isJobRunning $jobID`" ] ; then
            numMapTask=$(getNumTasksForJobId $jobID MAP running)
            numReduceTask=$(getNumTasksForJobId $jobID REDUCE running)
            numTask=$(( $numMapTask+$numReduceTask ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask"
        echo "Number of reduce tasks for jobID1: $numReduceTask"
        echo "Number of tasks for jobID1: $numTask"
        echo "###################"

        if [ "$numTask" -eq 0 ] ; then
            if [ "$numTask" -gt "$default_qc" ] ; then
                setFailCase "Number of tasks is greater than queue capacity"
            fi
        fi      
        sleep 15
    done

    wait $processID
    sleep 10
    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Normal job $jobID failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################################
# Function to check user limit (100%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit100_2normalJobsWithMapReduceTasksEqualNumNodes {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a first normal job which have number of map and reduce tasks equal half queue capacity"
    echo "3. Submit a second normal job which have number of map and reduce tasks equal half queue capacity"
    echo "4. Verify the 2 normal jobs can run in parallel with number of tasks not exceeded queue capacity" 
    echo "5. Verify the 2 jobs ran sucessfully"
    echo "*************************************************************"

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Submit a first normal job:"
    SubmitASleepJob $(( ${default_qc}/2 )) $(( ${default_qc}/2 )) 20000 20000 default 2>&1 &
    processID1=$!

    echo "Submit a second normal job:"
    SubmitASleepJob $(( ${default_qc}/2 )) $(( ${default_qc}/2 )) 20000 20000 default 2>&1 &
    processID2=$!
    sleep 10

    getJobIds 2
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    echo "JobIDs: $jobID1 and $jobID2"

    for i in 0 1 2 3 ; do
        listJobs
        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "Number of tasks for jobID1: $numTask1"
        echo "###################"
        if [ "${numTask1}" -gt "${default_qc}" ] ; then
            setFailCase "Number of tasks for jobID1 are greater than queue capacity"
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "Number of tasks for jobID2: $numTask2"
        echo "###################"
        if [ "${numTask2}" -gt "${default_qc}" ] ; then
            setFailCase "Number of tasks for jobID2 are greater than queue capacity"
        fi
        sleep 15
    done

    wait $processID1
    wait $processID2
    sleep 10
    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################################
# Function to check user limit (100%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit100_3normalJobsQueueUp {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a first normal job which have 3 times queue capacity map tasks and queue capacity reduce tasks"
    echo "3. Submit a second normal job which have 2 times queue capacity map tasks and queue capacity reduce tasks"
    echo "4. Submit a second normal job which have queue capacity map tasks and half queue capacity reduce tasks"
    echo "5. Verify the first normal job takes up all queue capacity to run and the other 2 queue up"
    echo "6. Verify the second job runs when first job map tasks completed"
    echo "7. Verify the third job runs when second job tasks completed"
    echo "8. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Submit first normal job:"
    SubmitASleepJob $(( ${default_qc} * 3 )) ${default_qc} 20000 20000 default 2>&1 &
    processID1=$!
    sleep 2

    echo "Submit second normal job:"
    SubmitASleepJob $(( ${default_qc} * 3 )) ${default_qc} 20000 20000 default 2>&1 &
    processID2=$!
    sleep 2

    echo "Submit third normal job:"
    SubmitASleepJob ${default_qc} $(( ${default_qc}/2 )) 20000 20000 default 2>&1 &
    processID3=$!
    sleep 15

    getJobIds 3
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 are running"
    sleep 5

    for i in 0 1 2 3 4 5 6 7 8 9 10; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numTask1=0
        numTask2=0
        numTask3=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
            numTask3=$(( $numMapTask3+$numReduceTask3 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"

        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "###### Number of tasks for jobID2: $numTask2"

        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        echo "###### Number of tasks for jobID3: $numTask3"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}"
        echo "###################"
        if [ "$numMapTask1" -gt ${default_qc} -a "$numMapTask2" -gt 0 ] ; then
            setFailCase "Map tasks from second job is running at the same time as map tasks of first job"
        fi
        if [ "$numMapTask2" -gt ${default_qc} -a "$numMapTask3" -gt 0 ] ; then
            setFailCase "Map tasks from third job is running at the same time as map tasks of second job"
        fi
        echo "##### Sleeping ..."
        sleep 20
    done

    wait $processID1
    wait $processID2
    wait $processID3

    sleep 10
    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        ${HADOOP_MAPRED_CMD} job -list all | grep $jobID1
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    echo "Return code: $return"
    if [ "$return" != "SUCCEEDED" ] ; then
        ${HADOOP_MAPRED_CMD} job -list all | grep $jobID2
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    echo "Return code: $return"
    if [ "$return" != "SUCCEEDED" ] ; then
        ${HADOOP_MAPRED_CMD} job -list all | grep $jobID3
        setFailCase "Third normal job $jobID3 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


###########################################################
# Function to check user limit (25%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit25_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a normal job which have number of map and reduce tasks equal 2 times queue capacity"
    echo "3. Verify the number of tasks run is not exceeded queue limit"
    echo "4. Verify the normal job can run sucessfully"
    echo "*************************************************************"

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    echo "Submit a normal job: "
    SubmitASleepJob "$(( ${default_qc} * 2 ))" "$(( ${default_qc} * 2 ))" 40000 40000 default 2>&1 &
    processID=$!
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID"

    listJobs
    for i in 0 1 2 3 4 5 ; do
        numMapTask=0
        numReduceTask=0
        numTask=0
        if [ -n "`isJobRunning $jobID`" ] ; then
            numMapTask=$(getNumTasksForJobId $jobID MAP running)
            numReduceTask=$(getNumTasksForJobId $jobID REDUCE running)
            numTask=$(( $numMapTask+$numReduceTask ))
        else
            break
        fi
        echo "###################"
        echo "Number of map tasks for jobID: $numMapTask"
        echo "Number of reduce tasks for jobID: $numReduceTask"
        echo "Number of tasks for jobID: $numTask"
        echo "###################"

                # got to compare with the ceiling of default_qc; so +1
        if [ $numTask -gt "$(( ${default_qc} + 1 ))" ] ; then
            setFailCase "Number of map tasks are greater than queue capacity"
        fi

        sleep 15
    done

    wait $processID
    sleep 10
    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Normal job $jobID failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################################
# Function to check user limit (25%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit25_2normalJobsWithMapReduceTasksEqualNumNodes {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity, queue capacity, and user limit"
    echo "2. Submit a first normal job as hadoop3 user which have number of map and reduce tasks equal half queue capacity"
    echo "3. Submit a second normal job as hadoopqa user which have number of map and reduce tasks equal half queue capacity"
    echo "4. Verify the 2 normal jobs can run in parallel each takes up 50% of queue capacity"
    echo "5. Verify the 2 jobs ran sucessfully"
    echo "*************************************************************"

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a normal job as hadoop3 user ..."
    SubmitASleepJob $(( ${default_qc}/2 )) $(( ${default_qc}/2 )) 20000 20000 default 2>&1 &
    processID1=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a second normal job as hadoopqa user ..."
    SubmitASleepJob $(( ${default_qc}/2 )) $(( ${default_qc}/2 )) 20000 20000 default 2>&1 &
    processID2=$!
    sleep 20

    ${HADOOP_MAPRED_CMD} job -list | sort
    getJobIds 2
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    echo "JobID: $jobID1 and $jobID2"
    ${HADOOP_MAPRED_CMD} job -list
    for i in 0 1 2 3; do
        numMapTask1=0
        numMapTask2=0
        numReduceTask1=0
        numReduceTask2=0
        numTask1=0
        numTask2=0
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        else
            break
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        else
            break
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "###### Number of tasks for jobID2: $numTask2"
        echo "###################"

        if [ $numTask1 -gt 0 -a $numTask2 -eq 0 ] ; then
            setFailCase "JobID2 $jobID2 is not running in parallel with jobID1 $jobID1"
        fi
        sleep 15
    done

    wait $processID1
    wait $processID2
    sleep 10
    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check user limit (25%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit25_4normalJobsQueueUp {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit first normal job which has 4 times queue capacity for map and reduce tasks"
    echo "2. Submit second normal job which has 3 times queue capacity for map and reduce tasks"
    echo "2. Submit third normal job which has 2 times queue capacity for map and reduce tasks"
    echo "2. Submit fourth normal job which has equal queue capacity for map and reduce tasks"
    echo "7. Verify all the jobs run, each take 25% of queue capacity"
    echo "8. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit first job: "
    SubmitASleepJob "$(( ${default_qc} * 4 ))" "$(( ${default_qc} * 4 ))" 20000 20000 default 2>&1 &
    processID1=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit second job: "
    SubmitASleepJob "$(( ${default_qc} * 3 ))" "$(( ${default_qc} * 3 ))" 20000 20000 default 2>&1 &
    processID2=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit third job: "
    SubmitASleepJob "$(( ${default_qc} * 2 ))" "$(( ${default_qc} * 2 ))" 20000 20000 default 2>&1 &
    processID3=$!
    sleep 2

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit third job: "
    SubmitASleepJob ${default_qc} ${default_qc} 20000 20000 default 2>&1 &
    processID4=$!
    sleep 20

    listJobs
    getJobIds 4
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4 are running"

    # for i in 0 1 2 3 4 ; do
    for i in 1 2 3 4 5; do
        listJobs
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        echo "##### Loop $i"
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
            numTask3=$(( $numMapTask3+$numReduceTask3 ))
        fi

        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
            numTask4=$(( $numMapTask4+$numReduceTask4 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "Number of tasks for jobID1: $numTask1"
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "Number of tasks for jobID2: $numTask2"
        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        echo "Number of tasks for jobID3: $numTask3"
        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        echo "Number of tasks for jobID4: $numTask4"
        echo "###################"

        if [ ${numTask1} -gt 0 -a ${numTask2} -eq 0 ] ; then
            local msg="Loop $i: jobID2 is not running with jobID1."
            echo $msg
            setFailCase $msg
        fi
        if [ ${numTask1} -gt 0 -a $numTask3 -eq 0 ] ; then
            local msg="Loop $i: jobID3 is not running with jobID1."
            echo $msg
            setFailCase $msg
        fi
        if [ ${numTask1} -gt 0 -a $numTask4 -eq 0 ] ; then
            local msg="Loop $i: jobID4 is not running with jobID1."
            echo $msg
            setFailCase $msg
        fi
        echo "##### Sleeping ..."
        sleep 15
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    sleep 15
    return=$(verifySucceededJob $jobID1)
    echo "Return code: $return"   
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    echo "Return code: $return"
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    echo "Return code: $return"
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    echo "Return code: $return"
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check user limit (25%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit25_6normalJobsQueueUp {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a first normal job which have 3 times queue capacity map tasks and queue capacity reduce tasks"
    echo "3. Submit a second normal job which have 2 times queue capacity map tasks and queue capacity reduce tasks"
    echo "4. Submit a second normal job which have queue capacity map tasks and half queue capacity reduce tasks"
    echo "5. Verify the first normal job takes up all queue capacity to run and the other 2 queue up"
    echo "6. Verify the second job runs when first job map tasks completed"
    echo "7. Verify the third job runs when second job tasks completed"
    echo "8. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit first job: "
    SubmitASleepJob "$(( ${default_qc} * 4 ))" "$(( ${default_qc} * 4 ))" 30000 30000 default 2>&1 &
    processID1=$!

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit second job: "
    SubmitASleepJob "$(( ${default_qc} * 3 ))" "$(( ${default_qc} * 3 ))" 30000 30000 default 2>&1 &
    processID2=$!

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit third job: "
    SubmitASleepJob "$(( ${default_qc} * 2 ))" "$(( ${default_qc} * 2 ))" 30000 30000 default 2>&1 &
    processID3=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit fourth job: "
    SubmitASleepJob ${default_qc} ${default_qc} 30000 30000 default 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit fifth job: "
    SubmitASleepJob ${default_qc} ${default_qc} 30000 30000 default 2>&1 &
    processID5=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit sixth job: "
    SubmitASleepJob ${default_qc} ${default_qc} 30000 30000 default 2>&1 &
    processID6=$!
    sleep 20

    listJobs
    getJobIds 6
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6 are running"
    sleep 5
    logFile="$ARTIFACTS/${TESTCASE_DESC}"

    for i in 0 1 2 3 4 5 6 7 8 9 10; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numMapTask5=0
        numMapTask6=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numReduceTask5=0
        numReduceTask6=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        numTask5=0
        numTask6=0
        echo "######## Loop $i ########" >> $logFile
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
        fi

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID5`" ] ; then
            numMapTask5=$(getNumTasksForJobId $jobID5 MAP running)
            numReduceTask5=$(getNumTasksForJobId $jobID5 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID6`" ] ; then
            numMapTask6=$(getNumTasksForJobId $jobID6 MAP running)
            numReduceTask6=$(getNumTasksForJobId $jobID6 REDUCE running)
        fi
        echo "###################" >> $logFile
        echo "Number of map tasks for jobID1: $numMapTask1" >> $logFile
        echo "Number of reduce tasks for jobID1: $numReduceTask1" >> $logFile
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1" >> $logFile
        echo "Number of map tasks for jobID2: $numMapTask2" >> $logFile
        echo "Number of reduce tasks for jobID2: $numReduceTask2" >> $logFile
        numTask2=$(( $numMapTask2+$numReduceTask2 ))
        echo "###### Number of tasks for jobID2: $numTask2" >> $logFile
        echo "Number of map tasks for jobID3: $numMapTask3" >> $logFile
        echo "Number of reduce tasks for jobID3: $numReduceTask3" >> $logFile
        numTask3=$(( $numMapTask3+$numReduceTask3 ))
        echo "###### Number of tasks for jobID3: $numTask3" >> $logFile
        echo "Number of map tasks for jobID4: $numMapTask4" >> $logFile
        echo "Number of reduce tasks for jobID4: $numReduceTask4" >> $logFile
        numTask4=$(( $numMapTask4+$numReduceTask4 ))
        echo "###### Number of tasks for jobID4: $numTask4" >> $logFile
        echo "Number of map tasks for jobID5: $numMapTask5" >> $logFile
        echo "Number of reduce tasks for jobID5: $numReduceTask5" >> $logFile
        numTask5=$(( $numMapTask5+$numReduceTask5 ))
        echo "###### Number of tasks for jobID5: $numTask5" >> $logFile
        echo "Number of map tasks for jobID6: $numMapTask6" >> $logFile
        echo "Number of reduce tasks for jobID6: $numReduceTask6" >> $logFile
        numTask6=$(( $numMapTask6+$numReduceTask6 ))
        echo "###### Number of tasks for jobID6: $numTask6" >> $logFile
        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3, $numMapTask4, $numMapTask5, $numMapTask6" >> $logFile
        echo "***** Number of reduce tasks for all Jobs: $numReduceTask1, $numReduceTask2, $numReduceTask3, $numReduceTask4, $numReduceTask5, $numReduceTask6" >> $logFile
        echo "***** Number of tasks for all Jobs: $numTask1, $numTask2, $numTask3, $numTask4, $numTask5, $numTask6" >> $logFile
        echo "###################" >> $logFile
        runningJob=0
        if [ "$numTask1" -gt $default_ul ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask2" -gt $default_ul ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask3" -gt $default_ul ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask4" -gt $default_ul ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask5" -gt $default_ul ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask6" -gt $default_ul ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$runningJob" -gt 4 ] ; then
            setFailCase "Number of concurrent jobs exceeded user limit"
        fi      
    done
    cat $logFile
    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    sleep 20

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID5 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check user limit (25%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit25_3normalJobsQueueUp {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a first normal job which have 8 times queue capacity map tasks and queue capacity reduce tasks"
    echo "3. After (2 times number of Queue capacity) map tasks of first job completed, submit a second normal job which have 6 times queue capacity map tasks and queue capacity reduce tasks"
    echo "4. Verify the first and second normal jobs take up equal queue capacity to run"
    echo "5. After all map tasks of first job completed, submit a third normal job which have 4 times queue capacity map tasks and half queue capacity reduce tasks"
    echo "6. Verify all the jobs runs with equal share of queue capacity"
    echo "7. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit a first normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 8 ))" "$(( ${default_qc} * 8 ))" 20000 20000 default 2>&1 &
    processID1=$!
    sleep 20

    listJobs
    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    echo "Jobs: $jobID1 is running"

    for i in 0 1 2 3 ; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        if [ "$numTask1" -gt "${default_qc}" ] ; then
            setFailCase "User $USER_ID running job with number of tasks $numTask1 greater than Queue capacity $default_qc"
        fi
        sleep 10
    done
    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit a second normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 6 ))" "$(( ${default_qc} * 6 ))" 20000 20000 default 2>&1 &
    processID2=$!
    sleep 15

    getJobIds 2
    echo ${JOBIDS[*]}
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    for i in 0 1 2 3; do
        numMapTask1=0
        numMapTask2=0
        numReduceTask1=0
        numReduceTask2=0
        numTask1=0
        numTask2=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            echo "###################"
            echo "Number of map tasks for jobID1: $numMapTask1"
            echo "Number of reduce tasks for jobID1: $numReduceTask1"
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
            echo "###### Number of tasks for jobID1: $numTask1"
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            echo "Number of map tasks for jobID2: $numMapTask2"
            echo "Number of reduce tasks for jobID2: $numReduceTask2"
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
            echo "###### Number of tasks for jobID2: $numTask2"
        fi
        if [ "$numTask1" -gt 0 -a "$numTask2" -eq 0 ]; then
            setFailCase "Job $jobID2 is not running in parallel with JobID1 $jobID1"
        fi
        sleep 10
    done

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a third normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 4 ))" "$(( ${default_qc} * 4 ))" 20000 20000 default 2>&1 &
    processID3=$!
    sleep 15

    getJobIds 3
    echo ${JOBIDS[*]}
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    for i in 1 1 2 3 4 5; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numTask1=0
        numTask2=0
        numTask3=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
            numTask3=$(( $numMapTask3+$numReduceTask3 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "###### Number of tasks for jobID2: $numTask2"
        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        echo "###### Number of tasks for jobID3: $numTask3"
        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}"
        echo "***** Number of tasks for all Jobs: $numTask1, $numTask2, $numTask3"
        echo "###################"
        echo "##### Sleeping ..."
        if [ "$numTask1" -gt 0 -a "$numTask3" -eq 0 ]; then
            setFailCase "Job $jobID3 is not running in parallel with JobID1 $jobID1"
        fi
        sleep 20
    done

    wait $processID1
    wait $processID2
    wait $processID3
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check user limit (25%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit25_7normalJobsAddingAndQueueUp {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job which have 6 times queue capacity map tasks and queue capacity reduce tasks"
    echo "3. User2 submits a J2 job which have 5 times queue capacity map tasks and queue capacity reduce tasks"
    echo "4. User3 submits a J3 job which have 4 times queue capacity map tasks and half queue capacity reduce tasks"
    echo "5. User4 submits a J4 job which have 3 times queue capacity map tasks and queue capacity reduce tasks"
    echo "6. Verify all 4 jobs running with equal number of tasks"
    echo "7. User2 submits a J5 job which have 2 times queue capacity map tasks and queue capacity reduce tasks"
    echo "8. User5 submits a J6 job which have 2 times queue capacity map tasks and half queue capacity reduce tasks"
    echo "9. User6 submits a J7 job which have 2 times queue capacity map tasks and half queue capacity reduce tasks"
    echo "10. Verify the last 3 jobs queued up"
    echo "11. Verify when all maps of J4 finishes then maps of J6 starts. And when maps of J3 finishes J7 maps starts. J5 starts after last set of maps of J2 finishes."
    echo "12. Verify when maps J5 and J1 remaining goes first to j7 then J5 as J7 started first. And J7 finishes all map slots are given to J5. Similarly when reducers of J4 finish, reducers of J6 starts. When reducer of J3 finishes (J3 finishes), reducers of J7 starts."
    echo "13. Verify when reducers of j6 finish (J6 finish). Now there are 3 users User1 (J1), User2 (J2, J5), User6 (J7). Limit re-distribution occurs and as J2 is running J5 will not considered until last set reducers from J2 starts finishing. So J1=J2=J7=capacity/3. And as last set reducers starts finishing reducers from J5 starts keeping limit = (33%). When J7 finishes user limit eventually again re-distributed to 50%. J1=J5=50%.When J1 finishes J5 get 100% slots"
    echo "14. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit first normal job: "
    SubmitASleepJob "$(( ${default_qc} * 5 ))" "$(( ${default_qc} * 5 ))" 20000 10000 default 2>&1 &
    processID1=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit a second normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 4 ))" "$(( ${default_qc} * 4 ))" 20000 10000 default 2>&1 &
    processID2=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a third normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 3 ))" "$(( ${default_qc} * 3 ))" 20000 10000 default 2>&1 &
    processID3=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit a fourth normal job ..."
    SubmitASleepJob $(( ${default_qc} * 2 )) $(( ${default_qc} * 2 )) 20000 10000 default 2>&1 &
    processID4=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit a fifth normal job ..."
    SubmitASleepJob $(( ${default_qc} * 2 )) $(( ${default_qc} * 2 )) 20000 10000 default 2>&1 &
    processID5=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit a sixth normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 2 ))" "$(( ${default_qc} * 2 ))" 20000 10000 default 2>&1 &
    processID6=$!
    sleep 2

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a seventh normal job ..."
    SubmitASleepJob $(( ${default_qc} * 2 )) $(( ${default_qc} * 2 )) 20000 10000 default 2>&1 &
    processID7=$!
    sleep 20

    listJobs
    getJobIds 7
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    jobID7=${JOBIDS[6]}
    if [ -z "$jobID7" ] ; then
        setFailCase "Cannot get Job ID7 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4 $jobID5 $jobID6 $jobID7 are running"
    sleep 5

    for i in 0 1 2 3 4 5 6 7 8 9; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numMapTask5=0
        numMapTask6=0
        numMapTask7=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numReduceTask5=0
        numReduceTask6=0
        numReduceTask7=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        numTask5=0
        numTask6=0
        numTask7=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi
        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi
        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
            numTask3=$(( $numMapTask3+$numReduceTask3 ))
        fi
        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
            numTask4=$(( $numMapTask4+$numReduceTask4 ))
        fi
        if [ -n "`isJobRunning $jobID5`" ] ; then
            numMapTask5=$(getNumTasksForJobId $jobID5 MAP running)
            numReduceTask5=$(getNumTasksForJobId $jobID5 REDUCE running)
            numTask5=$(( $numMapTask5+$numReduceTask5 ))
        fi
        if [ -n "`isJobRunning $jobID6`" ] ; then
            numMapTask6=$(getNumTasksForJobId $jobID6 MAP running)
            numReduceTask6=$(getNumTasksForJobId $jobID6 REDUCE running)
            numTask6=$(( $numMapTask6+$numReduceTask6 ))
        fi
        if [ -n "`isJobRunning $jobID7`" ] ; then
            numMapTask7=$(getNumTasksForJobId $jobID7 MAP running)
            numReduceTask7=$(getNumTasksForJobId $jobID7 REDUCE running)
            numTask7=$(( $numMapTask7+$numReduceTask7 ))
        fi
                # Verify the first 4 jobs running in parallel
        if [ $numTask1 -gt 0 -a $numTask2 -eq 0 ] ; then
            if [ `verifySucceededJob $jobID2` != "SUCCEEDED" ] ; then
                setFailCase "Job2 $jobID2 is not running in parallel with job1 $jobID1"
            fi
        fi
        if [ $numTask1 -gt 0 -a $numTask3 -eq 0 ] ; then
            if [ `verifySucceededJob $jobID3` != "SUCCEEDED" ] ; then
                setFailCase "Job3 $jobID3 is not running in parallel with job1 $jobID1"
            fi
        fi
        if [ $numTask1 -gt 0 -a $numTask4 -eq 0 ] ; then
            if [ `verifySucceededJob $jobID4` != "SUCCEEDED" ] ; then
                setFailCase "Job4 $jobID4 is not running in parallel with job1 $jobID1"
            fi
        fi
                # Verify job5 running submitted hadoop2 (already has job2 running) should not run before job6 or job7 submitted by new users (hadoop5 and hadoopqa)
        if [ $numTask5 -gt 0 -a $numTask6 -eq 0 ] ; then
            echo "NumTask5: $numTask5 vs. NumTask6 $numTask6"
            setFailCase "JobID5 $jobID5 started before jobID6 $jobID6"
        fi
        if [ $numTask5 -gt 0 -a $numTask7 -eq 0 ] ; then
            echo "NumTask5: $numTask5 vs. NumTask7 $numTask7"
            setFailCase "JobID5 $jobID5 started before jobID6 $jobID7"
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"

        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "###### Number of tasks for jobID2: $numTask2"

        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        echo "###### Number of tasks for jobID3: $numTask3"

        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        echo "###### Number of tasks for jobID4: $numTask4"

        echo "Number of map tasks for jobID5: $numMapTask5"
        echo "Number of reduce tasks for jobID5: $numReduceTask5"
        echo "###### Number of tasks for jobID5: $numTask5"

        echo "Number of map tasks for jobID6: $numMapTask6"
        echo "Number of reduce tasks for jobID6: $numReduceTask6"
        echo "###### Number of tasks for jobID6: $numTask6"

        echo "Number of map tasks for jobID7: $numMapTask7"
        echo "Number of reduce tasks for jobID7: $numReduceTask7"
        echo "###### Number of tasks for jobID7: $numTask7"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3, $numMapTask4, $numMapTask5, $numMapTask6, $numMapTask7"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}, ${numReduceTask4}, ${numReduceTask5}, ${numReduceTask6}, ${numReduceTask7}"
        echo "***** Number of tasks for each job: $numTask1, $numTask2, $numTask3, $numTask4, $numTask5, $numTask6, $numTask7"
        totalTasks=$(( $numTask1+$numTask2+$numTask3+$numTask4+$numTask5+$numTask6+$numTask7 ))
        echo "***** Total number of running tasks: $totalTasks"

        echo "###################"
        echo "##### Sleeping ..."
        sleep 50
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    wait $processID7
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed"
    fi
    return=$(verifySucceededJob $jobID7)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventh normal job $jobID7 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check user limit (41%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit41_4normalJobsQueueUp {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job which have 6 times queue capacity map tasks and queue capacity reduce tasks"
    echo "3. User2 submits a J2 job which have 6 times queue capacity map tasks and queue capacity reduce tasks"
    echo "4. User3 submits a J3 job which have 3 times queue capacity map tasks and half queue capacity reduce tasks"
    echo "5. User4 submits a J4 job which have 4 times queue capacity map tasks and half queue capacity reduce tasks"
    echo "6. Verify J1 and J2 jobs take up about 82% queue capacity to run, J3 takes about 18% queue capacity, and J4 remains in queue"
    echo "7. Verify when J1 and J2 map tasks completed, the slots are given to J3 first and then J4"
    echo "8. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit first job: "
    SubmitASleepJob "$(( ${default_qc} * 6 ))" "$(( ${default_qc} * 6 ))" 20000 20000 default 2>&1 &
    processID1=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit a second normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 6 ))" "$(( ${default_qc} * 6 ))" 20000 20000 default 2>&1 &
    processID2=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a third normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 3 ))" "$(( ${default_qc} * 3 ))" 20000 20000 default 2>&1 &
    processID3=$!
    sleep 2

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a fourth normal job ..."
    SubmitASleepJob $(( ${default_qc} * 4 )) $(( ${default_qc} * 4 )) 20000 20000 default 2>&1 &
    processID4=$!
    sleep 20

    listJobs
    getJobIds 4
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4 are running"

    for i in 0 1 2 3 4 5 6 7 8 9; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
            numTask3=$(( $numMapTask3+$numReduceTask3 ))
        fi
        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
            numTask4=$(( $numMapTask4+$numReduceTask4 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"

        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "###### Number of tasks for jobID2: $numTask2"

        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        echo "###### Number of tasks for jobID3: $numTask3"

        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        echo "###### Number of tasks for jobID4: $numTask4"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3, $numMapTask4"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}, ${numReduceTask4}"
        echo "***** Total number of tasks for each job: $numTask1, $numTask2, $numTask3, $numTask4"
        totalTasks=$(( $numTask1+$numTask2+$numTask3+$numTask4 ))
        echo "################## Total number of running tasks: $totalTasks"
        runningJob=0
        if [ "$numTask1" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask2" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask3" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask4" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi

        if [ "$runningJob" -gt 3 ] ; then
            setFailCase "Number of users' concurrent jobs exceeded user limit on default queue"
        fi

        echo "###################"
        echo "##### Sleeping ..."
        sleep 20
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    sleep 15

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to check user limit (41%) for normal job with map and reduce tasks equal multiple times number of nodes
###########################################
function test_userLimit41_4normalJobsRunningSequencially {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. Submit a first normal job which have 8 times queue capacity map tasks and queue capacity reduce tasks"
    echo "3. After map tasks of first job started, submit a second normal job which have 6 times queue capacity map tasks and queue capacity reduce tasks"
    echo "4. After the first and second normal jobs started running, submit a third normal job which have 3 times queue capacity map and reduce tasks"
    echo "5. Verify second job runs after (queue capacity) number of map tasks of first job completed"
    echo "6. Verify once second job started, number of map tasks for first and second job are equal"
    echo "7. Verify after first and second jobs completed the next (queue capacity) map tasks, third job started"
    echo "8. Verify once third job started, first and second jobs take 82% queue capacity and third one takes about 12%"
    echo "9. Verify J4 will remain in queue until last set maps from J1, J2 or J3 starts finishing or free maps slots are more than queued maps for J1,J2 and J3"
    echo "10. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit first job: "
    SubmitASleepJob "$(( ${default_qc} * 8 ))" "$(( ${default_qc} * 8 ))" 20000 20000 default 2>&1 &
    processID1=$!
    sleep 3

    listJobs
    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    echo "Jobs: $jobID1 is running"
    sleep 5

        # Once J1 is running, submit J2
    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit a second normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 6 ))" "$(( ${default_qc} * 6 ))" 20000 20000 default 2>&1 &
    processID2=$!
    getJobIds 2
    echo ${JOBIDS[*]}
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi

    sleep 5
        # Once J2 is running, submit J3
    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a third normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 3 ))" "$(( ${default_qc} * 3 ))" 20000 20000 default 2>&1 &
    processID3=$!
    getJobIds 3
    echo ${JOBIDS[*]}
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi

    sleep 5
        # Once J3 is running, submit J4
    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a fourth normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 4 ))" "$(( ${default_qc} * 4 ))" 20000 20000 default 2>&1 &
    processID3=$!
    getJobIds 4
    echo ${JOBIDS[*]}
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi

    for i in 0 1 2 3 4 5 6 7 8 9 10; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        echo "##### Loop $i"
        listJobs

        echo "###################"
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
        fi
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        numTask2=$(( $numMapTask2+$numReduceTask2 ))
        echo "###### Number of tasks for jobID2: $numTask2"

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
        fi
        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        numTask3=$(( $numMapTask3+$numReduceTask3 ))
        echo "###### Number of tasks for jobID3: $numTask3"

        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
        fi
        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        numTask4=$(( $numMapTask4+$numReduceTask4 ))
        echo "###### Number of tasks for jobID4: $numTask4"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3, $numMapTask4"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}, ${numReduceTask4}"
        echo "***** Total number of tasks for each job: $numTask1, $numTask2, $numTask3, $numTask4"
        totalTasks=$(( $numTask1+$numTask2+$numTask3+$numTask4 ))
        echo "################## Total number of running tasks: $totalTasks"
        runningJob=0
        if [ "$numTask1" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask2" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask3" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi
        if [ "$numTask4" -gt "$default_ul" ] ; then
            runningJob=$(( $runningJob+1 ))
        fi

        if [ "$runningJob" -gt 3 ] ; then
            setFailCase "Number of users' concurrent jobs exceeded user limit on default queue"
        fi
        echo "###################"
        echo "##### Sleeping ..."
        sleep 20
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    sleep 15

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###################################################
# Function to run Jobs when user limit set to greater than 100%
###################################################
function test_userLimit_GreaterThan100 {
    starttime="$(date +%s)"
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Set user Limit for queue default equals to 110%"
    echo "2. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "3. Submit a first normal job which have number of map and reduce tasks equal 6 times queue capacity"
    echo "4. Submit a second normal job which have number of map and reduce tasks equal 4 times queue capacity"
    echo "5. Verify map tasks from second job starts when last set of maps from first job starts finishing"
    echo "6. Verify reduce tasks from second job starts when last set of reduces from first job starts finishing"
    echo "6. Verify the 2 jobs ran sucessfully"
    echo "*************************************************************"

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    echo "Submit a first normal job"
    SubmitASleepJob $(( ${default_qc} * 6 )) $(( ${default_qc} * 6 )) 30000 30000 default 2>&1 &
    processID1=$!

    echo "Submit a second normal job"
    SubmitASleepJob $(( ${default_qc} * 4 )) $(( ${default_qc} * 4 )) 30000 30000 default 2>&1 &
    processID2=$!
    sleep 10

    listJobs
    getJobIds 2
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    echo "JobID: $jobID1 and $jobID2"

    listJobs
    for i in 0 1 2 3 4 5 6 7 8 9 10; do
        numMapTask1=0
        numMapTask2=0
        numCompletedMapTask1=0
        numCompletedMapTask2=0
        numReduceTask1=0
        numReduceTask2=0
        numCompletedReduceTask1=0
        numCompletedReduceTask2=0
        numTask1=0
        numTask2=0
        numCompletedTask1=0
        numCompletedTask2=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numCompletedMapTask1=$(getNumTasksForJobId $jobID1 MAP completed)
            numCompletedReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE completed)
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numCompletedMapTask2=$(getNumTasksForJobId $jobID2 MAP completed)
            numCompletedReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE completed)
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "Number of completed map tasks for jobID1: $numCompletedMapTask1"
        echo "Number of completed reduce tasks for jobID1: $numCompletedReduceTask1"
        numCompletedTask1=$(( $numCompletedMapTask1+$numCompletedReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "###### Number of completed tasks for jobID1: $numCompletedTask1"

        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        numTask2=$(( $numMapTask2+$numReduceTask2 ))
        echo "Number of completed map tasks for jobID2: $numCompletedMapTask2"
        echo "Number of completed reduce tasks for jobID2: $numCompletedReduceTask2"
        numCompletedTask2=$(( $numCompletedMapTask2+$numCompletedReduceTask2 ))
        echo "###### Number of tasks for jobID2: $numTask2"
        echo "###### Number of completed tasks for jobID2: $numCompletedTask2"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}"
        echo "***** Number of completed map tasks for all Jobs: $numCompletedMapTask1, $numCompletedMapTask2"
        echo "***** Number of completed reduce tasks for all Jobs: ${numCompletedReduceTask1}, ${numCompletedReduceTask2}"
        echo "###################"
        echo "##### Sleeping ..."
        sleep 20
    done

    wait $processID1
    wait $processID2

    sleep 10
    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi

    displayTestCaseResult
    stoptime="$(date +%s)"
    getElapseTime "$starttime" "$stoptime"
    return $COMMAND_EXIT_CODE
}

###################################################
# Function to run Jobs when user limit set to 0%
###################################################
function test_userLimit_Zero {
    starttime="$(date +%s)"
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Set user Limit for queue default equals to 0%"
    echo "2. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "3. Submit six normal jobs by 6 different users which have number of map and reduce tasks equal 2 times queue capacity"
    echo "4. Verify task slots are distributed equally among first 5 and the last one get the rest"
    echo "6. Verify all jobs ran sucessfully"
    echo "*************************************************************"

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "Minimum User limit on default queue: $default_ul"

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit first job"
    SubmitASleepJob "$(( ${default_qc} * 4 ))" "$(( ${default_qc} * 4 ))" 30000 30000 default 2>&1 &
    processID1=$!

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit a second normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 3 ))" "$(( ${default_qc} * 3 ))" 30000 30000 default 2>&1 &
    processID2=$!

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit a third normal job ..."
    SubmitASleepJob "$(( ${default_qc} * 2 ))" "$(( ${default_qc} * 2 ))" 30000 30000 default 2>&1 &
    processID3=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit a fourth normal job ..."
    SubmitASleepJob ${default_qc} ${default_qc} 30000 30000 default 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit a fifth normal job ..."
    SubmitASleepJob ${default_qc} ${default_qc} 30000 30000 default 2>&1 &
    processID5=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit a sixth normal job ..."
    SubmitASleepJob ${default_qc} ${default_qc} 30000 30000 default 2>&1 &
    processID6=$!
    sleep 3

    listJobs
    getJobIds 6
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6 are running"
    sleep 5

    listJobs
    for i in 0 1 2 3 4 5 6 7 8 9 10; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numMapTask5=0
        numMapTask6=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numReduceTask5=0
        numReduceTask6=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        numTask5=0
        numTask6=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi

        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
        fi

        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID5`" ] ; then
            numMapTask5=$(getNumTasksForJobId $jobID5 MAP running)
            numReduceTask5=$(getNumTasksForJobId $jobID5 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID6`" ] ; then
            numMapTask6=$(getNumTasksForJobId $jobID6 MAP running)
            numReduceTask6=$(getNumTasksForJobId $jobID6 REDUCE running)
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        numTask2=$(( $numMapTask2+$numReduceTask2 ))
        echo "###### Number of tasks for jobID2: $numTask2"
        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        numTask3=$(( $numMapTask3+$numReduceTask3 ))
        echo "###### Number of tasks for jobID3: $numTask3"
        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        numTask4=$(( $numMapTask4+$numReduceTask4 ))
        echo "###### Number of tasks for jobID4: $numTask4"
        echo "Number of map tasks for jobID5: $numMapTask5"
        echo "Number of reduce tasks for jobID5: $numReduceTask5"
        numTask5=$(( $numMapTask5+$numReduceTask5 ))
        echo "###### Number of tasks for jobID5: $numTask5"
        echo "Number of map tasks for jobID6: $numMapTask6"
        echo "Number of reduce tasks for jobID6: $numReduceTask6"
        numTask6=$(( $numMapTask6+$numReduceTask6 ))
        echo "###### Number of tasks for jobID4: $numTask6"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3, $numMapTask4, $numMapTask5, $numMapTask6"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}, ${numReduceTask4}, ${numReduceTask5}, ${numReduceTask6}"
        echo "***** Total number of tasks for each job: $numTask1, $numTask2, $numTask3, $numTask4, $numTask5, $numTask6"
        echo "###################"

        echo "##### Sleeping ..."
        sleep 20
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6

    sleep 10
    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed. Bug 4592235"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed. Bug 4592235"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed. Bug 4592235"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed. Bug 4592235"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed. Bug 4592235"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed. Bug 4592235"
    fi

    displayTestCaseResult
    stoptime="$(date +%s)"
    getElapseTime "$starttime" "$stoptime"
    return $COMMAND_EXIT_CODE
}

#####################################################
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by different users (User limit=25%)
#####################################################
function test_guaranteeCapacity_differentUsersOnDifferentQueues_userLimit25 {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.5 queue capacity"
    echo "3. User2 submits a J2 job to default queue which has map and reduce tasks=0.4 queue capacity"
    echo "4. User3 submits a J3 job to default queue which has map and reduce tasks=0.3 queue capacity"
    echo "5. User4 submits a J4 job to default queue which has map and reduce tasks=0.2 queue capacity"
    echo "6. User5 submits a J5 job to default queue which has map and reduce tasks=0.1 queue capacity"
    echo "7. User6 submits a J6 job to grideng queue which has map and reduce tasks=0.7 queue capacity"
    echo "8. User7 submits a J7 job to grideng queue which has map and reduce tasks=0.4 queue capacity"
    echo "9. User8 submits a J8 job to grideng queue which has map and reduce tasks=0.4 queue capacity"
    echo "10. Again User6 submits a J9 job to grideng queue which has map and reduce tasks=0.3 queue capacity"
    echo "11. User9 submits a J10 job to gridops queue which has map and reduce tasks=1.0 queue capacity"
    echo "12. User10 submits a J11 job to gridops queue which has map and reduce tasks=0.9 queue capacity"
    echo "13. User11 submits a J12 job to search queue which has map and reduce tasks=1.0 queue capacity"
    echo "14. User12 submits a J13 job to search queue which has map and reduce tasks=1.0 queue capacity"
    echo "15. Again User1 submits a J14 job to default queue which has map and reduce tasks=0.6 cluster capacity"
    echo "16. User13 submits a J15 job to default queue which has map and reduce tasks=0.3 queue capacity"
    echo "17. User14 submits a J16 job to grideng queue which has map and reduce tasks=0.4 queue capacity"
    echo "18. User15 submits a J17 job to grideng queue which has map and reduce tasks=0.05 queue capacity"
    echo "19. User16 submits a J18 job to gridops queue which has map and reduce tasks=0.4 queue capacity"
    echo "20. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    getUserLimit default
    default_ul=$UserLimit
    getQueueCapacity grideng
    grideng_qc=$QueueCapacity
    getUserLimit grideng
    grideng_ul=$UserLimit
    getQueueCapacity gridops
    gridops_qc=$QueueCapacity
    getUserLimit gridops
    gridops_ul=$UserLimit
    getQueueCapacity search
    search_qc=$QueueCapacity
    getUserLimit search
    search_ul=$UserLimit

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit J1: SubmitASleepJob ${default_qc}/2 ${default_qc}/2 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( ${default_qc} / 2 ))" "$(( ${default_qc} / 2 ))" 30000 30000 default 2>&1 &
    processID1=$!

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit J2: SubmitASleepJob ${default_qc}*.4 ${default_qc}*.4 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 4 )) / 10 ))" "$(( $(( ${default_qc} * 4 )) / 10 ))" 30000 30000 default 2>&1 &
    processID2=$!

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit J3: SubmitASleepJob ${default_qc}*.3 ${default_qc}*.3 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 3 )) / 10 ))" "$(( $(( ${default_qc} * 3 )) / 10 ))" 30000 30000 default 2>&1 &
    processID3=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J4: SubmitASleepJob ${default_qc}*.2 ${default_qc}*.2 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 2 )) / 10 ))" "$(( $(( ${default_qc} * 2 )) / 10 ))" 30000 30000 default 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit J5: SubmitASleepJob ${default_qc}*.1 ${default_qc}*.1 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 1 )) / 10 ))" "$(( $(( ${default_qc} * 1 )) / 10 ))" 30000 30000 default 2>&1 &
    processID5=$!

    sleep 5
    getKerberosTicketForUser ${HADOOP6_USER}
    setKerberosTicketForUser ${HADOOP6_USER}
    echo "Submit J6: SubmitASleepJob ${grideng_qc}*.7 ${grideng_qc}*.7 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( ${grideng_qc} * 7 )) / 10 ))" "$(( ${grideng_qc} * 7 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID6=$!

    getKerberosTicketForUser ${HADOOP7_USER}
    setKerberosTicketForUser ${HADOOP7_USER}
    echo "Submit J7: SubmitASleepJob ${grideng_qc}*.4 ${grideng_qc}*.4 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 4 )) / 10 ))" "$(( $(( ${grideng_qc} * 4 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID7=$!

    getKerberosTicketForUser ${HADOOP8_USER}
    setKerberosTicketForUser ${HADOOP8_USER}
    echo "Submit J8: SubmitASleepJob ${grideng_qc}*.4 ${grideng_qc}*.3 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 4 )) / 10 ))" "$(( $(( ${grideng_qc} * 4 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID8=$!

    getKerberosTicketForUser ${HADOOP6_USER}
    setKerberosTicketForUser ${HADOOP6_USER}
    echo "Submit J9: SubmitASleepJob ${grideng_qc}*.3 ${grideng_qc}*.3 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( ${grideng_qc} * 3 )) / 10 ))" "$(( ${grideng_qc} * 3 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID9=$!

    sleep 5
    getKerberosTicketForUser ${HADOOP9_USER}
    setKerberosTicketForUser ${HADOOP9_USER}
    echo "Submit J10: SubmitASleepJob ${gridops_qc} ${gridops_qc} 30000 30000 gridops 2>&1 &"
    SubmitASleepJob ${gridops_qc} ${gridops_qc} 30000 30000 gridops 2>&1 &
    processID10=$!

    getKerberosTicketForUser ${HADOOP10_USER}
    setKerberosTicketForUser ${HADOOP10_USER}
    echo "Submit J11: SubmitASleepJob ${gridops_qc}*.9 ${gridops_qc}*.9 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( ${gridops_qc} * 9 )) / 10 ))" "$(( ${gridops_qc} * 9 )) / 10 ))" 30000 30000 gridops 2>&1 &
    processID11=$!

    sleep 5
    getKerberosTicketForUser ${HADOOP11_USER}
    setKerberosTicketForUser ${HADOOP11_USER}
    echo "Submit J12: SubmitASleepJob ${search_qc} ${search_qc} 30000 30000 search 2>&1 &"
    SubmitASleepJob ${search_qc} ${search_qc} 30000 30000 search 2>&1 &
    processID12=$!

    getKerberosTicketForUser ${HADOOP12_USER}
    setKerberosTicketForUser ${HADOOP12_USER}
    echo "Submit J13: SubmitASleepJob ${search_qc} ${search_qc} 30000 30000 search 2>&1 &"
    SubmitASleepJob ${search_qc} ${search_qc} 30000 30000 search 2>&1 &
    processID13=$!

    sleep 5
    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit J14: SubmitASleepJob ${default_qc}*.6 ${default_qc}*.6 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 6 )) / 10 ))" "$(( $(( ${default_qc} * 6 )) / 10 ))" 30000 30000 default 2>&1 &
    processID14=$!

    getKerberosTicketForUser ${HADOOP13_USER}
    setKerberosTicketForUser ${HADOOP13_USER}
    echo "Submit J15: SubmitASleepJob ${default_qc}*.3 ${default_qc}*.3 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 3 )) / 10 ))" "$(( $(( ${default_qc} * 3 )) / 10 ))" 30000 30000 default 2>&1 &
    processID15=$!

    getKerberosTicketForUser ${HADOOP14_USER}
    setKerberosTicketForUser ${HADOOP14_USER}
    echo "Submit J16: SubmitASleepJob ${grideng_qc}*.4 ${grideng_qc}*.4 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 4 )) / 10 ))" "$(( $(( ${grideng_qc} * 4 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID16=$!

    getKerberosTicketForUser ${HADOOP15_USER}
    setKerberosTicketForUser ${HADOOP15_USER}
    echo "Submit J17: SubmitASleepJob ${grideng_qc}*.05 ${grideng_qc}*.05 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( ${grideng_qc} * 5 )) / 100 ))" "$(( ${grideng_qc} * 5 )) / 100 ))" 30000 30000 grideng 2>&1 &
    processID17=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit J18: SubmitASleepJob ${gridops_qc}*.4 ${gridops_qc}*.4 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( ${gridops_qc} * 4 )) / 10 ))" "$(( ${gridops_qc} * 4 )) / 10 ))" 30000 30000 gridops 2>&1 &
    processID18=$!

    sleep 20
    listJobs
    getJobIds 18
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    jobID7=${JOBIDS[6]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID7 for the sleep job"
    fi
    jobID8=${JOBIDS[7]}
    if [ -z "$jobID8" ] ; then
        setFailCase "Cannot get Job ID8 for the sleep job"
    fi
    jobID9=${JOBIDS[8]}
    if [ -z "$jobID9" ] ; then
        setFailCase "Cannot get Job ID9 for the sleep job"
    fi
    jobID10=${JOBIDS[9]}
    if [ -z "$jobID10" ] ; then
        setFailCase "Cannot get Job ID10 for the sleep job"
    fi
    jobID11=${JOBIDS[10]}
    if [ -z "$jobID11" ] ; then
        setFailCase "Cannot get Job ID11 for the sleep job"
    fi
    jobID12=${JOBIDS[11]}
    if [ -z "$jobID12" ] ; then
        setFailCase "Cannot get Job ID12 for the sleep job"
    fi
    jobID13=${JOBIDS[12]}
    if [ -z "$jobID13" ] ; then
        setFailCase "Cannot get Job ID13 for the sleep job"
    fi
    jobID14=${JOBIDS[13]}
    if [ -z "$jobID14" ] ; then
        setFailCase "Cannot get Job ID14 for the sleep job"
    fi
    jobID15=${JOBIDS[14]}
    if [ -z "$jobID15" ] ; then
        setFailCase "Cannot get Job ID15 for the sleep job"
    fi
    jobID16=${JOBIDS[15]}
    if [ -z "$jobID16" ] ; then
        setFailCase "Cannot get Job ID16 for the sleep job"
    fi
    jobID17=${JOBIDS[16]}
    if [ -z "$jobID17" ] ; then
        setFailCase "Cannot get Job ID17 for the sleep job"
    fi
    jobID18=${JOBIDS[17]}
    if [ -z "$jobID18" ] ; then
        setFailCase "Cannot get Job ID18 for the sleep job"
    fi

    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6, $jobID7, $jobID8 $jobID9 $jobID10 $jobID11, $jobID12, $jobID13, $jobID14, $jobID15, $jobID16, $jobID17, $jobID18 are running"
    sleep 5

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    wait $processID7
    wait $processID8
    wait $processID9
    wait $processID10
    wait $processID11
    wait $processID12
    wait $processID13
    wait $processID14
    wait $processID15
    wait $processID16
    wait $processID17
    wait $processID18
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed"
    fi
    return=$(verifySucceededJob $jobID7)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventh normal job $jobID7 failed"
    fi
    return=$(verifySucceededJob $jobID8)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Eighth normal job $jobID8 failed"
    fi
    return=$(verifySucceededJob $jobID9)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Ninth normal job $jobID9 failed"
    fi
    return=$(verifySucceededJob $jobID10)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Tenth normal job $jobID10 failed"
    fi
    return=$(verifySucceededJob $jobID11)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Eleventh normal job $jobID11 failed"
    fi
    return=$(verifySucceededJob $jobID12)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Twelveth normal job $jobID12 failed"
    fi
    return=$(verifySucceededJob $jobID13)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Thirteenth normal job $jobID13 failed"
    fi
    return=$(verifySucceededJob $jobID14)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourteenth normal job $jobID14 failed"
    fi
    return=$(verifySucceededJob $jobID15)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifteenth normal job $jobID15 failed"
    fi
    return=$(verifySucceededJob $jobID16)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixteenth normal job $jobID16 failed"
    fi
    return=$(verifySucceededJob $jobID17)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventeenth normal job $jobID17 failed"
    fi
    return=$(verifySucceededJob $jobID18)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Eighteenth normal job $jobID18 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#####################################################
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by different users (User limit=100%)
#####################################################
function test_guaranteeCapacity_differentUsersOnDifferentQueuesInParallel_userLimit100 {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.45 queue capacity"
    echo "3. User2 submits a J2 job to grideng queue which has map and reduce tasks=0.32 queue capacity"
    echo "4. User3 submits a J3 job to gridops queue which has map and reduce tasks=0.23 queue capacity"
    echo "5. Wait for all 3 jobs running"
    echo "6. User4 submits a J4 job to search queue which has map and reduce tasks=0.03 queue capacity"
    echo "7. User5 submits a J5 job to gridops queue which has map and reduce tasks=0.02 queue capacity"
    echo "8. Again User4 submits a J6 job to search queue which has map and reduce tasks=0.01 queue capacity"
    echo "9. User6 submits a J7 job to search queue which has map and reduce tasks=0.1 queue capacity"
    echo "10. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    getUserLimit default
    default_ul=$UserLimit
    getQueueCapacity grideng
    grideng_qc=$QueueCapacity
    getUserLimit grideng
    grideng_ul=$UserLimit
    getQueueCapacity gridops
    gridops_qc=$QueueCapacity
    getUserLimit gridops
    gridops_ul=$UserLimit
    getQueueCapacity search
    search_qc=$QueueCapacity
    getUserLimit search
    search_ul=$UserLimit

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit J1: SubmitASleepJob ${default_qc}*.45 ${default_qc}*.45 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( (( ${default_qc} * 45 )) / 100 ))" "$(( (( ${default_qc} * 45 )) / 100 ))" 20000 20000 default 2>&1 &
    processID1=$!

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit J2: SubmitASleepJob ${grideng_qc}*.32 ${grideng_qc}*.32 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 32 )) / 100 ))" "$(( $(( ${grideng_qc} * 32 )) / 100 ))" 20000 20000 grideng 2>&1 &
    processID2=$!

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit J3: SubmitASleepJob ${gridops_qc}*.23 ${gridops_qc}*.23 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( $(( ${gridops_qc} * 23 )) / 100 ))" "$(( $(( ${gridops_qc} * 23 )) / 100 ))" 20000 20000 gridops 2>&1 &
    processID3=$!

    sleep 10
    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J4: SubmitASleepJob ${search_qc}*.09 ${search_qc}*.09 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 9 )) / 100 ))" "$(( $(( ${search_qc} * 9 )) / 100 ))" 20000 20000 search 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit J5: SubmitASleepJob ${search_qc}*.08 ${search_qc}*.08 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 8 )) / 100 ))" "$(( $(( ${search_qc} * 8 )) / 100 ))" 20000 20000 search 2>&1 &
    processID5=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J6: SubmitASleepJob ${search_qc}*.1 ${search_qc}*.1 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 10 )) / 100 ))" "$(( $(( ${search_qc} * 10 )) / 100 ))" 20000 20000 search 2>&1 &
    processID6=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit J7: SubmitASleepJob ${search_qc}*.2 ${search_qc}*.2 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 2 )) / 10 ))" "$(( $(( ${search_qc} * 2 )) / 10 ))" 20000 20000 search 2>&1 &
    processID7=$!

    sleep 5
    ${HADOOP_MAPRED_CMD} job -list | sort
    getJobIds 7
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    jobID7=${JOBIDS[6]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID7 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6, $jobID7 are running"
    sleep 5

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    wait $processID7
    sleep 20

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed"
    fi
    return=$(verifySucceededJob $jobID7)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventh normal job $jobID7 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#####################################################
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by different users (User limit=100%)
#####################################################
function test_guaranteeCapacity_differentUsersOnDifferentQueuesInSequence_userLimit100 {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job to default queue which has map and reduce tasks=1 queue capacity"
    echo "3. User2 submits a J2 job to grideng queue which has map and reduce tasks=1 queue capacity"
    echo "4. User3 submits a J3 job to gridops queue which has map and reduce tasks=1 queue capacity"
    echo "5. Wait for all 3 jobs running"
    echo "6. User4 submits a J4 job to search queue which has map and reduce tasks=0.03 queue capacity"
    echo "7. User5 submits a J5 job to gridops queue which has map and reduce tasks=0.02 queue capacity"
    echo "8. Again User4 submits a J6 job to search queue which has map and reduce tasks=0.01 queue capacity"
    echo "9. User6 submits a J7 job to search queue which has map and reduce tasks=0.1 queue capacity"
    echo "10. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    getUserLimit default
    default_ul=$UserLimit
    getQueueCapacity grideng
    grideng_qc=$QueueCapacity
    getUserLimit grideng
    grideng_ul=$UserLimit
    getQueueCapacity gridops
    gridops_qc=$QueueCapacity
    getUserLimit gridops
    gridops_ul=$UserLimit
    getQueueCapacity search
    search_qc=$QueueCapacity
    getUserLimit search
    search_ul=$UserLimit

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit J1: SubmitASleepJob ${default_qc}*.45 ${default_qc}*.45 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( (( ${default_qc} * 45 )) / 100 ))" "$(( (( ${default_qc} * 45 )) / 100 ))" 20000 20000 default 2>&1 &
    processID1=$!
    sleep 2
    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit J2: SubmitASleepJob ${grideng_qc}*.32 ${grideng_qc}*.32 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 32 )) / 100 ))" "$(( $(( ${grideng_qc} * 32 )) / 100 ))" 20000 20000 grideng 2>&1 &
    processID2=$!
    sleep 2
    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit J3: SubmitASleepJob ${gridops_qc}*.23 ${gridops_qc}*.23 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( $(( ${gridops_qc} * 23 )) / 100 ))" "$(( $(( ${gridops_qc} * 23 )) / 100 ))" 20000 20000 gridops 2>&1 &
    processID3=$!

    sleep 10
    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J4: SubmitASleepJob ${search_qc}*.9 ${search_qc}*.9 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 9 )) / 100 ))" "$(( $(( ${search_qc} * 9 )) / 100 ))" 20000 20000 search 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit J5: SubmitASleepJob ${search_qc}*.8 ${search_qc}*.8 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 8 )) / 100 ))" "$(( $(( ${search_qc} * 8 )) / 100 ))" 20000 20000 search 2>&1 &
    processID5=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J6: SubmitASleepJob ${search_qc}*.1 ${search_qc}*.1 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 10 )) / 100 ))" "$(( $(( ${search_qc} * 10 )) / 100 ))" 20000 20000 search 2>&1 &
    processID6=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit J7: SubmitASleepJob ${search_qc}*.2 ${search_qc}*.2 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 2 )) / 10 ))" "$(( $(( ${search_qc} * 2 )) / 10 ))" 20000 20000 search 2>&1 &
    processID7=$!

    sleep 5
    ${HADOOP_MAPRED_CMD} job -list | sort
    getJobIds 7
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    jobID7=${JOBIDS[6]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID7 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6, $jobID7 are running"
    sleep 5

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    wait $processID7
    sleep 20

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed"
    fi
    return=$(verifySucceededJob $jobID7)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventh normal job $jobID7 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#####################################################
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by different users (User limit=100%)
#####################################################
function test_guaranteeCapacity_differentUsersOnDifferentQueuesResourceReclaimed_userLimit100 {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.5 queue capacity"
    echo "3. User2 submits a J2 job to default queue which has map and reduce tasks=0.4 queue capacity"
    echo "4. User3 submits a J3 job to default queue which has map and reduce tasks=0.3 queue capacity"
    echo "5. User4 submits a J4 job to grideng queue which has map and reduce tasks=0.2 queue capacity"
    echo "6. User5 submits a J5 job to grideng queue which has map and reduce tasks=0.1 queue capacity"
    echo "7. User6 submits a J6 job to grideng queue which has map and reduce tasks=0.7 queue capacity"
    echo "8. User7 submits a J7 job to gridops queue which has map and reduce tasks=0.4 queue capacity"
    echo "9. User8 submits a J8 job to gridops queue which has map and reduce tasks=0.4 queue capacity"
    echo "10. Again User6 submits a J9 job to grideng queue which has map and reduce tasks=0.3 queue capacity"
    echo "11. User9 submits a J10 job to gridops queue which has map and reduce tasks=1.0 queue capacity"
    echo "12. User10 submits a J11 job to gridops queue which has map and reduce tasks=0.9 queue capacity"
    echo "13. User11 submits a J12 job to search queue which has map and reduce tasks=1.0 queue capacity"
    echo "14. User12 submits a J13 job to search queue which has map and reduce tasks=1.0 queue capacity"
    echo "15. Again User1 submits a J14 job to default queue which has map and reduce tasks=0.6 cluster capacity"
    echo "16. User13 submits a J15 job to default queue which has map and reduce tasks=0.3 queue capacity"
    echo "17. User14 submits a J16 job to grideng queue which has map and reduce tasks=0.4 queue capacity"
    echo "18. User15 submits a J17 job to grideng queue which has map and reduce tasks=0.05 queue capacity"
    echo "19. User16 submits a J18 job to gridops queue which has map and reduce tasks=0.4 queue capacity"
    echo "20. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    getUserLimit default
    default_ul=$UserLimit
    getQueueCapacity grideng
    grideng_qc=$QueueCapacity
    getUserLimit grideng
    grideng_ul=$UserLimit
    getQueueCapacity gridops
    gridops_qc=$QueueCapacity
    getUserLimit gridops
    gridops_ul=$UserLimit
    getQueueCapacity search
    search_qc=$QueueCapacity
    getUserLimit search
    search_ul=$UserLimit

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit J1: SubmitASleepJob ${default_qc}*.3 ${default_qc}*.3 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 3 )) / 10 ))" "$(( $(( ${default_qc} * 3 )) / 10 ))" 30000 30000 default 2>&1 &
    processID1=$!

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit J2: SubmitASleepJob ${default_qc}*.2 ${default_qc}*.2 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 2 )) / 10 ))" "$(( $(( ${default_qc} * 2 )) / 10 ))" 30000 30000 default 2>&1 &
    processID2=$!

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit J3: SubmitASleepJob ${default_qc}*.15 ${default_qc}*.15 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 15 )) / 100 ))" "$(( $(( ${default_qc} * 15 )) / 100 ))" 30000 30000 default 2>&1 &
    processID3=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J4: SubmitASleepJob ${grideng_qc}*.2 ${grideng_qc}*.2 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 2 )) / 10 ))" "$(( $(( ${grideng_qc} * 2 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit J5: SubmitASleepJob ${grideng_qc}*.05 ${grideng_qc}*.05 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 5 )) / 100 ))" "$(( $(( ${grideng_qc} * 5 )) / 100 ))" 30000 30000 grideng 2>&1 &
    processID5=$!

    getKerberosTicketForUser ${HADOOP6_USER}
    setKerberosTicketForUser ${HADOOP6_USER}
    echo "Submit J6: SubmitASleepJob ${grideng_qc}*.4 ${grideng_qc}*.4 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 4 )) / 10 ))" "$(( $(( ${grideng_qc} * 4 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID6=$!

    getKerberosTicketForUser ${HADOOP7_USER}
    setKerberosTicketForUser ${HADOOP7_USER}
    echo "Submit J7: SubmitASleepJob ${gridops_qc}*.1 ${gridops_qc}*.1 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( $(( ${gridops_qc} * 1 )) / 10 ))" "$(( $(( ${gridops_qc} * 1 )) / 10 ))" 30000 30000 gridops 2>&1 &
    processID7=$!

    getKerberosTicketForUser ${HADOOP8_USER}
    setKerberosTicketForUser ${HADOOP8_USER}
    echo "Submit J8: SubmitASleepJob ${gridops_qc}*.1 ${gridops_qc}*.1 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( $(( ${gridops_qc} * 1 )) / 10 ))" "$(( $(( ${gridops_qc} * 1 )) / 10 ))" 30000 30000 gridops 2>&1 &
    processID8=$!

    getKerberosTicketForUser ${HADOOP9_USER}
    setKerberosTicketForUser ${HADOOP9_USER}
    echo "Submit J10: SubmitASleepJob ${gridops_qc}*.25 ${gridops_qc}*.25 30000 30000 gridops 2>&1 &"
    SubmitASleepJob $(( $(( ${gridops_qc} * 25 )) / 100 )) $(( $(( ${gridops_qc} * 25 )) / 100 )) 30000 30000 gridops 2>&1 &
    processID10=$!

    sleep 10
    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J4: SubmitASleepJob ${search_qc}*.9 ${search_qc}*.9 20000 20000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 9 )) / 100 ))" "$(( $(( ${search_qc} * 9 )) / 100 ))" 30000 30000 search 2>&1 &
    processID4=$!

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit J5: SubmitASleepJob ${search_qc}*.8 ${search_qc}*.8 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 8 )) / 100 ))" "$(( $(( ${search_qc} * 8 )) / 100 ))" 30000 30000 search 2>&1 &
    processID5=$!

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J6: SubmitASleepJob ${search_qc}*.1 ${search_qc}*.1 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 10 )) / 100 ))" "$(( $(( ${search_qc} * 10 )) / 100 ))" 30000 30000 search 2>&1 &
    processID6=$!

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit J7: SubmitASleepJob ${search_qc}*.2 ${search_qc}*.2 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( $(( ${search_qc} * 2 )) / 10 ))" "$(( $(( ${search_qc} * 2 )) / 10 ))" 30000 30000 search 2>&1 &
    processID7=$!

    sleep 5
    listJobs
    getJobIds 7
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    jobID7=${JOBIDS[6]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID7 for the sleep job"
    fi
    jobID8=${JOBIDS[7]}
    if [ -z "$jobID8" ] ; then
        setFailCase "Cannot get Job ID8 for the sleep job"
    fi
    jobID9=${JOBIDS[8]}
    if [ -z "$jobID9" ] ; then
        setFailCase "Cannot get Job ID9 for the sleep job"
    fi
    jobID10=${JOBIDS[9]}
    if [ -z "$jobID10" ] ; then
        setFailCase "Cannot get Job ID10 for the sleep job"
    fi
    jobID11=${JOBIDS[10]}
    if [ -z "$jobID11" ] ; then
        setFailCase "Cannot get Job ID11 for the sleep job"
    fi
    jobID12=${JOBIDS[11]}
    if [ -z "$jobID12" ] ; then
        setFailCase "Cannot get Job ID12 for the sleep job"
    fi
    jobID13=${JOBIDS[12]}
    if [ -z "$jobID13" ] ; then
        setFailCase "Cannot get Job ID13 for the sleep job"
    fi

    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6, $jobID7, $jobID8 $jobID9, $jobID10, $jobID11, $jobID12, $jobID13 are running"
    sleep 5

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    wait $processID7
    wait $processID8
    wait $processID9
    wait $processID10
    wait $processID11
    wait $processID12
    wait $processID13
    sleep 30

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed"
    fi
    return=$(verifySucceededJob $jobID7)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventh normal job $jobID7 failed"
    fi
    return=$(verifySucceededJob $jobID8)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Eighth normal job $jobID8 failed"
    fi
    return=$(verifySucceededJob $jobID9)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Ninth normal job $jobID9 failed"
    fi
    return=$(verifySucceededJob $jobID10)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Tenth normal job $jobID10 failed"
    fi
    return=$(verifySucceededJob $jobID11)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Eleventh normal job $jobID11 failed"
    fi
    return=$(verifySucceededJob $jobID12)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Twelveth normal job $jobID12 failed"
    fi
    return=$(verifySucceededJob $jobID13)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Thirteenth normal job $jobID13 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#####################################################
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by different users (User limit=100%)
#####################################################
function test_guaranteeCapacity_differentUsersOnDifferentQueuesResourceFreedUp_userLimit100 {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
    echo "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.4 queue capacity"
    echo "3. User2 submits a J2 job to default queue which has map and reduce tasks=0.25 queue capacity"
    echo "4. User3 submits a J3 job to grideng queue which has map and reduce tasks=0.32 queue capacity"
    echo "5. User4 submits a J4 job to grideng queue which has map and reduce tasks=0.1 queue capacity"
    echo "6. User5 submits a J5 job to gridops queue which has map and reduce tasks=0.18 queue capacity"
    echo "7. User6 submits a J6 job to gridops queue which has map and reduce tasks=0.02 queue capacity"
    echo "8. Once all of the above jobs are running, User7 submits a J7 job to search queue which has map and reduce tasks=0.1 queue capacity"
    echo "9. Verify J7 job completed before all other jobs"
    echo "9. Verify all the jobs completed sucessfully"
    echo "*************************************************************"
    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    getUserLimit default
    default_ul=$UserLimit
    getQueueCapacity grideng
    grideng_qc=$QueueCapacity
    getUserLimit grideng
    grideng_ul=$UserLimit
    getQueueCapacity gridops
    gridops_qc=$QueueCapacity
    getUserLimit gridops
    gridops_ul=$UserLimit
    getQueueCapacity search
    search_qc=$QueueCapacity
    getUserLimit search
    search_ul=$UserLimit

    getKerberosTicketForUser ${HADOOP1_USER}
    setKerberosTicketForUser ${HADOOP1_USER}
    echo "Submit J1: SubmitASleepJob ${default_qc}*.4 ${default_qc}*.4 30000 30000 default 2>&1 &"
    
    SubmitASleepJob "$(( $(( ${default_qc} * 4 )) / 10 ))" "$(( $(( ${default_qc} * 4 )) / 10 ))" 30000 30000 default 2>&1 &
    processID1=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP2_USER}
    setKerberosTicketForUser ${HADOOP2_USER}
    echo "Submit J2: SubmitASleepJob ${default_qc}*.25 ${default_qc}*.25 30000 30000 default 2>&1 &"
    SubmitASleepJob "$(( $(( ${default_qc} * 25 )) / 100 ))" "$(( $(( ${default_qc} * 25 )) / 100 ))" 30000 30000 default 2>&1 &
    processID2=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP3_USER}
    setKerberosTicketForUser ${HADOOP3_USER}
    echo "Submit J3: SubmitASleepJob ${grideng_qc}*.32 ${grideng_qc}*.32 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 32 )) / 100 ))" "$(( $(( ${grideng_qc} * 32 )) / 100 ))" 30000 30000 grideng 2>&1 &
    processID3=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP4_USER}
    setKerberosTicketForUser ${HADOOP4_USER}
    echo "Submit J4: SubmitASleepJob ${grideng_qc}*.1 ${grideng_qc}*.1 30000 30000 grideng 2>&1 &"
    SubmitASleepJob "$(( $(( ${grideng_qc} * 1 )) / 10 ))" "$(( $(( ${grideng_qc} * 1 )) / 10 ))" 30000 30000 grideng 2>&1 &
    processID4=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP5_USER}
    setKerberosTicketForUser ${HADOOP5_USER}
    echo "Submit J5: SubmitASleepJob ${gridops_qc}*.05 ${gridops_qc}*.05 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( $(( ${gridops_qc} * 5 )) / 100 ))" "$(( $(( ${gridops_qc} * 5 )) / 100 ))" 30000 30000 gridops 2>&1 &
    processID5=$!
    sleep 2

    getKerberosTicketForUser ${HADOOP6_USER}
    setKerberosTicketForUser ${HADOOP6_USER}
    echo "Submit J6: SubmitASleepJob ${gridops_qc}*.04 ${gridops_qc}*.04 30000 30000 gridops 2>&1 &"
    SubmitASleepJob "$(( $(( ${gridops_qc} * 4 )) / 100 ))" "$(( $(( ${gridops_qc} * 4 )) / 100 ))" 30000 30000 gridops 2>&1 &
    processID6=$!
    sleep 2

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    echo "Submit J7: SubmitASleepJob ${search_qc}*.1 ${search_qc}*.1 30000 30000 search 2>&1 &"
    SubmitASleepJob "$(( ${search_qc} / 10 ))" "$(( ${search_qc} / 10 ))" 30000 30000 search 2>&1 &
    processID7=$!

    sleep 5
    ${HADOOP_MAPRED_CMD} job -list | sort
    getJobIds 7
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID1 for the sleep job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID2 for the sleep job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID3 for the sleep job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID4 for the sleep job"
    fi
    jobID5=${JOBIDS[4]}
    if [ -z "$jobID5" ] ; then
        setFailCase "Cannot get Job ID5 for the sleep job"
    fi
    jobID6=${JOBIDS[5]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID6 for the sleep job"
    fi
    jobID7=${JOBIDS[6]}
    if [ -z "$jobID6" ] ; then
        setFailCase "Cannot get Job ID7 for the sleep job"
    fi
    echo "Jobs: $jobID1 $jobID2 $jobID3 $jobID4, $jobID5, $jobID6, $jobID7 are running"
    sleep 5

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    wait $processID5
    wait $processID6
    wait $processID7
    sleep 20

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi
    return=$(verifySucceededJob $jobID5)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fifth normal job $jobID5 failed"
    fi
    return=$(verifySucceededJob $jobID6)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Sixth normal job $jobID6 failed"
    fi
    return=$(verifySucceededJob $jobID7)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Seventh normal job $jobID7 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function runSimpleSleepJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC

    echo "Get number of nodes on the cluster"
    getNodesOnCluster
    getQueueCapacity default
    default_qc=$QueueCapacity
    default_ul=$(getUserLimit default)
    echo "Default queue limit: $default_qc"
    echo "User limit on default queue: $default_ul"

    echo "Submit a normal job:"
   #SubmitAHighRAMJob 2048 2048 20 20 50000 50000 default 2>&1 &
    SubmitASleepJob 100 100 30000 30000 default 2>&1 &
    sleep 5

    echo "Submit a normal job:"
    SubmitASleepJob $(( $(( ${default_qc} * 2 )) )) $(( $(( ${default_qc} * 2 )) )) 30000 30000 default 2>&1 &
    processID=$!
    sleep 10

    
    getJobIds 1
    echo ${JOBIDS[*]}
    jobID=${JOBIDS[0]}
    if [ -z "$jobID" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID"

    getKerberosTicketForUser ${MAPREDQA_USER}
    setKerberosTicketForUser ${MAPREDQA_USER}
    RMProcessID=$(getRMProcessId)
    echo "RM processID: $RMProcessID"

    for i in 0 1 2 3 ; do
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID`" ] ; then
            allocatedGB=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "default")
            allocatedContainers=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "default")
            numOfSlots=$(getNumSlots $allocatedGB $allocatedContainers)
            numMapTask=$(getNumTasksForJobId $jobID MAP running)
            numReduceTask=$(getNumTasksForJobId $jobID REDUCE running)
            numTask=$(( $numMapTask+$numReduceTask ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask"
        echo "Number of reduce tasks for jobID1: $numReduceTask"
        echo "Number of tasks for jobID1: $numTask"
        echo "###################"
        echo "AllocatedGB: $allocatedGB"
        echo "AllocatedContainers: $allocatedContainers"
        if [ $numMapTask -gt 0 -a $numReduceTask -eq 0 ]; then
            echo "Number of slots allocated for map task: $numOfSlots"
        fi
        if [ $numReduceTask -gt 0 -a $numMapTask -eq 0 ]; then
            echo "Number of slots allocated for reduce task: $numOfSlots"
        fi
        echo "AvailableGB:"
        getJTMetricsInfo $JOBTRACKER $RMProcessID "AvailableGB" "default"
        echo "PendingGB:"
        getJTMetricsInfo $JOBTRACKER $RMProcessID "PendingGB" "default"
        echo "ReservedGB:"
        getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedGB" "default"
        echo "ReservedContainers:"
        getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedContainers" "default"
        echo "PendingContainers:"
        getJTMetricsInfo $JOBTRACKER $RMProcessID "PendingContainers" "default"
        echo "###################"
        sleep 10

    done

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    wait $processID
    sleep 10
    return=$(verifySucceededJob $jobID)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM map requirement exceed cluster max memory limit
##################################################
function test_highRAM_hiRAMMapOnlyExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of map memory equal 9126"
    echo "2. Verify the highRAM job fails with exception: Exceeds the cluster's max-memory-limit"
    echo "*************************************************************"

    echo "Submit a highRAM job:"
    SubmitAHighRAMJob 10240 2048 20 20 20000 20000 default 2>&1 &
    processID=$!
    sleep 15

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"

    for i in 0 1 2 ; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        sleep 5
    done

    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 which exceed cluster max memory limit runs sucessfully "
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM reduce requirement exceed cluster max memory limit
##################################################
function test_highRAM_hiRAMReduceOnlyExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of reduce memory equal 9126"
    echo "2. Verify the highRAM job fails with exception: Exceeds the cluster's max-memory-limit"
    echo "*************************************************************"

    echo "Submit a highRAM job:"
    SubmitAHighRAMJob 2048 10240 20 20 20000 20000 default 2>&1 &
    processID=$!
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"

    for i in 0 1 2 ; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        sleep 5
    done

    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 which exceed cluster max memory limit runs sucessfully "
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM map and reduce requirement exceed cluster max memory limit
##################################################
function test_highRAM_hiRAMMapAndReduceExceedMemoryLimit {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of map memory equal 7168 and reduce memory equal 9126"
    echo "2. Verify the highRAM job fails with exception Exceeds the cluster's max-memory-limit"
    echo "*************************************************************"

    echo "Submit a highRAM job:"
    SubmitAHighRAMJob 10240 10240 20 20 20000 20000 default 2>&1 &
    processID=$!
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"

    for i in 0 1 2 ; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        sleep 5
    done

    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 which exceeded cluster max memory limit ran sucessfully"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM map requires 2 slots
##################################################
function test_highRAM_hiRAMMapTake2Slots {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of map memory equal 3072"
    echo "2. Verify the highRAM job can run sucessfully"
    echo "*************************************************************"

    getJobTracker
    echo "Submit a highRAM job:"
    mapMemory=2048
    reduceMemory=1024
    expectedSlots=$(( $mapMemory / 1024 ))
    echo "Expected slots for map task: $expectedSlots"
    SubmitAHighRAMJob $mapMemory $reduceMemory 20 20 50000 50000 default 2>&1 &
    processID=$!
    sleep 10

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"
    getKerberosTicketForUser ${MAPREDQA_USER}
    setKerberosTicketForUser ${MAPREDQA_USER}
    RMProcessID=$(getRMProcessId)
    echo "RM processID: $RMProcessID"

    for i in 0 1 2 3 ; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            allocatedGB=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "default")
            allocatedContainers=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "default")
            numOfSlots=$(getNumSlots $allocatedGB $allocatedContainers)
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "Number of tasks for jobID1: $numTask1"
        echo "###################"
        echo "AllocatedGB: $allocatedGB"
        echo "AllocatedContainers: $allocatedContainers"
        if [ $numMapTask1 -gt 0 -a $numReduceTask1 -eq 0 ]; then
            echo "Number of slots allocated for map task: $numOfSlots"
            if [ $expectedSlots -ne $numOfSlots ]; then
                setFailCase "Number of slots for Map Task is not taken $expectedSlots slots"
            fi
        fi
        if [ $numReduceTask1 -gt 0 -a $numMapTask1 -eq 0 ]; then
            echo "Number of slots allocated for reduce task: $numOfSlots"
        fi
        sleep 10
    done

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}

    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM reduce requires 2 slots
##################################################
function test_highRAM_hiRAMReduceTake2Slots {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of reduce memory equal 2048"
    echo "2. Verify the highRAM job can run sucessfully"
    echo "*************************************************************"
    mapMemory=1024
    reduceMemory=2048
    expectedSlots=$(( $reduceMemory / 1024 ))
    echo "Expected slots for reduce task: $expectedSlots"

    echo "Submit a highRAM job:"
    SubmitAHighRAMJob $mapMemory $reduceMemory 20 20 50000 50000 default 2>&1 &
    processID=$!
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"
    getKerberosTicketForUser ${MAPREDQA_USER}
    setKerberosTicketForUser ${MAPREDQA_USER}
    RMProcessID=$(getRMProcessId)
    echo "RM processID: $RMProcessID"

    for i in 0 1 2 3 ; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            allocatedGB=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "default")
            allocatedContainers=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "default")
            numOfSlots=$(getNumSlots $allocatedGB $allocatedContainers)
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "AllocatedGB: $allocatedGB"
        echo "AllocatedContainers: $allocatedContainers"
        if [ $numMapTask1 -gt 0 -a $numReduceTask1 -eq 0 ]; then
            echo "Number of slots allocated for map task: $numOfSlots"
        fi
        if [ $numReduceTask1 -gt 0 -a $numMapTask1 -eq 0 ]; then
            echo "Number of slots allocated for reduce task: $numOfSlots"
            if [ $expectedSlots -ne $numOfSlots ]; then
                setFailCase "Number of slots for Reduce Task is not taken $expectedSlots slots"
            fi
        fi
        sleep 10
    done

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}
    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM map takes 4 slots and reduce takes 2 slots
##################################################
function test_highRAM_hiRAMMapTake4SlotsAndReduceTake2Slots {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of map memory equal 2048 and reduce memory equal 2048"
    echo "2. Verify the highRAM job can run sucessfully"
    echo "*************************************************************"
    mapMemory=2048
    reduceMemory=2048
    expectedMapSlots=$(( $mapMemory / 1024 ))
    expectedReduceSlots=$(( $reduceMemory / 1024 ))
    echo "Expected slots for map task: $expectedSlots"

    echo "Submit a highRAM job:"
    SubmitAHighRAMJob 2048 2048 20 20 50000 50000 default 2>&1 &
    processID=$!
    sleep 10

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"
    getKerberosTicketForUser ${MAPREDQA_USER}
    setKerberosTicketForUser ${MAPREDQA_USER}
    RMProcessID=$(getRMProcessId)
    echo "RM processID: $RMProcessID"

    for i in 0 1 2 3; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            allocatedGB=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "default")
            allocatedContainers=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "default")
            numOfSlots=$(getNumSlots $allocatedGB $allocatedContainers)
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "AllocatedGB: $allocatedGB"
        echo "AllocatedContainers: $allocatedContainers"
        if [ $numMapTask1 -gt 0 -a $numReduceTask1 -eq 0 ]; then
            echo "Number of slots allocated for map task: $numOfSlots"
            if [ $expectedMapSlots -ne $numOfSlots ]; then
                setFailCase "Number of slots for Map Task is not taken $expectedMapSlots slots"
            fi
        fi
        if [ $numReduceTask1 -gt 0 -a $numMapTask1 -eq 0 ]; then
            echo "Number of slots allocated for reduce task: $numOfSlots"
            if [ $expectedReduceSlots -ne $numOfSlots ]; then
                setFailCase "Number of slots for Reduce Task is not taken $expectedReduceSlots slots"
            fi
        fi
        sleep 15
    done
    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}

    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with high RAM map and reduce memory requirement below memory limit
##################################################
function test_highRAM_hiRAMMapAndReduceLowMemory {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a highRAM job which have number of map and reduce memory equal 512"
    echo "2. Verify the highRAM job fails with exception: beyond memory-limits"
    echo "*************************************************************"

    echo "Submit a highRAM job:"
    SubmitAHighRAMJob 512 512 10 10 20000 20000 default 2>&1 &
    processID=$!
    sleep 20

    getJobIds 1
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the sleep job"
    fi
    echo "JobID: $jobID1"

    for i in 0 1 2; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        sleep 15
    done

    wait $processID
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "High RAM job $jobID1 with memory requirement beyond memory-limits runs sucessfully"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for high RAM job with user limit
##################################################
function test_highRAM_userLimitSupportHighRAMJob {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit 4 highRAM jobs which have number of map memory equals 3072 and reduce memory equals 6144 to different queues"
    echo "2. Verify highRAM jobs completed sucessfully"
    echo "*************************************************************"

    echo "Submit first highRAM job:"
    SubmitAHighRAMJob 2048 2048 10 10 30000 30000 default 2>&1 &
    processID1=$!
    echo "Submit second highRAM job:"
    SubmitAHighRAMJob 2048 2048 10 10 30000 30000 grideng 2>&1 &
    processID2=$!
    echo "Submit third highRAM job:"
    SubmitAHighRAMJob 2048 2048 10 10 30000 30000 gridops 2>&1 &
    processID3=$!
    echo "Submit fourth highRAM job:"
    SubmitAHighRAMJob 2048 2048 10 10 30000 30000 search 2>&1 &
    processID4=$!
    sleep 20

    getJobIds 4
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    echo "JobIDs: $jobID1, $jobID2, $jobID3, $jobID4"

    for i in 0 1 2 3 4; do
        numMapTask1=0
        numMapTask2=0
        numMapTask3=0
        numMapTask4=0
        numReduceTask1=0
        numReduceTask2=0
        numReduceTask3=0
        numReduceTask4=0
        numTask1=0
        numTask2=0
        numTask3=0
        numTask4=0
        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID2`" ] ; then
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID3`" ] ; then
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
        fi
        if [ -n "`isJobRunning $jobID4`" ] ; then
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
        fi

        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        numTask1=$(( $numMapTask1+$numReduceTask1 ))
        echo "###### Number of tasks for jobID1: $numTask1"
        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        numTask2=$(( $numMapTask2+$numReduceTask2 ))
        echo "###### Number of tasks for jobID2: $numTask2"
        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        numTask3=$(( $numMapTask3+$numReduceTask3 ))
        echo "###### Number of tasks for jobID3: $numTask3"
        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        numTask4=$(( $numMapTask4+$numReduceTask4 ))
        echo "###### Number of tasks for jobID4: $numTask4"
        sleep 15
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    sleep 10

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function for checking slot utilization for normal and high RAM jobs
# Option: set mapreduce.job.reduce.slowstart.completedmaps to 1
##################################################
function test_highRAM_slotUtilization {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit 1 normal job to default queue"
    echo "2. Submit 2 highRAM jobs to grideng and gridops queues"
    echo "3. Submit another normal job to search queue"
    echo "4. Verify highRAM task takes 2 slots and normal task takes 1 slot"
    echo "5. Verify all the jobs completed sucessfully"
    echo "*************************************************************"

    getJobTracker
    mapMemory_highRAM=2048
    mapMemory_normal=1024
    reduceMemory_normal=1024
    reduceMemory_highRAM=2048
    expectedSlots_normal=$(( $mapMemory_normal / 1024 ))
    expectedSlots_highRAM=$(( $mapMemory_highRAM / 1024 ))
    echo "Expected slots for normal task: $expectedSlots_normal"
    echo "Expected slots for highRAM task: $expectedSlots_highRAM"

    echo "Submit first normal job:"
    SubmitAHighRAMJob $mapMemory_normal $reduceMemory_normal 20 20 30000 30000 default 2>&1 &
    processID1=$!
    echo "Submit second highRAM job:"
    SubmitAHighRAMJob $mapMemory_highRAM $reduceMemory_highRAM 20 20 30000 30000 grideng 2>&1 &
    processID2=$!
    echo "Submit third highRAM job:"
    SubmitAHighRAMJob $mapMemory_highRAM $reduceMemory_highRAM 20 10 30000 30000 gridops 2>&1 &
    processID3=$!
    echo "Submit fourth normal job:"
    SubmitAHighRAMJob $mapMemory_normal $reduceMemory_normal 20 20 30000 30000 search 2>&1 &
    processID4=$!
    sleep 5

    getJobIds 4
    echo ${JOBIDS[*]}
    jobID1=${JOBIDS[0]}
    if [ -z "$jobID1" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    jobID2=${JOBIDS[1]}
    if [ -z "$jobID2" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    jobID3=${JOBIDS[2]}
    if [ -z "$jobID3" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    jobID4=${JOBIDS[3]}
    if [ -z "$jobID4" ] ; then
        setFailCase "Cannot get Job ID for the highRAM job"
    fi
    echo "JobIDs: $jobID1, $jobID2, $jobID3, $jobID4"

    getKerberosTicketForUser ${MAPREDQA_USER}
    setKerberosTicketForUser ${MAPREDQA_USER}
    RMProcessID=$(getRMProcessId)
    echo "RM processID: $RMProcessID"

    for i in 0 1 2 3 4; do
        numMapTask1=0
        numReduceTask1=0
        numTask1=0
        numMapTask2=0
        numReduceTask2=0
        numTask2=0
        numMapTask3=0
        numReduceTask3=0
        numTask3=0
        numMapTask4=0
        numReduceTask4=0
        numTask4=0
        allocatedGB_default=0
        allocatedContainers_default=0
        reservedGB_default=0
        reservedContainers_default=0
        allocatedGB_grideng=0
        allocatedContainers_grideng=0
        reservedGB_grideng=0
        reservedContainers_grideng=0
        allocatedGB_gridops=0
        allocatedContainers_gridops=0
        reservedGB_gridops=0
        reservedContainers_gridops=0
        allocatedGB_search=0
        allocatedContainers_search=0
        reservedGB_search=0
        reservedContainers_search=0

        echo "##### Loop $i"
        listJobs
        if [ -n "`isJobRunning $jobID1`" ] ; then
            allocatedGB_default=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "default")
            allocatedContainers_default=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "default")
            reservedGB_default=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedGB" "default")
            reservedContainers_default=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedContainers" "default")
            numOfSlots_default=$(getNumSlots $allocatedGB_default $allocatedContainers_default)
            numMapTask1=$(getNumTasksForJobId $jobID1 MAP running)
            numReduceTask1=$(getNumTasksForJobId $jobID1 REDUCE running)
            numTask1=$(( $numMapTask1+$numReduceTask1 ))
        fi
        if [ -n "`isJobRunning $jobID2`" ] ; then
            allocatedGB_grideng=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "grideng")
            allocatedContainers_grideng=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "grideng")
            reservedGB_grideng=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedGB" "grideng")
            reservedContainers_grideng=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedContainers" "grideng")
            numOfSlots_grideng=$(getNumSlots $allocatedGB_grideng $allocatedContainers_grideng)
            numMapTask2=$(getNumTasksForJobId $jobID2 MAP running)
            numReduceTask2=$(getNumTasksForJobId $jobID2 REDUCE running)
            numTask2=$(( $numMapTask2+$numReduceTask2 ))
        fi
        if [ -n "`isJobRunning $jobID3`" ] ; then
            allocatedGB_gridops=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "gridops")
            allocatedContainers_gridops=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "gridops")
            reservedGB_gridops=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedGB" "gridops")
            reservedContainers_gridops=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedContainers" "gridops")
            numOfSlots_gridops=$(getNumSlots $allocatedGB_gridops $allocatedContainers_gridops)
            numMapTask3=$(getNumTasksForJobId $jobID3 MAP running)
            numReduceTask3=$(getNumTasksForJobId $jobID3 REDUCE running)
            numTask3=$(( $numMapTask3+$numReduceTask3 ))
        fi
        if [ -n "`isJobRunning $jobID4`" ] ; then
            allocatedGB_search=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedGB" "search")
            allocatedContainers_search=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "AllocatedContainers" "search")
            reservedGB_search=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedGB" "search")
            reservedContainers_search=$(getJTMetricsInfo $JOBTRACKER $RMProcessID "ReservedContainers" "search")
            numOfSlots_search=$(getNumSlots $allocatedGB_search $allocatedContainers_search)
            numMapTask4=$(getNumTasksForJobId $jobID4 MAP running)
            numReduceTask4=$(getNumTasksForJobId $jobID4 REDUCE running)
            numTask4=$(( $numMapTask4+$numReduceTask4 ))
        fi
        echo "###################"
        echo "Number of map tasks for jobID1: $numMapTask1"
        echo "Number of reduce tasks for jobID1: $numReduceTask1"
        echo "###### Number of tasks for jobID1: $numTask1"

        echo "Number of map tasks for jobID2: $numMapTask2"
        echo "Number of reduce tasks for jobID2: $numReduceTask2"
        echo "###### Number of tasks for jobID2: $numTask2"

        echo "Number of map tasks for jobID3: $numMapTask3"
        echo "Number of reduce tasks for jobID3: $numReduceTask3"
        echo "###### Number of tasks for jobID3: $numTask3"

        echo "Number of map tasks for jobID4: $numMapTask4"
        echo "Number of reduce tasks for jobID4: $numReduceTask4"
        echo "###### Number of tasks for jobID4: $numTask4"

        echo "***** Number of map tasks for all Jobs: $numMapTask1, $numMapTask2, $numMapTask3, $numMapTask4"
        echo "***** Number of reduce tasks for all Jobs: ${numReduceTask1}, ${numReduceTask2}, ${numReduceTask3}, ${numReduceTask4}"
        echo "***** Total number of tasks for each job: $numTask1, $numTask2, $numTask3, $numTask4"
        totalTasks=$(( $numTask1+$numTask2+$numTask3+$numTask4 ))
        echo "################## Total number of running tasks: $totalTasks"

        echo "##########################################"
        echo "AllocatedGB default: $allocatedGB_default"
        echo "AllocatedContainers default: $allocatedContainers_default"
        echo "ReservedGB default: $reservedGB_default"
        echo "ReservedContainers default: $reservedContainers_default"
        echo "Number of slots allocated for map task normal job $jobID1 on default queue: $numOfSlots_default"
        
        echo "AllocatedGB grideng: $allocatedGB_grideng"
        echo "AllocatedContainers grideng: $allocatedContainers_grideng"
        echo "ReservedGB grideng: $reservedGB_grideng"
        echo "ReservedContainers grideng: $reservedContainers_grideng"
        echo "Number of slots allocated for map task highRAM $jobID2 on grideng queue: $numOfSlots_grideng"

        echo "AllocatedGB gridops: $allocatedGB_gridops"
        echo "AllocatedContainers gridops: $allocatedContainers_gridops"
        echo "ReservedGB gridops: $reservedGB_gridops"
        echo "ReservedContainers gridops: $reservedContainers_gridops"
        echo "Number of slots allocated for map task highRAM $jobID3 on gridops queue: $numOfSlots_gridops"

        echo "AllocatedGB search: $allocatedGB_search"
        echo "AllocatedContainers search: $allocatedContainers_search"
        echo "ReservedGB search: $reservedGB_search"
        echo "ReservedContainers search: $reservedContainers_search"
        echo "Number of slots allocated for map task normal job $jobID4 on search queue: $numOfSlots_search"
        echo "###########################################"

        if [ "$numMapTask1" -gt 0 -a "$numReduceTask1" -eq 0 ]; then
            if [ "$numOfSlots_default" -gt 0 -a "$expectedSlots_normal" -ne "$numOfSlots_default" ]; then
                setFailCase "Number of slots for normal Map Task on default queue is not taken $expectedSlots_normal slots but $numOfSlots_default"
            fi
        fi
        if [ $numReduceTask1 -gt 0 -a $numMapTask1 -eq 0 ]; then
            if [ "$numOfSlots_default" -gt 0 -a $expectedSlots_normal -ne $numOfSlots_default ]; then
                setFailCase "Number of slots for normal Reduce Task on default is not taken $expectedSlots_normal slots but $numOfSlots_default"
            fi
        fi
        if [ $numMapTask2 -gt 0 -a $numReduceTask2 -eq 0 ]; then
            if [ "$numOfSlots_grideng" -gt 0 -a $expectedSlots_highRAM -ne $numOfSlots_grideng ]; then
                setFailCase "Number of slots for highRAM Map Task on grideng is not taken $expectedSlots_highRAM slots but $numOfSlots_grideng"
            fi
        fi
        if [ $numReduceTask2 -gt 0 -a $numMapTask2 -eq 0 ]; then
            if [ "$numOfSlots_grideng" -gt 0 -a $expectedSlots_highRAM -ne $numOfSlots_grideng ]; then
                setFailCase "Number of slots for highRAM Reduce Task on grideng queue is not taken $expectedSlots_highRAM slots but $numOfSlots_grideng"
            fi
        fi
        if [ $numMapTask3 -gt 0 -a $numReduceTask3 -eq 0 ]; then
            if [ "$numOfSlots_gridops" -gt 0 -a $expectedSlots_highRAM -ne $numOfSlots_gridops ]; then
                setFailCase "Number of slots for Map Task of highRAM $jobID3 on gridops queue is not taken $expectedSlots_highRAM slots but $numOfSlots_gridops"
            fi
        fi
        if [ $numReduceTask3 -gt 0 -a $numMapTask3 -eq 0 ]; then
            if [ "$numOfSlots_gridops" -gt 0 -a $expectedSlots_highRAM -ne $numOfSlots_gridops ]; then
                setFailCase "Number of slots for Reduce Task of highRAM $jobID3 on gridops queue is not taken $expectedSlots_highRAM slots but $numOfSlots_gridops"
            fi
        fi
        if [ $numMapTask4 -gt 0 -a $numReduceTask4 -eq 0 ]; then
            if [ "$numOfSlots_search" -gt 0 -a $expectedSlots_normal -ne $numOfSlots_search ]; then
                setFailCase "Number of slots for Map Task of normal $jobID4 on search queue is not taken $expectedSlots_normal slots but $numOfSlots_search"
            fi
        fi
        if [ $numReduceTask4 -gt 0 -a $numMapTask4 -eq 0 ]; then
            if [ "$numOfSlots_search" -gt 0 -a $expectedSlots_normal -ne $numOfSlots_search ]; then
                setFailCase "Number of slots for Reduce Task of normal $jobID4 on search queue is not taken $expectedSlots_normal slots but $numOfSlots_search"
            fi
        fi
        sleep 3
    done

    wait $processID1
    wait $processID2
    wait $processID3
    wait $processID4
    sleep 10

    getKerberosTicketForUser ${HADOOPQA_USER}
    setKerberosTicketForUser ${HADOOPQA_USER}

    return=$(verifySucceededJob $jobID1)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "First normal job $jobID1 failed"
    fi
    return=$(verifySucceededJob $jobID2)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Second normal job $jobID2 failed"
    fi
    return=$(verifySucceededJob $jobID3)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Third normal job $jobID3 failed"
    fi
    return=$(verifySucceededJob $jobID4)
    if [ "$return" != "SUCCEEDED" ] ; then
        echo "Return code: $return"
        setFailCase "Fourth normal job $jobID4 failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


##################################################
# Main function
##################################################

# User Limit
# User limit at 100% - commented out due to bug #4588509
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler100.xml"
test_userLimit100_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes
test_userLimit100_2normalJobsWithMapReduceTasksEqualNumNodes
test_userLimit100_3normalJobsQueueUp
# User limit at 25%
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler25.xml"

test_userLimit25_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes
test_userLimit25_2normalJobsWithMapReduceTasksEqualNumNodes
test_userLimit25_4normalJobsQueueUp
test_userLimit25_6normalJobsQueueUp
test_userLimit25_3normalJobsQueueUp
test_userLimit25_7normalJobsAddingAndQueueUp

# User limit at 41%
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler41.xml"
test_userLimit41_4normalJobsQueueUp
test_userLimit41_4normalJobsRunningSequencially
# User limit at 110%
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler110.xml"
test_userLimit_GreaterThan100
# User limit at 0% - commented out due to invalid bug #4592235
#setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler0.xml"
#test_userLimit_Zero
# Guarantee Capacity
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler25.xml"
test_guaranteeCapacity_differentUsersOnDifferentQueues_userLimit25
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler100-multiplequeues.xml"
test_guaranteeCapacity_differentUsersOnDifferentQueuesInParallel_userLimit100 # always has one job failed since running as same user
test_guaranteeCapacity_differentUsersOnDifferentQueuesInSequence_userLimit100
test_guaranteeCapacity_differentUsersOnDifferentQueuesResourceReclaimed_userLimit100
test_guaranteeCapacity_differentUsersOnDifferentQueuesResourceFreedUp_userLimit100
# High RAM Jobs
setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler25.xml"
setupMetricsConfig
test_highRAM_hiRAMMapTake2Slots
test_highRAM_hiRAMReduceTake2Slots
test_highRAM_hiRAMMapTake4SlotsAndReduceTake2Slots
# commented out due to bug #4620158
#test_highRAM_hiRAMMapOnlyExceedMemoryLimit
#test_highRAM_hiRAMReduceOnlyExceedMemoryLimit
#test_highRAM_hiRAMMapAndReduceExceedMemoryLimit
test_highRAM_hiRAMMapAndReduceLowMemory
test_highRAM_userLimitSupportHighRAMJob
test_highRAM_slotUtilization

restartCluster_newConfig
#runSimpleSleepJob

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
