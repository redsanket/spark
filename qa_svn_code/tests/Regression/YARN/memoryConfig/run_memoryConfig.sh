#!/bin/bash 

. $WORKSPACE/lib/restart_cluster_lib.sh

# Setting Owner of this TestSuite
OWNER="bui"

export USER_ID=`whoami`
export IN_ACTIVE_TRACKERS=1
export IN_BLACKLISTED=0

######################################################################################
# This function returns the JOBID of the task submitted
######################################################################################
function getJobIdfromOutput {
    echo "job_"$(echo "$output" |  sed 's| |\n|g' | grep job_ | head -1 | cut -d ';' -f1 | sed 's|_|@|1' | cut -d '@' -f2)
    return $?
}

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
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/memoryConfig_conf/"
    if [ ! -d "$NEW_CONFIG_LOCATION" ] ; then
        ssh ${USER_ID}@$JOBTRACKER mkdir "$NEW_CONFIG_LOCATION"
        ssh ${USER_ID}@$JOBTRACKER chmod -R 755 "$NEW_CONFIG_LOCATION"
    fi
    # Copy all the conf files to new location in JT host
    ssh ${USER_ID}@"$JOBTRACKER" cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@"$JOBTRACKER" ls -l "$NEW_CONFIG_LOCATION"
    ssh ${USER_ID}@"$JOBTRACKER" chmod 755 ${NEW_CONFIG_LOCATION}mapred-site.xml
    # Setup new Config settings
    #ssh ${USER_ID}@"$JOBTRACKER" exec /usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER} hadoop_qa_restart_config.HADOOP_CONF_DIR="$NEW_CONFIG_LOCATION"
    # Restart JT or refreshNode
    resetJobTracker stop $NEW_CONFIG_LOCATION
    #stopJobTracker $JOBTRACKER
    sleep 10
    resetJobTracker start $NEW_CONFIG_LOCATION
    #startJobTracker $JOBTRACKER
    sleep 20
    echo "New HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "ps aux | grep -v grep | grep jobtracker | sed 's/classpath /@/' | cut -d '@' -f2 | cut -d ':' -f1"
}

#################################################
# Function to reset JT to default
#################################################
function resetJTToDefault {
    ssh ${USER_ID}@$JOBTRACKER rm -r $NEW_CONFIG_LOCATION
    #ssh ${USER_ID}@$JOBTRACKER exec /usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER} hadoop_qa_restart_config.HADOOP_CONF_DIR="undefined"
    resetJobTracker stop
    #stopJobTracker $JOBTRACKER
    sleep 10
    resetJobTracker start
    #startJobTracker $JOBTRACKER
    sleep 10
    echo "Default HADOOP_CONF_DIR:"
    ssh $JOBTRACKER "ps aux | grep -v grep | grep jobtracker | sed 's/classpath /@/' | cut -d '@' -f2 | cut -d ':' -f1"
}

###########################################
# Function to check if memory management parameters in mapred-site.xml exist
###########################################
function memoryConfig_Params {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_Params"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. SSH to each Task Tracker and get the required parameters"
    echo "3. Verify these params mapred.cluster.map.memory.mb=1536,"
    echo "mapred.cluster.reduce.memory.mb=4096, mapred.cluster.max.map.memory.mb=6144," 
    echo "mapred.cluster.max.reduce.memory.mb=8192"
    echo "*************************************************************"
    getJobTracker
    echo "Job Tracker host: $JOBTRACKER"
    scp $JOBTRACKER:${HADOOP_CONF_DIR}mapred-site.xml .

    mapMemorymb=`getValueFromField mapred-site.xml mapred.cluster.map.memory.mb`
    echo "Reading mapred.cluster.map.memory.mb param: $mapMemorymb"
    if [ $mapMemorymb -ne 1536 ] ; then
        setFailCase "mapred.cluster.map.memory.mb doesn't have correct param $mapMemorymb vs. 1536"
    fi
    
    reduceMemorymb=`getValueFromField mapred-site.xml mapred.cluster.reduce.memory.mb`
    echo "Reading mapred.cluster.reduce.memory.mb param: $reduceMemorymb"
    if [ $reduceMemorymb -ne 4096 ] ; then
        setFailCase "mapred.cluster.reduce.memory.mb doesn't have correct param $reduceMemorymb vs. 4096"
    fi

    maxmapMemorymb=`getValueFromField mapred-site.xml mapred.cluster.max.map.memory.mb`
    echo "Reading mapred.cluster.max.map.memory.mb param: $maxmapMemorymb"
    if [ $maxmapMemorymb -ne 6144 ] ; then
        setFailCase "mapred.cluster.max.map.memory.mb doesn't have correct param $maxmapMemorymb vs. 6144"
    fi

    maxreduceMemorymb=`getValueFromField mapred-site.xml mapred.cluster.max.reduce.memory.mb`
    echo "Reading mapred.cluster.max.reduce.memory.mb param: $maxreduceMemorymb"
    if [ $maxreduceMemorymb -ne 8192 ] ; then
        setFailCase "mapred.cluster.max.reduce.memory.mb doesn't have correct param $maxreduceMemorymb vs. 8192"
    fi

    # Clean up the file
    rm mapred-site.xml
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##########################################
# Function to check job running without specifying map and reduce memory mb
###########################################
function memoryConfig_NotSpecifiedInJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_NotSpecifiedInJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get Job Trackers"
    echo "2. SSH to Job Tracker and delete mapred.job.map.memory.mb and mapred.job.reduce.memory.mb values"
    echo "3. Restart the cluster"
    echo "4. Submit a job to the cluster" 
    echo "5. Verify exception thrown -1 memForMapTasks -1 memForReduceTasks): Invalid job requirements."
    echo "6. Clean up mapred-site.xml and scp back to JT"
    echo "7. Restart the cluster"
    echo "*************************************************************"   
    setupNewMapredConfigFile 
    scp $JOBTRACKER:${NEW_CONFIG_LOCATION}mapred-site.xml ./mapred-site.xml
    modifyValueOfAField mapred-site.xml mapred.job.map.memory.mb
    modifyValueOfAField mapred-site.xml mapred.job.reduce.memory.mb
    scp mapred-site.xml $JOBTRACKER:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $JOBTRACKER failed"
    fi
    echo "Submit a job now"
    output=$($HADOOP_COMMON_HOME/bin/hadoop --config ${NEW_CONFIG_LOCATION} jar $HADOOP_EXAMPLES_JAR sleep -m 10 -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID
    jobID=`getJobIdfromOutput "$output"`
    echo "Job ID: $jobID"
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    echo "Verify exception thrown ..."
    cat "$output" | grep "$jobID(-1 memForMapTasks -1 memForReduceTasks): Invalid job requirements"
    if [ $? -eq 0 ] ; then
        setFailCase "No Exception found for job without specified map and reduce memory"
    fi
    echo "Clean up mapred-site.xml file"
    modifyValueOfAField mapred-site.xml mapred.job.map.memory.mb 1536
    modifyValueOfAField mapred-site.xml mapred.job.reduce.memory.mb 4096
    echo "SCP back the clean script mapred-site.xml"
    scp mapred-site.xml hadoopqa@$JOBTRACKER:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $JOBTRACKER failed"
    fi
    resetJTToDefault
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##########################################
# Function to check job running without specifying map memory mb
###########################################
function memoryConfig_MapNotSpecified {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_MapNotSpecified"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get Job Trackers"
    echo "2. SSH to Job Tracker and delete mapred.job.map.memory.mb value"
    echo "3. Restart the cluster"
    echo "4. Submit a job to the cluster" 
    echo "5. Verify exception thrown -1 memForMapTasks -1 memForReduceTasks): Invalid job requirements."
    echo "6. Clean up mapred-site.xml and scp back to JT"
    echo "7. Restart the cluster"
    echo "*************************************************************"   
    setupNewMapredConfigFile
    scp $JOBTRACKER:${NEW_CONFIG_LOCATION}mapred-site.xml ./mapred-site.xml
    modifyValueOfAField mapred-site.xml mapred.job.map.memory.mb
    scp mapred-site.xml $JOBTRACKER:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $JOBTRACKER failed"
    fi

    echo "Submit a job now"
    output=$($HADOOP_COMMON_HOME/bin/hadoop --config ${NEW_CONFIG_LOCATION} jar $HADOOP_EXAMPLES_JAR sleep -m 10 -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID
    jobID=`getJobIdfromOutput "$output"`
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    echo "Verify exception thrown ..."
    grep "${jobID}(-1 memForMapTasks -1 memForReduceTasks): Invalid job requirements" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "No Exception found for job without specified map memory"
    fi
    echo "Clean up mapred-site.xml file"
    modifyValueOfAField mapred-site.xml mapred.job.map.memory.mb 1536
    echo "SCP back the clean script mapred-site.xml"
    scp mapred-site.xml hadoopqa@$JOBTRACKER:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $JOBTRACKER failed"
    fi
    resetJTToDefault
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##########################################
# Function to check job running without specifying reduce memory mb
###########################################
function memoryConfig_ReduceNotSpecified {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_ReduceNotSpecified"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get Job Trackers"
    echo "2. SSH to Job Tracker and delete mapred.job.reduce.memory.mb value"
    echo "3. Restart the cluster"
    echo "4. Submit a job to the cluster" 
    echo "5. Verify exception thrown -1 memForMapTasks -1 memForReduceTasks): Invalid job requirements."
    echo "6. Clean up mapred-site.xml and scp back to JT"
    echo "7. Restart the cluster"
    echo "*************************************************************"   
    setupNewMapredConfigFile
    scp $JOBTRACKER:${NEW_CONFIG_LOCATION}mapred-site.xml ./mapred-site.xml
    modifyValueOfAField mapred-site.xml mapred.job.map.memory.mb
    scp mapred-site.xml $JOBTRACKER:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $JOBTRACKER failed"
    fi

    echo "Submit a job now"
    output=$($HADOOP_COMMON_HOME/bin/hadoop --config ${NEW_CONFIG_LOCATION} jar $HADOOP_EXAMPLES_JAR sleep -m 10 -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID
    echo "$output"
    jobID=`getJobIdfromOutput "$output"`
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    echo "Verify exception thrown ..."
    grep "${jobID}(-1 memForMapTasks -1 memForReduceTasks): Invalid job requirements" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "No Exception found for job without specified reduce memory"
    fi

    echo "Clean up mapred-site.xml file"
    modifyValueOfAField mapred-site.xml mapred.job.reduce.memory.mb 4096
    echo "SCP back the clean script mapred-site.xml"
    scp mapred-site.xml hadoopqa@$JOBTRACKER:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $JOBTRACKER failed"
    fi
    resetJTToDefault
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##########################################
# Function to check job running with low memory requirement
###########################################
function memoryConfig_JobWithLowMemoryRequirement {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_JobWithLowMemoryRequirement"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a job with low map and reduce memory requirement (1024) than the default" 
    echo "2. Verify exception thrown running beyond memory-limits"
    echo "*************************************************************"  
    echo "Submit a job with low memory requirement ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.map.memory.mb=520 -Dmapred.job.reduce.memory.mb=520 -m 10  -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID

    grep "running beyond memory-limits" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "No Exception found for job running with low memory requirement"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run normal job
###########################################
function memoryConfig_NormalJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_NormalJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a normal job with defaul map (1536) and reduce memory (4096)"
    echo "2. Verify the job running sucessfully"
    echo "*************************************************************"
    echo "Submit a normal job ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.map.memory.mb=1536 -Dmapred.job.reduce.memory.mb=4096 -m 10  -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID
    if [ $? -eq 0 ] ; then
        echo "Process ended `date`"
    fi
    jobID=`getJobIdfromOutput "$output"`
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    grep "map 100% reduce 100% .* Job complete: ${jobID}" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "Normal job is not running sucessfully with default map and reduce memory"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run a high map memory job
###########################################
function memoryConfig_HighMapMemoryJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_HighMapMemoryJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a hi-RAM job with mapred.job.map.memory.mb=7168"
    echo "2. Verify the job fails with exception Exceeds the cluster's max-memory-limit."
    echo "*************************************************************"
    echo "Submit a hi-RAM job ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.map.memory.mb=7168 -m 10 -mt 100000 -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID

    grep "Exceeds the cluster's max-memory-limit" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "High Map memory job does not failed even though it exceeds max-memory-limit"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run a high reduce memory job
###########################################
function memoryConfig_HighReduceMemoryJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_HighReduceMemoryJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a hi-RAM job with mapred.job.map.memory.mb=7168"
    echo "2. Verify the job fails with exception Exceeds the cluster's max-memory-limit."
    echo "*************************************************************"
    echo "Submit a hi-RAM job ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.reduce.memory.mb=9126 -m 10 -mt 100000 -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID

    grep "Exceeds the cluster's max-memory-limit" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "High Reduce Memory job does not failed even though it exceeds max-memory-limit"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run a high map and high reduce memory job
###########################################
function memoryConfig_HighMapReduceMemoryJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_HighMapReduceMemoryJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a hi-RAM job with mapred.job.map.memory.mb=7168 and mapred.job.reduce.memory.mb=9126"
    echo "2. Verify the job fails with exception Exceeds the cluster's max-memory-limit."
    echo "*************************************************************"
    echo "Submit a hi-RAM job ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.map.memory.mb=7168 -Dmapred.job.reduce.memory.mb=9126 -m 10 -mt 100000 -r 10 2>&1)
    ProcessID=$!
    wait $ProcessID
    grep "Exceeds the cluster's max-memory-limit" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "High Map and Reduce Memory job does not failed even though it exceeds max-memory-limit"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run a job which occupies 2 map slots to run a map task
###########################################
function memoryConfig_2SlotsMapJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_2SlotsMapJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a job with mapred.job.map.memory.mb=3072"
    echo "2. Verify the job run sucessfully"
    echo "*************************************************************"
    echo "Submit a job using 2 map slots per map task ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.map.memory.mb=3072 -m 10 -r 10 -mt 10000 -rt 10000 2>&1)
    ProcessID=$!
    wait $ProcessID
    jobID=`getJobIdfromOutput "$output"`
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    grep "map 100% reduce 100% .* Job complete: ${jobID}" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "Map task requires 2 map slots failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run a job which occupies 2 reduce slots to run a reduce task
###########################################
function memoryConfig_2SlotsReduceJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_2SlotsReduceJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a job with mapred.job.reduce.memory.mb=8192"
    echo "2. Verify the job run sucessfully"
    echo "*************************************************************"
    echo "Submit a job using 2 reduce slots per reduce task ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.reduce.memory.mb=8192 -m 10 -r 10 -mt 10000 -rt 10000 2>&1)
    ProcessID=$!
    wait $ProcessID
    jobID=`getJobIdfromOutput "$output"`
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    grep "map 100% reduce 100% .* Job complete: ${jobID}" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "Reduce task requires 2 reduce slots failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

###########################################
# Function to run a job which occupies 4 map slots/map task and 2 reduce slots/reduce task
###########################################
function memoryConfig_4MapSlots2ReduceSlotsJob {
    echo "*************************************************************"
    TESTCASE_DESC="memoryConfig_4MapSlots2ReduceSlotsJob"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Submit a job with mapred.job.map.memory.mb=6144 and mapred.job.reduce.memory.mb=8192"
    echo "2. Verify the job run sucessfully"
    echo "*************************************************************"
    echo "Submit a job using 4 map slots and 2 reduce slots per task ..."
    output=$($HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_EXAMPLES_JAR sleep -Dmapred.job.map.memory.mb=6144 -Dmapred.job.reduce.memory.mb=8192 -m 10 -r 10 -mt 10000 -rt 10000 2>&1)
    ProcessID=$!
    wait $ProcessID
    jobID=`getJobIdfromOutput "$output"`
    if [ -z $jobID ] ; then
        setFailCase "Sleep job is not started"
    fi

    grep "map 100% reduce 100% .* Job complete: ${jobID}" -f "$output"
    if [ $? -eq 0 ] ; then
        setFailCase "4 map slots/task and 2 Reduce slots/task failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


###########################################
# Main program
###########################################

memoryConfig_Params
memoryConfig_NotSpecifiedInJob
memoryConfig_MapNotSpecified
memoryConfig_ReduceNotSpecified
memoryConfig_JobWithLowMemoryRequirement
memoryConfig_NormalJob
memoryConfig_HighMapMemoryJob
memoryConfig_HighReduceMemoryJob
memoryConfig_HighMapReduceMemoryJob
memoryConfig_2SlotsMapJob
memoryConfig_2SlotsReduceJob
memoryConfig_4MapSlots2ReduceSlotsJob


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
