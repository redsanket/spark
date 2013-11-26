#!/bin/bash

. $WORKSPACE/lib/restart_cluster_lib.sh

# Setting Owner of this TestSuite
OWNER="bui"

export USER_ID=`whoami`
export IN_ACTIVE_TRACKERS=1
export IN_BLACKLISTED=0
export SELECTED_TT="None"

###########################################
# Function to setup new mapred-site.xml config file at new location in TT and restart TT
###########################################
function setupNewMapredConfigFile {
    # Pick the first active TT
    sleep 10
    task_trackers=`getActiveTaskTrackers`
    if [ -z "$task_trackers" ] ; then
        setFailCase "Cannot get Active tracker list"
    fi
    SELECTED_TT=`echo $task_trackers | cut -d ' ' -f1`
    echo "Selected task tracker: $SELECTED_TT on $CLUSTER cluster"
    # Setup new location on TT
    NEW_CONFIG_LOCATION="/homes/${USER_ID}/nodeHealthCheck_conf/"
    NEW_HEALTHCHECK_LOCATION="/homes/${USER_ID}/nodeHealthCheck/"
    if [  -z "`ssh ${USER_ID}@$SELECTED_TT ls -d $NEW_CONFIG_LOCATION`" ] ; then
        echo "Create new Config folder in $NEW_CONFIG_LOCATION"
        ssh $SELECTED_TT mkdir "$NEW_CONFIG_LOCATION"
    fi
    if [ -z "`ssh ${USER_ID}@$SELECTED_TT ls -d $NEW_HEALTHCHECK_LOCATION`" ] ; then
        echo "Create new Health Check folder in $NEW_HEALTHCHECK_LOCATION"
        ssh $SELECTED_TT mkdir "$NEW_HEALTHCHECK_LOCATION"
    fi
    ssh $SELECTED_TT chmod -R 755 "$NEW_CONFIG_LOCATION"
    ssh $SELECTED_TT chmod -R 755 "$NEW_HEALTHCHECK_LOCATION"
    # Copy all the conf files to new location in TT host
    echo "Copy files from $HADOOP_CONF_DIR to $NEW_CONFIG_LOCATION"
    ssh "$SELECTED_TT" cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"
    echo "Change permissions of mapred-site.xml file"
    ssh $SELECTED_TT chmod 755 "$NEW_CONFIG_LOCATION/mapred-site.xml"
    echo "List files in $NEW_CONFIG_LOCATION:"
    ssh "$SELECTED_TT" ls -l "$NEW_CONFIG_LOCATION"
    # Modify mapred-site.xml file for nodeHealthCheck path
    echo "Modify $NEW_CONFIG_LOCATION/mapred-site.xml in host $SELECTED_TT with new value"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $SELECTED_TT $NEW_CONFIG_LOCATION/mapred-site.xml "mapreduce.tasktracker.healthchecker.script.path" "${NEW_HEALTHCHECK_LOCATION}health_check"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $SELECTED_TT $NEW_CONFIG_LOCATION/mapred-site.xml "mapreduce.tasktracker.healthchecker.interval" "10000"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $SELECTED_TT $NEW_CONFIG_LOCATION/mapred-site.xml "mapreduce.tasktracker.healthchecker.script.timeout" "5000"
    if [ $? != 0 ] ; then
        setFailCase "Failed to modify healthChecker path on $SELECTED_TT tasktracker"
    fi
    # Copy nodeHealthCheck to new location specified in the new mapred-site.xml
    ssh $SELECTED_TT cp "${HADOOP_QA_ROOT}/gs/conf/local/health_check" "$NEW_HEALTHCHECK_LOCATION"
    ssh $SELECTED_TT chmod -R 755 ${NEW_HEALTHCHECK_LOCATION}health_check
    ssh $SELECTED_TT ls -l $NEW_HEALTHCHECK_LOCATION
    # Restart TT or refreshNode
    resetNode $SELECTED_TT tasktracker stop $NEW_CONFIG_LOCATION
    resetNode $SELECTED_TT datanode stop $NEW_CONFIG_LOCATION
    sleep 5
    resetNode $SELECTED_TT datanode start $NEW_CONFIG_LOCATION
    resetNode $SELECTED_TT tasktracker start $NEW_CONFIG_LOCATION
    sleep 5
    echo "New HADOOP_CONF_DIR on $SELECTED_TT:"
    ssh $SELECTED_TT "${HADOOP_COMMON_CMD} --config $NEW_CONFIG_LOCATION classpath | cut -d ':' -f1"
}

#################################################
# Function to reset TT to default
#################################################
function resetTTToDefault {
    ssh ${USER_ID}@$SELECTED_TT rm -R $NEW_CONFIG_LOCATION
    ssh ${USER_ID}@$SELECTED_TT rm -R $NEW_HEALTHCHECK_LOCATION
    resetNode $SELECTED_TT tasktracker stop
    resetNode $SELECTED_TT datanode stop
    sleep 5
    resetNode $SELECTED_TT datanode start
    resetNode $SELECTED_TT tasktracker start
    sleep 5
    echo "Default HADOOP_CONF_DIR:"
    ssh $SELECTED_TT "${HADOOP_COMMON_CMD} --config $NEW_CONFIG_LOCATION classpath | cut -d ':' -f1"
}

###########################################
# Function to get full path and file name of node health check
###########################################
function getNodeHealthCheckFile {
    ssh $1  "echo `getValueFromField ${HADOOP_CONF_DIR}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" 
    return $?
}

########################################
# Function to insert a string before exit statement
# Params: $1 is the filename; $2 is the string to insert
##########################################
function insertAStringToAFileBeforeExit {
    # some health check file has exit 0 at the end
    err="$2"
    sed -i "$ i\
        $err" $1
}

##########################################
# Function to scp a file from JT, modify it, and scp back
# Params: $1 is the remote host; $2 is the file name with full path; $3 is the appended string
##########################################
function scpFileFromRemoteHost_AppendAStringAndSendBack {
    scp hadoopqa@$1:$2 .
    filepath=`echo $script_file`
    filename=$(basename $filepath)
    path=$(dirname $filepath)
    echo "File name: $filename"
    echo "Path: $filepath"
    appendStringToAFile $filename "$3"
    scp $filename hadoopqa@$1:$filepath
}

##########################################
# Function to scp a file from JT, modify it, and scp back
# Params: $1 is the remote host; $2 is the file name with full path; $3 is the appended string
##########################################
function scpFileFromRemoteHost_InsertAStringAndSendBack {
    scp hadoopqa@$1:$2 .
    filepath=`echo $2`
    filename=$(basename $filepath)
    path=$(dirname $filepath)
    echo "File name: $filename"
    echo "Path: $filepath"
    insertAStringToAFileBeforeExit $filename "$3"
    scp $filename hadoopqa@$1:$filepath
    #ssh hadoopqa@$1 cat $2
}

##########################################
# Function to scp a xml file from JT, modify a specified field 
# with new value, and scp back
# Params: $1 is the remote host; $2 is the file path; $3 is the field; 
# $4 is the new value; 
##########################################
function scpFileFromRemoteHost_ModifyAValueAndSendBack {
    scp hadoopqa@$1:$2 .
    filepath=`echo $2`
    filename=$(basename $filepath)
    path=$(dirname $filepath)
    modifyValueOfAField $filename "$3" "$4"
    scp $filename hadoopqa@$1:$path
}

##########################################
# Function to check if a TT is in the active tracker list
##########################################
function isInActiveTrackerList {
    trackers=`getActiveTaskTrackers`
    local matched=$(echo $trackers | tr -d '\n'| grep $1)
    if [ -z "$matched" ] ; then
        IN_ACTIVE_TRACKERS=0
    else 
        IN_ACTIVE_TRACKERS=1
    fi
}

#########################################
# Function to check if a TT is in the blacklisted list
##########################################
function isInBlacklistedList {
    blacklisted_trackers=`getBlacklistedTaskTrackers`
    local in_blacklisted=`echo $blacklisted_trackers | tr -d '\n' | grep $1`
    if [ -z "$in_blacklisted" ] ; then
        IN_BLACKLISTED=0
    else
        IN_BLACKLISTED=1
    fi
}

##########################################
# Function to check blacklisted info on WebUI
##########################################
function checkBlacklistedReasonOnWebUI {
    getJobTracker
    wget --quiet "http://$JOBTRACKER:8088/yarn/nodes" -O $ARTIFACTS_DIR/blacklisted 
    cat $ARTIFACTS_DIR/blacklisted | grep $1 | grep "$2" | grep "$3"
    return $?
}

###########################################
# Function to clean up node heath check script
###########################################
function cleanupHealthCheckScript {
    filepath=$2
    filename=$(basename $filepath)
    path=$(dirname $filepath)
    echo "File name: $filename"
    echo "Path: $filepath"

    deleteLinesWithPatternInFile $filename "$3"
    echo "SCP back the clean health_check script $filename"
    scp $filename hadoopqa@$1:$path
}

###########################################
# Function to check for existing of health check params
###########################################
function nodeHealthCheck_ParamsExist {
    echo "*************************************************************"
    TESTCASE_DESC="nodeHealthCheck_ParamsExist"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. SSH to each Task Tracker and get the required parameters"
    echo "3. Verify if they are not blank"
    echo "*************************************************************"
    task_trackers=`getActiveTaskTrackers`
    tt_hosts=`echo $task_trackers | tr -d '\n'`
    if [ -z "$tt_hosts" ] ; then
        setFailCase "Cannot get Active tracker list"
    fi
    echo "List of active Task Trackers: $tt_hosts"
    for tt_host in $tt_hosts; do
        echo "SSH to $tt_host to check parameters"
        ssh $tt_host "echo `getValueFromField ${HADOOP_CONF_DIR}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file 
        script_path=`cat ${ARTIFACTS}/file`
        echo "Script path: $script_path"
        if [ -z "$script_path" ] ; then
            setFailCase "Empty health check script path on $tt_host"
        fi
        ssh $tt_host "echo `getValueFromField ${HADOOP_CONF_DIR}mapred-site.xml mapreduce.tasktracker.healthchecker.interval`" > ${ARTIFACTS}/file 
        script_interval=`cat ${ARTIFACTS}/file`
        echo "Script interval: $script_interval"
        if [ -z "$script_interval" ] ; then
            setFailCase "Empty health check interval on $tt_host"
        fi
        ssh $tt_host "echo `getValueFromField ${HADOOP_CONF_DIR}mapred-site.xml mapreduce.tasktracker.healthchecker.script.timeout`" > ${ARTIFACTS}/file
        script_timeout=`cat ${ARTIFACTS}/file`
        echo "Script timeout: $script_timeout"
        if [ -z "$script_timeout" ] ; then
            setFailCase "Empty health check script timeout on $tt_host"
        fi
    done

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

############################################
# Function to check status of TT Node when script outputs ERROR at the beginning
############################################
function nodeHealthCheck_ErrAtBeginning {
    echo "*************************************************************"
    TESTCASE_DESC="nodeHealthCheck_ErrAtBeginning"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. SSH to the first Task Tracker and get the health_check script path"
    echo "3. scp the file to local machine (gateway)"
    echo "4. Append an ERROR string to the file"
    echo "5. scp the file back to the first Task Tracker host"
    echo "6. Wait for 2 minutes (default mapred.healthChecker.interval)" 
    echo "7. Verify the Task Tracker is removed from Active list"
    echo "8. Verify the Task Tracker is in the blacklisted list"
    echo "9. Verify NODE Unhealthy from JT log"
    echo "10. Clean up health_check script"
    echo "***************************************************************"
    local error_string="echo -n 'ERROR STRING'"
    getJobTracker
    echo "Selected Task Tracker: $SELECTED_TT"
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"
    scpFileFromRemoteHost_InsertAStringAndSendBack $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 20
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 1 ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is still in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 0 ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is still not in blacklisted list"
    fi 

    JTlogs=`checkBlacklistedInfoJTLogFile $SELECTED_TT NODE_UNHEALTHY`
    echo "Found JT logs: $JTlogs in Job Tracker $JOBTRACKER"
    if [ -z "$JTlogs" ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is not found in JT log"
    fi

    echo "Cleaning up health_check script"
    cleanupHealthCheckScript $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $first_host failed"
    fi
   # Wait for the node becomes healthy again before the next test
    sleep 30
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

############################################
# Function to check status of TT Node when script outputs ERROR not at the beginning
############################################
function nodeHealthCheck_ErrNotAtBeginning {
    echo "*************************************************************"
    TESTCASE_DESC="nodeHealthCheck_ErrNotAtBeginning"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "1. Get all active Task Trackers"
    echo "2. SSH to the first Task Tracker and get the health_check script path"
    echo "3. scp the file to local machine (gateway)"
    echo "4. Append an ERROR string to the file"
    echo "5. scp the file back to the first Task Tracker host"
    echo "6. Wait for 2 minutes (default mapred.healthChecker.interval)"
    echo "7. Verify the Task Tracker is not removed from Active list"
    echo "8. Verify the Task Tracker is not in the blacklisted list"
    echo "9. Verify No Node Unhealthy message in JT log"
    echo "10. Clean up health_check script"
    echo "***************************************************************"
    local error_string="echo -n 'Blacklist the node with ERROR STRING'"
    getJobTracker
    echo "Selected Task Tracker: $SELECTED_TT"
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"
    scpFileFromRemoteHost_InsertAStringAndSendBack $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi
    sleep 20

    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with ERROR not at the beginning is not found in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 1 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with ERROR not at the beginning is found in blacklisted list"
    fi

    cleanupHealthCheckScript $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi
    # Wait for the node becomes healthy again before the next test
    sleep 30

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

############################################
# Function to check status of TT Node when script outputs lowercase error at the beginning
############################################
function nodeHealthCheck_LowercaseErrAtBeginning {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. SSH to the first Task Tracker and get the health_check script path"
    echo "3. scp the file to local machine (gateway)"
    echo "4. Append an ERROR string to the file"
    echo "5. scp the file back to the first Task Tracker host"
    echo "6. Wait for 2 minutes (default mapred.healthChecker.interval)"
    echo "7. Verify the Task Tracker is not removed from Active list"
    echo "8. Verify the Task Tracker is not in the blacklisted list"
    echo "9. Verify NODE_UNHEALTHY is not in JT log"
    echo "10. Clean up health_check script"
    echo "***************************************************************"
    local error_string="echo -n 'error STRING'"
    echo "Selected Task Tracker: $SELECTED_TT"
    getJobTracker
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"
    scpFileFromRemoteHost_InsertAStringAndSendBack $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi
    sleep 20

    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with lower case error is not found in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    if [ $IN_BLACKLISTED -eq 1 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with lower case error is found in blacklisted list"
    fi

    cleanupHealthCheckScript $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $first_host failed"
    fi
    # Wait for the node becomes healthy again before the next test
    sleep 30

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

############################################
# Function to check status of TT Node when script outputs ERROR not at the beginning
############################################
function nodeHealthCheck_EmptyNonExistNonExecutable {
    echo "*************************************************************"
    TESTCASE_DESC="`echo $FUNCNAME`"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. SSH to the first Task Tracker and get the health_check script path"
    echo "3. scp the file to local machine (gateway), delete the value of the script path and scp it back to the TT"
    echo "4. Wait for 2 minutes (default mapred.healthChecker.interval)"
    echo "5. Verify the Task Tracker is not removed from Active list"
    echo "6. Verify the Task Tracker is not in the blacklisted list"
    echo "7. scp the file to local machine (gateway), replace the value of the script path with an invalid one, and scp it back to the TT"
    echo "8. Wait for 2 minutes (default mapred.healthChecker.interval)"
    echo "9. Verify the Task Tracker is not removed from Active list"
    echo "10. Verify the Task Tracker is not in the blacklisted list"
    echo "11. Clean up mapred-site.xml file and scp back to the TT" 
    echo "12. Change permission of health check script on TT to non-executable"
    echo "13. Wait for 2 minutes"
    echo "14. Verify the Task Tracker is not removed from Active list"
    echo "15. Verify the Task Tracker is not in the blacklisted list"
    echo "16. Change permission of health_check script back to executable again"
    echo "***************************************************************"
    getJobTracker
    echo "Selected Task Tracker: $SELECTED_TT"
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"

    filename="mapred-site.xml"
    echo "Change health check script to empty"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $SELECTED_TT ${NEW_CONFIG_LOCATION}$filename mapreduce.tasktracker.healthchecker.script.path  
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 20
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with no health check script path is not found in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 1 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with no health check script path is found in blacklisted list"
    fi

    echo "Change Health check script file to non existed one"
    scpFileFromRemoteHost_ModifyAValueAndSendBack $SELECTED_TT ${NEW_CONFIG_LOCATION}$filename mapreduce.tasktracker.healthchecker.script.path "non-exiting-file"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 20
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with non-existing health check script is not found in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 1 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with non-existing health check script is found in blacklisted list"
    fi

    echo "Clean up mapred-site.xml file"
    modifyValueOfAField $filename 'mapreduce.tasktracker.healthchecker.script.path' "${NEW_HEALTHCHECK_LOCATION}health_check"
    echo "SCP back the clean script mapred-site.xml"
    scp $filename hadoopqa@$SELECTED_TT:${NEW_CONFIG_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    echo "Change Health check script permission to non-executable"
    ssh $SELECTED_TT "chmod 600 ${NEW_HEALTHCHECK_LOCATION}health_check"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    ssh $SELECTED_TT "ls -l ${NEW_HEALTHCHECK_LOCATION}health_check"
    sleep 20
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with non-executable health check script is not found in active tracker list. Bug 4319822"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 1 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT with non-executable health check script is found in blacklisted list. Bug 4319822"
    fi

    echo "Reset mapred-site.xml file to be executable again"
    ssh $SELECTED_TT "chmod 755 ${NEW_HEALTHCHECK_LOCATION}health_check"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 30
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

############################################
# Function to check status of TT Node when ERROR message changes
############################################
function nodeHealthCheck_NewErrAtBeginning {
    echo "*************************************************************"
    TESTCASE_DESC="nodeHealthCheck_NewErrAtBeginning"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. SSH to the first Task Tracker and get the health_check script path"
    echo "3. scp the file to local machine (gateway)"
    echo "4. Append an ERROR string to the file"
    echo "5. scp the file back to the first Task Tracker host"
    echo "6. Wait for 2 minutes (default mapred.healthChecker.interval)" 
    echo "7. Verify NODE Unhealthy with old ERROR String from WebUI"
    echo "8. Append a new ERROR string to the script file"
    echo "9. scp the file back to the first Task Tracker host"
    echo "10. Wait for 2 minutes (default mapred.healthChecker.interval)" 
    echo "11. Verify the Task Tracker is removed from Active list"
    echo "12. Verify the Task Tracker is in the blacklisted list"
    echo "13. Verify NODE Unhealthy with new ERROR String from WebUI"
    echo "14. Clean up health_check script"
    echo "***************************************************************"
    local error_string="echo -n 'ERROR STRING'"
    getJobTracker
    echo "Selected Task Tracker: $SELECTED_TT"
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"
    scpFileFromRemoteHost_InsertAStringAndSendBack $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi
    sleep 20
    new_error_string="echo -n 'ERROR STRING new one'"
    filename=$(basename $script_file)
    path=$(dirname $script_file)
    modifyFileWithNewValue "$error_string" "$new_error_string" $filename
    scp $filename hadoopqa@$SELECTED_TT:$path
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 20
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 1 ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is still in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 0 ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is still not in blacklisted list"
    fi 

    echo "Clean up health check script"
    cleanupHealthCheckScript $SELECTED_TT $script_file "$new_error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 30
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Function to set new timeout value for Health check script
##################################################
function nodeHealthCheck_NewTimeoutValue {
    echo "*************************************************************"
    TESTCASE_DESC="nodeHealthCheck_NewTimeoutValue"
    displayTestCaseMessage $TESTCASE_DESC
    echo "1. Get all active Task Trackers"
    echo "2. scp the file from the first Active Task Tracker to local machine (gateway)"
    echo "4. Change the value of mapred.healthChecker.script.timeout to 30 secs"
    echo "5. scp the file back to the first Task Tracker host"
    echo "6. Wait for 30 secs"
    echo "7. Verify the Task Tracker is not removed from Active list"
    echo "8. Verify the Task Tracker is not in the blacklisted list"
    echo "9. Reset timeout value for healthcheck script in mapred-site.xml"
    echo "***************************************************************"
    getJobTracker
    echo "Selected Task Tracker: $SELECTED_TT"
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"

    echo "Make the health check script sleep so that timeout happens"
    healthcheck_filename="health_check"
    scp hadoopqa@$SELECTED_TT:${NEW_HEALTHCHECK_LOCATION}$healthcheck_filename .
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    modifyFileWithNewValue "err=0" "sleep 200" $healthcheck_filename 
    scp $healthcheck_filename hadoopqa@$SELECTED_TT:${NEW_HEALTHCHECK_LOCATION}

    sleep 270
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 1 ] ; then
        setFailCase "UnHealthy TT node $SELECTED_TT timed out but still found in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT timed out but not found in blacklisted list"
    fi

    echo "Clean up health script by removing sleep 200 line "
    modifyFileWithNewValue "sleep 200" "err=0" $healthcheck_filename
    scp $healthcheck_filename hadoopqa@$SELECTED_TT:${NEW_HEALTHCHECK_LOCATION}
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi

    sleep 240
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

############################################
# Function to check status of TT Node when script outputs ERROR at the beginning
############################################
function nodeHealthCheck_MakeNodeHealthyAgain {
    echo "*************************************************************"
    TESTCASE_DESC="nodeHealthCheck_MakeNodeHealthyAgain"
    displayTestCaseMessage $TESTCASE_DESC
    echo "*  TESTCASE: HealthCheck script back to healthy state again started ..."
    echo "1. Get all active Task Trackers"
    echo "2. SSH to the first Task Tracker and get the health_check script path"
    echo "3. scp the file to local machine (gateway)"
    echo "4. Append an ERROR string to the file"
    echo "5. scp the file back to the first Task Tracker host"
    echo "6. Wait for 2 minutes (default mapred.healthChecker.interval)"
    echo "7. Verify the Task Tracker is removed from Active list"
    echo "8. Verify the Task Tracker is in the blacklisted list"
    echo "9. Verify NODE Unhealthy from JT log"
    echo "10. Clean up health_check script"
    echo "11. Wait for 2 minutes"
    echo "12. Verify the Task Tracker is in Active list again"
    echo "13. Verify the Task Tracker is removed from the blacklisted list"
    echo "14. Verify the Task Tracker is Unblacklisted from JT log"
    echo "***************************************************************"
    local error_string="echo -n 'ERROR STRING'"
    getJobTracker
    echo "Selected Task Tracker: $SELECTED_TT"
    ssh $SELECTED_TT "echo `getValueFromField ${NEW_CONFIG_LOCATION}mapred-site.xml mapreduce.tasktracker.healthchecker.script.path`" > ${ARTIFACTS}/file
    script_file=`cat ${ARTIFACTS}/file`
    echo "Script file: $script_file"
    scpFileFromRemoteHost_InsertAStringAndSendBack $SELECTED_TT $script_file "$error_string"
    if [ $? != 0 ] ; then
        setFailCase "scp to $SELECTED_TT failed"
    fi
    sleep 20

    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 1 ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is still in active tracker list"
    fi
    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 0 ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is still not in blacklisted list"
    fi
    JTlogs=`checkBlacklistedInfoJTLogFile $SELECTED_TT NODE_UNHEALTHY`
    echo "Found JT logs: $JTlogs"
    if [ -z "$JTlogs" ] ; then
        setFailCase "Unhealthy TT node $SELECTED_TT with ERROR is not seen with status NODE_UNHEALTHY in JT log"
    fi

    cleanupHealthCheckScript $SELECTED_TT $script_file "$error_string"
    #echo "Cleaned health_check script on $SELECTED_TT"
    #ssh $SELECTED_TT "cat /homes/hadoopqa/nodeHealthCheck/health_check"

    sleep 60
    isInActiveTrackerList $SELECTED_TT
    echo "In Active list: $IN_ACTIVE_TRACKERS"
    if [ $IN_ACTIVE_TRACKERS -eq 0 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT is still not in active tracker list"
    fi

    isInBlacklistedList $SELECTED_TT
    echo "In Black list: $IN_BLACKLISTED"
    if [ $IN_BLACKLISTED -eq 1 ] ; then
        setFailCase "Healthy TT node $SELECTED_TT is still seen in blacklisted list"
    fi

    #JTNewlog=`checkBlacklistedInfoJTLogFile $SELECTED_TT Unblacklisting`
    #echo "Found JT logs: $JTNewlog"
    #if [ -z "$JTNewlog" ] ; then
    #   setFailCase "Healthy TT node $SELECTED_TT is not seen with status Unblacklisting in JT log"
    #fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

##################################################
# Main program
##################################################

# check for cp base path
if [ -d /homes/$HADOOPQA_USER/health_check ]; then
    echo "/homes/$HADOOPQA_USER/health_check exists";
else
    echo "Creating /homes/$HADOOPQA_USER/health_check";
    mkdir /homes/$HADOOPQA_USER/health_check;
fi

# Move HADOOP_CONF_DIR to new location and restart TT
nodeHealthCheck_ParamsExist
setupNewMapredConfigFile

# MAPREDUCE:211 and MAPREDUCE:1342
nodeHealthCheck_ErrNotAtBeginning
nodeHealthCheck_LowercaseErrAtBeginning
nodeHealthCheck_ErrAtBeginning
nodeHealthCheck_EmptyNonExistNonExecutable
nodeHealthCheck_NewErrAtBeginning
nodeHealthCheck_MakeNodeHealthyAgain
nodeHealthCheck_NewTimeoutValue

# Reset Task Tracker to default settings
resetTTToDefault

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

