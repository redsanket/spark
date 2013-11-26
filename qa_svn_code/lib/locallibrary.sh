#!/bin/bash
source ${WORKSPACE}/lib/restart_cluster_lib.sh

# This is a  local library for mapreduce related functions

##############################################
# This function stops the JobTracker with the new config file 
# This function takes the jobtracker host  as $1 
##############################################
function stopJTWithCustomConf_old {
    resetNodes $1 '' jobtracker stop $myconfDir
}

##############################################
# This function stops the ResourceManager  
# This function takes the ResourceManager host  as $1 
# This function takes the ConfDir as $2 
##############################################
function stopResourceManager {
    if [ ! -z $2 ] ; then
        export HADOOP_CONF_DIR=$2
    fi
    resetNodes $1 '' resourcemanager stop $HADOOP_CONF_DIR
}

##############################################
# This function starts the ResourceManager  
# This function takes the ResourceManager host  as $1 
# This function takes the ConfDir as $2 
##############################################
function startResourceManager {
    if [ ! -z $2 ] ; then
        export HADOOP_CONF_DIR=$2
    fi
    resetNodes $1 '' resourcemanager start $HADOOP_CONF_DIR
}

##############################################
# This function stops the NodeManager  
# This function takes the as $1 a list of Node Manager 
# separated by comma or a single NM      
# This function takes the ConfDir as $2 
##############################################
function stopNodeManager {
    if [ ! -z $2 ] ; then
        export HADOOP_CONF_DIR=$2
    fi
    
    resetNodes $1 '' nodemanager stop $HADOOP_CONF_DIR	
}

##############################################
# This function starts the NodeManager  
# This function takes the as $1 a list of Node Manager 
# separated by comma or a single NM   
# This function takes the ConfDir as $2 
##############################################
function startNodeManager {
    if [ ! -z $2 ] ; then
        export HADOOP_CONF_DIR=$2
    fi    
    resetNodes $1 '' nodemanager start $HADOOP_CONF_DIR
}

##############################################
# This function returns the ResourceManager hostname  
##############################################
function getResourceManagerHost {
    grep -A 2 '>yarn.resourcemanager.scheduler.address<'  ${HADOOP_CONF_DIR}/yarn-site.xml | tr '>' '\n' | tr '<' '\n' | grep com |  cut -f 1 -d ":"
    return $?
}

##############################################
# This function starts the JobTracker with the new config file 
# This function takes the jobtracker host  as $1 
##############################################
function startJTWithCustomConf_old {
    resetNodes $1 '' jobtracker start $myconfDir
}

##############################################
# This function stops the TaskTracker with the new config file 
# This function takes the jobtracker host  as $1 
##############################################
function stopTTWithCustomConf_old {
    resetNodes $1 '' tasktracker stop $myconfDir
}
##############################################
# This function starts the TaskTracker with the new config file 
# This function takes the jobtracker host  as $1 
##############################################
function startTTWithCustomConf_old {
    resetNodes $1 '' tasktracker start $myconfDir
}

##############################################
# This function  restarts Job tracker and Task tracker with new config
# This function takes the JobTrackerHost as $1
##############################################
function restartJTAndTT {
    local jtHost=$1
    local mycount=0
    stopTTWithCustomConf $jtHost
    stopJTWithCustomConf $jtHost
    sleep 15
    startJTWithCustomConf $jtHost
    sleep 15
    startTTWithCustomConf $jtHost
    sleep 15
    for (( i=0; i < 6; i++ )); do
        echo "Looping in to get the active trackers $i times "
        getActiveTaskTrackers > $ARTIFACTS_DIR/TT_host_temp
        cat $ARTIFACTS_DIR/TT_host_temp |grep yaho.com >$ARTIFACTS_DIR/TT_host_fulllist
        local mycount= $ARTIFACTS_DIR/TT_host_fulllist |wc -l
        if [ $mycount -gt 0 ] ; then
            cat $ARTIFACTS_DIR/TT_host_fulllist
            break
        else
            sleep 10
        fi
    done
}

##############################################
# This function confirms if the job is complete
# This function takes the job id as $1 
##############################################
function checkForJobCompletion {
    #response=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $1 | grep reduce | grep completion |grep 1.0 `
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $1 | grep reduce | grep completion |grep 1.0 
    #if [ -z "$response" ] ; then
    if [ $? -ne 0  ] ; then
        # echo " The job $1 is not completed yet "
        return 1
    else
        # echo " The job $1 is completed "
        return 0
    fi
}

##############################################
# This function sets the failure result and displays the result
##############################################
function windUp {
    # COMMAND_EXIT_CODE should be non-zero when there are failures
    if ( [[ $COMMAND_EXIT_CODE -eq 0 ]] ) ; then
        echo "WARN: COMMAND_EXIT_CODE should not be 0 in windUp: Set value to 1"
        COMMAND_EXIT_CODE=1
    fi

    TESTCASENAME="`caller 0|awk '{print $2}'`"

    # This will be done in library.sh displayTestCaseResult. Don't double count.
    # ((SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE+$COMMAND_EXIT_CODE))
    displayTestCaseResult
}

##############################################
# This function  gives the job tracker host  
# checks if the variable has value and if not fetches one
##############################################
function getJobTrackerHost {
    echo " Getting job tracker host"
    if [  "$JOBTRACKER" == "None" ] ; then
        getJobTracker
    fi
    echo " The job tracker host is $JOBTRACKER "
}


##############################################
# This function copies the config files of JT to $1 location $1
# This function takes the location as param
##############################################
function copyJTConfigs {
    local configLocation=$1
    local myJTHost=$2
    ssh hadoopqa@$myJTHost mkdir -p $configLocation
    ssh hadoopqa@$myJTHost touch $configLocation/mapred-queue-acls.xml
    ssh hadoopqa@$myJTHost cp -f $HADOOP_CONF_DIR/* $configLocation
    ssh hadoopqa@$myJTHost chmod -R 777 $configLocation
}

##############################################
# This function copies the config files to a remote host location
# This function takes the location as param $1
# This function takes the Remote Host as param $2
##############################################
function copyConfigs {
    local configLocation=$1
    local myRemoteHost=$2
    ssh hadoopqa@$myRemoteHost mkdir -p $configLocation
    ssh hadoopqa@$myRemoteHost cp -f $HADOOP_CONF_DIR/* $configLocation
}

##################################################
# This function stops jobtracker 
##################################################
function stopJobTracker_old {
    local jtHost=$1
    ssh hadoopqa@$jtHost /usr/local/bin/yinst stop -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER jobtracker >> $ARTIFACTS_FILE 2>&1     
}

##################################################
# This function start jobtracker 
##################################################
function startJobTracker_old {
    local jtHost=$1
    ssh hadoopqa@$jtHost /usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER jobtracker >> $ARTIFACTS_FILE 2>&1
}

##############################################
# This function does a restart of job tracker
##############################################
function restartJobTracker_old {
    local jtHost=$1
    ssh hadoopqa@$jtHost /usr/local/bin/yinst stop -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER jobtracker >> $ARTIFACTS_FILE 2>&1
    sleep 20
    ssh hadoopqa@$jtHost /usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER jobtracker >> $ARTIFACTS_FILE 2>&1
}


##############################################
# This function checks if the job is failed
# This function takes the job id as $1
# It also takes log file to capture the response as $2
# and the caller should parse the response 
##############################################
function checkForJobStatus {
    local captureResponse=$2
    if [ -z $captureResponse ] ; then               
        local resp=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $1 | grep FAILED `
    else
        $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -status $1 >$captureResponse 2>&1
        return
    fi              
    if [ -z "$resp" ] ; then
        echo " The job $1 is not in  failed status "
        return 1
    else
        echo " The job $1 is failed "
        return 0
    fi
}

##############################################
# This function does the yinst setting for the new config path
# restarts JT so that JT points to the editable config file
# This function also resets to the default config params
# This function takes Jobtracker host  $1
# Copied config file location as $2
# if $2 is set as NONE then it resets the env to default config 
##############################################
function setUpJTWithConfig {
    createFile $ARTIFACTS_DIR/remoteExecution.log 
    local jtHost=$1
    local myconfig
    if [ "$2" == "NONE" ] ; then
        myconfig=""
    else
        myconfig=$2
    fi
    echo " The config file used for restarting the job tracker is  $myconfig"
    stopTTWithCustomConf $jtHost
    sleep 5
    stopJTWithCustomConf $jtHost
    sleep 5
    startJTWithCustomConf $jtHost
    sleep 10
    startTTWithCustomConf $jtHost
    sleep 10
}


##############################################
# This function does the yinst setting for the new config path
# restarts RM and NM with the config file
# This function also resets to the default config params
# This function takes RM as $1, NMs as $2
# Copied config file location as $3 (RM) and $4 (NM)
# if $3 or $4 is set as NONE then it resets the env to default config 
##############################################
function setUpRMandNMWithConfig {
    createFile $ARTIFACTS_DIR/remoteExecution.log
    local rmHost=$1
    local nmHost=$2
    
    local myconfig
    if [ "$3" == "NONE" ] ; then
        myconfig=""
    else
        myconfig=$3
    fi
    
    local mynmconfig
    if [ "$4" == "NONE" ] ; then
        mynmconfig=""
    else
        mynmconfig=$4
    fi
    
    echo " The config file used for restarting the RM and NMs is  $myconfig"
    stopResourceManager $rmHost $myconfig 
    sleep 10
    stopNodeManager $nmHost $mynmconfig
    sleep 10
    startResourceManager $rmHost $myconfig
    sleep 10
    startNodeManager $nmHost $mynmconfig 
    sleep 10
}

function setUpRMWithConfig {
    createFile $ARTIFACTS_DIR/remoteExecution.log
    local rmHost=$1
    
    local myconfig
    if [ "$2" == "NONE" ] ; then
        myconfig=""
    else
        myconfig=$2
    fi
    
    echo " The config file used for restarting the RM is $myconfig"
    stopResourceManager $rmHost $myconfig 
    sleep 10
    startResourceManager $rmHost $myconfig
    sleep 10    
}
######################################################################################
# This function  fails the attempt for a given attempt id  $1
#####################################################################################`
function failJob {
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $1
     # resp=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $1 | grep Killed`
     # echo " The response of the fail task is $resp "  
}

######################################################################################
# This function  fails the attempt for a given attempt id  $1
#####################################################################################`
function failGivenAttemptId {
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $1
}

######################################################################################
# This function  kills the attempt id for a  given attempt id  $1
#####################################################################################`
function killTask {
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill-task $1
    # resp=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $1 | grep Killed`
    # echo " The response of the fail task is $resp "  
}


#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
#############################################################
function getAttemptIdsForJobId {
    local myjobid=$1
    local mytask=$2
    if [ -z $mytask ] ; then
        mytask="MAP"
    fi
    #$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobid $mytask running 
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobid $mytask running > $ARTIFACTS_DIR/attemptsList
    #?
    sleep 5
    cat $ARTIFACTS_DIR/attemptsList |grep attempt_ | awk 'NR==1 {print}'
    # return $?
}

#############################################################
# Function to wait for the attemptid for a JOBID
# Params: $1 is the JobID
# Params: $2 takes map or reduce and default is map
# Params: $3 max duration to wait
# Params: $4 the wait increment
#############################################################
function waitForAttemptIdsForJobId {
    local myjobid=$1
    local mytask=$2
    if [ -z $mytask ] ; then
        mytask="map"
    fi
    local max_duration=$3
    if [ -z $max_duration ] ; then
        max_duration=120
    fi
    local increment=$4
    if [ -z $increment ] ; then
        increment=5
    fi
    local duration=0
    while [ $duration -lt $max_duration ]; do
        attemptID=`getAttemptIdsForJobId $myjobid $mytask`
        if [ -z $attemptID ] ; then
            sleep $increment
            (( duration = $duration + $increment ))
        else
            break;
        fi
    done
    echo $attemptID
}

#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
# Params: $2 is the taskType as MAP REDUCE
# Params: $1 is the state as running or completed
#############################################################
function getAttemptIdsCountForJobId {
    local myjobId=$1
    local taskType=$2
    local state=$3
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobId $taskType $state > /tmp/t5.out 2>&1
    sleep 10
    # cat /tmp/t5.out
    # cat /tmp/t5.out |grep attempt_ | awk 'NR==1 {print}'
    cat /tmp/t5.out |grep attempt_ |wc -l
}

#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
# Params: $2 is the taskType as MAP REDUCE
# Params: $1 is the state as running or completed
#############################################################
function getOneAttemptIdForJobId {
    local myjobId=$1
    local taskType=$2
    local state=$3
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobId $taskType $state > /tmp/t5.out 2>&1
    sleep 10
    # cat /tmp/t5.out
    cat /tmp/t5.out |grep attempt_ | awk 'NR==1 {print}'
    # cat /tmp/t5.out |grep attempt_ |wc -l
}

##############################################################
# Function to get the tasktracker host for a given attemptid
# Params: $1 is the attempID
# Params: $2 is the task type 
##############################################################
function getTTHostForAttemptId {
    local id=$1
    local mytask=$2
    if [ -z $mytask ] ; then 
        mytask="MAP"  
    fi      
    getJobTracker
    # ssh into the jobtracker host and get the TT host that this attempt is being executed
    ssh $JOBTRACKER cat ${HADOOP_QA_ROOT}/hadoop/var/log/mapred/hadoop-mapred-jobtracker-$JOBTRACKER.log |grep $id |grep $mytask | tail -1 |awk -F'tracker_' '{print $2}' | cut -d ':' -f1
    return $?
}

##############################################################
# Function to wait for the tasktracker host for a given attemptid
# Params: $1 is the attempID
# Params: $2 takes map or reduce and default is map
# Params: $3 max duration to wait
# Params: $4 the wait increment
##############################################################
function waitForTTHostForAttemptId {
    local myid=$1
    local mytask=$2
    if [ -z $mytask ] ; then
        mytask="MAP"
    fi
    local max_duration=$3
    if [ -z $max_duration ] ; then
        max_duration=120
    fi
    local increment=$4
    if [ -z $increment ] ; then
        increment=5
    fi
    local duration=0
    while [ $duration -lt $max_duration ]; do
        taskTracker=`getTTHostForAttemptId $myid $mytask`
        if [ -z $taskTracker ] ; then
            sleep $increment
            (( duration = $duration + $increment ))
        else
            break;
        fi
    done
    echo $taskTracker;
}

##############################################################
# Function to get the JOBID of the task submitted
##############################################################
function getJobId_old {
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list | tail -1 | cut -f 1
    # return $?
}

# myloc is the filename containing the job stdout
# If a parameter is passed in, use it. 
# Otherwise, use the JOB_OUTPUT_FILE if it exists.
# If not, use the default filename sleep.0.log
function getJobId {
    local myloc=$1
    local default_file=${JOB_OUTPUT_FILE:="$ARTIFACTS_DIR/sleep.0.log"}
    local default_file=${default_file:="$ARTIFACTS_DIR/sleep.0.log"}
    local myloc=${myloc:=$default_file}

    local max_attempts=12
    for (( i=0; i < $max_attempts; i++ )); do
        local myjobId=`cat $myloc |grep "Running job"|awk -F'job:' '{print $2}'`
        # Stripping of the leading blank space quick and easy way   
        myjobId=`echo $myjobId`
        if [ ! "X${myjobId}X" == "XX" ] ; then
            break
        else
            sleep 10
        fi
    done
    validateJobId $myjobId
    if [ $? -ne 0 ] ; then
        echo "0"
    else
        echo $myjobId  
    fi
    # return $?
}

##############################################################
# Function to get the JOBID of the task submitted
# Params: $3 max duration to wait
# Params: $4 the wait increment
##############################################################
function waitForJobId {
    local max_duration=$1
    if [ -z $max_duration ] ; then
        max_duration=120
    fi
    local increment=$2
    if [ -z $increment ] ; then
        increment=5
    fi
    local duration=0
    while [ $duration -lt $max_duration ]; do
        jobID=`getJobId_old`
        if [ -z $jobID -o $jobID == "JobId" ] ; then
            sleep $increment
            (( duration = $duration + $increment ))
        else
            break;
        fi
    done
    echo $jobID
}

##############################################################
# Function to validate the format of job id
##############################################################
function validateJobId {
    local myjobId=$1
    pattern=`echo $myjobId |grep job_ `
    if [ -z "$pattern" ] ; then
        # echo " The job id does meet the format"
        return 1
    else
        # echo " The job id meets the format "
        return 0
    fi
}

##############################################################
# Function to  validate the attemptid format
##############################################################
function validateAttemptId {
    local myAttemptId=$1
    pattern=`echo $myAttemptId |grep attempt_ `
    if [ -z "$pattern" ] ; then
        echo " The attempt id does meet the format"
        return 1
    else
        echo " The attempt id meets the format "
        return 0
    fi
}

######################################################################################
# This function set the count for no of successfull jobs that will
# blacklist a tasktracker in mapred-site.xml and the param is mapred.max.tracker.blacklists
# This method  takes the input value to be set as $1
# $2 will be the config file location
######################################################################################
function updateMaxTrackerBlacklistsCount {
    local myblacklistCount=$1
    local myconfigDir=$2    
    echo " The count that needs to be set at $myconfigDir is $myblacklistCount "
    if [ -z "$JOBTRACKER" ] ; then
        echo "Empty job tracker host and so getting the hostname "
        getJobTracker
        echo " Got the job tracker host name  and it is  $JOBTRACKER "
    fi

    # Get the mapred-site.xml locally
    getFileFromRemoteServer $JOBTRACKER $myconfigDir/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    blacklistcount=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapreduce.jobtracker.tasktracker.maxblacklists`
    echo " The value of  black list count set on mapred site is $blacklistcount "
    if [ -z "$blacklistcount" ] ; then
        echo " unable to read the value set in the mapred xml site "
        return 1
    fi

    if [ "$blacklistcount" -eq 1 ] ; then
        echo "The value of blacklist count in mapred-site.xml is already set to 1 and so we are good to proceed "       
        return 0
    fi
    # Modify the mapred-site to xml
    modifyValueOfAField $ARTIFACTS_DIR/mapred-site.xml mapreduce.jobtracker.tasktracker.maxblacklists $myblacklistCount
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/mapred-site.xml $myconfigDir/mapred-site.xml
    stopJTWithCustomConf $JOBTRACKER        
    stopTTWithCustomConf $JOBTRACKER 
    sleep 15
    startJTWithCustomConf $JOBTRACKER 
    startTTWithCustomConf $JOBTRACKER       
    sleep 15
    return 0
}

##############################################################
# Function to reset the env to default settings 
# $1 takes the config dir that needs to be deleted
##############################################################
function resetEnv_old {
    local myconfig=$1
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    setUpJTWithConfig $JOBTRACKER "NONE"
    ssh hadoopqa@$JOBTRACKER rm -rf $myconfig
    customSetUp=false
}

##############################################
# This function takes the config file to use as $1
# and sets the env pointing to the confi dir
##############################################
function setUpTestEnv {
    local myFile=$1
    if [ $customSetUp != "CUSTOM" ] ; then
        local rmHost=`getResourceManagerHost`
        local nmHost=`getActiveTaskTrackers`
        
        if [ "X${rmHost}X" == "XX" ]; then
    		echo "There is no RM"
    		exit 1
		fi
		        
        if [ "X${nmHost}X" == "XX" ]; then
    		echo "There is no NN"
    		exit 1
		fi
		
		if [[ ! $customNMConfDir || ! $customConfDir ]]; then
        	echo "Please define customNMConfDir and customConfDir"
        	exit 1
        fi
        
		copyConfigs $customConfDir $rmHost
    	putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/$myFile $customConfDir/capacity-scheduler.xml
        
        for nm in $nmHost
		do
			copyConfigs $customNMConfDir $nm
		done
        
        local nmHost=`getActiveTaskTrackers | tr '\n' ','`                
        setUpRMandNMWithConfig $rmHost $nmHost $customConfDir $customNMConfDir 
        
        customSetUp="CUSTOM"
        sleep 15
    fi                                  
}

##############################################################
# Function to set the env to custom settings 
# $1 takes the configdir location
##############################################################
function setTestEnv {
    local myconfDir=$1
    getJobTrackerHost
    if [ "$JOBTRACKER" == "None" ] ; then
        echo " Job tracker host is empty and so exiting "
        setFailCase " Job tracker host is empty and so exiting "
        windUp
        return
    fi
    copyJTConfigs $myconfDir $JOBTRACKER
    setUpJTWithConfig $JOBTRACKER $myconfDir
    customSetUp="custom"
}

##############################################################
# Function to set the env to custom settings 
# $1 takes the configdir location
##############################################################
function setRMandNMTestEnv {
    local myconfDir=$1
    local rmHost=`getResourceManagerHost`
    local nmHost=`getActiveTaskTrackers`
	
	if [[ ! $myconfDir ]]; then
      echo  "Please define myConfDir"
      exit 1    	
    fi
	
	if [ "X${rmHost}X" == "XX" ]; then
    	echo "There is no RM"
    	exit 1
	fi
		    
		    
	if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no NN"
    	exit 1
	fi
	
	copyConfigs $myconfDir $rmHost
	for nm in $nmHost
	do
		echo $nm#copyConfigs $myconfDir $nm
	done
		
	local nmHost=`getActiveTaskTrackers | tr '\n' ','`
	setUpRMandNMWithConfig $rmHost $nmHost $myconfDir $myconfDir 
	
	sleep 15        	
}

######################################################################################
# This function gets the tasktracker host for a given attemptid takes JOBID as Input
######################################################################################
function getAttemptIdsForJobIdAndStoreInFile {
    local myjobId=$1
    local mytask=$2
    if [ -z $mytask ] ; then
        mytask="MAP"
    fi
    if [ ! -f $ARTIFACTS_DIR/AttemptIdFile ] ; then
        createFile $ARTIFACTS_DIR/AttemptIdFile >> $ARTIFACTS_FILE 2>&1 &
    fi
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobId $mytask running > $ARTIFACTS_DIR/AttemptIdFile
    sleep 5
    cat $ARTIFACTS_DIR/AttemptIdFile |grep attempt_ |wc -l
    # cat $ARTIFACTS_DIR/AttemptIdFile 
    return $?
}

######################################################################################
# This function  checks to see if there any more attempt ids for the jobid and returns the count
######################################################################################
function checkForNewAttemptIds { 
    local myjobId=$1
    local mytask=$2
    if [ -z $mytask ] ; then
        mytask="MAP"
    fi      
    attemptIdCount=`getAttemptIdsForJobIdAndStoreInFile $myjobId $mytask`
    echo " The attempt id count obtained is $attemptIdCount in the check for new attempt ids method "
    local x=0
    while [ $attemptIdCount -eq 0 ]; do
        echo " In the loop within the checkForNewAttemptIds"
        attemptIdCount=`getAttemptIdsForJobIdAndStoreInFile $myjobId $mytask`
        if [ $attemptIdCount -ne 0 ] ; then
            break   
        fi
        sleep 5
        x=`expr $x + 1`
        if [ $x -gt 2 ] ; then
            break
        fi
    done
    # echo $attemptIdCount
}


######################################################################################
# This function kills the job and takes jobid as param $1  
######################################################################################
function killAJob {
    resp=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $1 | grep Killed `
    if [ -z  "$resp" ] ; then
        echo " Job id $1 has not been killed "
    else
        echo " job id $1 has been killed successfully "
    fi
}

##############################################
# This function fails the attemptids
# Reads from the attemptsid file
##############################################
function failAttempts {
    echo " %%%%%%%%%%%%% Entering Fail Zone   %%%%%%%%%%%%%%%%%%%%%%" 
    local lineCount=`cat $ARTIFACTS_DIR/AttemptIdFile |wc -l`
    echo "Number of attempt ids to handle $lineCount "  
    for (( i=1; i <= $lineCount; i++ )); do
        echo "Getting attempt id"
        local attemptId=`cat $ARTIFACTS_DIR/AttemptIdFile |grep attempt_ |awk 'NR=='$i' {print}'`
        failJob $attemptId
    done
    >$ARTIFACTS_DIR/AttemptIdFile
    attemptIdCount=0
    echo " %%%%%%%%%%%%% done with all the killing   %%%%%%%%%%%%%%%%%%%%%%"
}

##############################################
#This function kills the attempt id
# by reading from the attemptId file
##############################################
function killAttempts {
    echo " %%%%%%%%%%%%% Entering Kill Zone   %%%%%%%%%%%%%%%%%%%%%%" 
    local lineCount=`cat $ARTIFACTS_DIR/AttemptIdFile |wc -l`
    echo "Number of attempt ids to handle $lineCount "  
    for (( i=0; i <= $lineCount; i++ )); do
        echo "Getting attempt id"
        local attemptId=`cat $ARTIFACTS_DIR/AttemptIdFile |awk 'NR=='$i' {print}'`
        killTask $attemptId
    done
    >$ARTIFACTS_DIR/AttemptIdFile
    attemptIdCount=0
    echo " %%%%%%%%%%%%% done with all the killing   %%%%%%%%%%%%%%%%%%%%%%"
}

function submitSleepJobToQueue {
    local queuename=$1
    local mylocation=$2
    local mymaps=$3
    local myreduce=$4
    local mymapwait=$5
    local myreducewait=$6
    if [ -z $queuename ] ; then
        queuename="default"
    fi
    if [ -z $mymaps ] ; then
        mymaps=1
    fi
    if [ -z $myreduce ] ; then
        myreduce=1
    fi
    if [ -z $mymapwait ] ; then
        mymapwait=1
    fi
    if [ -z $myreducewait ] ; then
        myreducewait=1
    fi

    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.job.queuename=$queuename   -Dmapreduce.job.user.name=$USER_ID -m $mymaps -r $myreduce -mt $mymapwait -rt $myreducewait  >$mylocation 2>&1&
    #$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_HOME/lib/hadoop-mapred-test-0.22.*.jar  sleep -Dmapred.job.queue.name=$queuename   -Dmapreduce.job.user.name=$USER_ID -m $mymaps -r $myreduce -mt $mymapwait -rt $myreducewait  >$mylocation 2>&1&
}

function submitSleepJob {
    TIME_STAMP=`date +%m%d%Y_%H%M%S_$$`
    FILES=$ARTIFACTS_DIR/sleep.$TIME_STAMP.log
    for (( i = 0; i < $5; i++ )); do
        JOB_OUTPUT_FILE=$ARTIFACTS_DIR/sleep."$TIME_STAMP"_$i.log
        echo $JOB_OUTPUT_FILE >> $FILES
        createFile $JOB_OUTPUT_FILE
        if [ -z "$6" ] ; then
            echo " I dont have any queue details and so submitting the job to default queue"
            set -x
            $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.job.user.name=$USER -m $1 -r $2 -mt $3  -rt $4 > $JOB_OUTPUT_FILE 2>&1&
            set +x
        else
            echo " I have queue details and so submitting the job to $6 queue"
            set -x
            $HADOOP_COMMON_HOME/bin/hadoop $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.job.user.name=$USER -m $1 -r $2 -mt $3  -rt $4 > $JOB_OUTPUT_FILE 2>&1&  
            set +x
        fi
    done
}

function failAJob {
    local myjobId=$1
    local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
    local myAttemptId2=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_1/g')
    local myAttemptId3=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_2/g')
    local myAttemptId4=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_3/g')
    local myAttemptIds=" $myAttemptId1 $myAttemptId2 $myAttemptId3 $myAttemptId4"
    for myAttemptId in $myAttemptIds; do
        failGivenAttemptId $myAttemptId
    done    
}       


#############################################################
# Function to get the attemptid for a JOBID
# Params: $1 is the JobID
# Params: $2 is the taskType as MAP REDUCE
# Params: $1 is the state as running or completed
#############################################################
function getAttemptIdsCountForJobId {
    local myjobId=$1
    local taskType=$2
    local state=$3
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list-attempt-ids $myjobId $taskType $state |grep attempt_ |wc -l
}

###########################################################
# This function resets the RM and NM to point to default config
# It does not take any param
###########################################################
function resetYarnEnv {
    export HADOOP_CONF_DIR=$HADOOP_QA_ROOT/gs/gridre/yroot.$CLUSTER/conf/hadoop
    
    local rmHost=`getResourceManagerHost`
    local nmHost=`getActiveTaskTrackers | tr '\n' ','`
    
    setUpRMandNMWithConfig $rmHost $nmHost $HADOOP_CONF_DIR $HADOOP_CONF_DIR          
}

###########################################################
# This function loops waiting for the Job History file to 
# become readable
# Arguments: 
# 1 - JobID, used to identify which job history file
# 2 - Max number of attempts to read job history file 
# 3 - Time in seconds to wait between read attempts 
###########################################################
function waitForJobHistFile {

 local JOBID=$1
 local TRYS=0
 if [ -z $JOBID ]  ; then
   echo "waitForJobHistFile requires JobID param" 
   return 1 
 fi 
 local MAX_TRYS=$2
 if [ -z $MAX_TRYS ] ; then 
    MAX_TRYS=20
 fi 
 local SLEEP_TIME=$3
 if [ -z $SLEEP_TIME ] ; then 
    SLEEP_TIME=30
 fi 

 COMMAND_EXIT_CODE=1
 
  while [[ $COMMAND_EXIT_CODE -ne 0 ]] ; do
    sleep $SLEEP_TIME
    FILE_LOCATION=`$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R /mapred/history/done | grep $JOBID |  grep -v conf`
    COMMAND_EXIT_CODE=$?
    (( TRYS = $TRYS +1 ))
    if [[ $TRYS -eq $MAX_TRYS ]] ; then
      REASONS="Could not locate the job history file after $MAX_TRYS trys waiting $SLEEP_TIME between attempts."
      echo $REASONS
      return 1
    fi
  done

  return 0
}
