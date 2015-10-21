#!/bin/sh
source $WORKSPACE/lib/user_kerb_lib.sh

###########################################
# Function to display a message on console
###########################################
function dumpmsg {
    local MSG=$*
    host=`hostname | cut -d'.' -f1`
    echo "[`date +%D-%T`] $USER@$host > $MSG"
}

###########################################
# Function used to display a message before the start of any test run
###########################################
function displayEntryMessage {
    echo "***************************************************************************************************************************************************************************"
    echo "BEGINNING TO EXECUTE  $JOB ON $CLUSTER CLUSTER"
    echo "***************************************************************************************************************************************************************************"
}

###########################################
# Function to display a heading on the console 
###########################################
function displayHeader {
    echo "============================================================="
    echo "$*"
    echo "============================================================="
}

###########################################
# Function to check the result of the test run
###########################################
function checkResult {
    (( RESULT = $RESULT + $1 ))
    if [ "$1" -eq 0 ] ; then
        displaySuccess
    else
        displayFailure
    fi
}

###########################################
# Function to display a message on success of a test
###########################################
function displayFailure {
    displayHeader "Execution of $JOB FAILED. Please see ${ARTIFACTS_DIR}/$JOB.log</a>  for more details"
}

###########################################
# Function to display a message on failure of a test
###########################################
function displaySuccess {
    displayHeader "Execution of $JOB PASSED"
}

###########################################
# Function to delete artifacts directory
###########################################
function deleteArtifacts {
    if [ -d ${ARTIFACTS} ] ; then
        rm -rf ${ARTIFACTS}
    fi
    if [ ! -d ${ARTIFACTS} ] ; then
        mkdir -p ${ARTIFACTS}
    fi

}

###########################################
# Function to create a directory on the Local FS
###########################################
function createLocalDir {
    if [ ! -d $1 ] ; then
        mkdir -p $1
    fi
}

###########################################
# Function to delete a directory on the local FS
###########################################
function deleteLocalDir {
    if [ -d $1 ] ; then
        rm -rf $1
    fi
}

###########################################
# Function to create a directory on HDFS 
# $1 - dir path
# $2 - optional, value of the fs
###########################################
function createHdfsDir {
    local dirName=$1
    local fs=$2
    
    local cmd1
    local cmd2

    if [ -z "$fs" ]; then
        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls $dirName"
        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -mkdir -p $dirName"
    else
        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -ls $dirName"
        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -mkdir -p $dirName"
    fi
    
    echo $cmd1
    eval $cmd1
    if [ "$?" -ne 0 ]; then
        echo $cmd2
        eval $cmd2
    fi
}

###########################################
# Function to delete a directory on HDFS
# $1 - dir path
# $2 - optional, value of the fs
# $3 - optional, 0 - skipTrash, else ignore
###########################################
function deleteHdfsDir {
    local dirName=$1
    local fs=$2
    local skipTrash=''

    #if $3 not empty and equal to 0 then skip trash
    if [ -n "$3" ] && [ "$3" -eq "0" ]; then
        skipTrash="-skipTrash"
    fi

    local cmd1
    local cmd2

    if [ -z "$fs" ]; then
        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls $dirName"
        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r $skipTrash $dirName"
    else
        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -ls $dirName"
        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -rm -r $skipTrash $dirName"
    fi
    echo $cmd1
    eval $cmd1
    if [ "$?" -eq 0 ]; then
        echo $cmd2
        eval $cmd2
    fi
}

###########################################
# Sets hadoop version
###########################################
function setHadoopVersion {
    VERSION=`$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR version | head -1`
}

###########################################
# Sets build description
###########################################
function setBuildDescription {
    setHadoopVersion
    echo "Ran $JOBS on $VERSION, $CLUSTER cluster"
}

###########################################
# Function to display a message on success of a test
###########################################
function displayStreamingFailure {
    displayHeader "$TESTCASE_DESC FAILED. Please see ${ARTIFACTS_DIR}/Streaming.log  for more details"
}

###########################################
# Function to display a message on failure of a test
###########################################
function displayStreamingSuccess {
    displayHeader "$TESTCASE_DESC PASSED"
}

##########################################
# Function to check the result of the test run
###########################################
function checkPipesResult {
    (( RESULT = $RESULT + $1 ))
    if [ "$1" -eq 0 ] ; then
        displayPipesSuccess
    else
        displayPipesFailure
    fi
}

###########################################
# Function to display a message on success of a test
###########################################
function displayPipesFailure {
    displayHeader "$TESTCASE_DESC FAILED. Please see ${ARTIFACTS_DIR}/Pipes.log  for more details"
}

###########################################
# Function to display a message on failure of a test
###########################################
function displayPipesSuccess {
    displayHeader "$TESTCASE_DESC PASSED"
}


###########################################
# This function is used to set the test case ID
###########################################
function setTestCaseID {
    TESTCASE_ID=$1
}

###########################################
# This function is used to set the test case description and also to display a brief information of the test case ID and description at the start of every test case run
###########################################
function setTestCaseDesc {
    TESTCASE_DESC=$@
    displayHeader $*
}

###########################################  
# THis function is used to get the test case ID
###########################################
function incrTestCaseId {
    (( TESTCASE_ID = ${TESTCASE_ID} + 10 ))
}

###########################################
# Function to validate a cluster by running a sample streaming word count job
###########################################
function validateCluster  {
    displayHeader "Validating the cluster by running a streaming word count job"
    deleteHdfsDir data >> ${ARTIFACTS_DIR}/validation.log 2>&1

    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -put $JOB_SCRIPTS_DIR_NAME/data/validation/wordCountInput data/wordCountInput >> ${ARTIFACTS_DIR}/validation.log 2>&1
    echo " $HADOOP_COMMON_HOME/bin/hadoop --config ${HADOOP_CONF_DIR} jar $HADOOP_STREAMING_JAR -input data/wordCountInput -output data/streamingWordCountOutput -file $JOB_SCRIPTS_DIR_NAME/data/validation/bin/streamingMapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/validation/streamingReducer.sh -mapper streamingMapper.sh -reducer streamingReducer.sh"
    echo "OUTPUT:"
    $HADOOP_COMMON_HOME/bin/hadoop --config ${HADOOP_CONF_DIR} jar $HADOOP_STREAMING_JAR -input data/wordCountInput -output data/streamingWordCountOutput -file $JOB_SCRIPTS_DIR_NAME/data/validation/streamingMapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/validation/streamingReducer.sh -mapper streamingMapper.sh -reducer streamingReducer.sh

    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat data/streamingWordCountOutput/part-* > ${ARTIFACTS_DIR}/streamingWordCountOutput.txt

    LINES=`cat ${ARTIFACTS_DIR}/streamingWordCountOutput.txt | wc -l`
    if [[ $LINES -ne 5318 ]] ; then
        REASONS="Validation job failed. Please check the cluster and re-run the job by specifying correct parameters"
        COMMAND_EXIT_CODE=1
        echo $REASONS
        displayTestCaseResult
        return 1
    else
        displayHeader "Validation jobs succeeded"
        return 0
    fi
}

###########################################
# Functiom to put local files/directories to HDFS
###########################################
function putLocalToHdfs {

    # Check for the destination directory and create it if
    # is not present because 'dfs put' used to do that 
    dir=`dirname $2`

    chkdir="$HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -ls $dir"
    
    eval "${chkdir}"
    if [ "$?" -ne "0" ]; then 
        echo "$HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -mkdir -p $dir"
        $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -mkdir -p $dir
    fi

    echo "$HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -put $1 $2"
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -put $1 $2
}

###########################################
# Function to get the Namenode hostname and port
###########################################
function getFileSytem {
    NAMENODE=`grep -A 2 fs.defaultFS  ${HADOOP_CONF_DIR}/core-site.xml | tr '>' '\n' | tr '<' '\n' | grep com`
}

###########################################
# Function to get the Jobtracker hostname
###########################################
function getJobTracker {
    JOBTRACKER=`grep -A 2 '>yarn.resourcemanager.resource-tracker.address<'  ${HADOOP_CONF_DIR}/yarn-site.xml | tr '>' '\n' | tr '<' '\n' | grep com |  cut -f 1 -d ":"`
}

###########################################
# Function to get the ResourceManager hostname
###########################################
#function getResourceManager {
#    RESOURCEMANAGER=`grep -A 2 '>yarn.resourcemanager.resource-tracker.address<'  ${HADOOP_CONF_DIR}/yarn-site.xml | tr '>' '\n' | tr '<' '\n' | grep com |  cut -f 1 -d ":"`
#}

###########################################
# Function to validate job summary logs
###########################################
function  validateJobSummary {
    testname=`caller 0|awk '{print $2}'`

    if [[ $1 -ne 0 ]] ; then
        REASONS="See BUG:4630357, BUG:4633680, BUG:4591501"
        return 1
    else
        return 0
    fi
}

###########################################
# Function to get the cluster map capacity
###########################################
function getclusterMapCapacity {
    getJobTracker
    wget --quiet "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jobtracker.jsp
    CLUSTER_MAP_CAPACITY=`grep -A 1 "Map Slot Capacity" $ARTIFACTS_DIR/jobtracker.jsp | tail -1 | cut -d '>' -f 25 | cut -f 1 -d '<'`
}

###########################################
# Function to get the cluster reduce capacity
###########################################
function getclusterReduceCapacity {
    getJobTracker
    wget --quiet  "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jobtracker.jsp
    CLUSTER_REDUCE_CAPACITY=`grep -A 1 "Reduce Slot Capacity" $ARTIFACTS_DIR/jobtracker.jsp | tail -1 | cut -d '>' -f 27 | cut -f 1 -d '<'`
}

##########################################
# Function to create Kerberos ticket. This requires a headless user as an argument
# Param: $1 is hadoopqa super user
##########################################
function createKerberosTicket {
    echo "Creating Kerberos ticket for $1"
    kinit -k -t /homes/$1/$1.dev.headless.keytab $1
    echo "`klist`"
}

#########################################
# Function to get the value of a field in a xml file
# Params: $1 is the xml file with complete path; $2 is the field in the xml file
#########################################
function getValueFromField {
    xmllint $1 | grep "<name>$2</name>" -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1
    return $?
}

#########################################
# Function to modify a value in a file with new value
# Params: $1 is old value; $2 is new value; $3 is the file
#########################################
function modifyFileWithNewValue {
    echo "Modify all "$1" with "$2" in file $3"
    sed -i "s|$1|$2|g" $3
}

#########################################
# Function to modify a value of a field in an xml file
# Params: $1 is the file with full path; $2 is the field; $3 is the new value
#########################################
function modifyValueOfAField {
    echo "Modify field $2 with new value "$3" in file $1"
    sed -i "/$2/ {
  N
  s|"$2"</name>\n.*<value>.*</value>|"$2"</name>\n<value>"$3"</value>|
  }" $1
}

###########################################
# Function to get all active task trackers
###########################################
function getActiveTaskTrackers {
    ${HADOOP_MAPRED_CMD} --config $HADOOP_CONF_DIR job -list-active-trackers | cut -d '_' -f2 | cut -d ':' -f1 | cut -d ':' -f1
    return $?
}

###########################################
# Function to get all blacklisted task trackers
###########################################
function getBlacklistedTaskTrackers {
    ${HADOOP_MAPRED_CMD} job -list-blacklisted-trackers | cut -d '_' -f2 | cut -d ':' -f1 | cut -d ':' -f1
    return $?
}

##########################################
# Function to append a string to a file
# Params: $1 is the file name; $2 is the string to be appended
##########################################
function appendStringToAFile {
    sed -i "$ a\ $2" $1
}

##########################################
# Function to delete a line with specified pattern in a file
# Params: $1 is the file name; $2 is the specified pattern in the file
##########################################
function deleteLinesWithPatternInFile {
    echo "Delete a line with pattern $2 in file $1"
    sed -i "/$2/d" $1
}

##########################################
# Function to check blacklisted info in JT log file
# Params: $1 is the JobTracker host; $2 is "NODE_UNHEALTHY" message
##########################################
function checkBlacklistedInfoJTLogFile { 
    getJobTracker
    ssh $JOBTRACKER "grep $1 $HADOOP_LOG_DIR/mapredqa/hadoop-mapredqa-jobtracker-$JOBTRACKER.log | grep $2"
    return $?
}

###########################################
# Function to get number of Data Nodes on cluster
# $1 path
# $2 fs
# FSCK command needs to run as super user. Will set to HDFSQA_USER and then set it to HADOOPQA_USER 
###########################################
function getNumDataNodes {
    local path=$1
    local fs=$2
    local cmd
    if [ -n "$fs" ]; then
        cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $fs $path -files | grep data-nodes: | cut -f 3"
    else
        cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $path -files | grep data-nodes: | cut -f 3"
    fi
    echo $cmd
    local NumDataNodes=`eval $cmd`
    return $NumDataNodes
}

###########################################
# Function to get number of racks on cluster
# $1 path
# $2 fs
# FSCK command needs to run as super user. Will set to HDFSQA_USER and then set it to HADOOPQA_USER 
###########################################
function getNumRacks {
    local path=$1
    local fs=$2
    local cmd
    if [ -n "$fs" ]; then
        cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $fs $path -files | grep racks: | cut -f 3"
    else
        cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $path -files | grep racks: | cut -f 3"
    fi
    echo $cmd
    local NumRacks=`eval $cmd`
    return $NumRacks
}

###########################################
# Function to get file space used on cluster
# $1 path
# $2 fs
# FSCK command needs to run as super user. Will set to HDFSQA_USER and then set it to HADOOPQA_USER 
###########################################
function getFsSize {
    local path=$1
    local fs=$2
    local cmd
    if [ -n "$fs" ]; then
        cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $fs $path -files | grep size: | cut -f 2 | cut -d ' ' -f 1"
    else
        cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $path -files | grep size: | cut -f 2 | cut -d ' ' -f 1"
    fi
    echo $cmd
    local FsSize=`eval $cmd`
    return $FsSize
}

###########################################
# Function to get average block replication
###########################################
function getAverageBlockReplication {
    echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $1 -files | grep "Average block replication:" | cut -f 2 | cut -d " " -f 1"
    FsSize=`$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $1 -files | grep "Average block replication:" | cut -f 2 | cut -d " " -f 1`
    return $FsSize
}

###########################################
# Function to get corrupt blocks on cluster
###########################################
function getAverageBlockReplication {
    echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $1 -files | grep "Corrupt blocks:" | cut -f 2 | cut -d " " -f 1"
    FsSize=`$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck $1 -files | grep "Corrupt blocks:" | cut -f 2 | cut -d " " -f 1`
    return $FsSize
}

###########################################
# Write random files into a directory
###########################################
function randomWriter {
    echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR randomwriter $1"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR randomwriter $1
}

###########################################
# Write generage terasort input data set
# $1 = number of files
# $2 = size of files
# $3 = output directory
###########################################
function teragen {
    echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR teragen -Dmapred.map.tasks=$1 $2 $3"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR teragen -Dmapred.map.tasks=$1 $2 $3
}

#################################################################
# This function creates a file  if it does not exist
# This function takes full path to file name as param
#################################################################
function createFile {
    if [ ! -f $1 ] ; then
        touch $1 > /dev/null 2>&1 && echo "File $1 created."  ||  echo "Error: Failed to create $1 files."
    else
        echo "Error: $1 file exists!"
    fi
}

######################################################################################
# This method removes all the excluded nodes entry
# thereby enabling all the nodes
######################################################################################
function getExcludedOnActiveNodes {
    # Getting JobTracker hostname
    getJobTracker
    echo $JOBTRACKER
    if [ -z "$JOBTRACKER" ] ; then
        COMMAND_EXIT_CODE=1
        echo "Empty job tracker host "
    fi
    #Create a empty file locally
    touch $ARTIFACTS_DIR/empty_mapred.exclude
    # Moving the file to taks tracker machine
    scp $ARTIFACTS_DIR/empty_mapred.exclude  vmotilal@$JOBTRACKER:${HADOOP_QA_ROOT}/gs/conf/local/mapred.exclude
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/empty_mapred.exclude ${HADOOP_QA_ROOT}/gs/conf/local/mapred.exclude
}

######################################################################################
# This function is to stop the Jobtracker
# It takes the host name of the jobtracker as param
######################################################################################
function stopJobTracker {
    ssh -i  /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$1 exec $HADOOP_MAPRED_HOME/bin/stop-mapred.sh > $ARTIFACTS_DIR/jtStop.log
    echo " The last line in the jtStop log is as follows"
    echo "The number of lines in the JT stop log is "
    cat $ARTIFACTS_DIR/jtStop.log |wc -l
    cat $ARTIFACTS_DIR/jtStop.log |tail -1 | grep -q tasktracker
    if [[ $? -eq 0 ]] ; then
        echo " Job tracker has been successfully stopped "
    else
        echo " Job tracker has not been stopped I think "
    fi

}

######################################################################################
# This function is to start the Jobtracker
# It takes the host name of the jobtracker as param
######################################################################################
function startJobTracker {
    ssh -i  /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$1 exec $HADOOP_MAPRED_HOME/bin/start-mapred.sh > $ARTIFACTS_DIR/jtStart.log
    echo " The last line in the jtStart log is as follows"
    echo "The number of lines in the JT stop log is "
    cat $ARTIFACTS_DIR/jtStart.log |wc -l
    cat $ARTIFACTS_DIR/jtStart.log |tail -1 | grep -q starting
    if [[ $? -eq 0 ]] ; then
        echo " Job tracker has been successfully started "
    else
        echo " Job tracker has not been started I think "
    fi
}

######################################################################################
# This function set the count for no of successfull jobs that will
# blacklist a tasktracker in mapred-site.xml and the param is mapred.max.tracker.blacklists
# This method takes the input value to be set
######################################################################################
function updateMaxTrackerBlacklistsCount {
    if [ -z "$JOBTRACKER" ] ; then
        echo "Empty job tracker host and so getting the hostname "
        getJobTracker
        echo " Got the job tracker host name  and it is  $JOBTRACKER "
    fi

    # Get the mapred-site.xml locally
    getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml

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
    modifyValueOfAField $ARTIFACTS_DIR/mapred-site.xml mapreduce.jobtracker.tasktracker.maxblacklists $1
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml 

    startJobTracker $JOBTRACKER
    sleep 15
    startJobTracker $JOBTRACKER
    sleep 15
    return 0
}

######################################################################################
#  This function triggers a sleep job provided in the example jar file
#  it takes the map $1, reduce $2, mapsleeptime $3, reducesleeptime  $4 and number of jobs $5  as parameters
######################################################################################
function triggerSleepJob {
    for (( i = 0; i < $5; i++ )); do
        createFile $ARTIFACTS_DIR/sleep.$i.log
        if [ -z "$6" ] ; then
            echo " I dont have any queue details and so submitting the job to default queue"
            $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_TEST_JAR sleep -m $1 -r $2 -mt $3 -rt $4 > $ARTIFACTS_DIR/sleep.$i.log 2>&1&
        else
            echo " I have queue details and so submitting the job to $6 queue"
            $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_TEST_JAR sleep $6 -m $1 -r $2 -mt $3 -rt $4 > $ARTIFACTS_DIR/sleep.$i.log 2>&1&
        fi
    done
}

######################################################################################
# This function returns the JOBID of the task submitted
######################################################################################
function getSubmittedJobId {
    JOBID=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list |tail -1 | awk '{print $1}'`
    if [ -z "$JOBID" ] ; then
        echo " Job id is empty "
        return 1
    else
        echo "Job ID of the running job is " $JOBID
        return 0
    fi
}

######################################################################################
# This function  kills the job for a given job id  $1
######################################################################################
function killJob {
    local captureFile=$2
    if [ "X"$captureFile"X" == "XX" ] ; then    
        $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $1 |tail -1 |cut -f 1 | awk -F" " '{print $1}'
    else
        $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $1 > $captureFile 2>&1
    fi
}

function killAllRunningJobs {
    JOBIDS=( `cat "$1" | grep job_ | awk '{print $1}'` )
    echo "${JOBSID[*]}"
    num=${#JOBIDS[*]}
    i=0
    while [ "$i" -lt "$num" ] ; do
        $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill "${JOBIDS[$i]}"
        (( i = $i + 1 ))
    done
}

######################################################################################
# This function builds the task ids for a given job id
# This takes 2 params : JobID $1, no of task ids to be generated $2
######################################################################################
function buildTaskIdsForJobId {
    TASKIDFORMAT=$(echo $1|sed 's/job/task/g'|sed 's/$/_m_00000/g')
    echo" task id format is  $TASKIDFORMAT"
    for (( j=0; j < $2; j++ )) ; do
        TASKID$j=$(echo $TASKIDFORMAT |sed 's/$/'$j'/g')
        echo "Created task ids : $TASKID$j"
    done
}

######################################################################################
# This function builds the attempt ids
# Based on the setting every task attempts 4 times
# This function takes the task id $1 and number of attempt ids $2 to generate
######################################################################################
function buildAttemptIdsForTaskId {
    ATTEMPTIDFORMAT=$(echo $1|sed 's/task/attempt/g')
    echo "attempt id format is $ATTEMPTIDFORMAT"
    for (( k=0; k < $2; k++ )) ; do
        ATTEMPTID$k=$(echo $ATTEMPTIDFORMAT |sed 's/$/_'$k'/g')
        echo "Created attempt ids: $ATTEMPTID$k "
    done
}

######################################################################################
# This function kills the process on task tracker
# takes  the TT host as $1 and the Process id as $2
######################################################################################
function killProcessOnTT {
    ssh $1 kill -9 $3
}

###########################################
# Function to  move files to different machines
# This function expects host,full path to file, where do u want to drop the file
function getFileFromRemoteServer {
    echo " Getting  $2 file from $1  and dropping it at $3 "
    echo "scp ${HADOOPQA_USER}@$1:$2 $3"
    scp ${HADOOPQA_USER}@$1:$2 $3
}

###########################################
# Function to  put files to different machines
# This function expects host,full path to file, where do u want to drop the file
###########################################
function putFileToRemoteServer {
    echo " putting  $1 file   $2 dropping it at $3 "
    echo "scp $2 ${HADOOPQA_USER}@$1:$3 "

    scp $2 ${HADOOPQA_USER}@$1:$3
}

##########################################
# Function to scp a file from JT, modify it, and scp back
# Params: $1 is the remote host; $2 is the file name with full path; $3 is the appended string
##########################################
function scpFileFromRemoteHost_AppendAStringAndSendBack {
    scp ${HADOOPQA_USER}@$1:$2 .
    filepath=`echo $script_file`
    filename=$(basename $filepath)
    path=$(dirname $filepath)
    echo "File name: $filename"
    echo "Path: $filepath"
    appendStringToAFile $filename "$3"
    scp $filename ${HADOOPQA_USER}@$1:$filepath
}

##########################################
# Function to scp a xml file from JT, modify a specified field
# with new value, and scp back
# Params: $1 is the remote host; $2 is the file path; $3 is the file name;
# $4 is the field in xml file; $5 is the new value
##########################################
function scpFileFromRemoteHost_ModifyAValueAndSendBack {
    scp ${HADOOPQA_USER}@$1:$2 .
    filepath=`echo $2`
    echo "full file path: $filepath"
    filename=$(basename $filepath)
    echo "File name: $filename"
    path=$(dirname $filepath)
    echo "Path: $path"
    modifyValueOfAField $filename "$3" "$4"
    scp $filename ${HADOOPQA_USER}@$1:$path
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
    blacklisted_trackers=` getBlacklistedTaskTrackers`
    local in_blacklisted=`echo $blacklisted_trackers | tr -d '\n' | grep $1`
    if [ -z "$in_blacklisted" ] ; then
        IN_BLACKLISTED=0
    else
        IN_BLACKLISTED=1
    fi
}

#########################################
# Function to run at the beginning of each test case
# Params: $1 is the test case name
#########################################
function displayTestCaseMessage {
    echo "TEST CASE: $1"
    echo "Start testing: `date`"
    export REASONS=""
    export COMMAND_EXIT_CODE=0
}

###########################################
# Function to set Reason and flag for each verification point
# $1 is the reason of the failure
###########################################
function setFailCase {
    local newReason=$*
    (( COMMAND_EXIT_CODE = $COMMAND_EXIT_CODE + 1 ))
    if [ "X${REASONS}X" == "XX" ];then
        REASONS=$newReason
    else
        REASONS="${REASONS} - ${newReason}"
    fi
}

###########################################
# Function to check the result of the test run
###########################################
function checkJobSummaryResult {
    (( RESULT = $RESULT + $1 ))
    if [ "$1" -eq 0 ] ; then
        displayHeader "$TESTCASE_DESC PASSED"
    else
        displayHeader "$TESTCASE_DESC FAILED"
    fi
}

##########################################
# Function to display Test Summary of the whole Hudson job
##########################################
function displayTestSummary {
    TOTAL_PASSED=0
    TOTAL_FAILED=0
    TOTAL=0

    for JOB in $JOBS ; do
        NPASSED=`grep 'TEST CASE RESULT' "${ARTIFACTS}/${JOB}/${JOB}.log" | grep PASSED | wc -l `
        NFAILED=`grep 'TEST CASE RESULT' "${ARTIFACTS}/${JOB}/${JOB}.log" | grep FAILED | wc -l `
        NTOTAL=`grep 'TEST CASE RESULT'  "${ARTIFACTS}/${JOB}/${JOB}.log" | wc -l `
        (( TOTAL_PASSED=$TOTAL_PASSED+$NPASSED ))
        (( TOTAL_FAILED=$TOTAL_FAILED+$NFAILED ))
        (( TOTAL=$TOTAL+$NTOTAL ))
    done
    echo "#################### TEST JOB SUMMARY #######################"
    echo "Total test cases ran: $TOTAL"
    echo "Total test cases passed: $TOTAL_PASSED"
    echo "Total test cases failed: $TOTAL_FAILED"
    echo "*************************************************************"
}

function process_testcasename {
    local taplog=$1
    local tcname=$2
    if [ -f $taplog ]; then
        # echo "cut -d':' -f2  $taplog | awk '{print \$1}' | grep -wq $tcname"
        cut -d':' -f2  $taplog | awk '{print $1}' | grep -wq $tcname
        local match=$?
        if [ $match -eq 0 ]; then
            # if the same test case name is found, modify the name to make it unique
            local num_dups=`/bin/cut -d':' -f2  $taplog | /bin/awk '{print $1}' | /bin/grep ^${tcname}_DUP | /usr/bin/wc -l`
            (( num_dups = $num_dups + 1 ))
            echo "Found '$num_dups' duplicate test case name '$tcname' in '$taplog': Use name '${tcname}_DUP_$num_dups':"
            tcname="${tcname}_DUP_$num_dups"
        fi
    fi
    newtestcasename=$tcname
}

# filter the character '#' from $msg so that it will not be intepretated
# by the TAP parser as a directive, and consequently ignore the trailing # skip
# directive that will be added to the end of the string.
function process_tap_message {
    msg=`echo $msg|sed 's/#/:/g'`
}

################################################################################
# Function to 1) display Test Case result, 2) create .pass/.fail file at 
# $ARTIFACTS_DIR and 3) update passSummary.log or failSummary.log. 
#
# Before calling this function in the testcase, set the status 0 or 1 of the 
# testcase in COMMAND_EXIT_CODE and populate the reason for failing in REASONS
# variable for failed testcases.   If TESTCASENAME is not set inside the testcase
# as exported variable, TESTCASENAME will be taken as the name of the caller
# function. It also expects description of the testcase is also populated inside
# the testcase
################################################################################
function displayTestCaseResult {
   ## Get the testcase name who called this function
    if [[ -z $TESTCASENAME ]]; then
        TESTCASENAME=`caller 0|awk '{print $2}'`
    fi
    local TESTCASEINDEX=$1

    if [[ -n $TESTCASEINDEX ]]; then
        newtestcasename="${TESTCASENAME}_${TESTCASEINDEX}"
    else
        newtestcasename=$TESTCASENAME
    fi
    TESTCASENAME=""

    if [ ! -f $TEST_SUMMARY_ARTIFACTS/countSummary ];then
        echo "FATAL :: $TEST_SUMMARY_ARTIFACTS/countSummary has been deleted just now !!"
        echo "         Test Regression can not be proceeded."
        exit 1
    fi

    # Process the test case name and make it unique if needed. Otherwise it can
    # cause a number of issues. e.g. test counts being out of sync. (BUG 5344759)
    taplog=$TEST_SUMMARY_ARTIFACTS/TAP.log
    process_testcasename $taplog $newtestcasename

    RUNSCOUNT=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f1`
    PASSCOUNT=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f2`
    FAILCOUNT=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f3`
    SKIPCOUNT=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f4`

    echo "*************************************************************"
    echo "End testing: `date`"
    if [ $COMMAND_EXIT_CODE -ne 0 -o $SCRIPT_EXIT_CODE -ne 0 ] ; then
        echo "COMMAND_EXIT_CODE=$COMMAND_EXIT_CODE: SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
    fi

    (( RUNSCOUNT = $RUNSCOUNT + 1 ))
    export RUNSCOUNT=$RUNSCOUNT

    tc_run_file="$ARTIFACTS_DIR/$newtestcasename.run"
    tc_pass_file="$ARTIFACTS_DIR/$newtestcasename.pass"
    tc_fail_file="$ARTIFACTS_DIR/$newtestcasename.fail"
    tc_skip_file="$ARTIFACTS_DIR/$newtestcasename.skip"
    touch $tc_run_file

    genArt=`echo $ARTIFACTS_DIR | sed 's/.*artifacts[-]*[A-Za-z]*[-]*[A-Za-z]*\/\(.*\)/\1/'`
    passfailtest=`echo $genArt | sed 's/.*-[0-9]*\/\(.*\)/\1/'`
    passsummarylog=$TEST_SUMMARY_ARTIFACTS/passSummary.log
    skipsummarylog=$TEST_SUMMARY_ARTIFACTS/skipSummary.log
    failsummarylog=$TEST_SUMMARY_ARTIFACTS/failSummary.log
    testhistorylog=$TEST_SUMMARY_ARTIFACTS/testHistory.log

    testdetails="TEST CASE RESULT: NAME=>$newtestcasename :: DESC=>$TESTCASE_DESC :: STATUS=>"
    recorddetails="$passfailtest:$newtestcasename|$TESTCASE_DESC|$OWNER|"

    # Include lower/upper case, singular/plural, with or without separator ':'
    shopt -s nocasematch

    # Known bug must be listed in the known_bugs.txt file. Once the bug has been
    # resolved, the test will be marked as failure when it passes because it is
    # expected to fail. This will given an alert to remove the bug from the
    # known_bugs.txt file. If it is not removed, it could potentially mask other
    # failures in the future for different reasons. 
    
    # One corner case where this could be problematic is when the bug has been
    # resovled but the fix did not address the problem so the test continues to
    # fail and be marked as skipped. This could also mask other failures in the
    # tests if not addressed. However this should not be a major problem as
    # manual verification of the ticket will detect that the problem still
    # persists, and therefore the ticket needs to be reopened.

    # Issue of granularity: Known bugs are associated to a single test case. If
    # there are multiple sub test cases within a single test case
    # (TESTCASENAME), failures not caused by the known bug could be masked.
    if [ -z ${KNOWNBUGS_FILE} ]; then
        KNOWNBUGS_FILE="known_bugs.txt"
    fi
    BUGFILE="${WORKSPACE}/conf/test/"${KNOWNBUGS_FILE}

    # Grep for exact word match in the known bug list file
    KNOWN_BUG=`/bin/grep -w $newtestcasename $BUGFILE|grep -v ^#|awk '{print $3}'|/usr/bin/tr -d ' '`
    ALWAYS_SKIP=`/bin/grep -w $newtestcasename $BUGFILE|grep -v ^#|awk '{print $4}'|/usr/bin/tr -d ' '`

    # Check if a failure should be marked todo if it has been failing
    # sporadically for a while.
    # Failures could be caused by either 1) test passes (zero exit code) with
    # known bug, or 2) if test fails (non-zero exit code) without known bug
    TODO_BUG=0
    if ( ( [[ $COMMAND_EXIT_CODE -ne 0 ]] && [[ -z $KNOWN_BUG ]] ) ||
         ( [[ $COMMAND_EXIT_CODE -eq 0 ]] && [[ -n $KNOWN_BUG ]] )    ); then 
        if ([ -f $testhistorylog ]); then
            echo "check if failed test '$newtestcasename' exists in old test archives: $testhistorylog"
            prev_failures=`grep $newtestcasename $testhistorylog| awk '{print $1}'`
            if [[ -n $prev_failures ]]; then
                num_archives=`grep 'out of' $testhistorylog | awk '{print $4}' | sed s/\'//g`
                REASONS="$REASONS: Failed $prev_failures in last $num_archives test cycles"
                
                cmd="use Math::Round; printf(\"%.2f\n\",nearest(0.01, ($prev_failures+1)/$num_archives));"
                fail_rate=`perl -e "$cmd"`
                freq=0.5
                if [[ $fail_rate > $freq ]]; then
                    TODO_BUG=1
                    REASONS="$REASONS: failure frequency > $freq"
                fi
            fi
        fi
    fi

    # 1) If test fails (non-zero exit code) with known bug, mark test as SKIP.
    # 2) If test succeeds with known bug and ALWAYS_SKIP is defined, then still
    #    mark test as SKIP.
    # 3) if this is a known test failure based on high failure frequency,
    # mark test as SKIP. Ideally this should be marked as TODO as supported by
    # TAP. However Hudson does not support this in its TAP output plugin.
    if ( ( [[ $COMMAND_EXIT_CODE -ne 0 ]] && [[ -n $KNOWN_BUG ]] ) ||
         ( [[ $COMMAND_EXIT_CODE -eq 0 ]] && [[ -n $KNOWN_BUG ]] && [[ $ALWAYS_SKIP == "skip" ]] ) ||
         ( [[ $TODO_BUG -eq 1 ]] )                                    ); then
        (( SKIPCOUNT = $SKIPCOUNT + 1 ))
        echo "${recorddetails}$tc_skip_file" >> $skipsummarylog
        echo "${testdetails} SKIPPED"
        if [[ -n $KNOWN_BUG ]]; then
            reasons_str="$REASONS (known bug $KNOWN_BUG)"
        else
            reasons_str="$REASONS"
        fi
        echo $reasons_str >> $tc_skip_file
        echo "REASONS :: $reasons_str"
        local msg="ok $RUNSCOUNT NAME:$newtestcasename   DESC:$TESTCASE_DESC   REASONS:$reasons_str"
        # filter the character '#' from $msg so that it will not be intepretated
        # by the TAP parser as a directive, and will consequently ignore the 
        # trailing # skip directive. 
        process_tap_message $msg
        if [[ -n $KNOWN_BUG ]]; then
            echo "$msg # skip" >> $taplog
        else
            echo "$msg # todo" >> $taplog
        fi
        touch $tc_skip_file
    # If test passes (zero exit code) without known bug, mark test as PASS
    elif ( [[ $COMMAND_EXIT_CODE -eq 0 ]] && [[ -z $KNOWN_BUG ]] ) ; then
        (( PASSCOUNT = $PASSCOUNT + 1 ))
        echo "${recorddetails}" >> $passsummarylog
        echo "${testdetails} PASSED"
        echo "ok $RUNSCOUNT NAME:$newtestcasename   DESC:$TESTCASE_DESC" >> $taplog
        touch $tc_pass_file
    # If test passes (zero exit code) with known bug, or if test fails
    # (non-zero exit code) without known bug, then mark test as FAIL
    else
        # Tests associated with known bug should not passs with exit code 0
        if ( [[ $COMMAND_EXIT_CODE -eq 0 ]] && [[ -n $KNOWN_BUG ]] ) ; then
            REASONS="Test tagged with '$KNOWN_BUG' is expected to fail but succeeded with exit code $COMMAND_EXIT_CODE. Please investigate."
        fi
        (( FAILCOUNT = $FAILCOUNT + 1 ))
        echo "${recorddetails}$tc_fail_file" >> $failsummarylog
        echo "$REASONS" >> $tc_fail_file
        echo "${testdetails} FAILED"
        echo "REASONS :: $REASONS"
        echo "not ok $RUNSCOUNT NAME:$newtestcasename   DESC:$TESTCASE_DESC   REASONS:$REASONS" >> $taplog
        (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + 1 ))
    fi
    shopt -u nocasematch

    if [ $COMMAND_EXIT_CODE -ne 0 -o $SCRIPT_EXIT_CODE -ne 0 ] ; then
        echo "COMMAND_EXIT_CODE=$COMMAND_EXIT_CODE: SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
    fi
    echo "*************************************************************"
    echo "${RUNSCOUNT},${PASSCOUNT},${FAILCOUNT},${SKIPCOUNT}" > $TEST_SUMMARY_ARTIFACTS/countSummary

    ## Reset Environment Variables
    TESTCASENAME=""
    TESTCASE_DESC=""
    REASONS=""
    COMMAND_EXIT_CODE=0
    ASSERTCOUNT=0
    return $SCRIPT_EXIT_CODE
}

##########################################
# Function to display Test Suite result
##########################################
function displayTestSuiteResult {
    logname=$1
    RUNSCOUNT=`ls -l ${ARTIFACTS_DIR}/*.run  | wc -l | awk '{print $1}'`
    PASSCOUNT=`ls -l ${ARTIFACTS_DIR}/*.pass | wc -l | awk '{print $1}'`
    FAILCOUNT=`ls -l ${ARTIFACTS_DIR}/*.fail | wc -l | awk '{print $1}'`
    SKIPCOUNT=`ls -l ${ARTIFACTS_DIR}/*.skip | wc -l | awk '{print $1}'`

    # MAINTENANCE NOTE: While a test suite runs, if the test neglects to call
    # displayTestCaseResult on one or more test case failures, there could be
    # a scenario where FAILCOUNT is less than SCRIPT_EXIT_CODE. Similarly, we
    # could have a case FAILCOUNT might be 0, and SCRIPT_EXIT_CODE is greater
    # than 0. This loophole should be properly dealt with.
    if [ $SCRIPT_EXIT_CODE -ne $FAILCOUNT ] ; then
        echo "SCRIPT_EXIT_CODE ($SCRIPT_EXIT_CODE) different than FAILCOUNT ($FAILCOUNT)"
    fi

    echo "******************** TEST SUITE SUMMARY *********************"
    echo "# TestSuite               : $TESTSUITE" "$TESTARGS"
    echo "# Total test cases ran    : $RUNSCOUNT"
    echo "# Total test cases passed : $PASSCOUNT"
    echo "# Total test cases skipped: $SKIPCOUNT" 
    echo "# Total test cases failed : $FAILCOUNT"
    echo "*************************************************************"
    echo "# TestSuite Log   :: $ARTIFACTS_FILE"
    echo "*************************************************************"

    if [ $SCRIPT_EXIT_CODE -eq 0 -a $FAILCOUNT -eq 0 ] ; then
        echo "TEST SUITE RESULT: PASSED"
    else
        echo "TEST SUITE RESULT: FAILED"
        echo 
        echo "To debug failed issues, check -"
        echo "    1. *.fail files at $ARTIFACTS_DIR"
        echo "    2. For detailed log see $ARTIFACTS_FILE"
    fi
    echo "*************************************************************"
}

##########################################
# Function to check Job Scheduling info on WebUI
##########################################
function checkJobSchedulingInfoOnWebUI {
    echo "JobID: $1"
    getJobTracker
    wget --quiet "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jobinfo
    cat $ARTIFACTS_DIR/jobinfo | grep "$1" | grep "$2"
    return $?
}

###########################################
# Sort files from one directory to another
# $1 - input
# $2 - output
# $3 - fs
###########################################
function sortJob {
    local input=$1
    local output=$2
    local fs=$3
    local cmd
    if [ -z "$fs" ]; then
        cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR sort $input $output"
    else
        cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR sort -fs $fs $input $output"
    fi
    echo $cmd
    eval $cmd
}

###########################################
# Generate a file locally
# $1 = filename
# $2 = size
###########################################
function Util_GenerateFileSizeOf {
    touch $1
    filesize=`ls -l $1 | cut -f 5 -d " "`
    while  [ $filesize -lt $2 ] ; do
        ps auwxx >> $1
        filesize=`ls -l $1 | cut -f 5 -d " "`
    done
}

###########################################
# Generate a file locally
# $1 = filename
###########################################
function Util_returnLocalFileSizeOf {
    filesize=`ls -l $1 | cut -f 5 -d " "`
    echo $filesize
    return $filesize
}

#########################################
# Function to modify a value of a field in an xml file
# Params: $1 is the file with full path; $2 is the property, $3 is the new value,$4 is the description,$5 is final value, any value you specify make sure it is escaped otherwise it wont work. THIS FUNCTION IS A WORK IN PROGRESS DONT USE IT RIGHT NOW.
#########################################
function addPropertyToXMLConf {
    #read the file name with full path
    local file=$1
    #get the property name
    local property=$2
    #get what value should be set for that
    local propValue=$3
    #get the description
    local desc=$4
    #get the value for the final tag
    local finalVal=$5

    #create the property text, make sure the / are escaped
    propText="<property>\n<name>$property<\/name>\n<value>$propValue<\/value>\n<description>$desc<\/description>\n<final>$finalVal<\/final>\n<\/property>\n"

    #check to make sure if the property exists or not
    local myresp=`xmllint $1 | grep "<name>${property}</name>" |wc -l`
    
    #get the status of the previous command
    #status="$?"
    if [ $myresp -ne 0 ] ; then
    #then call the available method to update the property
        modifyValueOfAField $file $property $propValue
    else
    #add the property to the file
        endText="<\/configuration>"
    #add the text using sed at the end of the file
        sed -i "s|$endText|$propText$endText|" $file
    fi
}

##########################################
# Function to get all the namenodes http address
# It will use the $HADOOP_CONF_DIR evn variable to determine the value
# $1 - whether you want to get the prort or not
########################################
function getNameNodes {
    local withPort=$1
    local configFile=${HADOOP_CONF_DIR}/${CLUSTER}.namenodeconfigs.xml

    #get the list of the manenodes from $CLUSTER.namenodeconfigs.xml
    local nns=$(getValueFromField $configFile dfs.federation.nameservices)

    local arr_nns=$(echo $nns | tr "," "\n")
    
    local property
    local hostname
    local all_nns=""
    #for each nanode
    local i=0
    for nn in $arr_nns ; do
        property=dfs.namenode.http-address.${nn}
        
    #if withPort=true is sent then return the host with the port
        if [ "$withPort" == "true" ]; then
            hostname=$(getValueFromField $configFile $property)
        else
            hostname=$(getValueFromField $configFile $property | cut -f 1 -d ":")
        fi

    #append the hostname to all_nns
        if [ $i -eq "0" ];then
            all_nns="${hostname}"
        else
            all_nns="${all_nns};${hostname}"
        fi
        i=$[ $i + 1 ]
    done

    echo $all_nns
}

######################################
# Function to get the default namenode
######################################
function getDefaultNameNode {
    #the default fs is defined in core-site.xml and is of the format hdfs://server:port
    getValueFromField ${HADOOP_CONF_DIR}/core-site.xml fs.defaultFS | cut -f 2 -d ":"  | cut -f 3 -d "/"
}

##########################################
# Function to get all the secondary namenode http address
# It will use the $HADOOP_CONF_DIR evn variable to determine the value
# $1 - whether you want to get the port or not
# RETURNS - comma separated list of all secondary namenodes
########################################
function getSecondaryNameNodes {
    local withPort=$1
    local configFile=${HADOOP_CONF_DIR}/${CLUSTER}.namenodeconfigs.xml

    #get the list of the manenodes from $CLUSTER.namenodeconfigs.xml
    local nns=$(getValueFromField $configFile dfs.federation.nameservices)

    local arr_nns=$(echo $nns | tr "," "\n")

    local property
    local hostname
    local all_snn=""
    #for each nameode
    local i=0
    for nn in $arr_nns ; do
        property=dfs.namenode.secondary.http-address.${nn}

    #if withPort=true is sent then return the host with the port
        if [ "$withPort" == "true" ]; then
            hostname=$(getValueFromField $configFile $property)
        else
            hostname=$(getValueFromField $configFile $property | cut -f 1 -d ":")
        fi

    #append the hostname to all_nns
        if [ $i -eq "0" ];then
            all_snn="${hostname}"
        else
            all_snn="${all_snn};${hostname}"
        fi
        i=$[ $i + 1 ]
    done

    echo $all_snn
}

##########################################
# Function to get the secondary namenode based on the namenode address
# It will use the $HADOOP_CONF_DIR evn variable to determine the value
# $1 - namenode name
# $2 - whether you want to get the port or not
# RETURNS - the hostname of the secondary namenode to the namenode
########################################
function getSecondaryNameNodeForNN {
    local nn=$1
    local nn_suffix=`echo $nn | cut -d'.' -f 1`
    local withPort=$2
    local configFile=${HADOOP_CONF_DIR}/${CLUSTER}.namenodeconfigs.xml

    local property=dfs.namenode.secondary.http-address.${nn_suffix}
    local hostname
    #if withPort=true is sent then return the host with the port
    if [ "$withPort" == "true" ]; then
        hostname=$(getValueFromField $configFile $property)
    else
        hostname=$(getValueFromField $configFile $property | cut -f 1 -d ":")
    fi

    echo $hostname
}

##########################################
# Function to get the datanodes http address
# It will use the $HADOOP_CONF_DIR evn variable to determine the value
########################################
function getDataNodes {
    #get the default namenode
    local nn=$(getDefaultNameNode)

    #get the list of datanodes from the NN
    #local dns=`ssh ${HADOOPQA_USER}@$nn exec "cat ${HADOOP_CONF_DIR}/slaves"`
    local dns=`ssh $nn "cat ${HADOOP_CONF_DIR}/slaves"`
    #replace space with ;
    dns=$(echo -e $dns | tr " " ";")
    echo $dns
}

##########################################
# Function to get the tasktrackers http address
# It will use the $HADOOP_CONF_DIR evn variable to determine the value
########################################
function getTaskTrackers {
    #get the job tracker
    local jt=$(getJobTracker_TMP)

    #get the list of tasktrackers from JT
    local tts=`ssh $jt "cat ${HADOOP_CONF_DIR}/slaves"`
    #replace space with ;
    tts=$(echo -e $tts | tr " " ";")
    echo $tts
}

###########################################
# Function to get the Jobtracker hostname
# $1 - whether you want to get the prort or not
###########################################
function getJobTracker_TMP {
    #use the method to get the resource manager
    getResourceManager $1
}

###########################################
# Function to get the resourcemanager hostname
# $1 - whether you want to get the prort or not
###########################################
function getResourceManager {
    local withPort=$1
    if [ "$withPort" == "true" ]; then
        getValueFromField ${HADOOP_CONF_DIR}/yarn-site.xml yarn.resourcemanager.scheduler.address 
    else
    #remove the port and return
        getValueFromField ${HADOOP_CONF_DIR}/yarn-site.xml yarn.resourcemanager.scheduler.address | cut -f 1 -d ":" 
    fi
}

##########################################
# Function to build passed/failed tests in log file
##########################################
function buildSummaryLog {
    numpass=0
    numfail=0
    for JOB in $JOBS ; do
        while read line ; do
            if echo $line | grep -q 'TEST CASE RESULT'; then
                if echo $line | grep -q 'PASSED'; then
                    (( numpass = $numpass + 1 ))
                    echo $line | sed "s|TEST CASE RESULT:|$numpass.|" >> ${ARTIFACTS}/passSummary.log
                elif echo $line | grep -q 'FAILED'; then
                    (( numfail = $numfail + 1 ))
                    echo $line | sed "s|TEST CASE RESULT:|$numfail.|" >> ${ARTIFACTS}/failSummary.log
                fi
            elif echo $line | grep -q 'Reasons'; then
                echo $line >> ${ARTIFACTS}/failSummary.log
            else
                continue
            fi
        done < "${ARTIFACTS}/${JOB}/${JOB}.log"
    done
}

##############################################
# Function to create a file of 0 lenght on hdfs
# $1 - file name including full path
##############################################
function createFileOnHDFS {
    local fName=$1
    local fs=$2

    #check if its empty
    if [ -z "$fName" ]; then
        echo "Empty or null file name sent"
        return 1
    fi
    
    local cmd1
    local cmd2

    if [ -z "$fs" ]; then
        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls $fName"
        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -touchz $fName"
    else
        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -ls $fName"
        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -touchz $fName"
    fi

    #make sure that the file does not exist
    echo $cmd1
    eval "${cmd1}"
    if [ "$?" -eq "0" ];then
        echo "file $fName already exists"
        return 1
    fi

    #run the hadoop touch command
    echo $cmd2
    eval "${cmd2}"
    if [ "$?" -ne "0" ];then
        echo "file $fName was not created"
        return 1
    fi
    return 0
}

###########################################
# Function to validate the output and to mark the test as a PASS/FAIL
###########################################
function validateDFSCallsFromStreamingOutput {
    testname=`caller 0|awk '{print $2}'`
    testcasedir=$ARTIFACTS_DIR/$testname
    mkdir -p $testcasedir
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat  DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out/* >  $testcasedir/actualOutput
    REASONS=`diff --ignore-space-change ${JOB_SCRIPTS_DIR_NAME}/data/dfsCallsFromStreaming-${TESTCASE_ID}/expectedOutput $testcasedir/actualOutput`
    if [ "X${REASONS}X" == "XX" ];then
        return 0
    else
        return 1
    fi
}

###########################################
# Function to validate the output and to mark the test as a PASS/FAIL
###########################################
function validateJobJarPermissionsOutput {
    testname=`caller 0|awk '{print $2}'`
    testcasedir=$ARTIFACTS_DIR/$testname
    mkdir -p $testcasedir
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat  JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/JobJarPermissions${TESTCASE_ID}.out/* >  $testcasedir/actualOutput
    REASONS=`diff ${JOB_SCRIPTS_DIR_NAME}/data/JobJarPermissions-${TESTCASE_ID}/expectedOutput $testcasedir/actualOutput`
    if [ "X${REASONS}X" == "XX" ];then
        return 0
    else
        return 1
    fi
}

###########################################
# Function to validate the output and to mark the test as a PASS/FAIL
###########################################
function validateRecursiveChmodOutput {
    testname=`caller 0|awk '{print $2}'`
    testcasedir=$ARTIFACTS_DIR/$testname
    mkdir -p $testcasedir
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat  RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/JobJarPermissions${TESTCASE_ID}.out/* >  $testcasedir/actualOutput
    REASONS=`cmp ${JOB_SCRIPTS_DIR_NAME}/data/RecursiveChmod-${TESTCASE_ID}/expectedOutput $testcasedir/actualOutput`
    if [ "X${REASONS}X" == "XX" ];then
        return 0
    else
        return 1
    fi
}

###########################################
# Function to validate the output and to mark the test as a PASS/FAIL
###########################################
function validateOutput {
    testname=`caller 0|awk '{print $2}'`
    testcasedir=$ARTIFACTS_DIR/$testname/$TESTCASE_ID
    mkdir -p $testcasedir
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat /tmp/Streaming/streaming-$TESTCASE_ID/Output/*  >  $testcasedir/actualOutput
    echo "REASONS=diff ${JOB_SCRIPTS_DIR_NAME}/data/streaming-${TESTCASE_ID}/expectedOutput $testcasedir/actualOutput"
    REASONS=`diff ${JOB_SCRIPTS_DIR_NAME}/data/streaming-${TESTCASE_ID}/expectedOutput $testcasedir/actualOutput`
    if [ "X${REASONS}X" == "XX" ];then
        return 0
    else
        return 1
    fi
}

###########################################
# Function to validate the pipes output and to mark the test as a PASS/FAIL
###########################################
function validatePipesOutput {
    testname=`caller 0|awk '{print $2}'`
    testcasedir=$ARTIFACTS_DIR/$testname
    mkdir -p $testcasedir
    $HADOOP_HDFS_CMD --config ${HADOOP_CONF_DIR} dfs -cat  Pipes/pipes-${TESTCASE_ID}/outputDir/* >  $testcasedir/actualOutput
    REASONS=`diff ${JOB_SCRIPTS_DIR_NAME}/data/pipes-${TESTCASE_ID}/expectedOutput $testcasedir/actualOutput`
    if [ "X${REASONS}X" == "XX" ];then
        return 0
    else
        return 1
    fi
}

######################################
# Function to get the default fs
######################################
function getDefaultFS {
    #the default fs is defined in core-site.xml and is of the format hdfs://server:port
    getValueFromField ${HADOOP_CONF_DIR}/core-site.xml fs.defaultFS
}


#################################################
#######################
# function to update the include in xml file
# $1 - file path
# $2 - old xi include value
# $3 - new xi include value
#######################
#################################################
function update_xi_include_in_file {
    local file=$1
    if [ -z "$file" ];then
        echo "empty value sent for file"
        return 1
    fi
    local oldVal=$2
    if [ -z "$oldVal" ];then
        echo "empty value sent for oldVal"
        return 1
    fi
    local newVal=$3
    if [ -z "$newVal" ];then
        echo "empty value sent for newVal"
        return 1
    fi

    #since " needs to be present in the string we have to escape it twice
    local oldString="<xi:include href=\\\"${oldVal}\\\"/>"
    local newString="<xi:include href=\\\"${newVal}\\\"/>"

    #check to see if the xi include exists
    local cmd="xmllint $1 | grep \"$oldString\""
    echo $cmd
    eval $cmd

    if [ "$?" -ne 0 ]; then
        echo "${oldString} does not exist in ${file}"
        return 1
    fi

    #update the text in the file
    cmd="sed -i \"s|${oldString}|${newString}|\" $file"
    echo $cmd
    eval $cmd

    return "$?"
}

###########################################
# Extract PassSummary and FailSummary
###########################################
function extractSummary {
    iFile=$1
    oFile=$2
    skipcount=0
    passcount=0
    failcount=0
    for tt in `cat $iFile|cut -d'/' -f1 | sort | uniq | tr '\n' ' '`; do
        echo "=============================" >> $oFile
        echo "  $tt " | tr '[a-z]' '[A-Z]'   >> $oFile
        echo "=============================" >> $oFile
        for comp in `grep "$tt/" $iFile | cut -d'/' -f2| sort | uniq | tr '\n' ' '`; do
            echo "-----------------------------" >> $oFile
            echo "     $comp " >> $oFile
            echo "-----------------------------" >> $oFile
            for job in `grep "$tt/$comp/" $iFile | cut -d'/' -f3 | cut -d ':' -f1 | sort | uniq | tr '\n' ' '`; do
                ### Fill execution time
                execTimeFile=$TEST_SUMMARY_ARTIFACTS/executionTime
                execTimeinSec=`grep "FEATURE:$job|" $execTimeFile | awk -F'|' '{SUM += $2} END { print SUM}'`
                startTime=`grep "FEATURE:$job|" $execTimeFile | head -1 | cut -d'|' -f3`
                endTime=`grep "FEATURE:$job|" $execTimeFile | tail -1 | cut -d'|' -f4`
                echo "     FEATURE :: \"$job\"      Took=`convertTime $execTimeinSec`  ST=$startTime  ET=$endTime" >> $oFile
                echo "     ............................" >> $oFile
                for testname in `grep "$tt/$comp/$job:" $iFile | cut -d':' -f2 | cut -d'|' -f1 | tr '\n' ' '`; do
                    desc=`grep "$tt/$comp/$job:$testname|" $iFile |sort | uniq | cut -d'|' -f2`
                    owner=`grep "$tt/$comp/$job:$testname|" $iFile | cut -d'|' -f3 | sort | uniq `
                    reason=`grep "$tt/$comp/$job:$testname|" $iFile | cut -d'|' -f4 | sort | uniq`
                    if [ "`echo $iFile| sed -e 's/.*\/\(.*\)/\1/'`" == "failSummary.log" ];then
                        (( failcount = $failcount + 1 ))
                                #echo "             $failcount. TEST : $testname    OWNER : $owner    DESC. : $desc" >> $oFile
                        echo "             $failcount. TEST    : $testname" >> $oFile
                        echo "                  OWNER   : $owner" >> $oFile
                        echo "                  DESC.   : $desc" >> $oFile
                        if [ -f $reason ];then
                            echo -n "                  REASONS :" >> $oFile
                            cat $reason  | sed -e 's/\(.*\)/ \1/'>> $oFile
                        else
                            echo "                  REASONS : NOT MENTIONED" >> $oFile
                        fi
                    else
                        if [ "`echo $iFile| sed -e 's/.*\/\(.*\)/\1/'`" == "skipSummary.log" ];then
                            (( skipcount = $skipcount + 1 ))
                                    #echo "             $passcount. TEST : $testname    OWNER : $owner    DESC. : $desc" >> $oFile
                            echo "             $skipcount. TEST    : $testname" >> $oFile
                            echo "                  DESC.   : $desc" >> $oFile
                            if [ -f $reason ];then
                                echo -n "                  REASONS :" >> $oFile
                                cat $reason  | sed -e 's/\(.*\)/ \1/'>> $oFile
                            else
                                echo "                  REASONS : NOT MENTIONED" >> $oFile
                            fi
                        else
                            (( passcount = $passcount + 1 ))
                                    #echo "             $passcount. TEST : $testname    OWNER : $owner" >> $oFile
                            echo "             $passcount. TEST : $testname" >> $oFile
                        fi
                    fi
                done
            done
        done
    done
}

###########################################
# Convert time in sec to hour min and sec
###########################################
function convertTime {
    local totalSec=$1
    local sec=0
    local totalMin=0
    local min=0
    local hr=0
    (( sec      = $totalSec % 60 ))
    (( totalMin = $totalSec / 60 ))
    (( min      = $totalMin % 60 ))
    (( hr       = $totalMin / 60 ))
    echo "${hr}h ${min}m ${sec}s"
}

##################################################
# Function to echo command to be executed 
# Execute command and automatically pass exit code 
# Capture STDOUT & STDERR for command
#################################################
function execcmdlocal {
    local cmd=$*
    host=`hostname | cut -d'.' -f1`
    echo "[`date +%D-%T`] $USER@$host > $cmd"
    $cmd 2>&1
}

##################################################
# executing only those testcase which are called
# with '-t' option with the testscripts, otherwise
# execute all the tests; testcase functions should
# be started with "test_"
##################################################
function executeTests {
    echo "========================== Executing testcases =========================="
    echo "$TESTARGS" | egrep "\-t *test_"
    if [ "$?" == 0 ];then
        TESTCASES=`echo $TESTARGS | sed -e 's/^.*-t\s\+\(.*\)\s*.*$/\1/' | sed -e 's/\s*//g'| tr ',' ' '`
    else
        TESTCASES=`egrep "function\s*test_" $TESTSUITE | egrep -v '^\s*#' | sed -e 's/^\s*function\s*\(test_.*\w\)\s*.*$/\1/' | tr '\n' ' '`
    fi

    ## Execute Tests
    for T in $TESTCASES; do
        ### Global SETUP
        echo
        echo "********************* STARTING TEST  : $T *********************"
        TESTCASENAME=$T
        COMMAND_EXIT_CODE=0
        REASONS=""
        ASSERTCOUNT=0
        TESTCASE_STARTTIME=`date +%s`
        ## Execute testcases 
        eval "${T}"

        echo "********************* FINISHING TEST : $T *********************"
        echo
    done
    ## Write Exit code in log
    echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
    ## Return SCRIPT_EXIT_CODE
    #exit $SCRIPT_EXIT_CODE
}

##################################################
# Function to handle failed assertions
##################################################
function handleFailedAssertions {
    local newreason=$*
    local reason="Assert-$ASSERTCOUNT: $newreason"
    echo "Assertion $ASSERTCOUNT - fail"
    setFailCase $reason
}
##################################################
# Assertion between two values
##################################################
function assertEqual {
    local result=$1
    shift
    local expected=$1
    shift
    local reason=$*
    local stat=0
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    if [ "${result}" == "${expected}" ];then
        echo "Assertion $ASSERTCOUNT - pass"
    else
        if [ "X${reason}X" == "XX" ];then
            reason="\"$result\" not equal to \"$expected\""
        fi
        handleFailedAssertions $reason
        stat=1
    fi
    return $stat
}

##################################################
# Assertion between two files
##################################################
function assertEqualTwoFiles {
    local result=$1
    shift
    local expected=$1
    shift
    local reason=$*
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    diff $expected $result
    local stat=$?
    if [ "$stat" == "0" ];then
        echo "Assertion $ASSERTCOUNT - pass"
    else
        if [ "X${reason}X" == "XX" ];then
            reason=`diff $expected $result`
        fi
        handleFailedAssertions $reason
    fi
    return $stat
}

##################################################
# Assertion between a command o/p and expected file
##################################################
function assertEqualCmdFile {
    local cmd=$1
    shift
    local expected=$1
    shift
    local reason=$*
    local result="$TMPDIR/result"
    echo "Asserting Command :: $cmd "
    $cmd > $result 2>&1 
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    diff $expected $result
    local stat=$?
    if [ "$stat" == "0" ];then
        echo "Assertion $ASSERTCOUNT - pass"
    else
        if [ "X${reason}X" == "XX" ];then
            reason=`diff $expected $result`
        fi
        handleFailedAssertions $reason
    fi
    return $stat
}

#####################################################
# Assertion between a command o/p and expected string
#####################################################
function assertEqualCmdString {
    local cmd=$1
    shift
    local expected=$1
    shift
    local reason=$*
    echo "Asserting Command :: $cmd "
    local result=`$cmd 2>&1`
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    if [ "X${result}X" == "X${expected}X" ];then
        echo "Assertion $ASSERTCOUNT - pass"
        return 0
    else
        if [ "X${reason}X" == "XX" ];then
            reason="$cmd output is not same with $expected"
        fi
        handleFailedAssertions $reason
        return 1
    fi
}

#####################################################
# Assertion between a command exit status 
#####################################################
function assertEqualCmdPassStatus {
    local cmd=$1
    shift
    local reason=$*
    echo "Asserting Command :: $cmd "
    $cmd
    local status=$?
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    if [ "$status" == "0" ];then
        echo "Assertion $ASSERTCOUNT - pass"
    else
        if [ "X${reason}X" == "XX" ];then
            reason="Exit code of $cmd is not zero"
        fi
        handleFailedAssertions $reason
    fi
    return $status
}

######################################################
# Assertion between a command o/p and expected pattern
######################################################
function assertLikeCmdPattern {
    local cmd=$1
    shift
    local expected=$1
    shift
    local reason=$*
    echo "Asserting Command :: $cmd "
    local result=`$cmd 2>&1`
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    echo $result | egrep "$expected"
    local status=$?
    if [ "$status" == "0" ];then
        echo "Assertion $ASSERTCOUNT - pass"
    else
        if [ "X${reason}X" == "XX" ];then
            reason="$cmd output does not match with pattern $expected"
        fi
        handleFailedAssertions $reason
    fi
    return $status
}

###########################################
# Function to create /benchmarks for DFSIO and NNBench benchmarks
###########################################
function createBenchmarksDir {
    getKerberosTicketForUser hdfs
    setKerberosTicketForUser hdfs

    echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls /benchmarks"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls /benchmarks
    if [ "$?" -ne 0 ]; then
        echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -mkdir /benchmarks"
        $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -mkdir /benchmarks
    fi

    # need 777 to allow different users (hdfs,hadoopqa) to access this path
    echo "$HADOOP_HDFS_CMD -config $HADOOP_CONF_DIR dfs -chmod -R 777 /benchmarks"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -chmod -R 777 /benchmarks
    getKerberosTicketForUser hadoopqa
    setKerberosTicketForUser hadoopqa
}

#####################################################
# Assertion between a command exit status 
# $1 - command you want to run
# $2 - reason you want to print in case it fails
# returns 0 if the command fails as expected or 1 if the command is succesfully run
#####################################################
function assertEqualCmdFailStatus {
    local cmd=$1
    shift
    local reason=$*
    echo "Asserting Command :: $cmd "
    $cmd
    local status=$?
    (( ASSERTCOUNT = $ASSERTCOUNT + 1 ))
    if [ "$status" -ne "0" ];then
        echo "Assertion $ASSERTCOUNT - pass"
        return 0
    else
        if [ "X${reason}X" == "XX" ];then
            reason="Exit code of $cmd is zero, command passed when it should not have"
        fi
        handleFailedAssertions $reason
        return 1
    fi
}

#########################################
# I/P $1 => hostname
#     $2 => command to be executed
#     $3 => options and arg for command
#########################################
function runasroot {
    local host=$1
    local command=$2
    local cmdoptsNargs=$3

    if [ "X${host}X" == "XX" ];then
        echo "ERROR :: Hostname not provided"
        return 1
    fi
    if [ "X${command}X" == "XX" ];then
        echo "ERROR :: Command not provided"
        return 1
    fi

    ssh $host exec "/usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot.command=\"$command\"" 2>&1 | grep -v 'ROOT='
    ssh $host exec "/usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot.cmdoptsNargs=\"$cmdoptsNargs\"" 2>&1 | grep -v 'ROOT='
    ssh $host exec "/usr/local/bin/yinst unset -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot.user" 2>&1 | grep -v 'ROOT='
    ssh $host exec "/usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot" 2>&1 | egrep -v  'ROOT=|hadoop_qe_runasroot'
    stat=$?
    if [ "$stat" != "0" ];then
        echo "FAILED :: ssh ${HADOOPQA_USER}@$host \"sudo /usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot"
        return 1
    fi
}

#########################################
# I/P $1 => hostname
#     $2 => command to be executed
#     $3 => user
#     $4 => options and arg for command
#########################################
function runasotheruser {
    local host=$1
    local command=$2
    local user=$3
    local cmdoptsNargs=$4

    if [ "X${host}X" == "XX" ];then
        echo "ERROR :: Hostname not provided"
        return 1
    fi
    if [ "X${command}X" == "XX" ];then
        echo "ERROR :: Command not provided"
        return 1
    fi
    if [ "X${user}X" == "XX" ];then
        echo "ERROR :: User not defined"
        return 1
    fi

    ssh $host exec "/usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot.command=\"$command\"" 2>&1 | grep -v 'ROOT='
    ssh $host exec "/usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot.cmdoptsNargs=\"$cmdoptsNargs\"" 2>&1 | grep -v 'ROOT='
    ssh $host exec "/usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot.user=\"$user\"" 2>&1 | grep -v 'ROOT='
    ssh $host exec "/usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot" 2>&1 | egrep -v 'ROOT=|hadoop_qe_runasroot'
    stat=$?
    if [ "$stat" != "0" ];then
        echo "FAILED :: ssh ${HADOOPQA_USER}@$host \"sudo /usr/local/bin/yinst start -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qe_runasroot"
        return 1
    fi
}

# Function to gather info when nn is in safemode
# $1 - fs; if none sent will use the default
# This method will capture the following
# 1. fsck result on the /user/hadoopqa
# 2. dfsadmin -report command
# 3. dfsadmin -metasave command if the namenode is in safemode
function getNSHealthInfo {
    local fs=$1
    local nn

    if [ -z "$fs" ]; then
        fs=$(getDefaultFS)
        nn=$(getDefaultNameNode)
    else
        nn=`echo $fs | cut -d'/' -f3 | cut -d':' -f 1`
    fi

    local logFile=${ARTIFACTS}/${nn}.debug.safemode.log
    local metasaveFile=hadoopqe.${nn}.debug.safemode.log.`date +%s`

    local dfsadmin_report_cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -fs $fs -report"
    local dfsadmin_metasave_cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -fs $fs -metasave ${metasaveFile}"
    local fsck_cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR fsck -fs $fs /"
    local dfsadmin_safemode_cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -fs $fs -safemode get | grep 'Safe mode is OFF'"
    
    #set the user to super user
    setKerberosTicketForUser $HDFS_SUPER_USER

    echo `date` | tee -a $logFile

    #issue the fsck command
    echo $fsck_cmd | tee -a $logFile
    eval $fsck_cmd >> $logFile


    echo `date` | tee -a $logFile
    
    #issue the dfsadmin report command
    echo $dfsadmin_report_cmd | tee -a $logFile
    eval $dfsadmin_report_cmd >> $logFile

    #if you are in safemode then issue the dfsadmin metasave command
    echo $dfsadmin_safemode_cmd
    eval $dfsadmin_safemode_cmd
    if [ "$?" -ne "0" ]; then
        echo `date` | tee -a $logFile
    #issue the dfsadmin metasave command
        echo $dfsadmin_metasave_cmd | tee -a $logFile
        eval $dfsadmin_metasave_cmd >> $logFile
    fi
    
    #set the user to hadoopqa
    setKerberosTicketForUser $HADOOPQA_USER
}

# Function to wait for a timeout to verify the namenode is out of safemode
# $1 - fs/nn you want to take out of safemode hdfs://host:8020
# $2 - timeout: how long you want to wait for the namenode to come out of safemode
function verifyNNOutOfSafemode {
    #set the user to hadoopqa
    setKerberosTicketForUser $HADOOPQA_USER
    local fs=$1
    local timeout=$2
    
    #if no fs is specified then use the default fs
    if [ -z $fs ]; then
        fs=$(getDefaultFS)
    fi

    local nn=`echo $fs | cut -d'/' -f3 | cut -d':' -f 1`

    #if no timeout is specified set it to 5 mins
    if [ -z $timeout ]; then
        timeout=$NN_SAFEMODE_TIMEOUT
    fi

    # NOTE: Don't pipe it to grep because we may need the pid for the wait process
    # to kill it later. $! returns the pid of the last process. Also the code is
    # not currently using the $? return value from the safemode_wait_cmd. When this
    # background task is not properly killed, it could cause the upstream test
    # function to takeNNOutOfSafemode to not be able to exit and hang.
    #
    # The original command that was causing test to hang:
    # local safemode_wait_cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfsadmin -fs $fs -safemode wait | grep 'Safe mode is OFF' &"
    local safemode_wait_cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfsadmin -fs $fs -safemode wait &"
    local safemode_get_cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfsadmin -fs $fs -safemode get | grep 'Safe mode is OFF'"
    
    #issuse the safemode wait command
    echo $safemode_wait_cmd
    eval $safemode_wait_cmd
    
    #get the process
    local safemode_wait_pid=$!

    # log the process for the process id
    /bin/ps -fp $safemode_wait_pid

    #for the time out duration wait and see if the namenode comes out of safemode
    local waitTime=30
    local i=1
    while [ $timeout -gt "0" ] ; do
        echo "TRY #${i}"
        echo $safemode_get_cmd
        eval $safemode_get_cmd
        #if it comes out of safemode then break
        if [ "$?" -eq "0" ]; then
            #wait for the safemode wait cmd to finish
            echo "Wait for safemode_wait_pid '$safemode_wait_pid' to finish"
            local cmd="wait $safemode_wait_pid"
            echo $cmd
            eval $cmd
            echo "INFO: NN $nn IS OUT OF SAFEMODE"
            
            #return 0 as namenode is out of safemode
            return 0
        fi
        echo "WAIT ${waitTime}s"
        sleep $waitTime
        timeout=$[$timeout - $waitTime]
        i=$[$i + 1]
    done

    #return 1 as nn is still in safemode
    echo "ALERT: NAMENODE $nn IS STILL IN SAFEMODE"
    #kill the process
    cmd="kill -9 $safemode_wait_pid"
    echo $cmd
    eval $cmd
    
    #captutre the info
    getNSHealthInfo $fs
    
    return 1
}

# Function to take the namenode out of safemode
# $1 - fs/nn you want to take out of safemode hdfs://host:8020
# $2 - timeout: how long you want to wait for the namenode to come out of safemode
function takeNNOutOfSafemode {
    #set the user to hadoopqa
    setKerberosTicketForUser $HADOOPQA_USER
    
    local fs=$1
    local timeout=$2
    local confDir=$3

    #if no fs is specified then use the default fs
    if [ -z $fs ]; then
        fs=$(getDefaultFS)
    fi

    local nn=`echo $fs | cut -d'/' -f3 | cut -d':' -f 1`

    #if no timeout is specified set it to 5 mins
    if [ -z $timeout ]; then
        timeout=$NN_SAFEMODE_TIMEOUT
    fi

    #if no conf dir is specified use the default
    if [ -z $confDir ]; then
        confDir=$HADOOP_CONF_DIR
    fi

    #verify if the namenode is out of safemode
    local status
    verifyNNOutOfSafemode $fs $timeout
    status="$?"
    #if the safemode is still on then restart the namenode
    if [ "$status" -ne "0" ]; then
        echo "ALERT: NAMENODE $nn IS STILL IN SAFEMODE. RESTARTING"
	cleanCorruptBlocks
        #restart the namenode
        resetNode $nn 'namenode' 'stop' $confDir
        resetNode $nn 'namenode' 'start' $confDir
        
        #wait for 5 secs
        sleep 5
        
        #verify if the namenode comes out of safemode
        verifyNNOutOfSafemode $fs $timeout
        status="$?"
    else
        #if it is out of safemode return
        return 0
    fi

    local safemode_leave_cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfsadmin -fs $fs -safemode leave"
    #if the namenode is still in safemode, then use the safemode leave command to leave
    if [ "$status" -ne "0" ]; then
        echo "ALERT: NAMENODE $nn IS STILL IN SAFEMODE. GOING TO ISSUE dfsadmin -safemode leave COMMAND"
        #set the user to hdfs super user
        setKerberosTicketForUser $HDFS_SUPER_USER
        echo $safemode_leave_cmd
        eval $safemode_leave_cmd
        status="$?"
        setKerberosTicketForUser $HADOOPQA_USER
        #if the above command was not successful then exit the run
        if [ "$status" -ne "0" ]; then
            echo "ALERT: EVEN AFTER dfsadmin -safemode leave COMMAND, $nn IS IN SAFEMODE. ENDING TEST EXECUTION"
            exit 1
        else
            echo "INFO: NN $nn HAS BEEN FORCEFULLY TAKEN OUT OF SAFEMODE"
        fi
    else
        #captutre the info
        getNSHealthInfo $fs
    fi

    #set the user to hadoopqa
    setKerberosTicketForUser $HADOOPQA_USER

    return
}

# Function to copy the config from a host
# This function will log onto the host and copy the config to the specified destination
# $1 - host(required): host from which you want to copy the config
# $2 - destDir(required): destination where you want to copy the config to. The dir will be deleted and then created in order to make sure we get new content
# $3 - confDir(optional): If you want to copy config from a location other than HADOOP_CONF_DIR please specify that in this var. If empty the default config dir will be user
function copyHadoopConfig {
    local host=$1
    local destDir=$2
    local confDir=$3

    #if host is empty return
    if [ -z "$host" ] ; then
        echo "No value sent for parameter host."
        return 1
    fi
    
    #if destDir is empty return
    if [ -z "$destDir" ] ; then
        echo "No value sent for parameter destDir."
        return 1
    fi
    
    #if confDir is empty then set it to HADOOP_CONF_DIR
    if [ -z "$confDir" ] ; then
        confDir=$HADOOP_CONF_DIR
    fi
    
    #remove the dir if present
    local cmd="rm -rf $destDir"
    echo $cmd
    eval $cmd
    if [ "$?" -ne "0" ] ; then
        echo "FATAL: Could not cleanup the old dir $confDir"
        return 1
    fi

    #create the dir
    cmd="mkdir -p $destDir"
    echo $cmd
    eval $cmd
    if [ "$?" -ne "0" ] ; then
        echo "FATAL: Could not create dir $confDir"
        return 1
    fi
	
    #copy the conf from snn
    cmd="ssh ${host} \"cp -r ${confDir}/* ${destDir}\""
    echo "$cmd"
    eval "$cmd"
    if [ "$?" -ne "0" ] ; then
        echo "FATAL: Could not copy conf dir $confDir from host $host "
        return 1
    fi

    #update the xml include for the namenode configs
    update_xi_include_in_file ${destDir}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${destDir}/${CLUSTER}.namenodeconfigs.xml"
    
    return 0
}

### Get list of LIVE datanodes
function getLiveDatanodes {
    curl -s "http://`getDefaultNameNode`:50070/dfsnodelist.jsp?whatNodes=LIVE" | grep 'browseDirectory.jsp' | sed -e 's/^.*title="\(.*\):1004" href="http:\/\/\(.*\):1006.*$/\2/g'| sort | tr '\n' ' '
}

### Get list of DEAD datanodes
function getDeadDatanodes {
    curl -s "http://`getDefaultNameNode`:50070/dfsnodelist.jsp?whatNodes=DEAD" | grep 'browseDirectory.jsp' | sed -e 's/^.*title="\(.*\):1004" href="http:\/\/\(.*\):1006.*$/\2/g'| sort | tr '\n' ' '
}

### Get list of DECOMMISSIONING datanodes
function getDecommDatanodes {
    curl -s "http://`getDefaultNameNode`:50070/dfsnodelist.jsp?whatNodes=DECOMMISSIONING" | grep 'browseDirectory.jsp' | sed -e 's/^.*title="\(.*\):1004" href="http:\/\/\(.*\):1006.*$/\2/g'| sort | tr '\n' ' '
}

#########################################################################################################
# Extract Log work with help of two functions called separately
# "TakeLogSnapShot"        : Before execting your command take a snapshot of log
# "ExtractDifferentialLog" : After  excuting your command call this to get differential log.
#       This gives 3 things for now
#       1. Gives in STDOUT all the differential content other than INFO
#       2. Save the whole differential content @ ARTIFACT_DIR/hadoop-${user}-${service}-${host}.log.diff
#       3. Assert whether any NullPointerException has come or not
#########################################################################################################
# I/P ->
#       $1 -> comp ( hdfs/mapred )
#       $2 -> service ( namenode/datanode
#            /jobtracker/tasktracker
#       $3 -> hostname
##################################################################
function TakeLogSnapShot {
    local comp=$1
    local service=$2
    local hosts=$3
    local user="${comp}qa"
    for host in `echo $hosts | tr ',' ' '`;do
        ssh $host "mkdir -p $TMPDIR;chmod -R 775 $TMPDIR"
        runasotheruser $host cp $user "$HADOOP_LOG_DIR/$user/hadoop-${user}-${service}-${host}.log $TMPDIR/hadoop-${user}-${service}-${host}.log.old"
        runasotheruser $host chmod $user "755 $TMPDIR/hadoop-${user}-${service}-${host}.log.old"
        ssh $host "mv  $TMPDIR/hadoop-${user}-${service}-${host}.log.old $SHARETMPDIR/hadoop-${user}-${service}-${host}.log.old"
        ssh $host "rm -rf $TMPDIR"
    done
}

##################################################################
# I/P ->
#       $1 -> comp ( hdfs/mapred )
#       $2 -> service ( namenode/datanode
#            /jobtracker/tasktracker
#       $3 -> hostname
#       $4 -> Ticket
# O/P -> $ARTIFACTS_DIR/hadoop-${user}-${service}-${host}.log.diff
##################################################################
function ExtractDifferentialLog {
    local comp=$1
    local service=$2
    local hosts=$3
    local ticket=$4
    if [ "$ticket" == "" ];then
        msg=""
    else
        msg="[ Bug - $ticket ]"
    fi
    local user="${comp}qa"
    for host in `echo $hosts | tr ',' ' '`;do
        ssh $host "mkdir -p $TMPDIR;chmod -R 775 $TMPDIR"
        runasotheruser $host cp $user "$HADOOP_LOG_DIR/$user/hadoop-${user}-${service}-${host}.log $TMPDIR/hadoop-${user}-${service}-${host}.log.new"
        runasotheruser $host chmod $user "755 $TMPDIR/hadoop-${user}-${service}-${host}.log.new"
        ssh $host "mv  $TMPDIR/hadoop-${user}-${service}-${host}.log.new $SHARETMPDIR/hadoop-${user}-${service}-${host}.log.new"
        ssh $host "rm -rf $TMPDIR"
        diff $SHARETMPDIR/hadoop-${user}-${service}-${host}.log.new $SHARETMPDIR/hadoop-${user}-${service}-${host}.log.old > $ARTIFACTS_DIR/hadoop-${user}-${service}-${host}.log.diff
        echo ">>>>>>>> Check WARN/ERROR/Exception in hadoop-${user}-${service}-${host}.log >>>>>>>>>>"
        cat $ARTIFACTS_DIR/hadoop-${user}-${service}-${host}.log.diff | grep -v ' INFO ' | sed -e 's/< //g'
        assertEqualCmdFailStatus "egrep -i 'NullPointerException' $ARTIFACTS_DIR/hadoop-${user}-${service}-${host}.log.diff" "NPE found in ${user}-${service}-${host}.log"
    done
}


#########################################################################################################
# Function to get the hadoop process
# $1 - host on which the process is running
# $2 - which service do you want to get the process for. It just greps for it. so essentially a grep term
##########################################################################################################
function getHadoopProcessId {
    local host=$1
    local service=$2
    
    if [ -z "$host" ] || [ -z "$service" ] ; then
        echo "FATAL: VALUE FOR host OR service NOT SENT."
        return 1
    fi
    local processId=''
    local cmd="ssh ${host} 'ps aux' | grep ${service} | grep [j]ava | awk '{print \$2}'"
    # echo $cmd
    processId=`eval $cmd`
    echo "$processId"
}

#####################################################################################################
# Function to get the jmk property value
# $1 - which host to get the property value from
# $2 - the process id of the service for which you want to get metrics for
# $3 - the service name for example some thing like Hadoop:name=NameNode,service=NameNode
# $4 - property for which you want to get the value for example FileInfoOp
# $5 - user as which this command should be run. If present then ssh will be executed as this user
# $6 - the keyFile. If present this file will be used as input to the ssh -i option.
######################################################################################################
function getJMXPropertyValue {
    local host=$1
    local processId=$2
    local serviceName=$3
    local property=$4
    local user=$5
    local keyFile=$6
    
    if [ -z "$processId" ] || [ -z "$serviceName" ] || [ -z "$property" ] || [ -z "$host" ] ; then
        echo "FATAL: ONE OF THE FOLLOWING PARAMETERS HAS NO VALUE: host, processId, serviceName, property"
        return 1
    fi

    if [ -n "$user" ] ; then
        if [ -n "$keyFile" ] ; then
            local cmd="ssh -i $keyFile ${user}@${host} '(echo open $processId; echo get -b $serviceName $property) | java -jar $JMX_JAR_FILE' | grep ${property} | awk '{print \$3}' | cut -d';' -f1"
        else
            local cmd="ssh ${user}@${host} '(echo open $processId; echo get -b $serviceName $property) | java -jar $JMX_JAR_FILE' | grep ${property} | awk '{print \$3}' | cut -d';' -f1"
        fi
    fi

    #echo $cmd
    local value=`eval $cmd`

    echo $value
}

#########################################
# Function to get the value from a properties file, where a proprety is defined in name value pairs in each line
# eg name=value
# Params:
# $1 file
# $2 property
#########################################
function getValueFromPropertiesFile {
    local file=$1
    local property=$2

    if [ -z "$file" ] || [ -z "$property" ] ; then
        echo "FATAL: ONE OR MORE OF THE FOLLOWING PARAMETERS ARE EMTPY: file, property"
        return 1
    fi

    local cmd="cat $file | grep '${property}=' | cut -d'=' -f2"
    local value=`eval $cmd`

    echo $value
}

function getElapseTime {
    before="$1"
    after="$2"
    elapsed_seconds="$(expr $after - $before)"
    echo "Elapsed time : `convertTime $elapsed_seconds`"
}

######################################################
# Function to get the cluster Id
#####################################################
function getClusterId {
    #get the defailt namenode
    local nn=$(getDefaultNameNode)
    
    #the curl command to get the clusterid
    local cmd="curl \"http://${nn}:50070/dfshealth.html\" | grep 'Cluster ID:' | awk -F'</td><td>' '{print \$2}' | cut -d'<' -f1"

    eval $cmd
}

######################################################
# Cleaning the cluster before starting
######################################################
function cleanCluster {
    getAllUserTickets
    setKerberosTicketForUser $HDFSQA_USER
    deletedDirs="/user/hadoop*/* /user/mapred*/* /benchmarks /mapred/history/done* /tmp/hadoop-yarn"
    removeCmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -R -skipTrash $deletedDirs"
    echo " >>>>>>>>> Cleaning Cluster >>>>>>>>>>>>"
    echo " $removeCmd"
    $removeCmd 2>&1 
    echo " >>>>>>>>> Checking HDFS    >>>>>>>>>>>>"
    echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R /"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R /
    setKerberosTicketForUser $HADOOPQA_USER
}

#########################################
#  Remove CORRUPT blocks from HDFS
#########################################
function cleanCorruptBlocks {
        setKerberosTicketForUser $HDFSQA_USER
        fsckOutFile="/tmp/FSCK"
        $HADOOP_HDFS_CMD fsck / 2>&1  > $fsckOutFile
        echo ">>>>>>>>> FSCK >>>>>>>>>>>"
        cat $fsckOutFile
        assertEqualCmdPassStatus "grep HEALTHY $fsckOutFile" "HDFS not Healthy"
        assertEqualCmdFailStatus "grep CORRUPT $fsckOutFile" "HDFS have corrupt block and/or file"

        corruptFiles=`grep ': CORRUPT block' $fsckOutFile | cut -d':' -f1 | tr '\n' ' '`
        if [ "$corruptFiles" != "" ];then
                safemodeLeve="$HADOOP_HDFS_CMD dfsadmin -safemode leave"
                $safemodeLeve
                removeFiles="$HADOOP_HDFS_CMD dfs -rm -skipTrash $corruptFiles"
                $removeFiles
        fi
        setKerberosTicketForUser $HADOOPQA_USER
}

function getHadoopVersion {
	ID_OUTPUT=`hadoop version`
	if [ $? != 0 ] ; then
		echo "BAD_HADOOP_VERSION";
		return 127
	else
		HVERSION=`echo $ID_OUTPUT | grep 'Hadoop ' | awk -F' ' '{print $2}' | awk -F. '{print $1"."$2}'`
		echo $HVERSION
		return 0
	fi		
}
