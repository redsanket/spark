#!/bin/bash

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

export NAMENODE="None"
export USER_ID=`whoami`
export TESTCASE_ID=10

deleteHdfsDir /user/$USER_ID/BackwardCompatibility  >> $ARTIFACTS_FILE 2>&1
deleteHdfsDir /tmp/BackwardCompatibility  >> $ARTIFACTS_FILE 2>&1
###########################################
# Function to check for the job.xml file
###########################################
function checkJobXml {
    FILE_NAME=$1
    CONFIG_NAME=$2
    CONFIG_VALUE=$3
    echo "Grep for $CONFIG_NAME with value $CONFIG_VALUE in $FILE_NAME:"
    echo "Found:"
    set -x
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR  dfs -cat $FILE_NAME/* | grep $CONFIG_NAME | grep $CONFIG_VALUE
    set +x
    if [[ "$?" -eq 0 ]] ; then 
        return 0
    else
        return 1
    fi
}

###########################################
# Function BackwardCompatibility-10 -  Test to check the backward compatibility
# of mapred.job.name and mapreduce.job.name 
###########################################
function  BackwardCompatibility-10  {
    TESTCASE_ID=10

    createHdfsDir /user/$USER_ID/BackwardCompatibility  >> $ARTIFACTS_FILE 2>&1

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.1 - Test to check if the old config parameter mapred.job.name is valid when passed via -D option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
         $YARN_OPTIONS \
         -Dmapred.job.name=BackwardCompatibility-$TESTCASE_ID \
         -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
         -mapper 'cat job.xml' \
         -reducer NONE \
         -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1" \
         -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    eval $CMD
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1 mapreduce.job.name BackwardCompatibility-$TESTCASE_ID
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.2 - Test to check if the old config parameter mapred.job.name is valid when passed via -jobconf option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
         $YARN_OPTIONS \
         -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
         -mapper 'cat job.xml' \
         -reducer NONE \
         -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2" \
         -jobconf mapreduce.job.acl-view-job=* \
         -jobconf mapred.job.name==BackwardCompatibility-$TESTCASE_ID"
    echo "$CMD"
    eval $CMD
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2 mapreduce.job.name BackwardCompatibility-$TESTCASE_ID
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.3 - Test to check if the new config parameter mapreduce.job.name is valid when passed via -D option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -Dmapreduce.job.name=BackwardCompatibility-$TESTCASE_ID \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3" \
        -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    eval $CMD
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3 mapreduce.job.name BackwardCompatibility-$TESTCASE_ID
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.4 - Test to check if the old config parameter mapreduce.job.name is valid when passed via -joconf option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4" \
        -jobconf mapreduce.job.acl-view-job=* \
        -jobconf  mapreduce.job.name=BackwardCompatibility-$TESTCASE_ID"

    echo "$CMD"
    eval $CMD
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4 mapreduce.job.name BackwardCompatibility-$TESTCASE_ID
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    export TESTCASENAME="BackwardCompatibility"
    displayTestCaseResult $TESTCASE_ID

    ((TESTCASE_ID=$TESTCASE_ID+10))

    return 0
} 


###########################################
# Function BackwardCompatibility-20 -  Test to check the backward compatibility
# of jobclient.output.filter and mapreduce.client.output.filter 
###########################################
function  BackwardCompatibility-20  {

    createHdfsDir /user/$USER_ID/BackwardCompatibility  >> $ARTIFACTS_FILE 2>&1

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.1 - Test to check if the old config parameter jobclient.output.filter is valid when passed via -D option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -Djobclient.output.filter=ALL \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1" \
        -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    output=`eval $CMD 2>&1 `
    echo "Output is"

    (IFS='';echo $output)
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1 mapreduce.client.output.filter ALL
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    tmpOutput=$(echo $output | grep 'Status : SUCCEEDED')
    EXIT_CODE=$?
    echo "GREPPING FOR Status : SUCCEEDED and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
    if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
        COMMAND_EXIT_CODE=1
        REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
    else
        COMMAND_EXIT_CODE=0
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.2 - Test to check if the old config parameter jobclient.output.filter is valid when passed via -jobconf option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2" \
        -jobconf mapreduce.job.acl-view-job=* \
        -jobconf jobclient.output.filter=ALL"
    echo "$CMD"
    output=`eval $CMD 2>&1 `

    (IFS='';echo $output)
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2 mapreduce.client.output.filter ALL
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    tmpOutput=$(echo $output | grep 'Status : SUCCEEDED')
    EXIT_CODE=$?
    echo "GREPPING FOR Status : SUCCEEDED and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
    if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
        COMMAND_EXIT_CODE=1
        REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
    else
        COMMAND_EXIT_CODE=0
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.3 - Test to check if the new config parameter mapreduce.client.output.filter is valid when passed via -D option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -Dmapreduce.client.output.filter=ALL \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3" \
        -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    output=`eval $CMD 2>&1 `

    (IFS='';echo $output)
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3 mapreduce.client.output.filter ALL
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    tmpOutput=$(echo $output | grep 'Status : SUCCEEDED')
    EXIT_CODE=$?
    echo "GREPPING FOR Status : SUCCEEDED and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
    if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
        COMMAND_EXIT_CODE=1
        REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
    else
        COMMAND_EXIT_CODE=0
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.4 - Test to check if the old config parameter mapreduce.client.output.filter is valid when passed via -jobconf option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4" \
        -jobconf mapreduce.job.acl-view-job=* \
        -jobconf mapreduce.client.output.filter=ALL"

    echo "$CMD"
    output=`eval $CMD 2>&1 `

    (IFS='';echo $output)
    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4 mapreduce.client.output.filter ALL
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    tmpOutput=$(echo $output | grep 'Status : SUCCEEDED')
    EXIT_CODE=$?
    echo "GREPPING FOR Status : SUCCEEDED and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
    if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
        COMMAND_EXIT_CODE=1
        REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
    else
        COMMAND_EXIT_CODE=0
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    export TESTCASENAME="BackwardCompatibility"
    displayTestCaseResult $TESTCASE_ID

    ((TESTCASE_ID=$TESTCASE_ID+10))

    return 0
}


###########################################
# Function BackwardCompatibility-30 -  Test to check the backward compatibility
# of mapred.job.queue.name and mapreduce.job.queuename
###########################################
function  BackwardCompatibility-30  {
    TESTCASE_ID=30

    createHdfsDir /user/$USER_ID/BackwardCompatibility  >> $ARTIFACTS_FILE 2>&1

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.1 - Test to check if the old config parameter mapred.job.queue.name is valid when passed via -D option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    DIR="Output1";
    TARGET_QUEUE='grideng'

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        -Dmapred.job.queue.name=$TARGET_QUEUE \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/$DIR" \
        -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    output=`eval $CMD 2>&1`
    (IFS='';echo $output)
    JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
    echo "JOBID=$JOBID"

    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/$DIR mapreduce.job.queuename $TARGET_QUEUE
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    set -x
    QUEUENAME=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -list all | grep $JOBID | awk -v OFS=" " '$1=$1' | cut -d" " -f 5`
    set +x
    echo "QUEUENAME=$QUEUENAME"
    if [[ "$QUEUENAME" ==  $TARGET_QUEUE ]] ; then 
        echo "pass"
        COMMAND_EXIT_CODE=0
    else
        echo "fail"
        COMMAND_EXIT_CODE=1
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.2 - Test to check if the old config parameter mapred.job.queue.name is valid when passed via -jobconf option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2" \
        -jobconf mapreduce.job.acl-view-job=* \
        -jobconf mapred.job.queue.name=$TARGET_QUEUE"
    echo "$CMD"
    output=`eval $CMD 2>&1`
    (IFS='';echo $output)
    JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
    echo "JOBID=$JOBID"

    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1 mapreduce.job.queuename $TARGET_QUEUE
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
	
	QUEUENAME=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -list all | grep $JOBID | awk -v OFS=" " '$1=$1' | cut -d" " -f 5`
    echo "QUEUENAME=$QUEUENAME"
    if [[ "$QUEUENAME" ==  $TARGET_QUEUE ]] ; then
        echo "pass"
        COMMAND_EXIT_CODE=0
    else
        echo "fail"
        COMMAND_EXIT_CODE=1
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.3 - Test to check if the new config parameter mapreduce.job.queuename is valid when passed via -D option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -Dmapreduce.job.queuename=$TARGET_QUEUE \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3" \
        -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    output=`eval $CMD 2>&1`
    (IFS='';echo $output)
    JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
    echo "JOBID=$JOBID"

    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1 mapreduce.job.queuename $TARGET_QUEUE
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    QUEUENAME=`$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR job  -list all | grep $JOBID | awk -v OFS=" " '$1=$1' | cut -d" " -f 5`
    echo "QUEUENAME=$QUEUENAME"
    if [[ "$QUEUENAME" ==  $TARGET_QUEUE ]] ; then
        echo "pass"
        COMMAND_EXIT_CODE=0
    else
        echo "fail"
        COMMAND_EXIT_CODE=1
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.4 - Test to check if the old config parameter mapreduce.job.queuename is valid when passed via -joconf option"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4" \
        -jobconf mapreduce.job.acl-view-job=* \
        -jobconf mapreduce.job.queuename=$TARGET_QUEUE"

    echo "$CMD"
    output=`eval $CMD 2>&1`
    (IFS='';echo $output)
    JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
    echo "JOBID=$JOBID"

    checkJobXml BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1 mapreduce.job.queuename $TARGET_QUEUE
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    QUEUENAME=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list all | grep $JOBID | awk -v OFS=" " '$1=$1' | cut -d" " -f 5`
    echo "QUEUENAME=$QUEUENAME"
    if [[ "$QUEUENAME" ==  $TARGET_QUEUE ]] ; then
        echo "pass"
        COMMAND_EXIT_CODE=0
    else
        echo "fail"
        COMMAND_EXIT_CODE=1
    fi
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    export TESTCASENAME="BackwardCompatibility"
    displayTestCaseResult $TESTCASE_ID

    ((TESTCASE_ID=$TESTCASE_ID+10))

    return 0
}


###########################################
# Function BackwardCompatibility-40 -  Test to check the backward compatibility
# of mapred.working.dir  and mapreduce.job.working.dir 
###########################################
function  BackwardCompatibility-40  {
    TESTCASE_ID=40

    createHdfsDir /tmp/BackwardCompatibility  >> $ARTIFACTS_FILE 2>&1

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.1 - Test to check if the old config parameter mapred.working.dir is valid when passed via -D optioni using a streaming job"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt /tmp/BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -Dmapred.working.dir=/tmp \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1" \
        -jobconf mapreduce.job.acl-view-job=*"
    
    echo "$CMD"
    eval $CMD
    checkJobXml /tmp/BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1 mapreduce.job.working.dir /tmp
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.1 - Test to check if the old config parameter mapred.working.dir is valid when passed via -D optioni using a wordcount job"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/inputDir  /tmp/BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/inputDir  >> $ARTIFACTS_FILE 2>&1
    
    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_EXAMPLES_JAR wordcount\
        $YARN_OPTIONS \
        -Dmapred.working.dir=/tmp \
        BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/inputDir \
        BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1b" 

    echo "$CMD"
    eval $CMD

    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls /tmp/BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output1b
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
    
    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.2 - Test to check if the old config parameter mapred.job.name is valid when passed via -jobconf option using a streaming job"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt /tmp/BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2" \
        -jobconf mapreduce.job.acl-view-job=* \
        -jobconf mapred.working.dir=/tmp"
    echo "$CMD"
    eval $CMD
    checkJobXml /tmp/BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output2 mapreduce.job.working.dir /tmp
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.3 - Test to check if the new config parameter mapreduce.job.name is valid when passed via -D option using a streaming job"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt /tmp/BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
        $YARN_OPTIONS \
        -Dmapreduce.job.working.dir=/tmp \
        -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
        -mapper 'cat job.xml' \
        -reducer NONE \
        -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3" \
        -jobconf mapreduce.job.acl-view-job=*"

    echo "$CMD"
    eval $CMD
    checkJobXml /tmp/BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3 mapreduce.job.working.dir /tmp
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.3 - Test to check if the new config parameter mapreduce.job.name is valid when passed via -D option using a wordcount job"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/inputDir  /tmp/BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/inputDir  >> $ARTIFACTS_FILE 2>&1 
    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_EXAMPLES_JAR wordcount\
        $YARN_OPTIONS \
        -Dmapreduce.job.working.dir=/tmp \
        BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt \
        BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3b"

    echo "$CMD"
    eval $CMD
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls /tmp/BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output3b
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    setTestCaseDesc "BackwardCompatibility-$TESTCASE_ID.4 - Test to check if the old config parameter mapreduce.job.name is valid when passed via -joconf option using a streaming job"
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/BackwardCompatibility-$TESTCASE_ID/input.txt /tmp/BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

    CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
         $YARN_OPTIONS \
         -input "BackwardCompatibility/BackwardCompatibility-${TESTCASE_ID}/input.txt" \
         -mapper 'cat job.xml' \
         -reducer NONE \
         -output "BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4" \
         -jobconf mapreduce.job.acl-view-job=* \
         -jobconf mapreduce.job.working.dir=/tmp"

    echo "$CMD"
    eval $CMD
    checkJobXml /tmp/BackwardCompatibility/BackwardCompatibility-$TESTCASE_ID/Output4 mapreduce.job.working.dir /tmp
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

    export TESTCASENAME="BackwardCompatibility"
    displayTestCaseResult $TESTCASE_ID

    ((TESTCASE_ID=$TESTCASE_ID+10))

    return 0
}

##########################################
# Main function
###########################################

#validateCluster
export TESTCASENAME="BackwardCompatibility"
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $? ))
if [ "${SCRIPT_EXIT_CODE}" -ne 0 ]; then
    echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
    exit $SCRIPT_EXIT_CODE
fi

displayHeader "STARTING BACKWARD COMPATIBILITY TEST SUITE"

getFileSytem

while [[ ${TESTCASE_ID} -le 40 ]] ; do
    BackwardCompatibility-${TESTCASE_ID}
    incrTestCaseId
done

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE

