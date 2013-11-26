#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh

#################################################################
#This script checks the queue acls related scenarios 
#       as user who part of admin job and submit job queue
#       as user who is part of admin job queue only
#       as user who is part of submit job queue only 
#################################################################

# Setting Owner of this TestSuite
OWNER="vmotilal"

function createTempFolder {
    local myrand=`echo $RANDOM$RANDOM`
    mkdir /tmp/ldlib$myrand
    myTempFolder="/tmp/ldlib$myrand"
    touch $myTempFolder/output1
    touch $myTempFolder/output2
    touch $myTempFolder/output3
    echo "my temp folder is $myTempFolder"
}

#########################################################
# This function will  insert the file in hdfs 
# 
#########################################################
function insertFileInHdfs {
    curUser=`whoami`
    echo "Copy data.txt file to hdfs "
    #putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/data.txt /user/$curUser >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/data.txt $1 >> $ARTIFACTS_FILE 2>&1
    return $?
}

#########################################################
# This function will check if the file is already in hdfs
# Takes the path to verify as $1  and string to look for as $2 and responds as 0 or 1 based on word count 
#########################################################
function checkIfFileExistInHdfs {
    echo " Checking if the file $1 exists in hdfs "
    curUser=`whoami`
    #$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -cat /user/$curUser/data.txt |grep "File does not exist"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -cat $1 > $myTempFolder/output1
    sleep 5
    cat $myTempFolder/output1 >> $ARTIFACTS_FILE 2>&1
    local resp=`cat $myTempFolder/output1 |grep "$2"`
    if [ "$resp" == "$2"  ] ; then
        return 0
    else
        return 1
    fi
    return $?
}

#########################################################
# This function will delete the folder in hdfs .
# Takes the folder path in hdfs as $1
#########################################################

function deleteFolderInHdfs {
    echo " Deleting file $1 in hdfs "
    curUser=`whoami`
    #$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm /user/$curUser/data.txt  |grep "Moved to trash"
    $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r $1  |grep "Moved to trash"
    return $?
}

#########################################################
# This function will set the env variable and verify the same
#########################################################
function verifyEnvVariables {
    local myparam1
    if [ "$1" == "NONE" ] ; then
        echo " Got myparam as $1 "
        myparam1=""
    else
        myparam1=$1
    fi
    local mylookout=$2
    echo $1
    curUser=`whoami`
    #Check if the input file exists in the hdfs and if not create one
    checkIfFileExistInHdfs "/user/$curUser/data.txt" "This is a test file"
    if [ $? -ne 0 ] ; then
        echo " The file /user/$curUser/data.txt does not exist and proceeding to insert the same into hdfs "
        insertFileInHdfs "/user/$curUser"
        if [ $? -ne 0 ] ; then
            echo " unable to insert the required file in hdfs "
            setFailCase "Unable to insert the file in to hdfs for the test to proceed and so failing "
	    COMMAND_EXIT_CODE=1
            return $COMMAND_EXIT_CODE
        fi
    fi
    checkIfFileExistInHdfs "/user/$curUser/out1/*" "File does not exist"
    if [ $? -ne 0 ] ; then
        echo " The file /user/$curUser/out1  exists in hdfs and so proceeding to delete teh file"
        deleteFolderInHdfs "/user/$curUser/out1"
    fi
    echo $myparam1
    # Submit a streaming job and echo the env for the task
    # passing the ubertask flag as false to bypass an issue that causes streaming to go through ubertask flow
    $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_STREAMING_JAR -Dmapreduce.job.ubertask.enable=false -$myparam1 -input /user/$curUser/data.txt  -output out1 -mapper "env" -reducer "NONE"
    $HADOOP_HDFS_CMD dfs -cat out1/* > $myTempFolder/output3
    sleep 5
    cat $myTempFolder/output3 >> $ARTIFACTS_FILE 2>&1       
    local output=$(cat $myTempFolder/output3 |grep $mylookout |wc -l)
    if [ $output -eq 0  ] ; then
        echo "Could not find variable $mylookout in env "
        setFailCase "Could not find variable $mylookout set in the task env"
        deleteFolderInHdfs "/user/$curUser/out1"
	COMMAND_EXIT_CODE=1
	return $COMMAND_EXIT_CODE
    else
        COMMAND_EXIT_CODE=0
        echo " Found myName variable defined in the task env "
    fi
    # Cleaning up the out file for the next run
    # deleteFolderInHdfs "/user/$curUser/out1"
    # displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testToVerifyLdLibraryPathInEnv {
    echo "*************************************************************"
    TESTCASE_DESC="Check if LD_LIBRARY_PATH is in the task env when echo'd "
    TESTCASE_ID="LdLibraryPath01"
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Trigger a streaming job and pass the env  "
    echo " 2. cat the output stored and check if LD_LIBRARY_PATH is set "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=1
    # local data="Dmapreduce.job.ubertask.enable=false "
    local data="NONE"
    local verify="LD_LIBRARY_PATH"
    verifyEnvVariables $data $verify
    COMMAND_EXIT_CODE=$?
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testToVerifyAdditionOfNewNVPairInEnv  {
    echo "*************************************************************"
    TESTCASE_DESC="Check if addition of new NVpair is in the task env when echo'd "
    TESTCASE_ID="LdLibraryPath01"
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Trigger a streaming job and pass the env  "
    echo " 2. cat the output stored and check if the new NV path is set "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=1
    local data="Dmapred.child.env=myName=myValue"
    local verify="myName=myValue"
    verifyEnvVariables $data $verify
    COMMAND_EXIT_CODE=$?
    displayTestCaseResult
    return $COMMAND_EXIT_CODE     
}

function testToVerifyAppendOfNVPairInEnv  {
    echo "*************************************************************"
    TESTCASE_DESC="Testcase to verify appending of NV pair to an existing variable in task env "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Trigger a streaming job and append a nv pair to Hadoop_Home varaible the env  "
    echo " 2. cat the output stored and check if the new nv pair is appended  "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=1
    local data="Dmapred.child.env=HADOOP_HOME=$HADOOP_HOME:newHadoopHome"
    local verify="newHadoopHome"
    verifyEnvVariables $data $verify
    COMMAND_EXIT_CODE=$?
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testToVerifyModificationOfNVPairInEnv  {
    echo "*************************************************************"
    TESTCASE_DESC="Check if modification of existing var is in the task env when echo'd "
    TESTCASE_ID="LdLibraryPath01"
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Trigger a streaming job and pass the env  "
    echo " 2. cat the output stored and check if LD_LIBRARY_PATH is set "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=1
    local data="Dmapred.child.env=HOME=/user/newuser"
    local verify="newuser"
    verifyEnvVariables $data $verify
    COMMAND_EXIT_CODE=$?
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testToVerifyEmptyValueInEnv  {
    echo "*************************************************************"
    TESTCASE_DESC="Check if empty value  is in the task env when echo'd "
    TESTCASE_ID="LdLibraryPath01"
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Trigger a streaming job and pass the env  "
    echo " 2. cat the output stored and check if LD_LIBRARY_PATH is set "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=1
    local data="Dmapred.child.env=HADOOP_HOME=$HADOOP_HOME:dummyName="
    local verify="dummyName="
    verifyEnvVariables $data $verify
    COMMAND_EXIT_CODE=$?
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testToVerifyAppendOfNVPairPathInEnv  {
    echo "*************************************************************"
    TESTCASE_DESC="Testcase to verify appending of NV pair to an existing variable PATH in task env "
    TESTCASE_ID=""
    displayTestCaseMessage $TESTCASE_DESC
    echo " 1. Trigger a streaming job and append a nv pair to Hadoop_Home varaible the env  "
    echo " 2. cat the output stored and check if the new nv pair is appended  "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=1
    local data="Dmapred.child.env=PATH=$PATH:newname=newvalue"
    local verify="newname=newvalue"
    verifyEnvVariables $data $verify
    COMMAND_EXIT_CODE=$?
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

createTempFolder
testToVerifyAdditionOfNewNVPairInEnv
testToVerifyLdLibraryPathInEnv
# testToVerifyAppendOfNVPairInEnv
testToVerifyModificationOfNVPairInEnv
testToVerifyEmptyValueInEnv
testToVerifyAppendOfNVPairPathInEnv

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
