#!/bin/sh

#load the library file
. $WORKSPACE/lib/restart_cluster_lib.sh

# Setting Owner of this TestSuite
OWNER="arpitg"

#############################################
#SETUP
#############################################
function setup
{
  NUM_TESTCASE=0
  NUM_TESTCASE_PASSED=0
  SCRIPT_EXIT_CODE=0;
  export  COMMAND_EXIT_CODE=0
  export TESTCASE_DESC="None"
  export REASONS=""
  USER=$HADOOPQA_USER
  
  #dir for delefation token tests
  BLOCK_TOKEN_TESTS_DIR="/user/${USER}/block_token_tests"
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $BLOCK_TOKEN_TESTS_DIR
  #create the directory for tests on hdfs
  createHdfsDir $BLOCK_TOKEN_TESTS_DIR
}


##############################################
# teardown
#############################################
function teardown
{
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $BLOCK_TOKEN_TESTS_DIR
}


#Block tokens work with any Data Node that has a block replica
#hadoop fs -cat should successes with block replicas.
#1.Create file with block replication 3.
#2.Manually delete 2 block replications.
#3.Try printing file content using hadoop fs -cat. 
#expected
#Hadoop fs -cat should print file contents.
#(Assumed that manual delete means hadoop fs -setrep 1)
function SF270_2
{
  TESTCASE_DESC="SF170_2" 
  echo "Running Test Case: $TESTCASE_DESC"

  local fName=${TESTCASE_DESC}_inputfile.txt
  local file=${WORKSPACE}/${fName}
  local txt="this is the content for file for test case $TESTCASE_DESC"

  #add the text tot he file
  echo $txt >> $file

  #copy the file to hdfs
  ${HADOOP_COMMON_HOME}/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal $file $BLOCK_TOKEN_TESTS_DIR
  if [ "$?" -ne "0"]; then
    REASONS="not able to copy $file to hdfs"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    return
  fi
  
  #make sure the replication factor is set to 3
  ${HADOOP_COMMON_HOME}/bin/hadoop --config $HADOOP_CONF_DIR dfs -setrep 3 $BLOCK_TOKEN_TESTS_DIR/${fName}
  if [ "$?" -ne "0"]; then
    REASONS="not able to set the replication factor of file $BLOCK_TOKEN_TESTS_DIR/${fName} to 3"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    return
  fi


  #now set the replication factor is set to 1
  ${HADOOP_COMMON_HOME}/bin/hadoop --config $HADOOP_CONF_DIR dfs -setrep 1 $BLOCK_TOKEN_TESTS_DIR/${fName}
  if [ "$?" -ne "0"]; then
    REASONS="not able to set the replication factor of file $BLOCK_TOKEN_TESTS_DIR/${fName} to 1"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    return
  fi

  #remove the file
  rm $file
}




#####################################################################################################
#MAIN 
#####################################################################################################
#setup the tests
setup

#teardown any unwanted info
teardown

#exit the script
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
