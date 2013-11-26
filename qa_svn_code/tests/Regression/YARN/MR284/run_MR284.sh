#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh

  
export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir MR284  >> $ARTIFACTS_FILE 2>&1
createHdfsDir MR284/  >> $ARTIFACTS_FILE 2>&1

########################################### 
#Function MR284_10 -  Test to check if SocketTimeoutException is seen while executing long running jobs
###########################################
function MR284_10 {

  setTestCaseDesc "MR284_$TESTCASE_ID - Test to check if SocketTimeoutException is seen while executing long running jobs"
  createHdfsDir MR284/MR284_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  m=0
  while [[ $m -le 3 ]] ; do

    echo "RANDOMTEXTWRITER COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter MR284/MR284_${TESTCASE_ID}/randomTextWriterOutput_$m"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter MR284/MR284_${TESTCASE_ID}/randomTextWriterOutput_$m
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $? ))

    echo "SORT COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR sort -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text MR284/MR284_${TESTCASE_ID}/randomTextWriterOutput_$m MR284/MR284_${TESTCASE_ID}/sortOutputDir_$m"  
    echo "OUTPUT"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR sort  -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text MR284/MR284_${TESTCASE_ID}/randomTextWriterOutput_$m MR284/MR284_${TESTCASE_ID}/sortOutputDir_$m  2>&1
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $? ))

    echo "SORT VALIDATION COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR testmapredsort -sortInput MR284/MR284_${TESTCASE_ID}/randomTextWriterOutput_$m -sortOutput MR284/MR284_${TESTCASE_ID}/sortOutputDir_$m"  | tee -a $ARTIFACTS_FILE
    echo "OUTPUT"
    $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR testmapredsort -sortInput MR284/MR284_${TESTCASE_ID}/randomTextWriterOutput_$m -sortOutput MR284/MR284_${TESTCASE_ID}/sortOutputDir_$m  2>&1
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $? ))
  (( m = $m + 1 ))
  done 

  displayTestCaseResult
  return 0

}

###########################################
# Main function
###########################################

displayHeader "STARTING MAPREDUCE:284 (Improvements to RPC between Child and TaskTracker) TEST SUITE"

while [[ ${TESTCASE_ID} -le 10 ]] ; do
  MR284_${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
