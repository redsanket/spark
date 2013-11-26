#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh

  
export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir MR1178  >> $ARTIFACTS_FILE 2>&1
createHdfsDir MR1178/  >> $ARTIFACTS_FILE 2>&1

########################################### 
#Function MR1178_10 -  Test to check if a job succeeds when multiple input files are specified to a MR job 
###########################################
function MR1178_10 {

  setTestCaseDesc "MR1178_$TESTCASE_ID - Test to check if a job succeeds when multiple input files are specified to a MR job"

  createHdfsDir MR1178/MR1178_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  echo "RANDOMTEXTWRITER COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter MR1178/MR1178_${TESTCASE_ID}/randomTextWriterOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter -Dmapreduce.randomtextwriter.totalbytes=1024 MR1178/MR1178_${TESTCASE_ID}/randomTextWriterOutput

  echo "WORDCOUNT COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount MR1178/MR1178_${TESTCASE_ID}/randomTextWriterOutput  MR1178/MR1178_${TESTCASE_ID}/wordcountOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount MR1178/MR1178_${TESTCASE_ID}/randomTextWriterOutput MR1178/MR1178_${TESTCASE_ID}/wordcountOutput 2>&1
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

###########################################
# Main function
###########################################

displayHeader "STARTING MAPREDUCE:1178 (MultipleInputs fails with ClassCastException) TEST SUITE"

getFileSytem
while [[ ${TESTCASE_ID} -le 10 ]] ; do
  MR1178_${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
