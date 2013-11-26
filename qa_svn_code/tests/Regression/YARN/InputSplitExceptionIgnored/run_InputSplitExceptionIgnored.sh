#!/bin/bash

. $WORKSPACE/lib/library.sh
  
# Setting Owner of this TestSuite
OWNER="rramya"

export TESTCASE_ID=10
export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir InputSplitExceptionIgnored  >> $ARTIFACTS_FILE 2>&1
createHdfsDir InputSplitExceptionIgnored  >> $ARTIFACTS_FILE 2>&1


########################################### 
#Function InputSplitExceptionIgnored test - InputSplitExceptionIgnored10 -  Test to check if exceptions are caught when IOE is thrown from get
###########################################
function InputSplitExceptionIgnored10 {

  setTestCaseDesc "InputSplitExceptionIgnored-$TESTCASE_ID -  Test to check if exceptions are caught when IOE is thrown from getPos"

  createHdfsDir InputSplitExceptionIgnored >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $JOB_SCRIPTS_DIR_NAME/data/InputSplitExceptionIgnored-${TESTCASE_ID}/hadoop-examples-0.20.202.0-SNAPSHOT.jar teragen 10 InputSplitExceptionIgnored/InputSplitExceptionIgnored-${TESTCASE_ID}"
  echo "OUTPUT:"

  output=$($HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $JOB_SCRIPTS_DIR_NAME/data/InputSplitExceptionIgnored-${TESTCASE_ID}/hadoop-examples-0.20.202.0-SNAPSHOT.jar teragen 10 InputSplitExceptionIgnored/InputSplitExceptionIgnored-${TESTCASE_ID} 2>&1)


  (IFS='';echo $output)
  tmpOutput=$(echo $output | grep 'java.io.IOException: Job failed!')
  EXIT_CODE=$?
  if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
    COMMAND_EXIT_CODE=1
    REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
  else
    COMMAND_EXIT_CODE=0
  fi
  displayTestCaseResult
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

  return 0

}

########################################### 
#Function InputSplitExceptionIgnored test - InputSplitExceptionIgnored20 - Test to check if exceptions are caught when IOE is thrown from getProgress
###########################################
function InputSplitExceptionIgnored20 {

  setTestCaseDesc "InputSplitExceptionIgnored-$TESTCASE_ID - Test to check if exceptions are caught when IOE is thrown from getProgress"

  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $JOB_SCRIPTS_DIR_NAME/data/InputSplitExceptionIgnored-${TESTCASE_ID}/hadoop-examples-0.20.202.0-SNAPSHOT.jar teragen 10 InputSplitExceptionIgnored/InputSplitExceptionIgnored-${TESTCASE_ID}"
  echo "OUTPUT:"
  output=$($HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $JOB_SCRIPTS_DIR_NAME/data/InputSplitExceptionIgnored-${TESTCASE_ID}/hadoop-examples-0.20.202.0-SNAPSHOT.jar teragen 10 InputSplitExceptionIgnored/InputSplitExceptionIgnored-${TESTCASE_ID} 2>&1)

  (IFS='';echo $output)
  tmpOutput=$(echo $output | grep 'java.io.IOException: Job failed!')
  EXIT_CODE=$?
  if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
    COMMAND_EXIT_CODE=1
    REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
  else
    COMMAND_EXIT_CODE=0
  fi
  displayTestCaseResult
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

  return 0

}

###########################################
# Main function
###########################################

(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $? ))
if [ "${SCRIPT_EXIT_CODE}" -ne 0 ]; then
  echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
  exit $SCRIPT_EXIT_CODE
fi

displayHeader "STARTING STREAMING TEST SUITE"

getFileSytem
while [[ ${TESTCASE_ID} -le 20 ]] ; do
  InputSplitExceptionIgnored${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
