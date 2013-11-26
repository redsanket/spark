#!/bin/bash

. $WORKSPACE/lib/library.sh
  
# Setting Owner of this TestSuite
OWNER="rramya"

export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir DFSCallsFromStreaming  >> $ARTIFACTS_FILE 2>&1

########################################### 
#Function DFSCallsFromStreaming test - DFSCallsFromStreaming10 - Test to check if streaming job completes successfully when its mapper script makes hadoop dfs calls
###########################################
function DFSCallsFromStreaming10 {
  setTestCaseDesc "DFSCallsFromStreaming-$TESTCASE_ID - Test to check if streaming job completes successfully when its mapper script makes hadoop dfs calls"  
  createHdfsDir dfsCallsFromStreaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1  

  echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh" -reducer "NONE" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*  -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/mapper.sh"

   echo "OUTPUT:"   

   ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh" -reducer "NONE" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*  -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/mapper.sh

  validateDFSCallsFromStreamingOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult

  return 0

}

########################################### 
#Function DFSCallsFromStreaming test - DFSCallsFromStreaming20 - Test to check if streaming job completes successfully when its reducer script makes hadoop dfs calls
###########################################
function DFSCallsFromStreaming20 {
  setTestCaseDesc "DFSCallsFromStreaming-$TESTCASE_ID - Test to check if streaming job completes successfully when its reducer script makes hadoop dfs calls"
  createHdfsDir dfsCallsFromStreaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "cat" -reducer "reducer.sh" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*  -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/reducer.sh"

   echo "OUTPUT:"   

  ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "cat" -reducer "reducer.sh" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*  -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/reducer.sh

  validateDFSCallsFromStreamingOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult

  return 0

}

########################################### 
#Function DFSCallsFromStreaming test - DFSCallsFromStreaming30 - Test to check if streaming job completes successfully when its mapper makes direct hadoop dfs calls
###########################################
function DFSCallsFromStreaming30 {
  setTestCaseDesc "DFSCallsFromStreaming-$TESTCASE_ID - Test to check if streaming job completes successfully when its mapper makes direct hadoop dfs calls"
  createHdfsDir dfsCallsFromStreaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -reducer "NONE" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*"

   echo "OUTPUT:"   

  ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -reducer "NONE" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*

  validateDFSCallsFromStreamingOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult

  return 0

}

########################################### 
#Function DFSCallsFromStreaming test - DFSCallsFromStreaming40 - Test to check if streaming job completes successfully when its reducer makes direct hadoop dfs calls
###########################################
function DFSCallsFromStreaming40 {
  setTestCaseDesc "DFSCallsFromStreaming-$TESTCASE_ID - Test to check if streaming job completes successfully when its reducer makes direct hadoop dfs calls"  
  createHdfsDir dfsCallsFromStreaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1
  
  echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "cat" -reducer "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=* "   
   echo "OUTPUT:"   
   
   ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "cat" -reducer "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*

  validateDFSCallsFromStreamingOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult

  return 0

}

########################################### 
#Function DFSCallsFromStreaming test - DFSCallsFromStreaming50 - Test to check if streaming job completes successfully when its mapper and reducer script both makes hadoop dfs calls
###########################################
function DFSCallsFromStreaming50 {
  setTestCaseDesc "DFSCallsFromStreaming-$TESTCASE_ID - Test to check if streaming job completes successfully when its mapper and reducer script both makes hadoop dfs calls"
  createHdfsDir dfsCallsFromStreaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1  
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}//input.txt DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh" -reducer "reducer.sh" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*  -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/reducer.sh"

   echo "OUTPUT:"   
  ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh" -reducer "reducer.sh" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*  -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/reducer.sh

  validateDFSCallsFromStreamingOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))  
  displayTestCaseResult
  return 0
}

########################################### 
#Function DFSCallsFromStreaming test - DFSCallsFromStreaming60 - Test to check if streaming job completes successfully when its mapper and reducer makes direct hadoop dfs calls
###########################################
function DFSCallsFromStreaming60 {
  setTestCaseDesc "DFSCallsFromStreaming-$TESTCASE_ID - Test to check if streaming job completes successfully when its mapper and reducer makes direct hadoop dfs calls"  
  createHdfsDir dfsCallsFromStreaming-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1  
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -reducer "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*"
  echo "OUTPUT:"   

  ${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR}  jar $HADOOP_STREAMING_JAR  -input "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/input.txt"  -mapper "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -reducer "$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help" -output "DFSCallsFromStreaming/dfsCallsFromStreaming-${TESTCASE_ID}/dfsCallsFromStreaming${TESTCASE_ID}.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="dfsCallsFromStreaming-${TESTCASE_ID}" -jobconf mapreduce.job.acl-view-job=*

  validateDFSCallsFromStreamingOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult

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
while [[ ${TESTCASE_ID} -le 60 ]] ; do
  DFSCallsFromStreaming${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
