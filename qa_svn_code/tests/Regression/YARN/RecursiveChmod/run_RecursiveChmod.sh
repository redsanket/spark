#!/bin/bash

. $WORKSPACE/lib/library.sh
  
# Setting Owner of this TestSuite
OWNER="rramya"

export TESTCASE_ID=10
export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir RecursiveChmod  >> $ARTIFACTS_FILE 2>&1
createHdfsDir RecursiveChmod  >> $ARTIFACTS_FILE 2>&1
deleteHdfsDir /tmp/RecursiveChmod/ >> $ARTIFACTS_FILE 2>&1
createHdfsDir /tmp/RecursiveChmod/ >> $ARTIFACTS_FILE 2>&1


########################################### 
#Function RecursiveChmod test - RecursiveChmod10 - Test to check the -cacheArchive option for a symlink to test.jar in private cache of HDFS when the jar has files with no executable permissions
###########################################
function RecursiveChmod10 {

  setTestCaseDesc "RecursiveChmod-$TESTCASE_ID - Test to check the -cacheArchive option for a symlink to test.jar in private cache of HDFS when the jar has files with no executable permissions"

  createHdfsDir RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/input.txt RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/test.jar  RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar   >> $ARTIFACTS_FILE 2>&1
  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out" -cacheArchive "$NAMENODE/user/$USER_ID/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh"
  echo "OUTPUT:"   
  $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out" -cacheArchive "$NAMENODE/user/$USER_ID/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh 2>&1

  validateRecursiveChmodOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function RecursiveChmod test - RecursiveChmod20 - Test to check the -cacheArchive option for a symlink to test.jar in public cache of HDFS when the jar has files with no executable permissions
###########################################
function RecursiveChmod20 {

  setTestCaseDesc "RecursiveChmod-$TESTCASE_ID - Test to check the -cacheArchive option for a symlink to test.jar in public cache of HDFS when the jar has files with no executable permissions"

  createHdfsDir /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/input.txt RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/test.jar  /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar   >> $ARTIFACTS_FILE 2>&1
  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out" -cacheArchive "$NAMENODE/tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh"
  echo "OUTPUT:"   
  $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out" -cacheArchive "$NAMENODE/tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh 2>&1

  validateRecursiveChmodOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function RecursiveChmod test - RecursiveChmod30 - Test to check the -archives option for a symlink to test.jar in private cache of HDFS when the jar has files with no executable permissions
###########################################
function RecursiveChmod30 {

  setTestCaseDesc "RecursiveChmod-$TESTCASE_ID - Test to check the -archives option for a symlink to test.jar in private cache of HDFS when the jar has files with no executable permissions"

  createHdfsDir RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/input.txt RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/test.jar  RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar   >> $ARTIFACTS_FILE 2>&1
  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "$NAMENODE/user/$USER_ID/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh"
  echo "OUTPUT:"   
  $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "$NAMENODE/user/$USER_ID/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh 2>&1

  validateRecursiveChmodOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function RecursiveChmod test - RecursiveChmod40 - Test to check the -archives option for a symlink to test.jar in public cache of HDFS when the jar has files with no executable permissions
###########################################
function RecursiveChmod40 {

  setTestCaseDesc "RecursiveChmod-$TESTCASE_ID - Test to check the -archives option for a symlink to test.jar in public cache of HDFS when the jar has files with no executable permissions"

  createHdfsDir /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/input.txt RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
  putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/test.jar  /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar   >> $ARTIFACTS_FILE 2>&1
  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "$NAMENODE/tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh"
  echo "OUTPUT:"   
  $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "$NAMENODE/tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh 2>&1

  validateRecursiveChmodOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function RecursiveChmod test - RecursiveChmod50 - Test to check the -archives option for a symlink to test.jar in private cache of local FS when the jar has files with no executable permissions
###########################################
function RecursiveChmod50 {

  setTestCaseDesc "RecursiveChmod-$TESTCASE_ID - Test to check the -archives option for a symlink to test.jar in private cache of local FS when the jar has files with no executable permissions"

  createHdfsDir RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  
  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/input.txt RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "file://$JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh"  
  echo "OUTPUT:"
  $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "file://$JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh 2>&1

  validateRecursiveChmodOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function RecursiveChmod test - RecursiveChmod60 - Test to check the -archives option for a symlink to test.jar in public cache of local FS when the jar has files with no executable permissions
###########################################
function RecursiveChmod60 {

  setTestCaseDesc "RecursiveChmod-$TESTCASE_ID - Test to check the -archives option for a symlink to test.jar in public cache of local FS when the jar has files with no executable permissions"

  createHdfsDir /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  createLocalDir /tmp/RecursiveChmod/ >> $ARTIFACTS_FILE 2>&1
  createLocalDir /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/input.txt RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
  cp  $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-$TESTCASE_ID/test.jar  /tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar   >> $ARTIFACTS_FILE 2>&1
  echo "HADOOP COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "file:///tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh"
  echo "OUTPUT:"   
  $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives "file:///tmp/RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/test.jar#testlink" -input "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/input.txt"  -mapper "mapper.sh"  -reducer "reducer.sh" -output "RecursiveChmod/RecursiveChmod-${TESTCASE_ID}/RecursiveChmod$TESTCASE_ID.out"  -jobconf mapred.map.tasks=1 -jobconf mapred.reduce.tasks=1 -jobconf mapred.job.name="RecursiveChmod-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=* -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/RecursiveChmod-${TESTCASE_ID}/reducer.sh 2>&1

  validateRecursiveChmodOutput
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
  RecursiveChmod${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
