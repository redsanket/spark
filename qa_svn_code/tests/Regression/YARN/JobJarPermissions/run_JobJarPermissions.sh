#!/bin/bash

. $WORKSPACE/lib/library.sh
  
# Setting Owner of this TestSuite
OWNER="rramya"

export TESTCASE_ID=10
export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir JobJarPermissions  >> $ARTIFACTS_FILE 2>&1
createHdfsDir JobJarPermissions  >> $ARTIFACTS_FILE 2>&1


########################################### 
#Function JobJarPermissions test - JobJarPermissions10 - Test to check that the file with no exe permission is executable in the job.jar
###########################################
function JobJarPermissions10 {

  setTestCaseDesc "JobJarPermissions-$TESTCASE_ID - Test to check that the file with no exe permission is executable in the job.jar"

  createHdfsDir JobJarPermissions/JobJarPermissions-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-$TESTCASE_ID/input.txt JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.maps=1 -Dmapreduce.job.reduces=1 -Dmapreduce.job.name=JobJarPermissions-$TESTCASE_ID -input JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/input.txt -mapper mapper.sh -reducer reducer.sh -output JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/JobJarPermissions$TESTCASE_ID.out -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/reducer.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/noExePermission.sh 2>&1"

  echo "OUTPUT:"   
  $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapreduce.job.maps=1 -Dmapreduce.job.reduces=1 -Dmapreduce.job.name="JobJarPermissions-$TESTCASE_ID" -input "JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/input.txt" -mapper "mapper.sh" -reducer "reducer.sh" -output "JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/JobJarPermissions$TESTCASE_ID.out" -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/reducer.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/noExePermission.sh 2>&1

  validateJobJarPermissionsOutput
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function JobJarPermissions test - JobJarPermissions20 - Test to check that the directory whose files have no exe permission is executable in the job.jar
###########################################
function JobJarPermissions20 {

  setTestCaseDesc "JobJarPermissions-$TESTCASE_ID - Test to check that the directory whose files have no exe permission is executable in the job.jar"

  createHdfsDir JobJarPermissions/JobJarPermissions-${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-$TESTCASE_ID/input.txt JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapred.map.tasks=1 -Dmapred.reduce.tasks=1 -Dmapred.job.name=JobJarPermissions-$TESTCASE_ID -Dmapreduce.job.acl-view-job=* -input JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/input.txt -mapper mapper.sh -reducer reducer.sh -output JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/JobJarPermissions$TESTCASE_ID.out -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/reducer.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/testDir 2>&1"

  echo "OUTPUT:"   
  $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -Dmapred.map.tasks=1 -Dmapred.reduce.tasks=1 -Dmapred.job.name="JobJarPermissions-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -input "JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/input.txt" -mapper "mapper.sh" -reducer "reducer.sh" -output "JobJarPermissions/JobJarPermissions-${TESTCASE_ID}/JobJarPermissions$TESTCASE_ID.out" -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/mapper.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/reducer.sh -file $JOB_SCRIPTS_DIR_NAME/data/JobJarPermissions-${TESTCASE_ID}/testDir 2>&1

  validateJobJarPermissionsOutput
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
while [[ ${TESTCASE_ID} -le 20 ]] ; do
  JobJarPermissions${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
