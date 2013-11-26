#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh
  
# Setting Owner of this TestSuite
OWNER="rramya"

export USER_ID=`whoami`
export CLUSTER_MAP_CAPACITY=0
export CLUSTER_REDUCE_CAPACITY=0

SUMMARY_LOG="${HADOOP_LOG_DIR}/mapredqa/mapred-jobsummary.log"

SLEEP_TIME=200

###########################################
# Function to display a message on failure of a test
###########################################
function displayJobSummarySuccess {
  displayHeader "$TESTCASE_DESC PASSED"
}


###########################################
# Function jobSummaryInfo Test - jobSummaryInfo10 - Test to check the job summary information after successful job completion
###########################################
function JobSummaryInfo10 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information after successful job completion"

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 "

  echo "OUTPUT:"   
   
  output=$($HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10  2>&1)
  (IFS='';echo $output)
  JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
  NUM_MAPS=10
  NUM_SLOTS_PER_MAP=1
  NUM_REDUCES=10
  NUM_SLOTS_PER_REDUCE=1
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]{13},firstReduceTaskLaunchTime=[0-9]{13},finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoopqa,queue=default,status=SUCCEEDED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+,jobName=Sleep\sjob"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $? 
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0
 } 


###########################################
# Function jobSummaryInfo Test - jobSummaryInfo20 - Test to check the job summary information for High RAM jobs
###########################################
function JobSummaryInfo20 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information for High RAM jobs"

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=*  -Dmapred.job.map.memory.mb=6144 -Dmapred.job.reduce.memory.mb=8192  -m 10 -r 10 "

  echo "OUTPUT:"   

  output=$($HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=*  -Dmapred.job.map.memory.mb=6144 -Dmapred.job.reduce.memory.mb=8192 -m 10 -r 10  2>&1)
  (IFS='';echo $output)
  JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
  NUM_MAPS=10
  NUM_SLOTS_PER_MAP=4
  NUM_REDUCES=10
  NUM_SLOTS_PER_REDUCE=2
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]{13},firstReduceTaskLaunchTime=[0-9]{13},finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoopqa,queue=default,status=SUCCEEDED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+,jobName=Sleep\sjob"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $?
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0
 }

###########################################
# Function jobSummaryInfo Test - jobSummaryInfo30 - Test to check the job summary information for failed jobs where the mappers failed
###########################################
function JobSummaryInfo30 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information for failed jobs where the mappers failed"

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR fail -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -failMappers"

  echo "OUTPUT:"  
  output=$($HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR fail -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -failMappers 2>&1 )
  (IFS='';echo $output)
  JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
  NUM_MAPS=2
  NUM_SLOTS_PER_MAP=1
  NUM_REDUCES=1
  NUM_SLOTS_PER_REDUCE=1
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]{13},firstReduceTaskLaunchTime=0,finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoopqa,queue=default,status=FAILED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+,jobName=Fail\sjob"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $?
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0
 }


###########################################
# Function jobSummaryInfo Test - jobSummaryInfo40 - Test to check the job summary information for failed jobs where the reducers failed
###########################################
function JobSummaryInfo40 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information for failed jobs where the reducers failed"

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR fail -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -failReducers"

  echo "OUTPUT:"  
  output=$($HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR fail -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -failReducers 2>&1 )
  (IFS='';echo $output)
  JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
  NUM_MAPS=2
  NUM_SLOTS_PER_MAP=1
  NUM_REDUCES=1
  NUM_SLOTS_PER_REDUCE=1
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]{13},firstReduceTaskLaunchTime=[0-9]{13},finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoopqa,queue=default,status=FAILED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $?  
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))  
  displayTestCaseResult
  return 0 
}

###########################################
# Function jobSummaryInfo Test - jobSummaryInfo50 - Test to check the job summary information when job is submitted as another user
###########################################
function JobSummaryInfo50 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information when job is submitted as another user"

  echo "SUBMITTING HADOOP COMMAND AS USER HADOOP1: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 "
  echo "OUTPUT:"     
  getKerberosTicketForUser hadoop1 
  setKerberosTicketForUser hadoop1

  output=$($HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10  2>&1)
  getKerberosTicketForUser hadoopqa
  setKerberosTicketForUser hadoopqa
  (IFS='';echo $output)
  JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
  NUM_MAPS=10
  NUM_SLOTS_PER_MAP=1
  NUM_REDUCES=10
  NUM_SLOTS_PER_REDUCE=1
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]{13},firstReduceTaskLaunchTime=[0-9]{13},finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoop1,queue=default,status=SUCCEEDED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+,jobName=Sleep\sjob"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $? 
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0
 } 

###########################################
# Function jobSummaryInfo Test - jobSummaryInfo60 - Test to check the job summary information when job is submitted to a different queue
###########################################
function JobSummaryInfo60 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information when job is submitted to a different queue"
  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.job.queuename=grideng -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 "
  echo "OUTPUT:"
  output=$($HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapreduce.job.queuename=grideng -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10  2>&1)
  (IFS='';echo $output)
  JOBID=$(echo $output |  sed 's| |\n|g' | grep job_ | head -1)
  NUM_MAPS=10
  NUM_SLOTS_PER_MAP=1
  NUM_REDUCES=10
  NUM_SLOTS_PER_REDUCE=1
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]{13},firstReduceTaskLaunchTime=[0-9]{13},finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoopqa,queue=grideng,status=SUCCEEDED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+,jobName=Sleep\sjob"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $?
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult  
  return 0
 } 


###########################################
# Function jobSummaryInfo Test - jobSummaryInfo70 - Test to check the job summary information for killed jobs
###########################################
function JobSummaryInfo70 {
  setTestCaseDesc "JobSummaryInfo-$TESTCASE_ID - Test to check the job summary information for killed jobs"

  echo "HADOOP COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 -mt 100000 -rt 100000 "

  echo "OUTPUT:"   

  $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 -mt  100000 -rt 100000  2>/dev/null &
  JOBID=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list | tail -1 |  awk '{print $1}' | grep job_`

  while [[ -z $JOBID ]] ; do
    JOBID=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list | tail -1 |  awk '{print $1}' | grep job_`
  done
  sleep 25
  echo "KILLING JOB $JOBID: $HADOOP_MAPRED_CMD  --config $HADOOP_CONF_DIR job -kill $JOBID"
  $HADOOP_MAPRED_CMD  --config $HADOOP_CONF_DIR job -kill $JOBID
  NUM_MAPS=10
  NUM_SLOTS_PER_MAP=1
  NUM_REDUCES=10
  NUM_SLOTS_PER_REDUCE=1
  JOB_QUERY="jobId=$JOBID"
  LOG_QUERY="${JOB_QUERY},submitTime=[0-9]{13},launchTime=[0-9]{13},firstMapTaskLaunchTime=[0-9]+,firstReduceTaskLaunchTime=[0-9]+,finishTime=[0-9]{13},resourcesPerMap=[0-9]+,resourcesPerReduce=[0-9]+,numMaps=$NUM_MAPS,numReduces=$NUM_REDUCES,user=hadoopqa,queue=default,status=KILLED,mapSlotSeconds=[0-9]+,reduceSlotSeconds=[0-9]+,jobName\
=Sleep\sjob"

  echo "Sleeping for ${SLEEP_TIME} seconds waiting for the summary logs to be updated"
  sleep ${SLEEP_TIME}

  echo "LOOKING FOR A JOB: ${JOB_QUERY} IN $RESOURCEMANAGER:${SUMMARY_LOG} "
  RESULT=$(ssh $RESOURCEMANAGER "grep -E ${JOB_QUERY} ${SUMMARY_LOG}")
  echo "FOUND: ${RESULT}"

  echo "Validating result matches ${LOG_QUERY}"
  echo $RESULT | grep -E $LOG_QUERY

  validateJobSummary $?
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0
 } 


###########################################
# Main function
###########################################

displayHeader "STARTING JOB SUMMARY INFO TEST SUITE"

RESOURCEMANAGER=$(getResourceManager)
getclusterMapCapacity
getclusterReduceCapacity

while [[ ${TESTCASE_ID} -le 70 ]] ; do
  JobSummaryInfo${TESTCASE_ID}
  incrTestCaseId
done

echo "Copying mapred-jobsummary.log to the gateway: scp $RESOURCEMANAGER:$HADOOP_LOG_DIR/mapredqa/mapred-jobsummary.log  $ARTIFACTS_DIR"
time scp $RESOURCEMANAGER:$HADOOP_LOG_DIR/mapredqa/mapred-jobsummary.log  $ARTIFACTS_DIR 2>&1

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
