#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh

  
export NAMENODE="None"
export USER_ID=`whoami`

deleteHdfsDir MR830  >> $ARTIFACTS_FILE 2>&1
createHdfsDir MR830/  >> $ARTIFACTS_FILE 2>&1

########################################### 
#Function MR830_10 -  Test to check if compressed bzip2 input data which is greater than split size is provided to MR job, it is being processed by multiple mappers
###########################################
function MR830_10 {

  setTestCaseDesc "MR830_$TESTCASE_ID - Test to check if compressed bzip2 input data which is greater than split size is provided to MR job, it is being processed by multiple mappers"

  createHdfsDir MR830/MR830_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  echo "RANDOMTEXTWRITER COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter -Dmapreduce.randomtextwriter.totalbytes=$((1024*1024*1024)) MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter -Dmapreduce.randomtextwriter.totalbytes=$((1024*1024*1024)) MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput

  echo "SORT COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  sort -r 3 -Dmapred.job.reduce.memory.mb=8192  -Dmapreduce.map.output.compress=true  -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.BZip2Codec  -Dmapreduce.output.fileoutputformat.compress=true  -Dmapreduce.output.fileoutputformat.compression.type=BLOCK -Dmapreduce.output.fileoutputformat.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput MR830/MR830_${TESTCASE_ID}/sortOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  sort  -Dmapred.job.reduce.memory.mb=8192  -Dmapreduce.map.output.compress=true  -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.BZip2Codec  -Dmapreduce.output.fileoutputformat.compress=true  -Dmapreduce.output.fileoutputformat.compression.type=BLOCK -Dmapreduce.output.fileoutputformat.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -r 3  -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput MR830/MR830_${TESTCASE_ID}/sortOutput

  echo "WORDCOUNT COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount MR830/MR830_${TESTCASE_ID}/sortOutput/part-r-00000 MR830/MR830_${TESTCASE_ID}/wordcountOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount MR830/MR830_${TESTCASE_ID}/sortOutput/part-r-00000 MR830/MR830_${TESTCASE_ID}/wordcountOutput & 2>&1
  export JOBID=""
  while [[ -z $JOBID ]] ; do
    JOBID=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list |tail -1 |cut -f 1 | grep job_`
    echo "Job ID of the running job is $JOBID"
  done
  sleep 180
  LAUNCHED_MAP_TASKS=`$HADOOP_MAPRED_HOME/bin/mapred --config $HADOOP_CONF_DIR job -status $JOBID | grep "Launched map tasks" | cut -f 2 -d "="`
  echo "LAUNCHED_MAP_TASKS=$LAUNCHED_MAP_TASKS"
  if [[ $LAUNCHED_MAP_TASKS -ge 2 ]] ; then
    COMMAND_EXIT_CODE=0
  else
   COMMAND_EXIT_CODE=1
   REASONS="Unable to verify the test. See BUG:4639493 "
   echo $REASONS
  fi
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function MR830_20 -  Test to check if compressed bzip2 input data of varying size is provided to MR job, it is being processed by multiple mappers
###########################################
function MR830_20 {

  setTestCaseDesc "MR830_$TESTCASE_ID - Test to check if compressed bzip2 input data of varying size is provided to MR job, it is being processed by multiple mappers"

  createHdfsDir MR830/MR830_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1

  echo "RANDOMTEXTWRITER COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  randomtextwriter  MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput

  echo "SORT COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  sort -r 3 -Dmapred.job.reduce.memory.mb=8192  -Dmapreduce.map.output.compress=true  -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.BZip2Codec  -Dmapreduce.output.fileoutputformat.compress=true  -Dmapreduce.output.fileoutputformat.compression.type=BLOCK -Dmapreduce.output.fileoutputformat.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput MR830/MR830_${TESTCASE_ID}/sortOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR  sort  -Dmapred.job.reduce.memory.mb=8192  -Dmapreduce.map.output.compress=true  -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.BZip2Codec  -Dmapreduce.output.fileoutputformat.compress=true  -Dmapreduce.output.fileoutputformat.compression.type=BLOCK -Dmapreduce.output.fileoutputformat.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -r 3  -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text MR830/MR830_${TESTCASE_ID}/randomTextWriterOutput MR830/MR830_${TESTCASE_ID}/sortOutput

  echo "WORDCOUNT COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount MR830/MR830_${TESTCASE_ID}/sortOutput/part-r-00000 MR830/MR830_${TESTCASE_ID}/wordcountOutput"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount MR830/MR830_${TESTCASE_ID}/sortOutput/part-r-00000 MR830/MR830_${TESTCASE_ID}/wordcountOutput & 2>&1
  export JOBID=""
  while [[ -z $JOBID ]] ; do
    JOBID=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -list |tail -1 |cut -f 1 | grep job_`
    echo "Job ID of the running job is $JOBID"
  done
  sleep 180
  LAUNCHED_MAP_TASKS=`$HADOOP_MAPRED_HOME/bin/mapred --config $HADOOP_CONF_DIR job -status $JOBID | grep "Launched map tasks" | cut -f 2 -d "="`
  echo "LAUNCHED_MAP_TASKS=$LAUNCHED_MAP_TASKS"
  if [[ $LAUNCHED_MAP_TASKS -ge 2 ]] ; then
    COMMAND_EXIT_CODE=0
  else
   COMMAND_EXIT_CODE=1
   REASONS="Unable to verify the test. See BUG:4639493 "
   echo $REASONS
  fi 
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}



###########################################
# Main function
###########################################

displayHeader "STARTING MAPREDUCE:830 (Providing BZip2 splitting support for Text data) TEST SUITE"

getFileSytem
while [[ ${TESTCASE_ID} -le 20 ]] ; do
  MR830_${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
