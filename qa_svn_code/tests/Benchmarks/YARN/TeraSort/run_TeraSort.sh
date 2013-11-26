#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="dcapwell"
YARN_OPTIONS="-Dmapreduce.job.acl-view-job=*"
GEN_DATA_SIZE=2

function localSetup {
  SUITESTART=`date +%s`

  cleanupTestData
}

function localTeardown {
  cleanupTestData
}

function cleanupTestData {
  echo "**** Cleaning up any old datasets"

  deleteHdfsDir teraInputDir 2>&1
  deleteHdfsDir teraOutputDir 2>&1
  deleteHdfsDir teraReportDir 2>&1
}

function test_terasort {
  TESTCASE_DESC="Create data on HDFS for TeraSort to read, sort it, and validate that the data is right"

  ## Create data
  CMD="$HADOOP_COMMON_CMD jar $HADOOP_MAPRED_EXAMPLES_JAR teragen ${YARN_OPTIONS} ${GEN_DATA_SIZE} teraInputDir"
  echo "TERAGEN COMMAND: $CMD"

  echo "OUTPUT:"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "TERAGEN EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Unable to generate data for TeraSort; response code was $COMMAND_EXIT_CODE"

  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
  fi

  ## sort data
  CMD="$HADOOP_COMMON_CMD jar $HADOOP_MAPRED_EXAMPLES_JAR terasort ${YARN_OPTIONS} -Dmapreduce.reduce.input.limit=-1 teraInputDir teraOutputDir"  
  echo "TERASORT COMMAND: $CMD"

  echo "OUTPUT"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "TERASORT EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "TeraSort Job failed; response code was $COMMAND_EXIT_CODE"

  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
  fi

  CMD="$HADOOP_COMMON_CMD jar $HADOOP_MAPRED_EXAMPLES_JAR teravalidate ${YARN_OPTIONS} -Dmapreduce.reduce.input.limit=-1 teraOutputDir teraReportDir"
  echo "TERASORT VALIDATION COMMAND: $CMD"

  echo "OUTPUT"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "TERASORT VALIDATION EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "TeraValidate Job failed; data is not sorted properly? Response code was $COMMAND_EXIT_CODE"

  displayTestCaseResult
}

## Setup
localSetup

## Execute tests
executeTests

## Cleanup
localTeardown
