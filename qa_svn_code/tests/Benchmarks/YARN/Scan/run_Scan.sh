#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

YARN_OPTIONS="-Dmapreduce.job.acl-view-job=*"

function localSetup {
  SUITESTART=`date +%s`

  cleanupTestData
}

function localTeardown {
  cleanupTestData
}

function cleanupTestData {
  echo "**** Cleaning up any old datasets"

  deleteHdfsDir scanInputDir >> $ARTIFACTS_FILE 2>&1
}

function test_scan {
  TESTCASE_DESC="Tests Scan"

  CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR randomwriter ${YARN_OPTIONS} scanInputDir"
  echo "RANDOMWRITER COMMAND: $CMD"   
  echo "OUTPUT:"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "RANDOMWRITER EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Unable to generate data for Scan; response code was $COMMAND_EXIT_CODE"

  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
  fi

  CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR loadgen ${YARN_OPTIONS} -m 18 -r 0 -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text -indir scanInputDir"  
  echo "LOAGEN COMMAND: $CMD"
  echo "OUTPUT"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "SCAN - LOAGEN EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Loadgen Job failed; response code was $COMMAND_EXIT_CODE"

  displayTestCaseResult
}

## Setup
localSetup

## Execute tests
executeTests

## Cleanup
localTeardown
