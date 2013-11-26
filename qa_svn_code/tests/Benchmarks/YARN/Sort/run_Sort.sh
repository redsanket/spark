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

  deleteHdfsDir sortInputDir 2>&1
  deleteHdfsDir sortOutputDir 2>&1
}

###############################################
function generatePlotValue {

  SECONDS=`grep "The job took " ${ARTIFACTS_FILE} | tail -n 1 | awk '{print $4}'`

  echo "YVALUE=${SECONDS}" > ${PLOT_FILE}
}

function test_sort {
  TESTCASE_DESC="Runs hadoop sort on random data and verifies that the data is properly sorted"

  CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR randomwriter ${YARN_OPTIONS} sortInputDir"   
  echo "RANDOMWRITER COMMAND: $CMD"

  echo "OUTPUT:"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "RANDOMWRITER EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Unable to generate data; response code was $COMMAND_EXIT_CODE"

  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
  else
    export PLOT_FILE=$ARTIFACTS_DIR/randomwriter.plot
    generatePlotValue
  fi

  CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR sort ${YARN_OPTIONS} sortInputDir sortOutputDir"  
  echo "SORT COMMAND: $CMD"

  echo "OUTPUT"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "SORT EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Unable to sort data; response code was $COMMAND_EXIT_CODE"

  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
  else
    export PLOT_FILE=$ARTIFACTS_DIR/sort.plot
    generatePlotValue
  fi

  CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR testmapredsort ${YARN_OPTIONS} -sortInput sortInputDir -sortOutput sortOutputDir"
  echo "SORT VALIDATION COMMAND: $CMD"

  echo "OUTPUT"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?
  echo "SORT VALIDATION EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Unable to sort data; response code was $COMMAND_EXIT_CODE"

  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
  else
    export PLOT_FILE=$ARTIFACTS_DIR/validate.plot
    generatePlotValue
  fi

  displayTestCaseResult
}

## Setup
localSetup

## Execute tests
executeTests

## Cleanup
localTeardown
