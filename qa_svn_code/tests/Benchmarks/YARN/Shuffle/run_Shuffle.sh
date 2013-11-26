#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

YARN_OPTIONS="-Dmapreduce.job.acl-view-job=*"

function generatePlotValue {

  ## do nothing to see if that fixes this test
  #SECONDS=`grep "The job took " ${ARTIFACTS_FILE} | tail -n 1 | awk '{print $4}'`

  #echo "YVALUE=${SECONDS}" > ${PLOT_FILE}
  echo "Skipping eneratePlotValue"
}

function test_shuffle {
  TESTCASE_DESC="Tests Hadoop Shuffle by having more maps than reduce tasks"

  CMD="$HADOOP_PREFIX/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR loadgen ${YARN_OPTIONS} -m 18 -r 9 -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text"

  echo "LOADGEN COMMAND: $CMD"
  echo "OUTPUT"
  $CMD 2>&1
  COMMAND_EXIT_CODE=$?


  echo "LOADGEN EXIT CODE: $COMMAND_EXIT_CODE"

  assertEqual $COMMAND_EXIT_CODE 0 "Unable to generate data; response code was $COMMAND_EXIT_CODE"

  export PLOT_FILE=$ARTIFACTS_DIR/shuffle.plot
  if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
    generatePlotValue
  fi

  TESTCASENAME="Shuffle"
  displayTestCaseResult
}

## Execute tests
executeTests
