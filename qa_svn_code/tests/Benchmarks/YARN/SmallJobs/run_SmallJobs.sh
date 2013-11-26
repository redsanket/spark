#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

###############################################
function generatePlotValue {

  SECONDS=`tail -1 ${ARTIFACTS_FILE} | awk '{print $4}'`


  echo "YVALUE=${SECONDS}" > ${PLOT_FILE}
}



deleteHdfsDir smallJobsBenchmark >> $ARTIFACTS_FILE 2>&1
createBenchmarksDir >> $ARTIFACTS_FILE 2>&1
SCRIPT_EXIT_CODE=0

NUM_RUNS=30
CLUSTER_SIZE=18
MAPS=`echo "scale=0 ; (${CLUSTER_SIZE} * 90 / 100 * 2)" | bc`
REDS=`echo "scale=0 ; (${CLUSTER_SIZE} * 90 / 100)" | bc` 
LINES=10000 

echo "SMALLJOBS COMMAND: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR mrbench -baseDir smallJobsBenchmark -numRuns ${NUM_RUNS} -maps ${MAPS} -reduces ${REDS} -inputLines ${LINES} -inputType ascending"  
echo "OUTPUT"
$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR mrbench -baseDir smallJobsBenchmark -numRuns ${NUM_RUNS} -maps ${MAPS} -reduces ${REDS} -inputLines ${LINES} -inputType ascending  2>&1 
COMMAND_EXIT_CODE=$?
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
export PLOT_FILE=$ARTIFACTS_DIR/smalljobs.plot
if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
  generatePlotValue
fi

TESTCASENAME="SmallJobs"
displayTestCaseResult
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

