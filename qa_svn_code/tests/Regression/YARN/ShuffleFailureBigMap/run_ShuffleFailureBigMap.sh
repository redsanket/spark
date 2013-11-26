#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

###############################################

deleteHdfsDir bigMapInputDir >> $ARTIFACTS_FILE 2>&1
deleteHdfsDir bigMapOutputDir >> $ARTIFACTS_FILE 2>&1

function ShuffleFailureBigMap {
echo "BIGMAPOUTPUT COMMAND: $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_TEST_JAR testbigmapoutput -input bigMapInputDir -output bigMapOutputDir -create 3000"   
echo "OUTPUT:"
START_TIME=`date +%s`
$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_TEST_JAR testbigmapoutput -input bigMapInputDir -output bigMapOutputDir -create 3000 2>&1 
COMMAND_EXIT_CODE=$?
displayTestCaseResult
END_TIME=`date +%s`
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
export PLOT_FILE=$ARTIFACTS_DIR/bigmapoutput.plot
if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
  JOB_EXECUTION_TIME=$(( $END_TIME - $START_TIME ))
  echo "YVALUE=$JOB_EXECUTION_TIME" > ${PLOT_FILE}
fi
}

ShuffleFailureBigMap

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

