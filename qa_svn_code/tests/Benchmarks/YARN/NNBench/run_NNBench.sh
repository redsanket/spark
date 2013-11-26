#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

###############################################
function generatePlotValue {
  TPS=`grep "TPS" "${ARTIFACTS_FILE}" | grep -v RAW | tail -1 | awk '{print $7}'` 
  AL=`grep "Avg Lat" "${ARTIFACTS_FILE}" | grep -v "Close" | tail -1 | awk '{print $9}'` 
  AET=`grep -i "Avg exec" "${ARTIFACTS_FILE}" | tail -1 | awk '{print $10}'` 

  TPS_PLOT_FILE=$ARTIFACTS_DIR/${j}_TPS.plot
  AL_PLOT_FILE=$ARTIFACTS_DIR/${j}_AL.plot
  AET_PLOT_FILE=$ARTIFACTS_DIR/${j}_AET.plot


  echo "YVALUE=${TPS}" > ${TPS_PLOT_FILE}
  echo "YVALUE=${AL}" > ${AL_PLOT_FILE}
  echo "YVALUE=${AET}" > ${AET_PLOT_FILE}
}



SCRIPT_EXIT_CODE=0
OPS="create_write open_read rename delete" 
CLUSTER_SIZE=18
NUM_FILE=500
BLOCK_PER_FILE=1
BYTES_PER_BLOCK=20
REP_FACTOR=1
MAPS=`echo "scale=0 ; (${CLUSTER_SIZE} * 96/100)" | bc`
REDUCES=1 

cd ${ARTIFACTS_DIR}/
createBenchmarksDir >> $ARTIFACTS_FILE 2>&1
for j in ${OPS}; do
  export j 
  EXE_START=`date +"%s"` 
#  ARTIFACTS_FILE=$ARTIFACTS_DIR/$j.log
  echo "NNBENCH $j COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR nnbench -operation ${j} -maps ${MAPS} -reduces ${REDUCES} -blockSize ${BLOCK_PER_FILE} -bytesToWrite ${BYTES_PER_BLOCK} -bytesPerChecksum ${BLOCK_PER_FILE} -numberOfFiles ${NUM_FILE} -replicationFactorPerFile ${REP_FACTOR} -startTime ${EXE_START}"  
  echo "OUTPUT"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR nnbench -operation ${j} -maps ${MAPS} -reduces ${REDUCES} -blockSize ${BLOCK_PER_FILE} -bytesToWrite ${BYTES_PER_BLOCK} -bytesPerChecksum ${BLOCK_PER_FILE} -numberOfFiles ${NUM_FILE} -replicationFactorPerFile ${REP_FACTOR} -startTime ${EXE_START}  2>&1 
  COMMAND_EXIT_CODE=$?
  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
    generatePlotValue
  fi
TESTCASENAME="NNBench"
TESTCASEINDEX=$j
displayTestCaseResult $TESTCASEINDEX
done
cd -
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

