#!/bin/sh

. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"
###############################################
function generatePlotValue {

  THROUGHPUT=`grep "Throughput mb/sec: " ${ARTIFACTS_FILE} | tail -1 | awk '{print $7}'`

  echo "YVALUE=${THROUGHPUT}" > ${PLOT_FILE}
}


OPERATION="write read"
PERCENTAGE="25 100 200" 
CLUSTER_SIZE=18
FILESIZE=320

cd ${ARTIFACTS_DIR}/
createBenchmarksDir >> $ARTIFACTS_FILE 2>&1

SCRIPT_EXIT_CODE=0
for i in ${PERCENTAGE}; do
  for j in ${OPERATION}; do
    NUM_FILES=`echo "scale=0 ; (${CLUSTER_SIZE} * ${i} / 100)" | bc`

    cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR TestDFSIO  -${j} -nrFiles ${NUM_FILES} -fileSize ${FILESIZE}"
    #cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR TestDFSIO  -Dmapreduce.framework.name=yarn  -${j} -nrFiles ${NUM_FILES} -fileSize ${FILESIZE}"
    echo "COMMAND :: $cmd"
    echo "OUTPUT"
    $cmd 2>&1
    COMMAND_EXIT_CODE=$?
    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
    export PLOT_FILE=$ARTIFACTS_DIR/${j}_${i}.plot
    if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then 
      generatePlotValue
    fi
TESTCASENAME="DFSIO"
TESTCASE_DESC="Mapreduce Benchmark - DFSIO with \"$j\" Operration and $i %"
REASONS="DFSIO COMMAND Failed"
displayTestCaseResult "${i}-${j}"
  done
done

cd - 

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

