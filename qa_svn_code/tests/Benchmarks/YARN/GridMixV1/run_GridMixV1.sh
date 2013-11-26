#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

deleteHdfsDir gridmixV1 >> $ARTIFACTS_FILE 2>&1
createHdfsDir gridmixV1/programs  >> $ARTIFACTS_FILE 2>&1
createHdfsDir gridmixV1/data >> $ARTIFACTS_FILE 2>&1

echo "Copying HADOOP_MAPRED_HOME to $ARTIFACTS/hadoop"
cp -R $HADOOP_MAPRED_HOME/ $ARTIFACTS/hadoop  >> $ARTIFACTS_FILE 2>&1

cd $ARTIFACTS/hadoop
export ANT_HOME=/homes/hadoopqa/tools/ant/latest/

echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/pipes/"
chmod -R 755 $ARTIFACTS/hadoop/src/c++/pipes/
echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/utils/"
chmod -R 755 $ARTIFACTS/hadoop/src/c++/utils/
echo "Changing permissions of $ARTIFACTS/hadoop/src/examples/pipes/"
chmod -R 755 $ARTIFACTS/hadoop/src/examples/pipes/

echo "Compiling the examples"
echo "$ANT_HOME/bin/ant -Dcompile.c++=yes examples"
$ANT_HOME/bin/ant -Dcompile.c++=yes examples
if [[ $? -eq 0 ]] ; then
 putLocalToHdfs $ARTIFACTS/hadoop/build/c++-examples/Linux-i386-32/bin/pipes-sort gridmixV1/programs  >> $ARTIFACTS_FILE 2>&1
fi
#cd - >> $ARTIFACTS_FILE 2>&1

export SCRIPT_EXIT_CODE=0
export COMMAND_EXIT_CODE=0
export GRID_MIX_HOME=$JOB_SCRIPTS_DIR_NAME/data/gridmix/
export HADOOP_HOME=$HADOOP_COMMON_HOME
export GRID_MIX_DATA=gridmixV1/data
export GRID_MIX_PROG=gridmixV1/programs
export EXAMPLE_JAR=${HADOOP_MAPRED_EXAMPLES_JAR}
export APP_JAR=${HADOOP_MAPRED_TEST_JAR}
export STREAM_JAR=${HADOOP_STREAMING_JAR}
export NUM_MAPS=20
export USE_REAL_DATASET=true 
chmod -R 777 ${GRID_MIX_HOME} 

echo "GENERATING GRIDMIXV1 DATA"
START_TIME=`date +%s`
sh $JOB_SCRIPTS_DIR_NAME/data/gridmix/generateData.sh
COMMAND_EXIT_CODE=$?
END_TIME=`date +%s`
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
export PLOT_FILE=$ARTIFACTS_DIR/gridmixV1GenerateData.plot
if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
  JOB_EXECUTION_TIME=$(( $END_TIME - $START_TIME ))
  echo "YVALUE=$JOB_EXECUTION_TIME" > ${PLOT_FILE}
fi


echo "BEGINNING RUN OF GRIDMIXV1 JOBS"
echo "The GridMixV1 run consists of the following"
echo "WebDataSort"
echo "Description:    Processes large, compressed datsets."
echo "Configuration : 20 small sized jobs with 2 reducers(keep 100% map, 100% reduce), 10 medium sized jobs with 17 reducers(keep 100% map, 100% reduce) and 3 large sized jobs with 37 reducers(keep 100% map, 100% reduce) "
echo " "
echo "MaxentLarge"
echo "Description:    An iterative job whose input data is given to the framework sans locality metadata. "
echo "Configuration : 10 iterations with 100 reducers where each map reads 1 input file, adding additional input files from the output of the previous iteration (keep 50% map, 100% reduce)"
echo " "
echo "Javasort, Pipesort, Streamsort"
echo "Description:    A benchmark to exercise each of the APIs to map/reduce"
echo "Configuration : 20 small sized sort jobs with 2 reducers, 10 medium sized sort jobs with 17 reducers and 3 large sized sort jobs with 37 reducers using pipes, streaming and java sort"
echo " "
echo "WebDataScan"
echo "Description:    Samples from a large, reference dataset"
echo "Configuration : 20 small sized jobs with 1 reducer(keep 1% map, 5% reduce), 10 medium sized jobs with 1 reducer(keep 1% map, 5% reduce), 3 large sized jobs with 1 reducer(keep 0.2% map, 5% reduce)"
echo " "
echo "MonsterQuery"
echo "Description:    Runs as a three stage map/reduce job to implement pipelined map/reduce jobs"
echo "Compute1:       keep 10% map, 40% reduce"
echo "Compute2:       keep 100% map, 77% reduce (Input from Compute1)"
echo "Compute3:       keep 116% map, 91% reduce (Input from Compute2)"
echo "Configuration : 20 small sized jobs with 2 reducers, 10 medium sized jobs with 17 reducers and 3 large sized jobs with 37 reducers"

START_TIME=`date +%s`
sh $JOB_SCRIPTS_DIR_NAME/data/gridmix/submissionScripts/allToSameCluster
COMMAND_EXIT_CODE=$?
END_TIME=`date +%s`
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
export PLOT_FILE=$ARTIFACTS_DIR/gridmixV1Job.plot
if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
  JOB_EXECUTION_TIME=$(( $END_TIME - $START_TIME ))
  echo "YVALUE=$JOB_EXECUTION_TIME" > ${PLOT_FILE}
fi

TESTCASENAME="GridMixV1"
displayTestCaseResult
# Removing the copied hadoop build
rm -rf $ARTIFACTS/hadoop

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

