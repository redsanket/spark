#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

deleteHdfsDir gridmixV2 >> $ARTIFACTS_FILE 2>&1
createHdfsDir gridmixV2/data >> $ARTIFACTS_FILE 2>&1

echo "Copying HADOOP_HOME to $ARTIFACTS/hadoop"
cp -R $HADOOP_HOME/ $ARTIFACTS/hadoop  >> $ARTIFACTS_FILE 2>&1

cd $ARTIFACTS/hadoop/src/benchmarks/gridmix2
export ANT_HOME=/homes/hadoopqa/tools/ant/latest/

echo "Building the gridmix jar"
echo "$ANT_HOME/bin/ant"
$ANT_HOME/bin/ant
if [[ $? -eq 0 ]] ; then
 cp build/gridmix.jar  $JOB_SCRIPTS_DIR_NAME/data/gridmix2/
fi
cd $JOB_SCRIPTS_DIR_NAME/data/gridmix2/ >> $ARTIFACTS_FILE 2>&1

export SCRIPT_EXIT_CODE=0
export COMMAND_EXIT_CODE=0
export GRID_MIX_DATA=gridmixV2/data
export GRID_MIX_HOME=${JOB_SCRIPTS_DIR_NAME}/data/gridmix2/
export EXAMPLE_JAR=${HADOOP_MAPRED_EXAMPLES_JAR}
export APP_JAR=${HADOOP_MAPRED_TEST_JAR}
export STREAMING_JAR=${HADOOP_STREAMING_JAR}
export USE_REAL_DATASET=true 

echo "GENERATING GRIDMIXV2 DATA"
START_TIME=`date +%s`
sh $JOB_SCRIPTS_DIR_NAME/data/gridmix2/generateGridmix2data.sh
COMMAND_EXIT_CODE=$?
END_TIME=`date +%s`
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
export PLOT_FILE=$ARTIFACTS_DIR/gridmixV2GenerateData.plot
if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
  JOB_EXECUTION_TIME=$(( $END_TIME - $START_TIME ))
  echo "YVALUE=$JOB_EXECUTION_TIME" > ${PLOT_FILE}
fi

echo "BEGINNING RUN OF GRIDMIXV2 JOBS"
echo "The GridMixV2 run consists of the following"
echo "| SIZE   |   JOBNAME    |   NUMBER OF JOBS  |    NUMBER OF REDUCES  | NUMBER OF MAP OUTPUT COMPRESSED JOBS | NUMBER OF JOB OUTPUT COMPRESSED JOBS |"
echo "| Small  | StreamSort   |        40         |        015            |                 20                   |                 20                   |"
echo "| Medium | StreamSort   |        16         |        170            |                 16                   |                 12                   |"
echo "| Large  | StreamSort   |        05         |        370            |                 05                   |                 03                   |"
echo "| Small  | javaSort     |        08,02      |        015,075        |                 10                   |                 03                   |"
echo "| Medium | javaSort     |        04,02      |        170,070        |                 06                   |                 04                   |"
echo "| Large  | javaSort     |        03         |        370            |                 03                   |                 02                   |"
echo "| Small  | combiner     |        11,04      |        010,001        |                 15                   |                 05                   |"
echo "| Medium | combiner     |        08         |        100            |                 08                   |                 02                   |"
echo "| Large  | combiner     |        04         |        360            |                 04                   |                 02                   |"
echo "| Small  | monsterQuery |        07         |        005            |                 07                   |                 04                   |"
echo "| Medium | monsterQuery |        05         |        100            |                 05                   |                 03                   |"
echo "| Large  | monsterQuery |        03         |        370            |                 03                   |                 01                   |"
echo "| Small  | webdataScan  |        24         |        015            |                 24                   |                 08                   |"
echo "| Medium | webdataScan  |        12         |        020            |                 12                   |                 04                   |"
echo "| Large  | webdataScan  |        02         |        070            |                 03                   |                 03                   |"
echo "| Small  | webdataSort  |        07         |        015            |                 07                   |                 07                   |"
echo "| Medium | webdataSort  |        04         |        170            |                 04                   |                 04                   |"
echo "| Large  | webdataSort  |        01         |        800            |                 01                   |                 01                   |"

START_TIME=`date +%s`
sh $JOB_SCRIPTS_DIR_NAME/data/gridmix2/rungridmix_2
COMMAND_EXIT_CODE=$?
END_TIME=`date +%s`
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
export PLOT_FILE=$ARTIFACTS_DIR/gridmixV2Job.plot
if [[ $COMMAND_EXIT_CODE -eq 0 ]] ; then
  JOB_EXECUTION_TIME=$(( $END_TIME - $START_TIME ))
  echo "YVALUE=$JOB_EXECUTION_TIME" > ${PLOT_FILE}
fi

TESTCASENAME="GridMixV2"
displayTestCaseResult
# Removing the copied hadoop build
rm -rf $ARTIFACTS/hadoop

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}

