#!/bin/bash

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

export USER_ID=`whoami`

deleteHdfsDir Pipes 2>&1

###########################################
# Function pipes test - pipes10 - Run a pipes wordcount job
###########################################
function pipes10 {
  setTestCaseDesc "Pipes-$TESTCASE_ID - Run a pipes wordcount job"
  createHdfsDir Pipes/pipes-${TESTCASE_ID} 2>&1

#  echo "Copying HADOOP_PREFIX to $ARTIFACTS/hadoop"
#  cp -R $HADOOP_PREFIX/ $ARTIFACTS/hadoop  >> $ARTIFACTS_FILE 2>&1

#  cd $ARTIFACTS/hadoop
#  export ANT_HOME=/homes/hadoopqa/tools/ant/latest/

#  echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/pipes/"
#  chmod -R 755 $ARTIFACTS/hadoop/src/c++/pipes/
#  echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/utils/"
#  chmod -R 755 $ARTIFACTS/hadoop/src/c++/utils/
#  echo "Changing permissions of $ARTIFACTS/hadoop/src/examples/pipes/"
#  chmod -R 755 $ARTIFACTS/hadoop/src/examples/pipes/

#  echo "Compiling the examples"
#  echo "$ANT_HOME/bin/ant -Dcompile.c++=yes examples"
#  $ANT_HOME/bin/ant -Dcompile.c++=yes examples 
#  if [[ $? -eq 0 ]] ; then
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/pipes-$TESTCASE_ID/c++-examples/Linux-i386-32/bin/ Pipes/pipes-${TESTCASE_ID} 2>&1
    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/pipes-$TESTCASE_ID/input.txt Pipes/pipes-${TESTCASE_ID}/input.txt 2>&1

    cd - >> $ARTIFACTS_FILE 2>&1

    echo "PIPES COMMAND: $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR  pipes -conf  $JOB_SCRIPTS_DIR_NAME/data/pipes-${TESTCASE_ID}/word.xml -input Pipes/pipes-${TESTCASE_ID}/input.txt -output Pipes/pipes-$TESTCASE_ID/outputDir -jobconf mapred.job.name="pipesTest-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=*"
    echo "OUTPUT:"
    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR  pipes -conf  $JOB_SCRIPTS_DIR_NAME/data/pipes-${TESTCASE_ID}/word.xml -input Pipes/pipes-${TESTCASE_ID}/input.txt -output Pipes/pipes-$TESTCASE_ID/outputDir -jobconf mapred.job.name="pipesTest-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=*
    validatePipesOutput
    COMMAND_EXIT_CODE=$?
#  else
#    REASONS="Compilation failed. Aborting the test. Bug 4213034"
#    echo $REASONS
#    COMMAND_EXIT_CODE=1
#  fi

  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0
 } 


###########################################
# Main function
###########################################

displayHeader "STARTING PIPES TEST SUITE"

while [[ ${TESTCASE_ID} -le 10 ]] ; do
  pipes${TESTCASE_ID}
  incrTestCaseId
done


echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
