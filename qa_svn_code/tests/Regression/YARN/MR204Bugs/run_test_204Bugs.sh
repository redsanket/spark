#!/bin/bash

set -x
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh

# Setting Owner of this TestSuite (ported to 20 from yarn 22)
OWNER="vmotilal"

export NAMENODE="None"
export USER_ID=`whoami`
export TESTCASE_ID=740

deleteHdfsDir /tmp/Streaming  >> $ARTIFACTS_FILE 2>&1
deleteHdfsDir /user/$USER_ID/Streaming  >> $ARTIFACTS_FILE 2>&1


# This testcase is  to test a bug fix BugId 3758784 MAPREDUCE:2409
function test_cacheKey_FilesArchives {
    # Trigger a streaming job with mapper trying to read an archives file which is world readable
    # Trigger a streaming job with mapper trying to read a -Files file which is world readable 
    #  The file submitted to the streaming job with -files option should be able to read the file
    # the bug is that the file submitted as -archives option with create a folder with the same name
    # and the subsequent job that uses the -files option will refer to the cache option
    deleteHdfsDir /tmp/out10
    deleteHdfsDir /tmp/out11
    local myNN=`getNameNode`
    putLocalToHdfs ${JOB_SCRIPTS_DIR_NAME}/data/data.txt /tmp/data.txt >> $ARTIFACTS_FILE 2>&1
    putLocalToHdfs ${JOB_SCRIPTS_DIR_NAME}/data/data.txt /user/${USER_ID}/data.txt >> $ARTIFACTS_FILE 2>&1
    #CMD="$HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR -archives hdfs://${myNN}/tmp/data.txt -input /tmp/data.txt -mapper mapper.sh -output /tmp/out10 -file $WORKSPACE/data/cachekey/mapper.sh "
    # Submit a job with -archives option      
    CMD="$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -archives hdfs://${myNN}:8020/tmp/data.txt -input /user/hadoopqa/data.txt -output /tmp/out10 -mapper mapper.sh -file ${JOB_SCRIPTS_DIR_NAME}/data/mapper.sh"
    echo "$CMD"
    eval $CMD

    # Submit a job with -files option using the same file that was submitted earlier with -archives option
    CMD="$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR -files hdfs://${myNN}:8020/tmp/data.txt -input /user/hadoopqa/data.txt -output /tmp/out11 -mapper mapper.sh -file ${JOB_SCRIPTS_DIR_NAME}/data/mapper.sh"
    echo "$CMD"
    eval $CMD       
    local myoutput=`$HADOOP_HOME/bin/hadoop dfs -cat /tmp/out11/*`
    echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
    echo $myoutput  
    echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
    local validate=`echo $myoutput |grep "This is a text file"`
    if [ "X${validate}X" == "XX" ] ; then
        echo " The mapper did not open the cache file and so failing the test "
        setFailCase "The mapper did not open the cache file and so failing the test "
        windUp
        return
    fi      
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

# This is a bugfix Mapreduce2412 / bugzilla id 4316998 
function test_invalidQueueName {
    # Submit a job with invalid jobqueue name
    # expect the job to fail with a valid reason displayed
    TESTCASE_DESC="test_invalidQueueName"
    TESTCASE_ID=""
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    # Trigger sleep job with invalid queue name
    triggerSleepJob 1 1 0 0 1 "-Dmapred.job.queue.name=invalid"
    sleep 5

    local myresponse=`cat $ARTIFACTS_DIR/sleep.0.log |grep "java.lang.IllegalArgumentException: There is no queue named invalid"`
    if [ "X${myresponse}X" == "XX" ] ; then
        echo " Did not get the valid exception for invalid queue name and so failing the test cases " >> $ARTIFACTS_FILE 2>&1
        setFailCase " Did not get the valid exception for invalid queue name and so failing the test cases "
        windUp
        return
    fi
    echo " Got the exception for invalud queue as expected and so passing the test case " >> $ARTIFACTS_FILE 2>&1
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_GetJobIdMethod {
    # submit a job that prints 
    # expect the job id to be printed in the console
    TESTCASE_DESC="test_GetJobIdMethod"
    TESTCASE_ID=""
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    # Cleaning up the log file
    >/tmp/vinod9988
    TTLOG="${TTLogPath}${TTHOST}.log"
    putLocalToHdfs ${JOB_SCRIPTS_DIR_NAME}/data/data10.txt data10.txt
    deleteHdfsDir /tmp/out101
    # Trigger a job that will print the job id via getjobId method
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR  jar ${JOB_SCRIPTS_DIR_NAME}/data/TestGetJobId.jar org.apache.hadoop.examples.JobIdTest /user/${USER_ID}/data10.txt /tmp/out101 >>/tmp/vinod9988 2>&1&                
    sleep 10
    echo "Console output of the submitted job ">> $ARTIFACTS_FILE 2>&1
    cat /tmp/vinod9988 >> $ARTIFACTS_FILE 2>&1  
    local submittedJobId=`cat /tmp/vinod9988 | grep Submitted_Job_Id |cut -d ':' -f2`
    validateJobId $submittedJobId
    if [ $? -ge 1 ] ; then
        echo " The getJobId method did not return a valid job id and so failing the test case " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The getJobId method did not return a valid job id and so failing the test case "
        windUp
        return
    fi
    echo " The getJobId method returned a valid job id and so marking the test case as passed "
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}       


function test_CounterExceptionDisplayedInCLI {
    # Submit a job that has 100 counters defined
    # Expect the job to fail and the CLI to display "Exceeded limits on number of counters"
    TESTCASE_DESC="test_CounterExceptionDisplayedInCLI"
    TESTCASE_ID=""
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1
    displayTestCaseMessage "$TESTCASE_DESC" >> $ARTIFACTS_FILE 2>&1
    echo "*************************************************************" >> $ARTIFACTS_FILE 2>&1  
    deleteHdfsDir rw2
    >/tmp/clioutput
    $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${JOB_SCRIPTS_DIR_NAME}/data/RandomWriterWith100Counters.jar org.apache.hadoop.examples.RandomWriter -D test.randomwrite.total_bytes=$((1024)) rw2 >>/tmp/clioutput 2>&1&
    sleep 25
    cat /tmp/clioutput |grep "Exceeded limits on number of counters"
    if [ $? -ne 0 ] ; then
        echo " The error message for counters over 50 is not dislayed for the job failure " >> $ARTIFACTS_FILE 2>&1
        setFailCase "The error message for counters over 50 is not dislayed for the job failure "
        windUp
        return
    fi
    echo " The counter exception is displayed in the CLI and so passing the test case ">> $ARTIFACTS_FILE 2>&1
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}               
##########################################
# Main function
###########################################

#validateCluster

test_cacheKey_FilesArchives
#test_invalidQueueName
#test_GetJobIdMethod
#test_CounterExceptionDisplayedInCLI 

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
