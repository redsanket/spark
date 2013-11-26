#!/bin/sh 

# Let Hadoop set JAVA_HOME, may be set incorrectly in Jenkins
unset JAVA_HOME

##############################################################################
# File Name     : driver.sh
# Type          : Shell Script
# Description   : This script is useful for invoking a single testcase or 
#                 Collection of testcases from yHudson or Locally
# Framework Twiki : http://twiki.corp.yahoo.com/view/Grid/HadoopQEAutomation 
##############################################################################

# This file should have executable permision
# Capturing starting time of execution
STARTTIME=`date +%m-%d-%Y_%H-%M-%S`
START_IN_SEC=`date +%s`
CURRENT_DIR=$(dirname $0)

export PATH=$PATH:/usr/kerberos/bin/

##### Usage Function
function usage
{
    echo 
    echo "USAGE"
    echo "-----------------------------------------------------------------------------------"
    echo "/bin/sh $0 <option1> <values1> <option2> <values2> ...

The options
        -c <cluster name> 
        -w <workspace location> 
        [ -a <artifacts root directory> ]
        [ -e <test type name> ]
        [ -p <test component name> ]
        [ -u <user to whom mail to be sent> ]
        [ -t <testconfig file> ]                ( To run a set of testcases
                                                Give the filename of the file which have the list
                                                of testcase to run,e.g. \"sampleTestSuiteFile.txt\"
                                                It will automatically pickedup from 
                                                WORKSPACE/conf/test/sampleTestSuiteFile.txt )

        [ -s <Single Test Suite to execute> ]   ( To run a single test suite, 
                                                Give the absolute path of the script strating 
                                                e.g. \$WORKSPACE/tests/Regression/HDFS/Trash/run_Trash.sh
                                                ** It can also take any number of arguements with the script
                        e.g /bin/sh $0 -c <clustername> -w <workspace> -s \"./tests/Regression/HDFS/Trash/run_Trash.sh --heap 200\")
        
        [ -n If mentioned , start and stop of cluster will not happen before execution ]"
    exit 1
}

# TESTTYPE - 
# 'setup-hudson-aggregation' - This is used by the first Hudson job to start
#                              subsequent downstream test jobs.
# 'aggregate-hudson-results' - This is used for the last Hudson job (e.g.
#                              NightlyHadoopQEAutomation-20-All-Status) to
#                              aggregate all the upstream test results.
#
# COMPONENT - 
# 'HDFS', 'MAPREDUCE', 'YARN', 'Benchmark', 'EndToEnd', 'Scanmus', 'Regression' 
#
# KEYWORD -
# E.g. can be used to break down large component such as HDFS, YARN using
# keywords like 'Part1', 'Part2'.

### Checking if it is run from yHudson Job
if [ "X${CLUSTER}X" == "XX" -o "X${TESTSUITEFILE}X" == "XX" -o "X${WORKSPACE}X" == "XX" ];then

    ### Commandline Input Variables
    ARGC=$#
    CLUSTER=""
    WORKSPACE=""
    ARTIFACTS=""
    TESTTYPE=""
    COMPONENT=""
    KEYWORD=""
    CLEANUP_ALL_ARCHIVES=""
    ARCHIVE_ARTIFACTS=0
    TESTSUITEFILE=""
    VERSION=""
    SINGLETESTSUITE=""
    NOSTOPSTART=""
    EMAIL_USER=""
	ADMIN_HOST=""

    if [ $ARGC -lt 6 ];then
        echo "ERROR : The number of arguments to the script is less than 6 !!"
        usage
    fi

    ### Commandline Input Validation
    while getopts "c:w:a:b:p:k:e:d:t:s:u:nh" opt; do
        case $opt in
            c)  CLUSTER=$OPTARG
                ;;
            w)  WORKSPACE=$OPTARG
                ;;
            a)  ARTIFACTS="$OPTARG/artifacts"
                ;;
            b)  ARCHIVE_ARTIFACTS=0
                ;;
            p)  COMPONENT=$OPTARG
                ;;
            k)  KEYWORD=$OPTARG
                ;;
            e)  TESTTYPE=$OPTARG
                ;;
            t)  TESTSUITEFILE=$OPTARG
                echo "The testconfig file               :: $TESTSUITEFILE"
                MODE="M"
                ;;
            s)  SINGLETESTSUITE=$OPTARG
                echo "The Single TestSuite              :: $SINGLETESTSUITE"
                MODE="S"
                ;;
            u)  EMAIL_USER=$OPTARG
                echo "EMAIL will be sent to $EMAIL_USER"
                ;;
            d)  CLEANUP_ALL_ARCHIVES="yes"
                ;;
            n)  NOSTOPSTART="yes"
                echo "Cluster will not be Restarted before execution"
                ;;
            h)  usage
                ;;
			r)  ADMIN_HOST=$OPTARG
				;;
            *)  usage;;
        esac
    done
else
    MODE="M"
fi

### Input Validation
if [ "X$CLUSTER" == "X" ];then
    echo "ERROR : CLUSTER name not provided, use -c option"
    usage
else
    ### From .20.204  deployments will be residing in /home/gs 
    HADOOP_QA_ROOT=`ls -ltr {/home,/grid/0}/gs/gridre/yroot.${CLUSTER}/share/{hadoop-current,hadoopcommon,hadoop}/bin/hadoop 2> /dev/null | sort | tail -1 | awk '{print $9}' | sed -e 's/\(.*\)\/gs.*$/\1/'`

    if [ "X${HADOOP_QA_ROOT}X" == "XX" ];then
        echo "FATAL :: Hadoop Binary is not found for cluster ${CLUSTER} - HADOOP_QA_ROOT cannot be defined!"
        exit 1
    fi

    ### From .20.204  deployments will be residing in /home/gs/var/log 
    if [ "X${HADOOP_QA_ROOT}X" == "X/homeX" ];then
        HADOOP_LOG_DIR="${HADOOP_QA_ROOT}/gs/var/log"
    else
        HADOOP_LOG_DIR="${HADOOP_QA_ROOT}/hadoop/var/log"
    fi
fi

if [ -d "${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER" ]; then
    echo "The Valid cluster         :: $CLUSTER"
else
    echo "ERROR : The Cluster \"$CLUSTER\" is not valid as ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER does not exists !!"
    usage
fi
if [ "X$WORKSPACE" == "X" ];then
    echo "ERROR : WORKSPACE not provided, use -w option"
    usage
fi
if [ -d $WORKSPACE ]; then
    echo "The Valid Workspace               :: $WORKSPACE"
else
    echo "ERROR : The Workspace \"$WORKSPACE\" does not exists !!"
    usage
fi
if [ -d $ADMIN_HOST ]; then
	echo "The admin host is: \"$ADMIN_HOST\" "
fi
if [ "X$TESTSUITEFILE" == "X" -a "X$SINGLETESTSUITE" == "X" ];then
    echo "ERROR : Use -s to execute single test suite or -t to execute list of testcases"
    usage
elif [ "X$TESTSUITEFILE" != "X" -a "X$SINGLETESTSUITE" != "X" ];then
    echo "ERROR : Choose either -s or -t option, both together not supported"
    usage
fi
HADOOP_QA_ROOT_TMP="/grid/0/tmp/hadoopqe.${CLUSTER}.${START_IN_SEC}"

function cleanupArtifacts {
    echo "Cleanup artifacts directories:"
    rm -rf $ARTIFACTS*
}

########### Set Up Hadoop Product Related Environment Variables ######
#
#       CLUSTER, HADOOP_HOME, HADOOP_PREFIX, HADOOP_COMMON_HOME, HADOOP_MAPRED_HOME,
#       HADOOP_HDFS_HOME, HADOOP_CONF_DIR, HADOOP_MAPRED_EXAMPLES_JAR
#       HADOOP_MAPRED_TEST_JAR, HADOOP_STREAMING_JAR, JAVA_HOME
#
#######################################################################

if [ "X$ARTIFACTS" == "X" ];then
    ARTIFACTS=$WORKSPACE/artifacts
fi

# ARTIFACT directory name is set in Hudson job configuration. The root directory
# needs to contain all component level artifiacts directories. This is required
# to aggregate test results at the end of each test cycle.
# E.g. ARTIFACTS=/homes/hadoopqa/hudson_artifacts/hudson_artifacts-0.23
ARTIFACTS_ROOT_DIR=$(dirname $ARTIFACTS)

if [ "X$CLEANUP_ALL_ARCHIVES" != "X" ] || [ "$TESTTYPE" == "setup-hudson-aggregation" ];then
    if [ "$TESTTYPE" != "aggregate-hudson-results" ];then
        cleanupArtifacts
    fi
fi

if [ "X$TESTTYPE" != "X" ];then
    ARTIFACTS=$ARTIFACTS-$TESTTYPE
fi

if [ "X$COMPONENT" != "X" ];then
    ARTIFACTS=$ARTIFACTS-$COMPONENT
fi

if [ "X$KEYWORD" != "X" ];then
    ARTIFACTS=$ARTIFACTS-$KEYWORD
fi

# If Hudson test aggregation ('setup-hudson-aggregation') is the target, a unique
# fingerprint file will need to be created by the upstream parent job so it can
# be shared by the downstream jobs for the purpose of association or grouping
# identification. 
if [ "$TESTTYPE" == "setup-hudson-aggregation" ];then
    # FINGERPRINT_FILE=$WORKSPACE/artifacts.stamp
    FINGERPRINT_FILE=$ARTIFACTS_ROOT_DIR/hudson_artifacts/artifacts.stamp
    date > $FINGERPRINT_FILE
fi

# If component is 'aggregate-hudson-results', don't do stop/start cluster and
# aggregate all component level test artifacts to the parent level artifacts
# directory.
if [ "$TESTTYPE" == "aggregate-hudson-results" ];then
    NOSTOPSTART="yes"
    ARTIFACTS=$ARTIFACTS_ROOT_DIR/artifacts
fi

if [ -d $ARTIFACTS ];then
    echo "$ARTIFACTS exists"
    ls -l $WORKSPACE

    # Archive artifacts so we can be compare tests results against a later run.
    if [ "${ARCHIVE_ARTIFACTS}" -gt 0 2> /dev/null ];then
        echo "Archive artifacts directories:"
        $CURRENT_DIR/rotate_artifacts_dir $ARTIFACTS $ARCHIVE_ARTIFACTS
    fi
    echo "Delete the old artifacts directory: $ARTIFACTS"
    rm -rf $ARTIFACTS
fi

TEST_SUMMARY_ARTIFACTS=$ARTIFACTS/TestSummary
mkdir -p $TEST_SUMMARY_ARTIFACTS

# Changing permission of WORKSPACE
chmod -R 775 $ARTIFACTS
if [ ! -d $ARTIFACTS ];then
    echo "FATAL !! $ARTIFACTS does not exists"
    exit 1
fi

if [ ! -d $TEST_SUMMARY_ARTIFACTS ];then 
   echo "FATAL !! $TEST_SUMMARY_ARTIFACTS does not exists"
    exit 1
fi

# Initialize common hadoop environment variables
. $WORKSPACE/conf/hadoop/common_hadoop_env.sh $CLUSTER $WORKSPACE $ARTIFACTS $HADOOP_QA_ROOT $HADOOP_LOG_DIR

# Load Library 
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

# get the tickets for all users
getAllUserTickets

# set the user to hadoopqa
setKerberosTicketForUser $HADOOPQA_USER

## According to http://bug.corp.yahoo.com/show_bug.cgi?id=4812435&mark=12#c12
## cp $HADOOP_PREFIX/lib/junit-4.8.2.jar to $HADOOP_PREFIX/share/hadoop/common/lib
## Workaround : MAPREDUCE-2972 : START
if [ -f  ${HADOOP_PREFIX}/share/hadoop/mapreduce/lib/junit*jar ];then
	echo ">>>>>>>>>>>>>> `ls ${HADOOP_PREFIX}/share/hadoop/mapreduce/lib/junit*jar` >>>>>>>>>>>>"
	junitJar=`basename  ${HADOOP_PREFIX}/share/hadoop/mapreduce/lib/junit*jar`
	echo "JUNIT JAR => $junitJar"
	if [ -f  ${HADOOP_PREFIX}/share/hadoop/common/lib/$junitJar ]; then
            echo `ls -l ${HADOOP_PREFIX}/share/hadoop/common/lib/$junitJar`
	else
            # WARNING: If not running as hadoopqa, this will cause the test to
            # hang, without even a warning.
            runasroot `hostname` "ln" "-s ${HADOOP_PREFIX}/lib/$junitJar ${HADOOP_PREFIX}/share/hadoop/common/lib/$junitJar"
            echo `ls -l ${HADOOP_PREFIX}/share/hadoop/common/lib/$junitJar`
	fi
else
	echo ">>>>>>>>>>>>>> FATAL :: ${HADOOP_PREFIX}/share/hadoop/mapreduce/lib/junit*jar not present. Exiting "
	exit 1
fi
## Workaround : MAPREDUCE-2972 : END

# Test Specific Generic Variables
TESTDIR=$WORKSPACE/tests
FAILSUMMARY=${TEST_SUMMARY_ARTIFACTS}/failSummary.log
PASSSUMMARY=${TEST_SUMMARY_ARTIFACTS}/passSummary.log
SKIPSUMMARY=${TEST_SUMMARY_ARTIFACTS}/skipSummary.log
TESTSUMMARY=${TEST_SUMMARY_ARTIFACTS}/testSummary.log
TEST_INFO_LOG=${TEST_SUMMARY_ARTIFACTS}/testInfo.log
TEST_HISTORY=${TEST_SUMMARY_ARTIFACTS}/testHistory.log

# Test execution may not result in one or more of the following types.
# Touch the file so that at least an empty file will exists.
/bin/touch $FAILSUMMARY
/bin/touch $PASSSUMMARY
/bin/touch $SKIPSUMMARY

if [ "X${JOB_URL}X" != "XX" ];then
    HUDSON_JOB=`echo $JOB_URL | sed -e 's/.*job\/\(.*\)\/$/\1/'`
    echo "Hudson Job = '$HUDSON_JOB'"
    echo "$CURRENT_DIR/check_archives -type failures -project $HUDSON_JOB > $TEST_HISTORY"
    $CURRENT_DIR/check_archives -type failures -project $HUDSON_JOB > $TEST_HISTORY
fi

# Failed test list file
TIME=`date +%Y%m%d_%H%M%S`
TESTLIST_DIR=$WORKSPACE/conf/test
# FAILED_LIST=${TESTLIST_DIR}/failed_tests.${TIME}
FAILED_LIST=${TESTLIST_DIR}/failed_tests
if [ "X$TESTTYPE" != "X" ];then
    FAILED_LIST=$FAILED_LIST-$TESTTYPE
fi

if [ "X$COMPONENT" != "X" ];then
    FAILED_LIST=$FAILED_LIST-$COMPONENT
fi
FAILED_LIST=$FAILED_LIST.$TIME

echo "****************************************************************************"
echo "BEGINNING TO EXECUTE HADOOP QA TESTS ON  CLUSTER => \"$CLUSTER\""
echo "****************************************************************************"

# Create log to track test information (i.e. hadoop version and build URL) at the
# beginning of the test execution instead of waiting until the test execution is
# completed, which could potentially be a long time. This would be useful
# information to have before the test execution completes. 
setHadoopVersion
cat >> $TEST_INFO_LOG<<EOF
HADOOP VERSION : $VERSION
BUILD_URL : $BUILD_URL
EOF

LOCAL_RESULT=0   #### ??
FINAL_RESULT=0

### The function to execute test scripts
function runTestSuite {
    testlogfile=$1
    shift
    suitefileWithArgs=$@
    suitefilename=`echo $suitefileWithArgs|cut -d' ' -f1`

    #get the tickets for all users
    getAllUserTickets

    #set the user to hadoopqa
    setKerberosTicketForUser $HADOOPQA_USER
    takeNNOutOfSafemode

    #set the user to hadoopqa
    setKerberosTicketForUser $HADOOPQA_USER

    SCRIPT_EXIT_CODE=0
    echo $suitefilename
    TEST_TIMEOUT=${TEST_TIMEOUT:=240}
    TEST_TIMEOUT_SEC=$(($TEST_TIMEOUT*60))

    local time=`date`
    echo "runTestSuite started: $time"
    TEST_START_TIME=`date +%m-%d-%Y_%H-%M-%S:%s`
    STSec=`echo $TEST_START_TIME | cut -d':' -f2`

    # DEBUG: used to have issues where jdk is wiped out by new deployment
    # /bin/ls -l /home/gs/java/jdk32/

    # The last line of the $testlogfile has to contain the line: "SCRIPT_EXIT_CODE=<>"
    echo "/usr/local/bin/perl -e 'alarm shift @ARGV; exec @ARGV' $TEST_TIMEOUT_SEC $suitefileWithArgs 2>&1 | tee -a $testlogfile"
    /usr/local/bin/perl -e 'alarm shift @ARGV; exec @ARGV' $TEST_TIMEOUT_SEC $suitefileWithArgs 2>&1 | tee -a $testlogfile

    local time=`date`
    echo "runTestSuite ended: $time"
    TEST_END_TIME=`date +%m-%d-%Y_%H-%M-%S:%s`
    ETSec=`echo $TEST_END_TIME | cut -d':' -f2`
    DIFF=$(( $ETSec - $STSec ))
    echo "Test suite executed for $DIFF seconds"

    if [ $DIFF -ge $TEST_TIMEOUT_SEC ]; then
        echo "WARN: test suite execution exceeded the configured time limit of $TEST_TIMEOUT_SEC seconds!!!"
    fi

    ### Find out the Result of the Test Suite
    if [ ! -e $testlogfile ];then
        echo "FATAL : $testlogfile NOT FOUND !!!"
        SCRIPT_EXIT_CODE=1
    else
        exitcodeline=`grep SCRIPT_EXIT_CODE $testlogfile | tail -1`

        # Fatal error occurred. It needs to be recorded and accounted for.
        if [ "X$exitcodeline" == "X" ];then
            MSG="FATAL : SCRIPT_EXIT_CODE is not found inside $testlogfile!!!"
            echo "$MSG"
            COMMAND_EXIT_CODE=1
            TESTCASENAME=$(basename $suitefilename)
            TESTCASE_DESC=$TESTCASENAME
            REASONS="$MSG"
            displayTestCaseResult
        else
            SCRIPT_EXIT_CODE=`echo $exitcodeline|cut -d"=" -f2`

            # If exitcodeline is non-numeric, it needs to be recorded and
            # counted as a failure.
            if [[ $SCRIPT_EXIT_CODE == *[!0-9]* ]]; then
                MSG="ERROR: Got non-numeric SCRIPT_EXIT_CODE '$SCRIPT_EXIT_CODE' from line '$exitcodeline' in file $testlogfile!!!"
                echo "$MSG"
                COMMAND_EXIT_CODE=1
                TESTCASENAME=$(basename $suitefilename)
                TESTCASE_DESC=$TESTCASENAME
                REASONS="$MSG"
                displayTestCaseResult
            fi

            # Fatal error occurred. It needs to be recorded and accounted for.
            if [ "X$SCRIPT_EXIT_CODE" == "X" ];then
                MSG="FATAL : SCRIPT_EXIT_CODE value is not found inside $testlogfile!!!"
                echo "$MSG"
                COMMAND_EXIT_CODE=1
                TESTCASENAME=$(basename $suitefilename)
                TESTCASE_DESC=$TESTCASENAME
                REASONS="$MSG"
                displayTestCaseResult
            fi 
        fi
    fi

    ### Safemode Sanity Check after TestSuite execution
    local getcmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR  dfsadmin -safemode get | grep 'Safe mode is OFF'"
    echo $getcmd
    eval $getcmd
    local safemodeStat=$?
    if [ "$safemodeStat" != "0" ];then
        MSG="FATAL :: Safemode Sanity Check FAILED after '$suitefilename': Increment SCRIPT_EXIT_CODE by 1"
        echo "$MSG"
        echo "$MSG" >> ${TEST_SUMMARY_ARTIFACTS}/safemodeIssue
        COMMAND_EXIT_CODE=1
        TESTCASENAME=$(basename $suitefilename)
        TESTCASE_DESC=$TESTCASENAME
        REASONS="$MSG"
        displayTestCaseResult
    fi

    ## Restroring cluster in default state if TESTSUITE Fails
    if [ "$SCRIPT_EXIT_CODE" != "0" ];then
        if [ "${NOSTOPSTART}" != "yes" ];then
            restartCluster
        fi
    fi
    #clear all tickets
    clearAllUserTickets
    return $SCRIPT_EXIT_CODE
}

# Aggregate the component level test results if the target is
# 'aggregate-hudson-results'.
if [ "$TESTTYPE" == "aggregate-hudson-results" ];then
    echo "$CURRENT_DIR/aggregate_test_results $ARTIFACTS_ROOT_DIR"
    $CURRENT_DIR/aggregate_test_results $ARTIFACTS_ROOT_DIR
else
    ## Create a file to store count of test run, pass and fail on the fly
    echo "0,0,0,0" > $TEST_SUMMARY_ARTIFACTS/countSummary
fi

if [ ! -f $TEST_SUMMARY_ARTIFACTS/countSummary ];then
    echo "FATAL !! $TEST_SUMMARY_ARTIFACTS/countSummary does not exists"
    exit 1
else
    echo -n "Content of $TEST_SUMMARY_ARTIFACTS/countSummary: "
    cat $TEST_SUMMARY_ARTIFACTS/countSummary
fi

## Restroring cluster in default state
if [ "${NOSTOPSTART}" != "yes" ];then
    restartCluster
fi

# MODE can be 'M' for multiple suites using test config file, or 'S' for single suite
if [ "$MODE" == "M" ];then
    TESTSUITEFILE=$TESTLIST_DIR/$TESTSUITEFILE
    echo "Using test list file: '$TESTSUITEFILE'"
    ### Parse Test Suite File and exceute tests
    tmpSuite=`cat $TESTSUITEFILE | egrep -v "^#|^ *$"| sed -e 's/\s\+|\s\+/|/g'|sed -e 's/\s\+/:/g' | tr '\n' ' '`
    for line in $tmpSuite; do

        # If the type parameter is defined, and it does not match
        # the type from the current line in the test list file, then
        # skip the current line.
        TESTTYPES=`echo $line | awk -F\|  '{print $1}'|tr "," " "`
        if [ "X$TESTTYPE" != "X" ];then
            LC_TESTTYPE=`echo "$TESTTYPE" | awk '{ print tolower($1) }'`
            LC_TESTTYPES=`echo "$TESTTYPES" | awk '{ print tolower($1) }'`
            if [ "$LC_TESTTYPE" != "$LC_TESTTYPES" ];then
                continue
            fi
        fi

        # If the component parameter is defined, and it does not match
        # the component from the current line in the test list file, then
        # skip the current line.
        COMPONENTS=`echo $line | awk -F\|  '{print $2}'|tr "," " "`
        if [ "X$COMPONENT" != "X" ];then
            LC_COMPONENT=`echo "$COMPONENT" | awk '{ print tolower($1) }'`
            LC_COMPONENTS=`echo "$COMPONENTS" | awk '{ print tolower($1) }'`
            if [ "$LC_COMPONENT" != "$LC_COMPONENTS" ];then
                continue
            fi
        fi
        
        JOBLIST=`echo $line | awk -F\|  '{print $3}'|tr "," " "`
        RUN_TESTS_SCRIPTS=`echo $line | awk -F\|  '{print $4}' |tr ',' '|'`
        TESTARGS=`echo $line | awk -F\|  '{print $5}' | tr ':' ' '`
        TEST_TIMEOUT=`echo $line | awk -F\|  '{print $6}' | tr ':' ' '`

        # If the keyword parameter is defined, and it does not match
        # the keyword from the current line in the test list file, then
        # skip the current line.
        KEYWORDS=`echo $line | awk -F\|  '{print $7}'|tr "," " "`
        if [ "X$KEYWORD" != "X" ];then
            LC_KEYWORD=`echo "$KEYWORD" | awk '{ print tolower($1) }'`
            LC_KEYWORDS=`echo "$KEYWORDS" | awk '{ print tolower($1) }'`
            if [ "$LC_KEYWORD" != "$LC_KEYWORDS" ];then
                continue
            fi
        fi

        if [ "$TESTTYPES" == "*" ];then
            TESTTYPES=`ls $TESTDIR`
        fi
        TESTDIR_TYPE=""
        
        TESTTYPES_EXIT_CODE=0
        for T in $TESTTYPES;do
            TESTDIR_TYPE=$TESTDIR/$T
            NEWCOMPONENTS=
            if [ "$COMPONENTS" == "*" ];then
                NEWCOMPONENTS=`ls $TESTDIR_TYPE`
            else
                NEWCOMPONENTS=$COMPONENTS
            fi      
            JOBS_SCRIPTS_DIR=""
            COMP_EXIT_CODE=0
            for C in $NEWCOMPONENTS; do
                JOBS_SCRIPTS_DIR=$TESTDIR_TYPE/$C
                JOBS=""
                if [ "$JOBLIST" == "*" ];then
                    JOBS=`ls $JOBS_SCRIPTS_DIR`
                else
                    JOBS=$JOBLIST
                fi
                export JOBS=$JOBS ### Needs to be removed
                JOB_SCRIPTS_DIR_NAME=""
                JOB_EXIT_CODE=0
                for JOB in $JOBS; do
                    TEST_START_TIME=`date +%m-%d-%Y_%H-%M-%S:%s`
                    STSec=`echo $TEST_START_TIME | cut -d':' -f2`
                    JOB_SCRIPTS_DIR_NAME=$JOBS_SCRIPTS_DIR/$JOB
                    TESTSUITES=""
                    if [ "$RUN_TESTS_SCRIPTS" == "*" ];then
                        TESTSUITES=`find $JOB_SCRIPTS_DIR_NAME -name run_\*.* | egrep -v "\.svn-base$" | egrep '\.sh$|\.pl$|\.t$' | tr "\n" " " 2> /dev/null`
		else
                        TESTSUITES=`find $JOB_SCRIPTS_DIR_NAME -name run_\*.* | egrep -v "\.svn-base$" | egrep '\.sh$|\.pl$|\.t$' | egrep "$RUN_TESTS_SCRIPTS"  | tr "\n" " " 2> /dev/null`
                    fi
                    
                    SUITES_EXIT_CODE=0
                    for TESTSUITE in $TESTSUITES; do
                        # TEST EXECUTION OF EACH TEST SUITE STARTS HERE
                        TESTSUITENAME=`echo $TESTSUITE | sed "s/.*run_\(.*\)\.\w\+$/\1/"`
                        ARTIFACTS_DIR=$ARTIFACTS/$T/$C/$JOB
                        ARTIFACTS_FILE=$ARTIFACTS_DIR/${TESTSUITENAME}.log
                        TMPDIR="${HADOOP_QA_ROOT_TMP}.${JOB}/"
                        cmd="mkdir -p  $TMPDIR $ARTIFACTS_DIR"
                        echo $cmd
                        eval $cmd
                        #if the above command is not successful exit
                        if [ "$?" -ne 0 ]; then
                            echo "$cmd DID NOT RUN. EXITING SUITE. PLEASE FIX THE ABOVE PROBLEM"
                            exit 1
                        fi
                        if [ ! -d $TEST_SUMMARY_ARTIFACTS ];then
                            echo "FATAL :: $TEST_SUMMARY_ARTIFACTS has been deleted just now !!"
                            echo "         Test Regression can not be proceeded."
                            exit 1
                        fi
                        ########### Set Up Test Related Environment Variables ######
                        . $WORKSPACE/conf/test/common_test_env.sh $WORKSPACE $JOBS_SCRIPTS_DIR $JOB_SCRIPTS_DIR_NAME $ARTIFACTS_DIR $LOCAL_RESULT $JOB $STARTTIME $TMPDIR $ARTIFACTS_FILE $TEST_SUMMARY_ARTIFACTS $TESTSUITE "$TESTARGS"
                        
                        ## Actual Execution
                        SCRIPT_EXIT_CODE=0
                        TESTSUITEWITHARGS="$TESTSUITE $TESTARGS"
                        displayHeader "Executing TestType:$T Component:$C  JOB:$JOB  SUITE_WITH_ARGS:$TESTSUITEWITHARGS"
                        runTestSuite $ARTIFACTS_FILE $TESTSUITEWITHARGS
                        SCRIPT_EXIT_CODE=$?

                        # Create a failed test list file
                        if [ $SCRIPT_EXIT_CODE -ne 0 ]; then
                            TESTSUITE_BASENAME=$(basename $TESTSUITE)
                            ERR_MSG="# $SCRIPT_EXIT_CODE failures"
                            echo "$TESTTYPES|$COMPONENTS|$JOB|$TESTSUITE_BASENAME|$TESTARGS|$TEST_TIMEOUT| $ERR_MSG >> $FAILED_LIST"
                            echo "$TESTTYPES|$COMPONENTS|$JOB|$TESTSUITE_BASENAME|$TESTARGS|$TEST_TIMEOUT| $ERR_MSG" >> $FAILED_LIST
                        fi

                        (( SUITES_EXIT_CODE = $SUITES_EXIT_CODE + $SCRIPT_EXIT_CODE ))
                        displayTestSuiteResult $TESTSUITENAME
                        #remove the TMPDIR
                        cmd="rm -rf $TMPDIR $SHARETMPDIR"
                        echo $cmd
                        eval $cmd
                    done    
                    TEST_END_TIME=`date +%m-%d-%Y_%H-%M-%S:%s`
                    ETSec=`echo $TEST_END_TIME | cut -d':' -f2`
                    DIFF=$(( $ETSec - $STSec ))
                    echo "FEATURE:$JOB|$DIFF|`echo $TEST_START_TIME | cut -d':' -f1`|`echo $TEST_END_TIME | cut -d':' -f1`" >> $TEST_SUMMARY_ARTIFACTS/executionTime
                    (( JOB_EXIT_CODE = $JOB_EXIT_CODE + $SUITES_EXIT_CODE ))
                done
                (( COMP_EXIT_CODE = $COMP_EXIT_CODE + $JOB_EXIT_CODE ))
            done
            (( TESTTYPES_EXIT_CODE = $TESTTYPES_EXIT_CODE + $COMP_EXIT_CODE ))
        done
        (( FINAL_RESULT = $FINAL_RESULT + $TESTTYPES_EXIT_CODE ))
    done
elif [ "$MODE" == "S" ];then
    TESTSUITE=`echo $SINGLETESTSUITE | awk '{print $1}'`
    if [ `echo $TESTSUITE | egrep -v "^/"` ];then
        echo "ERROR :: Please provide the absolute path of $SINGLETESTSUITE"
        exit 1
    fi
    TESTARGS=`echo $SINGLETESTSUITE | awk '{for( i=2; i<=NF; i++) print $i}' | tr "\n" " "` ## Not Implemented for Single mode
    if [ -e $TESTSUITE ];then       
        TESTSUITENAME=`echo $TESTSUITE | sed "s/.*run_\(.*\)\.\w\+$/\1/"`
        JOB_SCRIPTS_DIR_NAME=`echo $TESTSUITE | sed "s/\(.*\)\/run_.*$/\1/"`
        JOBS_SCRIPTS_DIR=`echo $JOB_SCRIPTS_DIR_NAME | sed "s/\(.*\)\/\w*/\1/"`
        JOB=`echo $JOB_SCRIPTS_DIR_NAME | awk -F\/ '{print $(NF) }'`
        COMP=`echo $JOB_SCRIPTS_DIR_NAME | awk -F\/ '{print $(NF-1) }'`
        TTYPE=`echo $JOB_SCRIPTS_DIR_NAME | awk -F\/ '{print $(NF-2) }'`
        ARTIFACTS_DIR=$ARTIFACTS/$TTYPE/$COMP/$JOB
        ARTIFACTS_FILE=$ARTIFACTS_DIR/${TESTSUITENAME}.log
        TMPDIR="${HADOOP_QA_ROOT_TMP}.${JOB}/"
        TEST_START_TIME=`date +%m-%d-%Y_%H-%M-%S:%s`
        cmd="mkdir -p  $TMPDIR $ARTIFACTS_DIR"
        echo $cmd
        eval $cmd
        #if the above command is not successful exit
        if [ "$?" -ne 0 ]; then
            echo "$cmd DID NOT RUN. EXITING SUITE. PLEASE FIX THE ABOVE PROBLEM"
            exit 1
        fi
        STSec=`echo $TEST_START_TIME | cut -d':' -f2`
        ########### Set Up Test Related Environment Variables ######
        . $WORKSPACE/conf/test/common_test_env.sh $WORKSPACE $JOBS_SCRIPTS_DIR $JOB_SCRIPTS_DIR_NAME $ARTIFACTS_DIR $LOCAL_RESULT $JOB $STARTTIME $TMPDIR $ARTIFACTS_FILE $TEST_SUMMARY_ARTIFACTS $TESTSUITE "$TESTARGS"
        ## Actual Execution
        SCRIPT_EXIT_CODE=0
        TESTSUITEWITHARGS="$TESTSUITE $TESTARGS"
        displayHeader "Executing TestType:$TTYPE Component:$COMP  JOB:$JOB  SUITE_WITH_ARGS:$TESTSUITEWITHARGS"
        runTestSuite $ARTIFACTS_FILE $TESTSUITEWITHARGS
        SCRIPT_EXIT_CODE=$?
        (( FINAL_RESULT = $FINAL_RESULT + $SCRIPT_EXIT_CODE ))
        displayTestSuiteResult $TESTSUITENAME
        #remove the TMPDIR
        cmd="rm -rf $TMPDIR $SHARETMPDIR"
        echo $cmd
        eval $cmd
        TEST_END_TIME=`date +%m-%d-%Y_%H-%M-%S:%s`
        ETSec=`echo $TEST_END_TIME | cut -d':' -f2`
        DIFF=$(( $ETSec - $STSec ))
        echo "FEATURE:$JOB|$DIFF|`echo $TEST_START_TIME | cut -d':' -f1`|`echo $TEST_END_TIME | cut -d':' -f1`" >> $TEST_SUMMARY_ARTIFACTS/executionTime
    else
        echo "FATAL : $TESTSUITE does not exists !!!"
        exit 1 
    fi
fi

## Restroring cluster in default state
if [ "${NOSTOPSTART}" != "yes" ];then
    restartCluster
fi

### Work on Test Summary
NUMBER_OF_TESTS_RAN=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f1`
NUMBER_OF_TESTS_PASSED=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f2`
NUMBER_OF_TESTS_FAILED=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f3`
NUMBER_OF_TESTS_SKIPPED=`cat $TEST_SUMMARY_ARTIFACTS/countSummary | cut -d',' -f4`
VERSION=`$HADOOP_PREFIX/bin/hadoop  --config $HADOOP_CONF_DIR version 2>&1 | grep -o "Hadoop [0-9\.]*" `
if [ "$HADOOPQE_TEST_BRANCH" == "0.20" ];then
    NAMENODES=`getNameNode`
    SNAMENODES=`getSecondaryNameNode`
else
    NAMENODES=`getNameNodes`
    SNAMENODES=`getSecondaryNameNodes`
fi
JOBTRACKER=`getJobTracker_TMP`

### Record total execution time 
if [ "$TESTTYPE" == "aggregate-hudson-results" ];then
    TOTAL_EXECUTION_TIME_IN_SEC=`$CURRENT_DIR/aggregate_test_exec_time $ARTIFACTS_ROOT_DIR|tail -1`
else
    END_IN_SEC=`date +%s`
    (( TOTAL_EXECUTION_TIME_IN_SEC = $END_IN_SEC - $START_IN_SEC ))
fi
TOTAL_EXECUTION_TIME=`convertTime $TOTAL_EXECUTION_TIME_IN_SEC`

cat >> $TESTSUMMARY<<EOF
********************************************************************************************
 HADOOP VERSION  :: $VERSION
********************************************************************************************
 TEST SUMMARY    :: EXECUTED = $NUMBER_OF_TESTS_RAN      PASSED = $NUMBER_OF_TESTS_PASSED      SKIPPED = $NUMBER_OF_TESTS_SKIPPED     FAILED = $NUMBER_OF_TESTS_FAILED
********************************************************************************************
 EXECUTION TIME  :: $TOTAL_EXECUTION_TIME
********************************************************************************************
 ARTIFACTS:
        BUILD_URL       = $BUILD_URL
        BUILD_ARTIFACTS = ${BUILD_URL}artifact/artifacts/
        PLOTS           = ${JOB_URL}plot
********************************************************************************************
 CLUSTER DETAILS:
        GATEWAY         = `hostname`
        NAMENODE        = $NAMENODES
        JOBTRACKER      = http://$JOBTRACKER:8088
        S.NAMENODE      = $SNAMENODES
        DATANODES       = `getDataNodes`
********************************************************************************************
 CLUSTER HEALTH CHECK FAILURES:
EOF
cat ${TEST_SUMMARY_ARTIFACTS}/safemodeIssue >> $TESTSUMMARY 2> /dev/null

cat >> $TESTSUMMARY<<EOF
********************************************************************************************
 TESTS FAILED:
EOF
extractSummary  $FAILSUMMARY $TESTSUMMARY
cat >> $TESTSUMMARY<<EOF
********************************************************************************************
 TESTS SKIPPED:
EOF
extractSummary  $SKIPSUMMARY $TESTSUMMARY
cat >> $TESTSUMMARY<<EOF
********************************************************************************************
 TESTS PASSED:
EOF
extractSummary $PASSSUMMARY $TESTSUMMARY

### Data to plot Test Summary
echo "YVALUE=$NUMBER_OF_TESTS_RAN" > ${TEST_SUMMARY_ARTIFACTS}/testsRan.plot
echo "YVALUE=$NUMBER_OF_TESTS_PASSED" > ${TEST_SUMMARY_ARTIFACTS}/testsPassed.plot
echo "YVALUE=$NUMBER_OF_TESTS_SKIPPED" > ${TEST_SUMMARY_ARTIFACTS}/testsSkipped.plot
echo "YVALUE=$NUMBER_OF_TESTS_FAILED" > ${TEST_SUMMARY_ARTIFACTS}/testsFailed.plot

cat $TESTSUMMARY 2> /dev/null

### Compare runtime against past runtime in Hudson archive directories
if [ "X${JOB_URL}X" != "XX" ];then
    TEST_RUNTIMES=${TEST_SUMMARY_ARTIFACTS}/testRuntimes.log
    echo "$CURRENT_DIR/check_archives -type runtime -threshold 20 -runtime $TOTAL_EXECUTION_TIME_IN_SEC -project $HUDSON_JOB > $TEST_RUNTIMES"
    $CURRENT_DIR/check_archives -type runtime -threshold 20 -runtime $TOTAL_EXECUTION_TIME_IN_SEC -project $HUDSON_JOB > $TEST_RUNTIMES
    RUNTIME_CHECK_EXIT_CODE=$?
    tail -1 $TEST_RUNTIMES
    if [ "$RUNTIME_CHECK_EXIT_CODE" != "0" ];then
        echo "ERROR: Found problem with test runtime!!!"
    fi
fi

## Sending mail to user 
if [ "X${EMAIL_USER}X" != "XX" ];then
    if [ "X${JOB_URL}X" == "XX" ];then
        sub="HadoopQERegression-${HADOOPQE_TEST_BRANCH}"
    else
        sub=`echo $JOB_URL | sed -e 's/.*job\/\(.*\)\/$/\1/'`
    fi
    SUBJECT="TEST_REPORT: $sub   VERSION: $VERSION   STARTTIME: $STARTTIME"
    EMAIL="$EMAIL_USER@yahoo-inc.com"

    # Debug: 5354045
    # /usr/bin/du -xsh /var/lib/mlocate/*
    /bin/df -h /

    set -x
    /bin/mail -s "$SUBJECT" "$EMAIL" < $TESTSUMMARY
    set +x
    if [ "$?" == "0" ];then
        echo "Email is sent successfully to $EMAIL"
    else
        echo "Email could not be sent to $EMAIL"
    fi
fi

## Clean Up
rm -rf $ARTIFACTS/hadoop

echo "******************************************************************************************************************"
echo "FINISHING EXECUTION OF HADOOP QA TESTS ON  CLUSTER => \"$CLUSTER\" with Hadoop Version :: $VERSION"
echo "******************************************************************************************************************"
echo "Execution exit code =  ${FINAL_RESULT}"

exit 0
