#!/bin/bash
# This is the Hadoop Core Test script for 0.23.1 release lines, it's based off of the 
# hadoopcoretestdata-0.20.104.2 scripts, these tests reuse the data from the 20 design,
# and therefore require the data pkg hadoopcoretestdata-0.20.104.2. 

# additional settings needed from original 20 code
HIT_PASS=${HIT_PASS:-0}
HIT_GW=${HIT_GW:-`uname -a| cut -d ' ' -f2`}

# ====== Functions =====
function getValue () {
  if [ "x$1" = "x" ]; then
    #Missing argument
    echo ""
  fi;
  ret=`echo $1 | awk -F = '{print $2}'`
  echo  $ret;
}

function getKey () {
  if [ "x$1" = "x" ]; then
    #Missing argument
    echo ""
  fi;
  ret=`echo $1 | awk -F = '{print $1}'`
  echo  $ret;
}
 
function makeDirs () {
  mkdir -p $LOGS/${HIT_EXEC_ID}
  mkdir -p $TEMP
  mkdir -p $LOCAL_VALIDATION_DIR
}

function err () {
  echo "$* ..."
  echo "Hadoop integration test quitting ..."
  exit $HIT_FAIL
}

function progress () {
  echo -n "."
}

function copyData () {
   KrbInit 
   $HADOOP dfs -rmr ${HIT_HDFS_STORAGE_DIR}/validation ;
   $HADOOP dfs -mkdir ${HIT_HDFS_STORAGE_DIR}/validation ;
   $HADOOP dfs -copyFromLocal $DATA/data ${HIT_HDFS_STORAGE_DIR}/validation ;
   $HADOOP dfs -copyFromLocal $TEST/bin ${HIT_HDFS_STORAGE_DIR}/validation ;
   cp -r $TEST/bin $LOCAL_VALIDATION_DIR ;
}

function KrbInit () {
    kdestroy; \
    kinit -k -t /homes/${USER}/${USER}.${DEPLOYMENT_TYPE}.headless.keytab ${USER} 2> /dev/null; \
    klist -f ;
}
# ====== End of unctions =====

# ====== Environment settings =====
export PACKAGE_NAME='hadoopcoretest23'

export HADOOP=$HADOOP_COMMON_HOME/bin/hadoop

export ROOT=$(getValue `yinst env ${PACKAGE_NAME} | grep ^ROOT`)
export DATA=$ROOT/share/data/hadoop/hadoopcoretestdata/
export TEST=$ROOT/share/test/hadoop/${PACKAGE_NAME}/
#modified the suite name to match the HIT requirements
#export SUITE="hadoop.integration.test"
export SUITE="Hadoop_Integration_Test_Results"
# Getting the name of the configuration package and extracting type of the
# deploy (dev or prod)
hadoopConfigPackageName=`yinst ls | grep HadoopConfig | sed -e 's/-.*$//'`
if [ "x$hadoopConfigPackageName" != "x" ]; then
  export DEPLOYMENT_TYPE=$(getValue `yinst env ${hadoopConfigPackageName} | grep ^TODO_KERBEROS_ZONE`)
else
  export DEPLOYMENT_TYPE="dev"
fi
if [ "x$DEPLOYMENT_TYPE" = "x" ]; then
    # In a config package has been installed differently or hasn't been
    # installed at all a reasonable default has to be set
    export DEPLOYMENT_TYPE='prod'
fi
SCRIPT_DIR=$TEST/bin

# The following variables are expected to be set by HIT framework.
# However, some reasonable defaults have to be present
if [ "x$HIT_EXEC_ID" = "x" ]; then
  export HIT_EXEC_ID=hadoopcore-`date +%s`
else
  export HIT_EXEC_ID=hadoopcore-${HIT_EXEC_ID}
fi
if [ "x$HIT_HDFS_STORAGE_DIR" = "x" ]; then
  export HIT_HDFS_STORAGE_DIR=/user/$USER/${HIT_EXEC_ID}/
fi

# local directory where execution related activities might happen
EXECUTION_DIR=/homes/$USER/${HIT_EXEC_ID}
TEMP=${EXECUTION_DIR}/temp
LOCAL_VALIDATION_DIR=${EXECUTION_DIR}/validation

export HIT_TIMEOUT=1800 #30 mins
 
# Actual set of test scripts
# see bug 5343722 - runPipesWordCount needs to be fixed, rm for now
# TESTS_LIST="runJavaWordCount runStreamingWordCount runPipesWordCount runStreamingNoReduce validateTrash";
TESTS_LIST="runJavaWordCount runStreamingWordCount runStreamingNoReduce validateTrash";

# ====== End of environment settings =====

usage="Usage: $0 -results-dir=/location/to/store/test/results/ -logs-dir=/location/to/store/logs/"

# Get command line arguments
if [ $# -ne 2 ]; then
  echo $usage;
  exit 3;
fi

RESULTS=""
if [ "x$1" = "x" ]; then
  echo $usage;
  exit 3;
fi;
if [ $(getKey $1) != "-results-dir" ]; then
  echo $usage;
  exit 3;
else 
  export RESULTS=$(getValue $1);
  shift # move one
fi

LOGS=""
if [ "x$1" = "x" ]; then
  echo $usage;
  exit 3;
fi;

if [ $(getKey $1) != "-logs-dir" ]; then
  echo $usage;
  exit 3;
else
  export LOGS=$(getValue $1);
  shift
fi

# Prepare the environment
makeDirs
copyData 2>&1 > $LOGS/${HIT_EXEC_ID}.log

# Some transitive variables needed for report generation
number_of_errors=0
number_of_failures=0
tests=0
time=0
timestamp=`date +%Y-%m-%dT%H:%M:%S`
# End of transitive variables section

# XML elements and formats
header='<?xml version="1.0" encoding="UTF-8" ?>
<testsuite errors="%s" failures="%s" hostname="%s" name="%s" tests="%s" time="%s" timestamp="%s">'
fmt='  <testcase name="%s" time="%s"/>'
fmt_error='  <testcase name="%s" time="%s">
    <error message="%s" type="%s">%s</error>
  </testcase>'
footer='</testsuite>'
# End of XML elements definition

# Let's jazz
echo "Running hadoop integration tests..."
for test in $TESTS_LIST; do
  start=`date +%s` # mark
  $SCRIPT_DIR/$test.sh $HIT_GW $HADOOP $LOCAL_VALIDATION_DIR ${HIT_HDFS_STORAGE_DIR}/validation \
    1>$LOGS/${HIT_EXEC_ID}/$test.out \
    2>$LOGS/${HIT_EXEC_ID}/$test.err
  retCode=$?

  progress

  end=`date +%s` # mark again
  elapsed=`expr $end - $start`

  # calculate trans. var values
  if [ $retCode != 0 ]; then
    (( number_of_failures = number_of_failures + 1 ))
    printf "$fmt_error\n" "$test" "$elapsed" "Error in the $test" "Script error" "<![CDATA[ `cat $LOGS/${HIT_EXEC_ID}/$test.err` ]]>" \
      >> $TEMP/${HIT_EXEC_ID}.testcases.content
  fi
  if [ $elapsed -gt $HIT_TIMEOUT ]; then
    printf "$fmt_error\n" "$test" "$elapsed" "$test has timeouted" "Script timeout" "`<![CDATA[ cat $LOGS/${HIT_EXEC_ID}/$test.err` ]]>" \
      >> $TEMP/${HIT_EXEC_ID}.testcases.content
    (( number_of_errors = number_of_errors + 1 ))
  fi
  if [ $retCode = 0 ]; then
    printf "$fmt\n" "$test" "$elapsed" >> $TEMP/${HIT_EXEC_ID}.testcases.content
  fi

  (( tests = tests + 1 ))
  (( time = time + $elapsed ))
done

## A very special case of the integration  test for gridmix
## This script can be executed as an integration test by itself but 
## for the matter of uniformity it will be exected from within the 
## hadoopcore integration test suite.

## see bug 5343722 - test disabled until it is reworked
##
## start=`date +%s` # mark
## test="hadoopgridmix"; # reset test name
## ${SCRIPT_DIR}/run-hadoopgridmix-integration-tests.sh -results-dir=${RESULTS} -logs-dir=${LOGS}
## retCode=$? # remember the return code
## 
## end=`date +%s` # mark again
## elapsed=`expr $end - $start`
## 
## (( tests = tests + 1 ))
## (( time = time + $elapsed ))
## 
## echo $retCode
## 
## if [ $retCode -ne $HIT_PASS ]; then
##  printf "$fmt_error\n" "$test" "$elapsed" "Error in the $test" "Script error" "" \
##    >> $TEMP/${HIT_EXEC_ID}.testcases.content
##  # Increase number of failures
##  (( number_of_failures = number_of_failures + 1 ))
##  echo "Failure of gridmix with return code: $retCode"
##else
##  printf "$fmt\n" "$test" "$elapsed" >> $TEMP/${HIT_EXEC_ID}.testcases.content
##fi
##
## end of disable section

#Generate report
  printf "$header\n" "$number_of_errors" "$number_of_failures" `hostname` "$SUITE" "$tests" "$time" "$timestamp" > $RESULTS/$SUITE.xml
  cat $TEMP/${HIT_EXEC_ID}.testcases.content >> $RESULTS/$SUITE.xml
  printf "\n%s\n" "$footer" >> $RESULTS/$SUITE.xml

# Generate return code
if [ $number_of_failures -ne 0 ]; then
    err
fi
echo
echo "Finishing hadoop integration tests..."

# Exit normally, unless a failure...
exit $HIT_PASS
