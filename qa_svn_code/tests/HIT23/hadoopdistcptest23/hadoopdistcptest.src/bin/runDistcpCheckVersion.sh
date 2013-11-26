#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

echo "DISTCP TESTCASE5: Confirm that we are running the DistCp rewrite aka DistCpV2"

# Note: distcp version check will go in with http://bug.corp.yahoo.com/show_bug.cgi?id=5535496
# need to update this test to use that when the bug is committed
#
# have not found a version identifier or equivalent to look at to confirm distcp v2, the
# best check seems to be to look at the help text and the associated exception text
# because this is easy and obvious to access, and has changed significantly in the 
# rewrite 
#
# DistCp v2 has this:
#    java.lang.IllegalArgumentException: Target path not specified
# DistCp v1 contains this text:
#    java.lang.IllegalArgumentException: Missing dst path
#

help_txt_exception_str=`$HADOOP distcp 2>&1|grep "java.lang.IllegalArgumentException: Target path not specified"` 

echo ""
echo "INFO help_txt_exception_str is:  $help_txt_exception_str"
echo ""

# check the output
if [ "$help_txt_exception_str" == "java.lang.IllegalArgumentException: Target path not specified" ]
then
  # distcp v2, pass 
  echo PASS
else
  # distcp not v2, fail 
  echo FAIL
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT

