#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

echo "DISTCP TESTCASE3: Basic local Distcp using default FS type (HDFS) and a specific file by name"
echo "This is a test file that we will distcp from DFS /tmp/in/file.txt to /tmp/out" > /tmp/file.txt
$HADOOP fs -mkdir /tmp/in
$HADOOP fs -mkdir /tmp/out
$HADOOP fs -put /tmp/file.txt /tmp/in
$HADOOP distcp /tmp/in/file.txt /tmp/out 

# check the output
reference=`openssl dgst /tmp/file.txt | cut -d ' ' -f2`
target=`$HADOOP fs -cat  /tmp/out/file.txt | openssl dgst| cut -d ' ' -f2`

echo "Reference is: $reference"
echo "Target is: $target"

if [ $reference == $target ]
then
  # distcp src and dst matched, pass
  echo PASS
  # if we pass, clean up the local tmp file, and dfs
  rm /tmp/file.txt
  $HADOOP fs -rm -r -skipTrash /tmp/in
  $HADOOP fs -rm -r -skipTrash /tmp/out
else
  # distcp src and dst does not match, fail
  echo FAIL
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT

