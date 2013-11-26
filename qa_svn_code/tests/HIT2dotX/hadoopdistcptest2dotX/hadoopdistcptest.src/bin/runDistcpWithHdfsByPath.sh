#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

echo "DISTCP TESTCASE2: Basic local Distcp using default FS type HDFS and dir path"
echo "This is a test file that we will distcp from DFS /tmp/in2/ to /tmp/out2" > /tmp/file1.txt
echo "This is another test file that we will distcp from DFS /tmp/in2/ to /tmp/out2" > /tmp/file2.txt
$HADOOP fs -mkdir /tmp/in2
$HADOOP fs -mkdir /tmp/out2
$HADOOP fs -put /tmp/file1.txt /tmp/in2
$HADOOP fs -put /tmp/file2.txt /tmp/in2
$HADOOP distcp /tmp/in2 /tmp/out2

# check the output
reference=`$HADOOP fs -cat  /tmp/in2/* | openssl dgst| cut -d ' ' -f2`
target=`$HADOOP fs -cat  /tmp/out2/in2/* | openssl dgst| cut -d ' ' -f2`

echo "Reference is: $reference"
echo "Target is: $target"

if [ $reference == $target ]
then
  # distcp src and dst matched, pass
  echo PASS
  # if we pass, clean up the local tmp file, and dfs
  rm /tmp/file1.txt /tmp/file2.txt
  $HADOOP fs -rm -r -skipTrash /tmp/in2
  $HADOOP fs -rm -r -skipTrash /tmp/out2
else
  # distcp src and dst does not match, fail
  echo FAIL
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT

