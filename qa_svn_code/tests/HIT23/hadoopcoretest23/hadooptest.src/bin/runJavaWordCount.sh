#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

### Run mapreduce word count example.
  # check for the specific versioned examples jar
  echo "INFO: HIT_HADOOP_VERSION is: $HIT_HADOOP_VERSION"
  if [ -f $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HIT_HADOOP_VERSION.jar ]
  then
      # found the specific jar we expected 
      $HADOOP jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HIT_HADOOP_VERSION.jar wordcount -Dmapred.job.queue.name=grideng \
        $DFS_VALIDATION_DIR/data/wordCountInput  $DFS_VALIDATION_DIR/data/mapredWordCountOutput
  elif [ -f $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar ]
  then
      # did not find specific versioned jar, but found something that matched the base jar name
      echo "WARN: Can not find the explicit hadoop-mapreduce-examples jar file, hadoop-mapreduce-examples-$HIT_HADOOP_VERSION.jar, using: "
      echo "$HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar"
      $HADOOP jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapred.job.queue.name=grideng \
        $DFS_VALIDATION_DIR/data/wordCountInput  $DFS_VALIDATION_DIR/data/mapredWordCountOutput
  else
      # can not find the jar we need, bail
      echo "ERROR: Can not find the hadoop-mapreduce-examples jar file"
      (( RESULT = RESULT + 1 ))
      exit $RESULT
  fi

   mkdir -p $VALIDATION_DIR/output/mapredWordCountOutput
   $HADOOP  fs -copyToLocal $DFS_VALIDATION_DIR/data/mapredWordCountOutput/part-* $VALIDATION_DIR/output/mapredWordCountOutput/
   cat $VALIDATION_DIR/output/mapredWordCountOutput/part-* > $VALIDATION_DIR/output/mapredWordCountOutput.txt

if [ ! -e "$VALIDATION_DIR/output/mapredWordCountOutput.txt" ]; then
    echo "Result file doesn't exist"
    exit $HIT_ABORT;
fi

LINES=`wc -l $VALIDATION_DIR/output/mapredWordCountOutput.txt | awk '{print $1}'`
if [[ $LINES -ne 5315 ]] ; then
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT

