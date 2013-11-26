#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

### Run streaming word count example.
  # check for the specific versioned streaming jar
  echo "INFO: HIT_HADOOP_VERSION is: $HIT_HADOOP_VERSION"
  if [ -f $HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming-$HIT_HADOOP_VERSION.jar ]
  then
      # found the specific jar we expected
      $HADOOP  jar  $HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming-$HIT_HADOOP_VERSION.jar  -Dmapred.job.queue.name=grideng \
        -input $DFS_VALIDATION_DIR/data/wordCountInput \
        -output $DFS_VALIDATION_DIR/data/streamingWordCountOutput \
        -file $VALIDATION_DIR/bin/streamingMapper.sh \
        -file $VALIDATION_DIR/bin/streamingReducer.sh \
        -mapper streamingMapper.sh -reducer streamingReducer.sh

  elif [ -f $HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming-*.jar ]
  then
      # did not find specific versioned jar, but found something that matched the base jar name
      echo "WARN: Can not find the explicit hadoop-streaming jar file, hadoop-streaming-$HIT_HADOOP_VERSION.jar, using: "
      echo "$HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming-*.jar"
      $HADOOP  jar  $HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming-*.jar  -Dmapred.job.queue.name=grideng \
        -input $DFS_VALIDATION_DIR/data/wordCountInput \
        -output $DFS_VALIDATION_DIR/data/streamingWordCountOutput \
        -file $VALIDATION_DIR/bin/streamingMapper.sh \
        -file $VALIDATION_DIR/bin/streamingReducer.sh \
        -mapper streamingMapper.sh -reducer streamingReducer.sh

  else
      # can not find the jar we need, bail
      echo "ERROR: Can not find the hadoop-streaming jar file"
      (( RESULT = RESULT + 1 ))
      exit $RESULT
  fi

   mkdir -p $VALIDATION_DIR/output/streamingWordCountOutput
   $HADOOP fs -copyToLocal $DFS_VALIDATION_DIR/data/streamingWordCountOutput/part-* $VALIDATION_DIR/output/streamingWordCountOutput/
   cat $VALIDATION_DIR/output/streamingWordCountOutput/part-* > $VALIDATION_DIR/output/streamingWordCountOutput.txt

if [ ! -e "$VALIDATION_DIR/output/streamingWordCountOutput.txt" ]; then
    echo "Result file doesn't exist"
    exit $HIT_ABORT;
fi

LINES=`wc -l $VALIDATION_DIR/output//streamingWordCountOutput.txt | awk '{print $1}'`
if [[ $LINES -ne 5315 ]] ; then
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT

