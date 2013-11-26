#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

## phw - bug 5343722
# NEED TO FIX THE INVOCATION of 'pipes' command
# THIS NOW COMES FROM 'mapred' AND NOT 'hadoop'
##
### Run pipes word count example.
   $HADOOP  pipes -Dmapred.job.queue.name=grideng -program $DFS_VALIDATION_DIR/bin/wordcount-simple -reduces 1 \
     -jobconf hadoop.pipes.java.recordreader=true,hadoop.pipes.java.recordwriter=true \
     -input $DFS_VALIDATION_DIR/data/wordCountInput -output $DFS_VALIDATION_DIR/data/pipesWordCountOutput
   mkdir -p $VALIDATION_DIR/output/pipesWordCountOutput
   $HADOOP dfs -copyToLocal $DFS_VALIDATION_DIR/data/pipesWordCountOutput/part-* $VALIDATION_DIR/output/pipesWordCountOutput/
   cat $VALIDATION_DIR/output/pipesWordCountOutput/part-* > $VALIDATION_DIR/output/pipesWordCountOutput.txt

if [ ! -e "$VALIDATION_DIR/output/pipesWordCountOutput.txt" ]; then
    echo "Result file doesn't exist"
    exit $HIT_ABORT;
fi

LINES=`wc -l $VALIDATION_DIR/output/pipesWordCountOutput.txt | awk '{print $1}'`
if [[ $LINES -ne 5315 ]] ; then
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT

