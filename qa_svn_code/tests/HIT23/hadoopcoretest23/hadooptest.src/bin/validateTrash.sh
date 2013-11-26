#!/bin/bash

set -x 

GATEWAY=$1
HADOOP=$2
VALIDATION_DIR=$3
DFS_VALIDATION_DIR=$4

RESULT=0

### Test trash is configured and functioning
TRASH_FILE="$DFS_VALIDATION_DIR/trash_`date +"%s"`"
   $HADOOP dfs -touchz $TRASH_FILE ;
   $HADOOP dfs -rm $TRASH_FILE > $VALIDATION_DIR/output/removeTrashFile.txt

if [[ `grep -c "^Moved: " $VALIDATION_DIR/output/removeTrashFile.txt` -ne 1 ]] ; then
  (( RESULT = RESULT + 1 ))
fi

exit $RESULT
