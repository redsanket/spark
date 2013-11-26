#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

HADOOP_QA_HOME=$bin/..
LOG_HOME=${HADOOP_QA_HOME}/logs

DATE=$(date +%Y-%m-%d-%H-%M-%S)

ACC_LOG="acceptance.log"
EXEC_LOG="executionTime.log"

mkdir -p $LOG_HOME
rm ${LOG_HOME}/$ACC_LOG
ln -s $ACC_LOG.${DATE} $LOG_HOME/$ACC_LOG


${HADOOP_QA_HOME}/bin/driver.sh \
  -c ${1:-omegab} \
  -w $HADOOP_QA_HOME \
  -n \
  -t acceptance.txt 2>&1 | tee $LOG_HOME/$ACC_LOG.$DATE

#3 copy execution time log to montiro how things progress over time

cp artifacts/TestSummary/executionTime ${LOG_HOME}/$EXEC_LOG..${DATE}
rm ${LOG_HOME}/$EXEC_LOG
ln -s $EXEC_LOG.${DATE} $LOG_HOME/$EXEC_LOG
