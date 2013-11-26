#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh

  
export NAMENODE="None"
export USER_ID=`whoami`
export COMMAND_EXIT_CODE=0

deleteHdfsDir MR2037  >> $ARTIFACTS_FILE 2>&1
createHdfsDir MR2037/  >> $ARTIFACTS_FILE 2>&1

########################################### 
# Verify the counters value
########################################### 
function verifyValue {
  echo "Actual Value = $1, Expected Value = $2, Type = $3, Attempt = $4"
  if [[ $1 -ne $2 ]] ; then
   COMMAND_EXIT_CODE=1 
    echo "Attempt $4 for $3 failed to match. The expected value $2 does not match with the actual value $1"
  fi


}

########################################### 
#Function MR2037_10 -  Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for successfully completed tasks 
###########################################
function MR2037_10 {
  
  setTestCaseDesc "MR2037_$TESTCASE_ID - Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for successfully completed tasks"

  createHdfsDir MR2037/MR2037_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  COMMAND_EXIT_CODE=1

  echo "SLEEP COMMAND: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -m 5 -r 5"
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -m 5 -r 5 & 2>&1

  JOBID=`waitForJobId 300`

  echo "waiting for job history file" 
  waitForJobHistFile $JOBID

  echo "FILE_LOCATION=$FILE_LOCATION"

  JOBHISTORY_FILENAME=`echo $FILE_LOCATION | tr -s ' ' | cut -f 8  -d ' '`
  echo "JobHistory file is located at $JOBHISTORY_FILENAME"
  export JOBHISTORY_FILENAME

  getKerberosTicketForUser hdfs
  setKerberosTicketForUser hdfs
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -get $JOBHISTORY_FILENAME $ARTIFACTS_DIR/
  getKerberosTicketForUser hadoopqa
  setKerberosTicketForUser hadoopqa

  # instead of changing JOBID in multiple places below (and to preserve history filename metadata)
  JOBID=`basename $JOBHISTORY_FILENAME`

  NUMBER_OF_MAP_ATTEMPTS_FINISHED=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_MAP_ATTEMPTS_FINISHED=$NUMBER_OF_MAP_ATTEMPTS_FINISHED"
  NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=$NUMBER_OF_REDUCE_ATTEMPTS_FINISHED"

  m=1
  while [[ $m -le $NUMBER_OF_MAP_ATTEMPTS_FINISHED ]] ; do

  MAP_ATTEMPT_FINISHED_LINE=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2 `
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 MAP_ATTEMPT_FINISHED $m
 
  CPUUSAGES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 MAP_ATTEMPT_FINISHED $m

  VMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '|  tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m

  PHYSMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m

 
  (( m = $m +1 ))
  done

  m=1
  while [[ $m -le $NUMBER_OF_REDUCE_ATTEMPTS_FINISHED ]] ; do
y
  REDUCE_ATTEMPT_FINISHED_LINE=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2`
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE |  awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m

  CPUUSAGES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' |  sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m

  VMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '  | tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m

  PHYSMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m

  (( m = $m +1 ))
  done

  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function MR2037_20 -  Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for failed map tasks 
###########################################
function MR2037_20 {

  setTestCaseDesc "MR2037_$TESTCASE_ID - Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for failed map tasks"

  createHdfsDir MR2037/MR2037_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  COMMAND_EXIT_CODE=1
  echo "FAILED MAP JOB: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR fail -failMappers"
  echo "OUTPUT:" 
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR fail -failMappers & 2>&1

  JOBID=`waitForJobId 300`

  echo "waiting for job history file" 
  waitForJobHistFile $JOBID

  echo "FILE_LOCATION=$FILE_LOCATION"

  JOBHISTORY_FILENAME=`echo $FILE_LOCATION | tr -s ' ' | cut -f 8  -d ' '`
  echo "JobHistory file is located at $JOBHISTORY_FILENAME"
  export JOBHISTORY_FILENAME

  getKerberosTicketForUser hdfs
  setKerberosTicketForUser hdfs
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -get $JOBHISTORY_FILENAME $ARTIFACTS_DIR/
  getKerberosTicketForUser hadoopqa
  setKerberosTicketForUser hadoopqa

  JOBID=`basename $JOBHISTORY_FILENAME`

  NUMBER_OF_MAP_ATTEMPTS_FINISHED=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_MAP_ATTEMPTS_FINISHED=$NUMBER_OF_MAP_ATTEMPTS_FINISHED"
  NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=$NUMBER_OF_REDUCE_ATTEMPTS_FINISHED"
  NUMBER_OF_MAP_ATTEMPTS_FAILED=`grep '{"type":"MAP_ATTEMPT_FAILED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_MAP_ATTEMPTS_FAILED=$NUMBER_OF_MAP_ATTEMPTS_FAILED"


  m=1
  while [[ $m -le $NUMBER_OF_MAP_ATTEMPTS_FINISHED ]] ; do

  MAP_ATTEMPT_FINISHED_LINE=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2 `
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 MAP_ATTEMPT_FINISHED $m
  
  CPUUSAGES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"  
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 MAP_ATTEMPT_FINISHED $m
  
  VMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '|  tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m
  
  PHYSMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m
  
  
  (( m = $m +1 ))
  done

  m=1
  while [[ $m -le $NUMBER_OF_REDUCE_ATTEMPTS_FINISHED ]] ; do

  REDUCE_ATTEMPT_FINISHED_LINE=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2`
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE |  awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m

  CPUUSAGES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' |  sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  VMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '  | tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  PHYSMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  (( m = $m +1 ))
  done

  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function MR2037_30 -  Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for failed reduce tasks 
###########################################
function MR2037_30 {
  
  setTestCaseDesc "MR2037_$TESTCASE_ID - Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for failed reduce tasks"
  createHdfsDir MR2037/MR2037_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  COMMAND_EXIT_CODE=1

  echo "FAILED MAP JOB: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR fail -failReducers"
  echo "OUTPUT:" 
  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR fail -failReducers & 2>&1

  JOBID=`waitForJobId 300`

  echo "waiting for job history file" 
  waitForJobHistFile $JOBID

  echo "FILE_LOCATION=$FILE_LOCATION"

  JOBHISTORY_FILENAME=`echo $FILE_LOCATION | tr -s ' ' | cut -f 8  -d ' '`
  echo "JobHistory file is located at $JOBHISTORY_FILENAME"
  export JOBHISTORY_FILENAME

  getKerberosTicketForUser hdfs
  setKerberosTicketForUser hdfs
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -get $JOBHISTORY_FILENAME $ARTIFACTS_DIR/
  getKerberosTicketForUser hadoopqa
  setKerberosTicketForUser hadoopqa

  JOBID=`basename $JOBHISTORY_FILENAME`

  NUMBER_OF_MAP_ATTEMPTS_FINISHED=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_MAP_ATTEMPTS_FINISHED=$NUMBER_OF_MAP_ATTEMPTS_FINISHED"
  NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=$NUMBER_OF_REDUCE_ATTEMPTS_FINISHED"

  m=1
  while [[ $m -le $NUMBER_OF_MAP_ATTEMPTS_FINISHED ]] ; do

  MAP_ATTEMPT_FINISHED_LINE=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2 `
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 MAP_ATTEMPT_FINISHED $m

  CPUUSAGES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"  
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 MAP_ATTEMPT_FINISHED $m

  VMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '|  tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"  
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m

  PHYSMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m


  (( m = $m +1 ))
  done

  m=1
  while [[ $m -le $NUMBER_OF_REDUCE_ATTEMPTS_FINISHED ]] ; do

  REDUCE_ATTEMPT_FINISHED_LINE=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2`
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE |  awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
    
  CPUUSAGES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' |  sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  VMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '  | tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  PHYSMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  (( m = $m +1 ))
  done

  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

########################################### 
#Function MR2037_40 -  Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for killed jobs  
###########################################
function MR2037_40 {
  setTestCaseDesc "MR2037_$TESTCASE_ID - Test to check the clockSplits, cpuUsages, vMemKbytes, physMemKbytes for killed jobs"

  createHdfsDir MR2037/MR2037_${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
  COMMAND_EXIT_CODE=1

  echo "KILLED JOB: $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 -mt 100000 -rt 100000"

  echo "OUTPUT:" 

  $HADOOP_COMMON_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep -Dmapred.job.name="jobSummaryInfo-$TESTCASE_ID" -Dmapreduce.job.acl-view-job=* -m 10 -r 10 -mt 100000 -rt 100000  2>/dev/null &

  JOBID=`waitForJobId 300`

  # Allow some time before killing job, otherwise no job history file 
  sleep 20

  echo "KILLING JOB $JOBID: $HADOOP_MAPRED_HOME/bin/mapred  --config $HADOOP_CONF_DIR job -kill $JOBID"
  $HADOOP_MAPRED_HOME/bin/mapred  --config $HADOOP_CONF_DIR job -kill $JOBID

  echo "waiting for job history file" 
  waitForJobHistFile $JOBID

  echo "FILE_LOCATION=$FILE_LOCATION"
  
  JOBHISTORY_FILENAME=`echo $FILE_LOCATION | tr -s ' ' | cut -f 8  -d ' '`
  echo "JobHistory file is located at $JOBHISTORY_FILENAME"
  export JOBHISTORY_FILENAME

  getKerberosTicketForUser hdfs
  setKerberosTicketForUser hdfs
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -get $JOBHISTORY_FILENAME $ARTIFACTS_DIR/
  getKerberosTicketForUser hadoopqa
  setKerberosTicketForUser hadoopqa

  JOBID=`basename $JOBHISTORY_FILENAME`
  
  NUMBER_OF_MAP_ATTEMPTS_FINISHED=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_MAP_ATTEMPTS_FINISHED=$NUMBER_OF_MAP_ATTEMPTS_FINISHED"
  NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID | wc -l`
  echo "NUMBER_OF_REDUCE_ATTEMPTS_FINISHED=$NUMBER_OF_REDUCE_ATTEMPTS_FINISHED"

  m=1
  while [[ $m -le $NUMBER_OF_MAP_ATTEMPTS_FINISHED ]] ; do

  MAP_ATTEMPT_FINISHED_LINE=`grep '{"type":"MAP_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2 `
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 MAP_ATTEMPT_FINISHED $m 

  CPUUSAGES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"  
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 MAP_ATTEMPT_FINISHED $m
  
  VMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '|  tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"  
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m
  
  PHYSMEMKBYTES_VALUE=`echo $MAP_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 MAP_ATTEMPT_FINISHED $m


  (( m = $m +1 ))
  done

  m=1
  while [[ $m -le $NUMBER_OF_REDUCE_ATTEMPTS_FINISHED ]] ; do

  REDUCE_ATTEMPT_FINISHED_LINE=`grep '{"type":"REDUCE_ATTEMPT_FINISHED"' $ARTIFACTS_DIR/$JOBID  | cat -n | grep "^     $m" | cut -f 2`
  echo "MAP_ATTEMPT_FINISHED_LINE=$MAP_ATTEMPT_FINISHED_LINE"

  CPUSPLITS_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE |  awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 4 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "CPUSPLITS_VALUE=$CPUSPLITS_VALUE"
  CPUSPLITS_NUMBER=`echo $CPUSPLITS_VALUE | tr "," "\n" | wc -l`
  echo "CPUSPLITS_NUMBER=$CPUSPLITS_NUMBER"
  verifyValue $CPUSPLITS_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
    
  CPUUSAGES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 5 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' |  sed 's|}||g' | sed 's|{||g'`
  echo "CPUUSAGES_VALUE=$CPUUSAGES_VALUE"
  CPUUSAGES_NUMBER=`echo $CPUUSAGES_VALUE | tr "," "\n" | wc -l`
  echo "CPUUSAGES_NUMBER=$CPUUSAGES_NUMBER"
  verifyValue $CPUUSAGES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  VMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 '  | tail -1 | cut -f 6 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "VMEMKBYTES_VALUE=$VMEMKBYTES_VALUE"
  VMEMKBYTES_NUMBER=`echo $VMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "VMEMKBYTES_NUMBER=$VMEMKBYTES_NUMBER"
  verifyValue $VMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m
  
  PHYSMEMKBYTES_VALUE=`echo $REDUCE_ATTEMPT_FINISHED_LINE | awk -F'{"name":' -v OFS="\n" ' $1=$1 ' | tail -1 | cut -f 7 -d ":" | sed 's|\[||g' | sed 's|\],||g' | tr "[a-z|A-Z]" " " | sed 's|"||g' | sed 's|}||g' | sed 's|{||g'`
  echo "PHYSMEMKBYTES_VALUE=$PHYSMEMKBYTES_VALUE"
  PHYSMEMKBYTES_NUMBER=`echo $PHYSMEMKBYTES_VALUE  | tr "," "\n" | wc -l`
  echo "PHYSMEMKBYTES_NUMBER=$PHYSMEMKBYTES_NUMBER"
  verifyValue $PHYSMEMKBYTES_NUMBER 12 REDUCE_ATTEMPT_FINISHED $m

  (( m = $m +1 ))
  done

  (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
  displayTestCaseResult
  return 0

}

###########################################
# Main function
###########################################

displayHeader "STARTING MAPREDUCE:2037 (Capturing interim progress times, CPU usage, and memory usage, when tasks reach certain progress thresholds ) TEST SUITE"

getFileSytem

while [[ ${TESTCASE_ID} -le 40 ]] ; do
  MR2037_${TESTCASE_ID}
  incrTestCaseId
done

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
~             
