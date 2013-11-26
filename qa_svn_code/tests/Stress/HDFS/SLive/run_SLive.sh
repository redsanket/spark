#!/bin/bash

#load the library file
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

HADOOP_COMMON_CMD=$HADOOP_COMMON_HOME/bin/hadoop

if [ X"$ARTIFACTS_DIR" == X ]
then
  ARTIFACTS_DIR=.
fi

# SHARETMPDIR should be part of the environment. If it's not make it.
if [ X"$SHARETMPDIR" == X ]
then
  myTmp=/tmp
else
  myTmp=$SHARETMPDIR
fi

declare -r BLOCKSIZE=67108864 # 64M

declare -r BASEDIR=/user/$USER/S-Live

declare -r     APPEND_DIST_IDX=0
declare -r      APPEND_MAX_IDX=1
declare -r      APPEND_MIN_IDX=2
declare -r      APPEND_PCT_IDX=3
declare -r  BLOCK_SIZE_MAX_IDX=4
declare -r  BLOCK_SIZE_MIN_IDX=5
declare -r     CREATE_DIST_IDX=6
declare -r      CREATE_PCT_IDX=7
declare -r     DELETE_DIST_IDX=8
declare -r      DELETE_PCT_IDX=9
declare -r        DIR_SIZE_IDX=10
declare -r        DURATION_IDX=11
declare -r         LS_DIST_IDX=12
declare -r          LS_PCT_IDX=13
declare -r MAX_OPS_PER_MAP_IDX=14
declare -r      MKDIR_DIST_IDX=15
declare -r       MKDIR_PCT_IDX=16
declare -r       NUM_FILES_IDX=17
declare -r        NUM_MAPS_IDX=18
declare -r     NUM_REDUCES_IDX=19
declare -r     PACKET_SIZE_IDX=20
declare -r       READ_DIST_IDX=21
declare -r        READ_PCT_IDX=22
declare -r   READ_SIZE_MAX_IDX=23
declare -r   READ_SIZE_MIN_IDX=24
declare -r     RENAME_DIST_IDX=25
declare -r      RENAME_PCT_IDX=26
declare -r        REPL_MAX_IDX=27
declare -r        REPL_MIN_IDX=28
declare -r            SEED_IDX=29
declare -r       SLEEP_MAX_IDX=30
declare -r       SLEEP_MIN_IDX=31
declare -r    WRITE_SZ_MAX_IDX=32
declare -r    WRITE_SZ_MIN_IDX=33

declare -a TP

# Reset the global TP variable to the base values.
# These parameters can't be used "as-is" to run a test. The parameters must be
# modified in order to specify the percetage and distribution of each type of
# test.
# These parameters correspond to the params given to
# org.apache.hadoop.fs.slive.SliveTest
function resetTestParms {
# -append <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$APPEND_PCT_IDX]='0'
TP[$APPEND_DIST_IDX]='uniform'

# -appendSize <min>,<max>
#          where <min> and <max> is the size range for the appended op.
TP[$APPEND_MIN_IDX]='1'
TP[$APPEND_MAX_IDX]="$BLOCKSIZE"

# -blockSize <min>,<max>
#          where <min> and <max> is the dfs block size range for the create op.
TP[$BLOCK_SIZE_MIN_IDX]="$BLOCKSIZE"
TP[$BLOCK_SIZE_MAX_IDX]="$BLOCKSIZE"

# -create <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$CREATE_PCT_IDX]='0'
TP[$CREATE_DIST_IDX]='uniform'

#  -delete <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$DELETE_PCT_IDX]='0'
TP[$DELETE_DIST_IDX]='uniform'

# -dirSize <arg>       Max files per directory
TP[$DIR_SIZE_IDX]='16'

# -duration <arg>  Duration of a map task in seconds
TP[$DURATION_IDX]='300'

# -ls <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$LS_PCT_IDX]='0'
TP[$LS_DIST_IDX]='uniform'

# -ops <arg>           Max number of operations per map
TP[$MAX_OPS_PER_MAP_IDX]='10000'

#  -mkdir <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$MKDIR_PCT_IDX]='0'
TP[$MKDIR_DIST_IDX]='uniform'

# -files <arg>         Max total number of files
TP[$NUM_FILES_IDX]='1024'

# -maps <arg>          Number of maps
TP[$NUM_MAPS_IDX]='20'

# -reduces <arg>       Number of reduces
TP[$NUM_REDUCES_IDX]='5'

# -packetSize <arg>    Dfs write packet size
TP[$PACKET_SIZE_IDX]='65536'

# -read <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$READ_PCT_IDX]='0'
TP[$READ_DIST_IDX]='uniform'

# -readSize <min>,<max>
#          where <min> and <max> is the size range for the read op.
TP[$READ_SIZE_MIN_IDX]='1'
TP[$READ_SIZE_MAX_IDX]='4294967295'

# -rename <pct>,<distribution>
#          where
#            <pct> is a number between 0 and 100. All of the <pct> params
#                (of -append, -create, -delete, -ls, -mkdir, -read, and -rename)
#                must add up to 100.
#            <distribution> is one of beg,end,uniform,mid
TP[$RENAME_PCT_IDX]='0'
TP[$RENAME_DIST_IDX]='uniform'

# -replication <min>,<max>
#          where <min> and <max> is the repl range for the create op.
TP[$REPL_MIN_IDX]='1'
TP[$REPL_MAX_IDX]='3'

# -seed <arg>          Random number seed
TP[$SEED_IDX]='12345678'

# -sleep <min>,<max>
#          where <min> and <max> is the randome sleep range between ops in ms.
TP[$SLEEP_MIN_IDX]='100'
TP[$SLEEP_MAX_IDX]='1000'

# -writeSize <min>,<max>
#        where <min> and <max> is the range of bytes to write for the create op.
TP[$WRITE_SZ_MIN_IDX]='1'
TP[$WRITE_SZ_MAX_IDX]="$BLOCKSIZE"
}

function runSliveTest {
  local tmpFile=$myTmp/$(date +%s)
  local outFile

  if [ $# -eq 1 ]
  then
    outFile=$ARTIFACTS_DIR/$1
  else
    outFile=$ARTIFACTS_DIR/slive.${TESTCASENAME}.txt
  fi

  COMMAND_EXIT_CODE=0
  TESTCASE_DESC="$TESTCASENAME"
  TESTCASE_ID="$TESTCASENAME"
  displayTestCaseMessage "$TESTCASE_DESC"

  # Remove testroot so it can be created again by SLiveTest.
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r -skipTrash $BASEDIR/slive/output

  echo $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR \
    org.apache.hadoop.fs.slive.SliveTest \
      -appendSize  ${TP[$APPEND_MIN_IDX]},${TP[$APPEND_MAX_IDX]} \
      -append      ${TP[$APPEND_PCT_IDX]},${TP[$APPEND_DIST_IDX]} \
      -baseDir     $BASEDIR \
      -blockSize   ${TP[$BLOCK_SIZE_MIN_IDX]},${TP[$BLOCK_SIZE_MAX_IDX]} \
      -create      ${TP[$CREATE_PCT_IDX]},${TP[$CREATE_DIST_IDX]} \
      -delete      ${TP[$DELETE_PCT_IDX]},${TP[$DELETE_DIST_IDX]} \
      -dirSize     ${TP[$DIR_SIZE_IDX]} \
      -duration    ${TP[$DURATION_IDX]} \
      -files       ${TP[$NUM_FILES_IDX]} \
      -ls          ${TP[$LS_PCT_IDX]},${TP[$LS_DIST_IDX]} \
      -maps        ${TP[$NUM_MAPS_IDX]} \
      -mkdir       ${TP[$MKDIR_PCT_IDX]},${TP[$MKDIR_DIST_IDX]} \
      -ops         ${TP[$MAX_OPS_PER_MAP_IDX]} \
      -packetSize  ${TP[$PACKET_SIZE_IDX]} \
      -readSize    ${TP[$READ_SIZE_MIN_IDX]},${TP[$READ_SIZE_MAX_IDX]} \
      -read        ${TP[$READ_PCT_IDX]},${TP[$READ_DIST_IDX]} \
      -reduces     ${TP[$NUM_REDUCES_IDX]} \
      -rename      ${TP[$RENAME_PCT_IDX]},${TP[$RENAME_DIST_IDX]} \
      -replication ${TP[$REPL_MIN_IDX]},${TP[$REPL_MAX_IDX]} \
      -resFile     $tmpFile \
      -seed        ${TP[$SEED_IDX]} \
      -sleep       ${TP[$SLEEP_MIN_IDX]},${TP[$SLEEP_MAX_IDX]} \
      -writeSize   ${TP[$WRITE_SZ_MIN_IDX]},${TP[$WRITE_SZ_MAX_IDX]} > $outFile

  echo >> $outFile

  $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR \
    org.apache.hadoop.fs.slive.SliveTest \
      -appendSize  ${TP[$APPEND_MIN_IDX]},${TP[$APPEND_MAX_IDX]} \
      -append      ${TP[$APPEND_PCT_IDX]},${TP[$APPEND_DIST_IDX]} \
      -baseDir     $BASEDIR \
      -blockSize   ${TP[$BLOCK_SIZE_MIN_IDX]},${TP[$BLOCK_SIZE_MAX_IDX]} \
      -create      ${TP[$CREATE_PCT_IDX]},${TP[$CREATE_DIST_IDX]} \
      -delete      ${TP[$DELETE_PCT_IDX]},${TP[$DELETE_DIST_IDX]} \
      -dirSize     ${TP[$DIR_SIZE_IDX]} \
      -duration    ${TP[$DURATION_IDX]} \
      -files       ${TP[$NUM_FILES_IDX]} \
      -ls          ${TP[$LS_PCT_IDX]},${TP[$LS_DIST_IDX]} \
      -maps        ${TP[$NUM_MAPS_IDX]} \
      -mkdir       ${TP[$MKDIR_PCT_IDX]},${TP[$MKDIR_DIST_IDX]} \
      -ops         ${TP[$MAX_OPS_PER_MAP_IDX]} \
      -packetSize  ${TP[$PACKET_SIZE_IDX]} \
      -readSize    ${TP[$READ_SIZE_MIN_IDX]},${TP[$READ_SIZE_MAX_IDX]} \
      -read        ${TP[$READ_PCT_IDX]},${TP[$READ_DIST_IDX]} \
      -reduces     ${TP[$NUM_REDUCES_IDX]} \
      -rename      ${TP[$RENAME_PCT_IDX]},${TP[$RENAME_DIST_IDX]} \
      -replication ${TP[$REPL_MIN_IDX]},${TP[$REPL_MAX_IDX]} \
      -resFile     $tmpFile \
      -seed        ${TP[$SEED_IDX]} \
      -sleep       ${TP[$SLEEP_MIN_IDX]},${TP[$SLEEP_MAX_IDX]} \
      -writeSize   ${TP[$WRITE_SZ_MIN_IDX]},${TP[$WRITE_SZ_MAX_IDX]}

  ret=$?

  cat $tmpFile >>$outFile
  rm $tmpFile

  return $ret
}

# Crate 1024 files
function test_slive_001 {
  TESTCASENAME=test_slive_001

  resetTestParms
  TP[$CREATE_PCT_IDX]=100
  runSliveTest slive.create.blocksize.64M.txt
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Create files with S-Live: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Read 1024 files
function test_slive_002 {
  TESTCASENAME=test_slive_002

  resetTestParms
  TP[$READ_PCT_IDX]='100'
  runSliveTest
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Read files with S-Live: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Append 1024 files
function test_slive_003 {
  TESTCASENAME=test_slive_003

  resetTestParms
  TP[$APPEND_PCT_IDX]='100'
  runSliveTest slive.append.range.1-blocksize.txt
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Append files with S-Live: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Mkdir
function test_slive_004 {
  TESTCASENAME=test_slive_004

  resetTestParms
  TP[$MKDIR_PCT_IDX]='100'
  runSliveTest
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Make dirs with S-Live: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Rename 1024 files
function test_slive_005 {
  TESTCASENAME=test_slive_005

  resetTestParms
  TP[$RENAME_PCT_IDX]='100'
  runSliveTest
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Rename files with S-Live: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Delete 1024 files
function test_slive_006 {
  TESTCASENAME=test_slive_006

  resetTestParms
  TP[$DELETE_PCT_IDX]='100'
  runSliveTest
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Delete files with S-Live: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Set append range to be min=max=BLOCKSIZE
function test_slive_007 {
  TESTCASENAME=test_slive_007

  resetTestParms
  TP[$APPEND_PCT_IDX]='100'
  TP[$APPEND_MIN_IDX]="$BLOCKSIZE"
  runSliveTest slive.append.range.blocksize-blocksize.txt
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Append files with S-Live using append size=min=mzx=BLOCKSIZE: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Expect more blocks to be written by the test where the range's min and max
# are both BLOCKSIZE than the range where min=1 and max=BLOCKSIZE.
function test_slive_008 {
  TESTCASENAME=test_slive_008

  BW1=$(grep 'Measurement "bytes_written" = ' \
                  $ARTIFACTS_DIR/slive.append.range.1-blocksize.txt | \
                       sed 's/^Measurement "bytes_written" = //')
  BW2=$(grep 'Measurement "bytes_written" = ' \
                  $ARTIFACTS_DIR/slive.append.range.blocksize-blocksize.txt | \
                       sed 's/^Measurement "bytes_written" = //')

  compareAppendSize=$(echo "$BW2 > $BW1" | bc)
  if [[ "$compareAppendSize" -eq 0 ]]
  then
    setFailCase "Setting min to 64M in append size range didn't result in appending more bytes."
  fi
  displayTestCaseResult
}

# Expect less operations per second when all appends must write full blocksize.
function test_slive_009 {
  TESTCASENAME=test_slive_009

  BW1=$(grep 'Rate for measurement "op_count" = ' \
        $ARTIFACTS_DIR/slive.append.range.1-blocksize.txt | \
        head -1 | \
        sed -e 's/Rate for measurement "op_count" = //' -e 's:operations/sec::')
  BW1=$(grep 'Rate for measurement "op_count" = ' \
        $ARTIFACTS_DIR/slive.append.range.blocksize-blocksize.txt | \
        head -1 | \
        sed -e 's/Rate for measurement "op_count" = //' -e 's:operations/sec::')

  compareAppendSize=$(echo "$BW2 < $BW1" | bc)
  if [[ "$compareAppendSize" -eq 0 ]]
  then
    setFailCase "Setting min=max=64M in append size range didn't result in less operations per second."
  fi

  displayTestCaseResult
}

# Append with append range being greater than the blockslize
function test_slive_010 {
  TESTCASENAME=test_slive_010

  resetTestParms
  TP[$APPEND_PCT_IDX]='100'
  TP[$APPEND_MIN_IDX]="$BLOCKSIZE"
  (( TP[$APPEND_MAX_IDX] = BLOCKSIZE * 10 ))
  runSliveTest slive.append.range.blocksize-Xblocksize.txt
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Append files with S-Live using append min=BLOCKSIZE, mzx=10*BLOCKSIZE: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Expect more blocks to be written by the test where the range's min and max are
# BLOCKSIZE or greater than where range is 1-BLOCKSIZE.
function test_slive_011 {
  TESTCASENAME=test_slive_011

  BW1=$(grep 'Measurement "bytes_written" = ' \
                  $ARTIFACTS_DIR/slive.append.range.1-blocksize.txt | \
                       sed 's/^Measurement "bytes_written" = //')
  BW2=$(grep 'Measurement "bytes_written" = ' \
                  $ARTIFACTS_DIR/slive.append.range.blocksize-Xblocksize.txt | \
                       sed 's/^Measurement "bytes_written" = //')

  compareAppendSize=$(echo "$BW2 > $BW1" | bc)
  if [[ "$compareAppendSize" -eq 0 ]]
  then
    setFailCase "Setting min to 64M and max to 640M append size range didn't result in appending more bytes."
  fi
  displayTestCaseResult
}

# Expect less operations per second when all appends must write more than full
# blocksize.
function test_slive_012 {
  TESTCASENAME=test_slive_012

  BW1=$(grep 'Rate for measurement "op_count" = ' \
        $ARTIFACTS_DIR/slive.append.range.1-blocksize.txt | \
        head -1 | \
        sed -e 's/Rate for measurement "op_count" = //' -e 's:operations/sec::')
  BW1=$(grep 'Rate for measurement "op_count" = ' \
        $ARTIFACTS_DIR/slive.append.range.blocksize-Xblocksize.txt | \
        head -1 | \
        sed -e 's/Rate for measurement "op_count" = //' -e 's:operations/sec::')

  compareAppendSize=$(echo "$BW2 < $BW1" | bc)
  if [[ "$compareAppendSize" -eq 0 ]]
  then
    setFailCase "Setting min to 64M and max to 640M in append size range didn't result in less operations per second."
  fi
  displayTestCaseResult
}

# Create with blocksize range being 512-64M.
function test_slive_013 {
  TESTCASENAME=test_slive_013

  # Since this is a create operation, clean out the data and the output dirs.
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r -skipTrash $BASEDIR/slive/output $BASEDIR/slive/data

  resetTestParms
  TP[$CREATE_PCT_IDX]=100
  TP[$BLOCK_SIZE_MIN_IDX]=512
  runSliveTest slive.create.blocksize.range.512-64M.txt
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Crate files with S-Live where blocksize range is 512-64M: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

# Expect less blocks to be written when the BLOCKSIZE range's min is 512 than
# when the BLOCKSIZE is min=max=64M.
function test_slive_014 {
  TESTCASENAME=test_slive_014

  BW1=$(grep 'Measurement "bytes_written" = ' \
                  $ARTIFACTS_DIR/slive.create.blocksize.64M.txt | \
                       sed 's/^Measurement "bytes_written" = //')
  BW2=$(grep 'Measurement "bytes_written" = ' \
                  $ARTIFACTS_DIR/slive.create.blocksize.range.512-64M.txt | \
                       sed 's/^Measurement "bytes_written" = //')

  compareCreateResults=$(echo "$BW2 < $BW1" | bc)
  if [[ "$compareCreateResults" -eq 0 ]]
  then
    setFailCase "Setting min to 512 and max to 640M blocksize range didn't result in writing more bytes."
  fi
  displayTestCaseResult
}

# Expect more operations per second when the BLOCKSIZE range's min is 512 than
# when the BLOCKSIZE is min=max=64M.
function test_slive_015 {
  TESTCASENAME=test_slive_015

  BW1=$(grep 'Rate for measurement "op_count" = ' \
        $ARTIFACTS_DIR/slive.create.blocksize.64M.txt | \
        head -1 | \
        sed -e 's/Rate for measurement "op_count" = //' -e 's:operations/sec::')
  BW1=$(grep 'Rate for measurement "op_count" = ' \
        $ARTIFACTS_DIR/slive.create.blocksize.range.512-64M.txt | \
        head -1 | \
        sed -e 's/Rate for measurement "op_count" = //' -e 's:operations/sec::')

  compareCreateResults=$(echo "$BW2 > $BW1" | bc)
  if [[ "$compareCreateResults" -eq 0 ]]
  then
    setFailCase "Setting min to 512 and max to 640M in blocksize range didn't result in more operations per second."
  fi
  displayTestCaseResult
}

# Perform all operations at once
function test_slive_016 {
  TESTCASENAME=test_slive_016

  # Clean up the data and the output dirs.
  $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r -skipTrash $BASEDIR/slive/output $BASEDIR/slive/data

  resetTestParms
  TP[$APPEND_PCT_IDX]=14
  TP[$CREATE_PCT_IDX]=16
  TP[$DELETE_PCT_IDX]=14
  TP[$LS_PCT_IDX]=14
  TP[$MKDIR_PCT_IDX]=14
  TP[$READ_PCT_IDX]=14
  TP[$RENAME_PCT_IDX]=14
  runSliveTest slive.all.maps.20.reds.5.txt
  ret=$?

  if [ "$ret" -ne 0 ]
  then
      setFailCase "Crate files with S-Live where blocksize range is 512-64M: Call to org.apache.hadoop.fs.slive.SliveTest failed with return code ${ret}."
  fi

  displayTestCaseResult
}

OWNER=ericp

# Clean out SLive HDFS dir.
$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r -skipTrash $BASEDIR/slive

executeTests

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
