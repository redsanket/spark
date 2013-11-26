#!/bin/sh

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

JOB_COMPRESSION_CODEC="org.apache.hadoop.io.compress.GzipCodec org.apache.hadoop.io.compress.DefaultCodec org.apache.hadoop.io.compress.BZip2Codec org.apache.hadoop.io.compress.LzoCodec "
MAP_COMPRESSION_CODEC="org.apache.hadoop.io.compress.GzipCodec org.apache.hadoop.io.compress.DefaultCodec org.apache.hadoop.io.compress.BZip2Codec org.apache.hadoop.io.compress.LzoCodec "
#COMPRESSION_LEVEL="NO_COMPRESSION BEST_SPEED BEST_COMPRESSION DEFAULT_COMPRESSION"
#COMPRESSION_STRATEGY="FILTERED HUFFMAN_ONLY RLE FIXED DEFAULT_STRATEGY"
COMPRESSION_TYPE="NONE BLOCK RECORD"
OUTPUT_FORMAT="TextFormat SequenceFormat"
OUTPUT_DATA_TYPE="byte text"
i=1

#YARN_OPTIONS="-Dmapreduce.job.acl-view-job=* -Dmapred.child.java.opts='-Djava.library.path=${HADOOP_PREFIX}/lib/native/Linux-i386-32'"
#YARN_OPTIONS="-Dmapreduce.job.acl-view-job=* -Dmapred.child.java.opts='-Djava.library.path=${HADOOP_PREFIX}/lib/native/Linux-amd64-64' -Dmapreduce.map.memory.mb=2048 -Dmapreduce.reduce.memory.mb=4096"
YARN_OPTIONS="-Dmapreduce.job.acl-view-job=* -Dmapred.child.java.opts='-Djava.library.path=${HADOOP_PREFIX}/lib/native/Linux-i386-32' -Dmapreduce.map.memory.mb=2048 -Dmapreduce.reduce.memory.mb=4096"
#YARN_OPTIONS="-Dmapreduce.job.acl-view-job=* -Dmapred.child.java.opts='-XX:+UseCompressedOops -Djava.library.path=${HADOOP_PREFIX}/lib/native/Linux-amd64-64'"


HADOOP="$HADOOP_COMMON_CMD --config ${HADOOP_CONF_DIR}"
HADOOP_EXAMPLES="$HADOOP jar $HADOOP_MAPRED_EXAMPLES_JAR"
COMPRESS="-Dmapreduce.map.output.compress=true -Dmapreduce.output.fileoutputformat.compress=true"
NO_COMPRESS="-Dmapreduce.map.output.compress=false -Dmapreduce.output.fileoutputformat.compress=false"
TOTAL_BYTES=1024

COMMAND_EXIT_CODE=0	    

function hadoop_exec_fail_fast {
  local CMD=$1
  echo $CMD
  $CMD
  COMMAND_EXIT_CODE=$?
  if [ $COMMAND_EXIT_CODE -ne 0 ]; then
    REASONS=$2
    echo $REASONS
    displayTestCaseResult
    exit 1
  fi
}

function hadoop_exec {
  local CMD=$1
  echo $CMD
  $CMD
  (( COMMAND_EXIT_CODE = $COMMAND_EXIT_CODE + $? ))
}

function generateRandomByteData {
  local cmd="$HADOOP_EXAMPLES randomwriter ${YARN_OPTIONS} ${NO_COMPRESS} -Dmapreduce.randomwriter.totalbytes=${TOTAL_BYTES} Compression/byteinput"
  hadoop_exec_fail_fast "$cmd" "Generation Random Byte Data Failed"
}

function generateRandomTextData {
  local cmd="$HADOOP_EXAMPLES randomtextwriter ${YARN_OPTIONS} ${NO_COMPRESS} -Dmapreduce.randomtextwriter.totalbytes=${TOTAL_BYTES} Compression/textinput"
  hadoop_exec_fail_fast "$cmd" "Generation Random Text Data Failed"
}

function testParallel {

    echo "Starting tests in parallel" 

    generateRandomByteData >> $ARTIFACTS_FILE 2>&1
    generateRandomTextData >> $ARTIFACTS_FILE 2>&1

    for jobcodec in $JOB_COMPRESSION_CODEC ; do
	(testCompression $jobcodec) & 
	sleep 15
    done

    echo "Waiting for parallel test completion" 
    wait 
} 


function testCompression {
    local jobcodec=$1

    for mapcodec in $MAP_COMPRESSION_CODEC ; do
      for type in $COMPRESSION_TYPE ; do
        for data_type in $OUTPUT_DATA_TYPE ; do

          COMMAND_EXIT_CODE=0	    
          TESTCASE_DESC="$jobcodec JOB COMPRESSION, $mapcodec MAP COMPRESSION WITH $type TYPE and WITH $data_type input/output"
          TESTCASEINDEX="${jobcodec}_${i}"
          displayHeader "RUNNING TestCompression_${jobcodec}_${i} - ${TESTCASE_DESC}"
          DATE=`date +%s`
          SORT_OUTPUT="Compression/${jobcodec}/${data_type}output-$DATE"

          SORT_CMD="$HADOOP_EXAMPLES sort ${YARN_OPTIONS} ${COMPRESS}"
          SORT_CMD="$SORT_CMD -Dmapreduce.output.fileoutputformat.compress.codec=$jobcodec -Dmapreduce.map.output.compress.codec=$mapcodec -Dmapreduce.output.fileoutputformat.compress.type=$type"
          if [ "$data_type" == "text" ]; then
          ## output Text data
            SORT_CMD="$SORT_CMD -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text"
          fi
          SORT_CMD="$SORT_CMD Compression/${data_type}input $SORT_OUTPUT"
          hadoop_exec "$SORT_CMD"

          if [ $COMMAND_EXIT_CODE -ne 0 ]; then
            ## Fail the test
            REASONS="Unable to run SORT job for jobCodec $jobcodec, mapCodec $mapcodec, with type $type.  Job completed with exit code $COMMAND_EXIT_CODE"
            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
            displayTestCaseResult $TESTCASEINDEX
          else
            ## Validate sort went well
            hadoop_exec "$HADOOP jar $HADOOP_MAPRED_TEST_JAR testmapredsort ${YARN_OPTIONS} -sortInput Compression/${data_type}input -sortOutput $SORT_OUTPUT"
            if [ $COMMAND_EXIT_CODE -ne 0 ]; then
              REASONS="Unable to run SORT VALIDATION job for jobCodec $jobcodec, mapCodec $mapcodec, with type $type.  Job completed with exit code $COMMAND_EXIT_CODE"
              (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
              displayTestCaseResult $TESTCASEINDEX
            else
              ## verify that the data is really compressed
              echo "Checking output partitions : "
              $HADOOP fs -ls $SORT_OUTPUT

              for part in $($HADOOP fs -ls $SORT_OUTPUT | grep -v "items" | awk '{print $NF}'); 
              do
                ##TODO This logic feels iffy to me.  Need to clean up
                DATA_CHECK=0
                echo "Checking part file $part"
                echo "$HADOOP jar lib/test-utils-*.jar com.yahoo.hadoop.SequenceFileUtil getCompressionType $part"
                PART_TYPE=$($HADOOP jar lib/test-utils-*.jar com.yahoo.hadoop.SequenceFileUtil getCompressionType $part 2>&1 | egrep -v "INFO|WARN|ERROR")
                if [ "$type" != "$PART_TYPE" ]; then
                  REASONS="Compression Type does not match expected ($type) but got ($PART_TYPE);"
                  (( DATA_CHECK = $DATA_CHECK + 1 ))
                  (( COMMAND_EXIT_CODE = $COMMAND_EXIT_CODE + $DATA_CHECK ))
                fi

               # ## Maybe if file is part-m use mapcoded, if r then use jobcodec?
                echo "$HADOOP jar lib/test-utils-*.jar com.yahoo.hadoop.SequenceFileUtil getCompressionCodec $part"
                PART_CODEC=$($HADOOP jar lib/test-utils-*.jar com.yahoo.hadoop.SequenceFileUtil getCompressionCodec $part 2>&1 | egrep -v "INFO|WARN|ERROR")
                if [ "$PART_TYPE" != "NONE" ]; then
                  ## If type is NONE then there should be no codec, so make sure you only check if codec is needed
                  if [ "$jobcodec" != "$PART_CODEC" ]; then
                    REASONS="${REASONS} Compression Codec does not match expected ($jobcodec) but got ($PART_CODEC); "
                    (( DATA_CHECK = $DATA_CHECK + 1 ))
                    (( COMMAND_EXIT_CODE = $COMMAND_EXIT_CODE + $DATA_CHECK ))
                  fi
                else
                  ## If compression is off, codec is null.  So make sure output is ""
                  if [ "" != "$PART_CODEC" ]; then
                    REASONS="${REASONS} Compression Codec does not match expected ('') but got ($PART_CODEC); "
                    (( DATA_CHECK = $DATA_CHECK + 1 ))
                    (( COMMAND_EXIT_CODE = $COMMAND_EXIT_CODE + $DATA_CHECK ))
                  fi
                fi

                if [ "$DATA_CHECK" -ne 0 ]; then
                  break
                fi
              done

              (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
              displayTestCaseResult $TESTCASEINDEX
            fi
          fi
          (( i = $i + 1 ))
        done
      done
    done
}

deleteHdfsDir Compression 2>&1

# Execute Tests
testParallel

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
