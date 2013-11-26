#!/bin/sh

#load the library file
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

###################################################################
# Set up global variables
randdate=$(date +%s)

# SHARETMPDIR should be part of the environment. If it's not make it.
if [ X"$SHARETMPDIR" == X ]
then
  myTmp=/homes/$HADOOPQA_USER/tmp.$randdate
else
  myTmp=$SHARETMPDIR
fi

# Set up conf dir in /homes/hadoopqa rather than /tmp
CONF_DIR="$myTmp/SyntheticLoadGenerator_CONF_${randdate}"
# Select the first namenode. Only one is needed for this test
NN=$(getDefaultNameNode)
# The gateway also needs it's own copy of the configs.
gateway=$(hostname)
# Use secondary NN and JT as well as regular DNs for simulating datanodes.
dataNodes=$(echo $(getJobTracker_TMP)\;$(getSecondaryNameNodes)\;$(getDataNodes) | tr ';' '\n' | sort -u)
# The namenode's conf dir is referenced several times, so specify it separately.
nnConfDir=$CONF_DIR/$NN
# The gateway's conf dir is referenced several times.
gwConfDir=$CONF_DIR/$gateway

FS=hdfs://$NN:9000

# Create a unique root for simulted cluster. $HADOOP_QA_ROOT is usually /grid/0.
dataNodeClusterDir="$HADOOP_QA_ROOT/tmp/DataNodeCluster.${randdate}"
# Create a unique root for logs.
testVar="$HADOOP_QA_ROOT/tmp/var".${randdate}
# Create variables that will be used to override those in hadoop-env.sh
logDir=$testVar'/log/\$USER'
dnLogDir=$testVar'/log/\$HADOOP_SECURE_DN_USER'
pidDir=$testVar'/run/\$USER'
dnPidDir=$testVar'/run/\$HADOOP_SECURE_DN_USER'

# Data Generation variables
TESTROOT=/user/hadoopqa/SLG/data
structureFilesDir=$myTmp/structurefiles.${randdate}

# Output results
RESULTS=$myTmp/results.$randdate

# Make core-site.xml and hdfs-site.xml minimal with only the bare necessities.
# Include properties to tell MiniDFSCluster to use 0.0.0.0 instead of
# 127.0.0.1 in the case where the NN is not on the same host as the datanode.
CORE_SITE_XML_STRING="
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>$FS</value>
    <description></description>
  </property>

  <property>
    <name>hadoop.security.authentication</name>
    <value>simple</value>
    <description></description>
  </property>

  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:0</value>
    <description></description>
  </property>
</configuration>
"

HDFS_SITE_XML_STRING="<configuration>
  <property>
    <name>dfs.data.dir</name>
    <value>$dataNodeClusterDir</value>
    <description></description>
  </property>

  <property>
    <name>dfs.name.dir</name>
    <value>file://$dataNodeClusterDir/hdfs/name</value>
    <description></description>
  </property>

  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:0</value>
  </property>

  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:0</value>
  </property>

  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:0</value>
    <description></description>
  </property>

</configuration>
"

testIsUp=0

# Benchmarks against which the metircs will be measured. They were calculated
# after several runs in a slice of the sigma blue cluster. These benchmoarks
# represent an additional 75-90% of actual observed values in order to allow for
# variations in hardware and network speeds and interferences.

# These times are in milliseconds
avgOpenTimeBenchMark=5
avgDelTimeBenchMark=5
avgCreateTimeBenchMark=9
avgWriteTimeBenchMark=100
# The following metric is operations per second
avgOpsPerSecBenchMark=200

############################################
# tear down simulated cluster
#############################################
function tearDownCluster {
  # Tear the test down as well, just in case it didn't exit normally.
  if [ "$testIsUp" -eq 1 ]
  then
    tearDownTest
  fi

  # Clean up temporary config dirs.
  for node in $NN $dataNodes
  do
    ssh $node "rm -rf $CONF_DIR"
  done
  # On gateway, just remove $CONF_DIR
  rm -rf $CONF_DIR

  # start cluster
  getKerberosTicketForUser $HDFSQA_USER
  setKerberosTicketForUser $HDFSQA_USER

  startCluster
  verifyNNOutOfSafemode $(getDefaultFS) 300

  getKerberosTicketForUser $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER

  cat $RESULTS

  # if SHARETMPDIR does not exist in the environment, we created it, so we must
  # also delete it.
  if [ X"$SHARETMPDIR" == X ]
  then
    rm -r $myTmp
  fi
}

function tearDownTest {
  # Stop DataNodeCluster
  for node in $dataNodes
  do
    confDir=${CONF_DIR}/$node
    ssh $node "$HADOOP_COMMON_HOME/bin/hadoop-daemon.sh \
      --config $confDir stop org.apache.hadoop.hdfs.DataNodeCluster"
  done

  # Stop the NN
  ssh $NN "export HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME;\
           export HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME;\
           export HADOOP_CONF_DIR=$nnConfDir;\
           $HADOOP_COMMON_HOME/bin/hadoop-daemon.sh --config $nnConfDir stop namenode"

  # Clean up temporary datanode root and log root on NN and DNs
  for node in $NN $dataNodes
  do
    ssh $node "rm -rf $dataNodeClusterDir $testVar"
  done
  # On gateway, clean up datanode root, log root, and file generator tmp files.
  rm -rf $dataNodeClusterDir $testVar $structureFilesDir

  testIsUp=0
}

################################################################################
# SETUP only happens once.
#  Copies configs for each node and modifies config files as needed.
################################################################################

function setUpSimCluster {
  NUM_TESTCASE=0
  NUM_TESTCASE_PASSED=0
  SCRIPT_EXIT_CODE=0;
  export  COMMAND_EXIT_CODE=0
  export TESTCASE_DESC="None"
  export REASONS=""

  # Stop cluster
  getKerberosTicketForUser $HDFSQA_USER
  setKerberosTicketForUser $HDFSQA_USER

  stopCluster

  getKerberosTicketForUser $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER

  # SHARETMPDIR should be part of the environment. If it's not, temprory dir
  # must be created.
  if [ X"$SHARETMPDIR" == X ]
  then
    mkdir -p $myTmp
  fi

  # Copy gateway, NN, and DN configs to tmp location
  # Even though this is running on the gatewy, I'm including the gateway's host
  # name so copyHadoopConfig() can be used.
  for node in $gateway $NN $dataNodes
  do
    if [ "$node" == $NN ]; then heapSize=14000; else heapSize=7000; fi
    confDir=${CONF_DIR}/$node
    copyHadoopConfig $node $confDir
    # Use only the minimal settings for the simulated name and datanodes.
    # Set environment variables in hadoop-env.sh to specify log dirs and to
    # specify JVM memory constraints.
    ssh $node "rm -rf $dataNodeClusterDir $testVar;\
      echo \"$CORE_SITE_XML_STRING\" >$confDir/core-site.xml;\
      echo \"$HDFS_SITE_XML_STRING\" >$confDir/hdfs-site.xml;\
      cp $HADOOP_COMMON_HOME/conf/log4j.properties $confDir;\
      sed -i \"s|export HADOOP_LOG_DIR=.*$|export HADOOP_LOG_DIR=$logDir|\" $confDir/hadoop-env.sh;\
      sed -i \"s|export HADOOP_SECURE_DN_LOG_DIR=.*$|export HADOOP_SECURE_DN_LOG_DIR=$dnLogDir|\" $confDir/hadoop-env.sh;\
      sed -i \"s|export HADOOP_PID_DIR=.*$|export HADOOP_PID_DIR=$pidDir|\"  $confDir/hadoop-env.sh;\
      sed -i \"s|export HADOOP_SECURE_DN_PID_DIR=.*$|export HADOOP_SECURE_DN_PID_DIR=$dnPidDir|\" $confDir/hadoop-env.sh;\
      sed -i \"s|HADOOP_CLIENT_OPTS=\\(.*\\)-Xmx[1-9][0-9]*[MmGg] |HADOOP_CLIENT_OPTS=\\1 -Xmx7000m -XX:NewRatio=8 |\" $confDir/hadoop-env.sh;\
      sed -i \"s|HADOOP_DATANODE_OPTS=\\(.*\\)-Xmx[1-9][0-9]*[MmGg] |HADOOP_DATANODE_OPTS=\\1 -Xmx7000m -XX:NewRatio=8 |\" $confDir/hadoop-env.sh;\
      echo export JAVA_HOME=$HADOOP_QA_ROOT/gs/java/jdk64/current >> $confDir/hadoop-env.sh;\
      echo export HADOOP_HEAPSIZE=$heapSize >> $confDir/hadoop-env.sh;"
  done
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx512m  -XX:NewRatio=8"

  echo 'Simulated datanodes per real node,Number of Simulated files,Average file size (in bytes) of simulted files,Max Depth in the directory structure,Max Width in the directory structure,Read probability,Write probability,Max delay (in ms) between ops,Number of threads requesting operations,Test requested duration,Test actual duration,Time delay (in ms) before starting threads,Average open execution time,Average deletion execution time,Average create execution time,Average write_close execution time,Average operations per second,' >$RESULTS
}

################################################################################
# simStartCluster:  Formats and starts NN, generates data, and starts datanodes
#                   based on the specified parameters
#    Parameters:
#      $1: number of simulated nodes per real node
#      $2: number of simulated files
#      $3: average file size
################################################################################
function setUpSimTest {
  numSimulatedNodesPerRealNode=$1
  numSimulatedFiles=$2
  avgFileSize=$3
  maxDepth=$4
  maxWidth=$5

  # Format and start the NN
  ssh $NN "rm -rf $dataNodeClusterDir;\
           export HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME;\
           export HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME;\
           export HADOOP_CONF_DIR=$nnConfDir;\
           $HADOOP_HDFS_CMD --config $nnConfDir namenode -format -clusterid $randdate;\
           $HADOOP_COMMON_HOME/bin/hadoop-daemon.sh --config $nnConfDir start namenode"

  if [ $? -ne 0 ]
  then
    echo "$0: Error formatting/starting NameNode in simulated environment"
    tearDonwCluster
    exit 1
  fi

  # Wait for NN to come out of safemode
  OLD_HADOOP_CONF_DIR=$HADOOP_CONF_DIR
  export HADOOP_CONF_DIR=$nnConfDir
  verifyNNOutOfSafemode $FS 30
  status="$?"
  # if the safemode is still on then force it out. It shouldn't be, since we
  # formatted it above.
  if [ "$status" -ne "0" ]
  then
    echo "WARN: Forcing NameNode ($NN) out of safe mode."
    $HADOOP_HDFS_CMD --config $nnConfDir dfsadmin -fs $FS -safemode leave
  fi
  export HADOOP_CONF_DIR=$OLD_HADOOP_CONF_DIR

  # Start 20 symulated DNs on each DN
  for node in $dataNodes
  do
    confDir=${CONF_DIR}/$node
    ssh $node "$HADOOP_COMMON_HOME/bin/hadoop-daemon.sh \
      --config $confDir \
      start org.apache.hadoop.hdfs.DataNodeCluster \
      -simulated -n $numSimulatedNodesPerRealNode \
      -d $dataNodeClusterDir \
      -checkDataNodeAddrConfig"
  done

  # TODO Implement code to wait until all simulated datanodes have started.
  # (( lastNode = $numSimulatedNodesPerRealNode - 1 ))
  # notDone=1
  # while [ $notDone ]
  # do
  #   notDone=0
  #   for node in $dataNodes
  #   do
  #     ssh $node "tail -1 $logDir/hadoop-$USER-org.apache.hadoop.hdfs.DataNodeCluster-$node.out | grep \"Starting DataNode $lastNode\" >/dev/null 2>&1"
  #     if [ "$?" -ne 0 ]
  #     then
  #       (( notDone++ ))
  #     fi
  #   done
  # done

  # Wait for all simulated datanodes to start up before starting DataGenerator.
  # Time out if they don't all start.
  (( sleeptime = numSimulatedNodesPerRealNode / 2 ))
  sleep $sleeptime

  mkdir $structureFilesDir
  # populate DNs
  export TESTROOT=/user/hadoopqa/SLG/data
  $HADOOP_COMMON_HOME/bin/hadoop --config $gwConfDir \
    org.apache.hadoop.fs.loadGenerator.StructureGenerator \
    -maxDepth $maxDepth -minWidth 1 -maxWidth $maxWidth \
    -numOfFiles $numSimulatedFiles -avgFileSize $avgFileSize \
    -outDir $structureFilesDir -seed $randdate
  $HADOOP_COMMON_HOME/bin/hadoop --config $gwConfDir \
    org.apache.hadoop.fs.loadGenerator.DataGenerator \
    -inDir $structureFilesDir -root $TESTROOT

  testIsUp=1
}

function runSimTest {
  local readProbability=$1; shift
  local writeProbability=$1; shift
  local maxDelayBetweenOps=$1; shift
  local numThreads=$1; shift
  local testDuration=$1; shift
  local threadTimeDelay=$1;

  $HADOOP_COMMON_HOME/bin/hadoop \
    --config $gwConfDir \
    org.apache.hadoop.fs.loadGenerator.LoadGenerator \
        -readProbability "$readProbability" \
        -writeProbability "$writeProbability" -root $TESTROOT \
        -maxDelayBetweenOps "$maxDelayBetweenOps" \
        -numOfThreads "$numThreads" -elapsedTime "$testDuration" \
        -startTime "$threadTimeDelay" -seed $randdate

  return $?
}

function SyntheticLoadGenerator {
  (( NUM_TESTCASE++ ))
  COMMAND_EXIT_CODE=0
  TESTCASE_DESC="SyntheticLoadGenerator_$NUM_TESTCASE"
  TESTCASE_ID="SyntheticLoadGenerator_$NUM_TESTCASE"
  displayTestCaseMessage "$TESTCASE_DESC"

  # Simulated datanodes per real node
  local simDataNodesPerNode=$1; shift
  # Number of Simulated files
  local numSimulatedFiles=$1; shift
  # Average file size (in bytes) of simulted files
  local avgFileSize=$1; shift
  # Max Depth in the directory structure
  local maxDirDepth=$1; shift 
  # Max Width in the directory structure
  local maxDirWidth=$1; shift
  # Read probability
  local readProbability=$1; shift
  # Write probability
  local writeProbability=$1; shift
  # Max delay (in ms) between ops
  local maxDelay=$1; shift
  # Number of threads requesting operations
  local numThreads=$1; shift
  # Time duration: Amount of time (in seconds) to run
  local testDuration=$1; shift
  # Time delay (in ms) before starting threads
  local timeDelay=10

  local timeItReallyTook
  local startTime=$(date +%s)
  local endTime

  echo "--------------------------------------------------------------------"
  echo -e "Simulated datanodes per real node:\t\t\t$simDataNodesPerNode"
  echo -e "Number of Simulated files:\t\t\t\t$numSimulatedFiles"
  echo -e "Average file size (in bytes) of simulted files:\t\t$avgFileSize"
  echo -e "Max Depth in the directory structure:\t\t\t$maxDirDepth"
  echo -e "Max Width in the directory structure:\t\t\t$maxDirWidth"
  echo -e "Read probability:\t\t\t\t\t$readProbability"
  echo -e "Write probability:\t\t\t\t\t$writeProbability"
  echo -e "Max delay (in ms) between ops:\t\t\t\t$maxDelay"
  echo -e "Number of threads requesting operations:\t\t$numThreads"
  echo -e "Time duration: Amount of time (in seconds) to run:\t$testDuration"
  echo -e "Time delay (in ms) before starting threads:\t\t$timeDelay"
  echo "--------------------------------------------------------------------"

  setUpSimTest "$simDataNodesPerNode" "$numSimulatedFiles" "$avgFileSize" "$maxDirDepth" "$maxDirWidth"

  runSimTest "$readProbability" "$writeProbability" "$maxDelay" "$numThreads" "$testDuration" "$timeDelay" > $myTmp/$randdate

  if [ "$?" -ne 0 ]
  then
    setFailCase "Running of namenode stress tools failed."
    displayTestCaseResult $NUM_TESTCASE
  else
    endTime=$(date +%s)
    (( timeItReallyTook = endTime - startTime ))

    # Catpure the results for export to metrics table and for analysis below.
    avgOpenTime=$(grep 'Average open execution time:' $myTmp/$randdate | \
                  sed -e 's/^.*: //' -e 's/ms$//')
    avgDelTime=$(grep 'Average deletion execution time:' $myTmp/$randdate | \
                  sed -e 's/^.*: //' -e 's/ms$//')
    avgCreateTime=$(grep 'Average create execution time:' $myTmp/$randdate | \
                  sed -e 's/^.*: //' -e 's/ms$//')
    avgWriteTime=$(grep 'Average write_close execution time:' $myTmp/$randdate|\
                  sed -e 's/^.*: //' -e 's/ms$//')
    avgOpsPerSec=$(grep 'Average operations per second:' $myTmp/$randdate | \
                  sed -e 's/^.*: //' -e 's:ops/s::')

    echo $simDataNodesPerNode,$numSimulatedFiles,$avgFileSize,$maxDirDepth,$maxDirWidth,$readProbability,$writeProbability,$maxDelay,$numThreads,$testDuration,$timeItReallyTook,$timeDelay,$avgOpenTime,$avgDelTime,$avgCreateTime,$avgWriteTime,$avgOpsPerSec >>$RESULTS

    # Truncate to milliseconds so comparisons can be made with benchmarks.
    avgOpenTime=$(echo $avgOpenTime | sed 's/\..*$//')
    avgDelTime=$(echo $avgDelTime | sed 's/\..*$//')
    avgCreateTime=$(echo $avgCreateTime | sed 's/\..*$//')
    avgWriteTime=$(echo $avgWriteTime | sed 's/\..*$//')
    avgOpsPerSec=$(echo $avgOpsPerSec | sed 's/\..*$//')

    # $avgOpenTime is measureing read times, so it won't exist in the
    # LoadGenerator output if read probability is 0%
    if [[ -z "$avgOpenTime" ]]
    then
      if [[ "$readProbability" != "0.0" && "$readProbability" != "0" ]]
      then
        setFailCase "Average open execution time metric did not exist in the output of LoadGenerator even though read probability is non-zero."
        displayTestCaseResult $NUM_TESTCASE
        cat $myTmp/$randdate
      fi
    fi

    # $avgDelTime, $avgCreateTime, and $avgWriteTime are measureing write
    # operation times, so they won't exist in the LoadGenerator output if write
    # probability is 0%
    if [[ -z "$avgDelTime" || -z "$avgCreateTime" || -z "$avgWriteTime" ]]
    then
      if [[ "$writeProbability" != "0.0" && "$writeProbability" != "0" ]]
      then
        setFailCase "One or more of the write metrics did not exist in the output of LoadGenerator even though write probability is non-zero."
        displayTestCaseResult $NUM_TESTCASE
        cat $myTmp/$randdate
      fi
    fi

    # Adjust benchmark based on the avg size of the files. The larger the files,
    # the longer things take.
    # Fudge Factor
    FF=1

    if (( avgFileSize >= 10000000 ))
    then
      FF=40
    else
      if (( avgFileSize >= 1000000 ))
      then
        FF=4
      fi
    fi

    # Test that the average times and number of operations are not beyond 
    # the benchmark.

    if [[ -n "$avgOpenTime" ]] && (( avgOpenTime > (avgOpenTimeBenchMark * FF) ))
    then
      # Average open time is more than expected
      setFailCase "Average open time ( $avgOpenTime ms ) is greater than the benchmark: $avgOpenTimeBenchMark ms."
      displayTestCaseResult $NUM_TESTCASE
      cat $myTmp/$randdate
    fi

    if [[ -n "$avgDelTime" ]] && (( avgDelTime > (avgDelTimeBenchMark * FF) ))
    then
      # Average deletion time is more than expected
      setFailCase "Average deletion time ( $avgDelTime ms ) is greater than the benchmark: $avgDelTimeBenchMark ms."
      displayTestCaseResult $NUM_TESTCASE
      cat $myTmp/$randdate
    fi

    if [[ -n "$avgCreateTime" ]] && (( avgCreateTime > (avgCreateTimeBenchMark * FF) ))
    then
      # Average create time is more than expected
      setFailCase "Average creation time ( $avgCreateTime ms ) is greater than the benchmark: $avgCreateTimeBenchMark ms."
      displayTestCaseResult $NUM_TESTCASE
      cat $myTmp/$randdate
    fi

    if [[ -n "$avgWriteTime" ]] && (( avgWriteTime > (avgWriteTimeBenchMark * FF) ))
    then
      # Average write close time is more than expected
      setFailCase "Average write close time ( $avgWriteTime ms ) is greater than the benchmark: $avgWriteTimeBenchMark ms."
      displayTestCaseResult $NUM_TESTCASE
      cat $myTmp/$randdate
    fi

    if [[ -n "$avgOpsPerSec" ]] && (( (avgOpsPerSec * FF) < avgOpsPerSecBenchMark ))
    then
      # Average operations per second is less than expected benchmark
      setFailCase "Average operations per second ( $avgOpsPerSec ops/sec ) is less than the benchmark: $avgOpsPerSecBenchMark ops/sec."
      displayTestCaseResult $NUM_TESTCASE
      cat $myTmp/$randdate
    fi

  fi

  rm $myTmp/$randdate
  tearDownTest
  displayTestCaseResult $NUM_TESTCASE
}

OWNER=ericp

# KNOBS
  #------|-------|------|-------|-------|------|-------|-------|-------|-------|
  # DNs  | # Sim | Avg  | Max   | Max   | Read | Write | delay | # of  | test  |
  # per  | files | file | dir   | dir   | prob | prob  | (ms)  | thrds | dur-  |
  # node |       | sz   | depth | width |      |       |       |       | ation |
  #------|-------|------|-------|-------|------|-------|-------|-------|-------|

# Test metrics with varying # of simulated DNs per node
TEST1='70     200      1      10      10    0.5     0.5       2       6     300'
TEST2='80     200      1      10      10    0.5     0.5       2       6     300'
TEST3='90     200      1      10      10    0.5     0.5       2       6     300'
TEST4='100    200      1      10      10    0.5     0.5       2       6     300'
TEST5='110    200      1      10      10    0.5     0.5       2       6     300'
TEST6='120    200      1      10      10    0.5     0.5       2       6     300'
TEST7='130    200      1      10      10    0.5     0.5       2       6     300'
TEST8='140    200      1      10      10    0.5     0.5       2       6     300'
TEST9='150    200      1      10      10    0.5     0.5       2       6     300'
TEST10='160   200      1      10      10    0.5     0.5       2       6     300'
TEST11='170   200      1      10      10    0.5     0.5       2       6     300'
# Incrementally alter the # of simulated files.
# NOTE: The number of files did not seem to affect operation times.
TEST12='170         10 1      10      10    0.5     0.5       2       6     300'
TEST13='170        100 1      10      10    0.5     0.5       2       6     300'
TEST14='170       1000 1      10      10    0.5     0.5       2       6     300'
TEST15='170      10000 1      10      10    0.5     0.5       2       6     300'
TEST16='170     100000 1      10      10    0.5     0.5       2       6     300'
# Incrementally alter the size of each simulated file.
TEST17='170   500          10 10      10    0.5     0.5       2       6     300'
TEST18='170   500         100 10      10    0.5     0.5       2       6     300'
TEST19='170   500        1000 10      10    0.5     0.5       2       6     300'
TEST20='170   500       10000 10      10    0.5     0.5       2       6     300'
TEST21='170   500      100000 10      10    0.5     0.5       2       6     300'
TEST22='170   500     1000000 10      10    0.5     0.5       2       6     300'
TEST23='170   500    10000000 10      10    0.5     0.5       2       6     300'
TEST24='170   500   100000000 10      10    0.5     0.5       2       6     300'
# Incrementally alter the read and write probabilities.
TEST25='170   500  1000000    10      10    0.0     1.0       2       6     300'
TEST26='170   500  1000000    10      10    0.2     0.8       2       6     300'
TEST27='170   500  1000000    10      10    0.4     0.6       2       6     300'
TEST28='170   500  1000000    10      10    0.6     0.4       2       6     300'
TEST29='170   500  1000000    10      10    0.8     0.2       2       6     300'
TEST30='170   500  1000000    10      10    1.0     0.0       2       6     300'
# Incrementally alter the number of client threads
TEST31='170   500  1000000    10      10    0.3     0.7       2       1     300'
TEST32='170   500  1000000    10      10    0.3     0.7       2       3     300'
TEST33='170   500  1000000    10      10    0.3     0.7       2       5     300'
TEST34='170   500  1000000    10      10    0.3     0.7       2       7     300'
TEST35='170   500  1000000    10      10    0.3     0.7       2       9     300'
TEST36='170   500  1000000    10      10    0.3     0.7       2      11     300'
TEST37='170   500  1000000    10      10    0.3     0.7       2      13     300'
TEST38='170   500  1000000    10      10    0.3     0.7       2      15     300'
TEST39='170   500  1000000    10      10    0.3     0.7       2      17     300'
TEST40='170   500  1000000    10      10    0.3     0.7       2      19     300'
# Incrementally alter the time between operations
TEST41='170   500  1000000    10      10    0.3     0.7       0       6     300'
TEST42='170   500  1000000    10      10    0.3     0.7       2       6     300'
TEST43='170   500  1000000    10      10    0.3     0.7       4       6     300'
TEST44='170   500  1000000    10      10    0.3     0.7       6       6     300'
TEST45='170   500  1000000    10      10    0.3     0.7       8       6     300'
TEST46='170   500  1000000    10      10    0.3     0.7      10       6     300'


setUpSimCluster

numTests=46
num=1
while [ $num -le $numTests ]
do
  eval SyntheticLoadGenerator \$TEST$num
  (( num++ ))
done

tearDownCluster

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
