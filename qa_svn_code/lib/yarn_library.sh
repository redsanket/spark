#!/bin/sh

## Import Libraries
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh
. $WORKSPACE/lib/library.sh

# WORKAROUND for 5366194
acl_view_arg='-Dmapreduce.job.acl-view-job=*'

function readAFieldInFileFromRemoteHost {
    # set -x
    ssh $1 cat $2 | grep "<name>$3</name>" -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1
    # set +x
}

###############################################################
# Function to setup new HADOOP_CONF_DIR in all nodes in a cluster
# Params: $1: nodes (JT/NN/DN); $2: new config directory; 
###############################################################
function setupConfig {
    local nodes=$1
    local customConfDir=$2
    NN=$(getNameNode)
    getJobTracker
    ACTIVEDATANODES=$(getActiveDataNodes)

    echo "Setup new configs at $customConfDir for $nodes"
    for n in $nodes; do
        mkdir $customConfDir
        ssh $n "cp -r $HADOOP_CONF_DIR/* $customConfDir"
    done

    echo "Stopping Cluster ..."
    stopCluster

    echo "Starting Namenode ..."
    resetNode $NN namenode start $customConfDir
    echo "Starting Datanodes ..."
    for n in $ACTIVEDATANODES; do 
        resetNode $n datanode start $customConfDir
    done

    echo "Starting JobTracker ..."
    resetNode $JT jobtracker start $customConfDir
    echo "Starting TaskTrackers ..."
    for n in $ACTIVEDATANODES; do 
        resetNode $n tasktracker start $customConfDir
    done
    
    takeNNOutOfSafemode
}

function isJobRunning {
    ${HADOOP_MAPRED_CMD} job -list | grep $1
}

function listJobs {
    ${HADOOP_MAPRED_CMD} job -list | grep job_ | sort
}

################################################################
# Function to get all JobIDs for currently running job
################################################################
function getJobIds {
    for i in 0 1 2 3 4
      do
      joblist=`${HADOOP_MAPRED_CMD} job -list`
      JOBIDS=( `echo "$joblist" | grep job_ | awk '{print $1}' | sort` )
      if (( ${#JOBIDS[*]} < $1 )) ; then
          echo "Waiting for job Ids ..."
          sleep 10
      else
          break
      fi
    done
}

################################################################
# Function to kill all running jobs
################################################################
function killAllRunningJobs {
    JOBIDS=( `${HADOOP_MAPRED_CMD} job -libjars $YARN_CLIENT_JAR -list | grep job_ | awk '{print $1}'` )
    echo "Killing running jobs: ${JOBIDS[*]}"
    num=${#JOBIDS[*]}
    i=0
    while [ "$i" -lt "$num" ]
      do
      ${HADOOP_MAPRED_CMD} --config ${HADOOP_CONF_DIR} job -kill "${JOBIDS[$i]}"
      sleep 5
      (( i = $i + 1 ))
    done
}

##############################################################
# Function to get number of nodes on a cluster using RM WebUI
##############################################################
function getNodesOnCluster {
    # getJobTracker
    # # wget --quiet "http://$JOBTRACKER:8088/yarn/nodes" -O ${ARTIFACTS_DIR}/clusterNodes
    # wget --quiet "http://$JOBTRACKER:8088/cluster/nodes" -O ${ARTIFACTS_DIR}/clusterNodes
    # cat "$ARTIFACTS_DIR/clusterNodes"
    # NODES_ON_CLUSTER=`cat "$ARTIFACTS_DIR/clusterNodes" | grep "10240" | wc -l`
    NODES_ON_CLUSTER=`CLUSTER=${CLUSTER} $WORKSPACE/utils/get_cluster status datanode|grep "=> 1"|wc -l`;
}

##############################################################
# Function to get all attemptIDs for a jobID
# Params: $1: jobID, $2: MAP/REDUCE, $3: state (running/completed)
##############################################################
function getAttemptIdsForJobId {
    ATTEMPTIDS=( `${HADOOP_MAPRED_CMD} job -list-attempt-ids $1 $2 $3 | grep "attempt_"` )
}

##############################################################
# Function to get number of tasks running
# Params: $1: jobID, $2: MAP/REDUCE, $3: state (running/completed)
##############################################################
function getNumTasksForJobId {
    for i in 0 1 2 3 4 5 6 7 8 9
      do
      NUMTASKS=( `${HADOOP_MAPRED_CMD} job -list-attempt-ids $1 $2 $3 | wc -l` )
      if (( ${#NUMTASKS[*]} -eq 0 )); then
          echo "Waiting for NUMTASKS ..."
          sleep 2
      else
          break
      fi
    done
}

function getOnwerOfJob {
    ${HADOOP_MAPRED_CMD} job -list | grep $1 | awk '{print $4}'
}

function getMasterUI {
    ${HADOOP_MAPRED_CMD} job -list | grep $1 | awk '{print $12}'
}

function getAMHost {
    ${HADOOP_MAPRED_CMD} job -list | grep $1 | awk '{print $12}' | cut -d ':' -f1
}

# $1 is the IP address of the host
function getHostnameFromIP {
    host $1 | awk '{print $5}'
}

function getTTHostForJobId {
    ${HADOOP_MAPRED_CMD} job -list | grep job_ | awk '{print $12}' | cut -d ':' -f2 | cut -d '/' -f3
    return $?
}

function getNN1 { 
    getNN true | cut -d ';' -f1
} 

function getNN {
    local withPort=$1
    local configFile=${HADOOP_CONF_DIR}/${CLUSTER}.namenodeconfigs.xml

    # get the list of the manenodes from $CLUSTER.namenodeconfigs.xml
    local nns=$(getValueFromField $configFile dfs.federation.nameservices)

    local arr_nns=$(echo $nns | tr "," "\n")

    local property
    local hostname
    local all_nns=""
    # for each nanode
    local i=0
    for nn in $arr_nns; do
        property=dfs.namenode.rpc-address.${nn}

        #if withPort=true is sent then return the host with the port
        if [ "$withPort" == "true" ]; then
            hostname=$(getValueFromField $configFile $property)
        else
            hostname=$(getValueFromField $configFile $property | cut -f 1 -d ":")
        fi
        
        #append the hostname to all_nns
        if [ $i -eq "0" ];then
            all_nns="${hostname}"
        else
            all_nns="${all_nns};${hostname}"
        fi
        i=$[ $i + 1 ]
    done

    echo $all_nns
}

function verifySucceededJob {
    # set -x
    ${HADOOP_MAPRED_CMD} job -list all | grep "$1" | awk '{print $2}'
    # set +x
}

###############################################################
# Params: $1:input,$2:output,$3:queue name
################################################################
function SubmitWordCountJob {
    ${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR wordcount $acl_view_arg -Dmapreduce.job.queuename=$3 $1 $2
}

################################################################
# Function to run a sleep Job
# Params: $1: number of map tasks, $2: number of reduce tasks, $3: mt, $4: rt, $5: queue name
################################################################
function SubmitASleepJob {
    echo "${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep $acl_view_arg -Dmapreduce.map.memory.mb=1024 -Dmapreduce.reduce.memory.mb=1024 -Dmapreduce.job.queuename=$5 -m $1 -r $2 -mt $3 -rt $4"
    ${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep $acl_view_arg -Dmapreduce.map.memory.mb=1024 -Dmapreduce.reduce.memory.mb=1024 -Dmapreduce.job.queuename=$5 -m $1 -r $2 -mt $3 -rt $4
}

################################################################
# Function to run a highRAM job
# Params: $1: number of memory for map, $2: number of memory for reduce, $3: number of map tasks, $4: number of reduce tasks, $5: mt, $6: rt, $7: username, $8:queue name
################################################################
function SubmitAHighRAMJob {
    echo "${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep $acl_view_arg -Dmapred.job.map.memory.mb=$1 -Dmapred.job.reduce.memory.mb=$2 -Dmapreduce.job.queuename=$7 -m $3 -r $4 -mt $5 -rt $6"
    ${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_TEST_JAR sleep $acl_view_arg -Dmapreduce.map.memory.mb=$1 -Dmapreduce.reduce.memory.mb=$2 -Dmapreduce.job.queuename=$7 -m $3 -r $4 -mt $5 -rt $6
}

