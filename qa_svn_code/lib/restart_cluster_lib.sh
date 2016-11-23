#!/bin/sh

#source the library
source $WORKSPACE/lib/library.sh

#####################################
#function to check if env vars are setup
# this function will check the following vars are set
# CLUSTER
#####################################
function checkEnvVars
{
  #if any of the vars is empty or null exit out
  if [ -z "$CLUSTER" ]; then
    echo "CLUSTER env var is not set"
    exit 1
  fi
}

##########################
# Function to check if action is set to the correct value
# either start or stop
#################################
function checkAction
{
  local action=$1
  
  #if its not equal to either start or stop then return an error
  if [ "$action" != "start" ] && [ "$action" != "stop" ]; then
    echo "Invalid value: $action sent for action, valid values are 'start' or 'stop'"
    exit 1
  fi
}

##########################
# Function to check if pkg is set to the correct value
# valid values are namenode, datanode, tasktracker, jobtracker
#################################
function checkPkg
{
  local pkg=$1
  
  #if its not equal to either namenode, datanode, tasktracker, jobtracker then return an error
  if [ "$pkg" != "namenode" ] && [ "$pkg" != "secondarynamenode" ] && [ "$pkg" != "datanode" ] && [ "$pkg" != "resourcemanager" ] && [ "$pkg" != "nodemanager" ]; then
    echo "Invalid value: $pkg sent for pkg, valid values are 'namenode', 'secondarynamenode','datanode', 'resourcemanager', 'nodemanager'"
    exit 1
  fi
}

########################################
# Function to stop a node
# $1 - host
# $2 - pkg (namenode,datanode,jobtracker,tasktracker)
# $3 - action (start,stop)
# $4 - hadoopConfDir - if you want to use a different conf for the reset node
########################################
function resetNode
{
  local host=$1
  local pkg=$2
  local action=$3
  local hadoopConfDir=$4

  #check action
  checkAction $action
  
  #check pkg
  checkPkg $pkg

  echo "----------------------------------------------------"
  echo "$action $pkg on host $host"
  
  #if hadoopConfDir is not empty then set it to that value
  if [ -z "$hadoopConfDir" ]; then
    #set it to the default value of undefined
    temp=`ssh ${HADOOPQA_USER}@$host exec "sudo /usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qa_restart_config.HADOOP_CONF_DIR=undefined"`
  else
    #set the setting to what the user provided
    temp=`ssh ${HADOOPQA_USER}@$host exec "sudo /usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER hadoop_qa_restart_config.HADOOP_CONF_DIR=$hadoopConfDir"`
  fi

  #start/stop the host
  output=`ssh ${HADOOPQA_USER}@$host exec "sudo /usr/local/bin/yinst $action -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.$CLUSTER $pkg 2>&1"`

 
  echo -e "`date` $output" 
  status=0
  echo -e $output | grep "yinst $action failed"
  #if the above command returns with exit code 0, then stop failed
  if [ 0 -eq $? ]; then
    status=1
  fi
  
  echo "----------------------------------------------------"
  echo ""

  return $status
}

########################################
# Function to reset nodes
# $1 - hosts - a list of hosts
# $2 - delim - delimiter used to separate hosts, if empty it will be assumed that they are delimited by ','
# $3 - pkg (namenode,datanode,jobtracker,tasktracker)
# $4 - action (start,stop)
# $5 - hadoopConfDir - if you want to use a different conf for the reset node
########################################
function resetNodes
{
  local hosts=$1
  local delim=$2
  #if delim is not empty then use it to correctly delimt the hosts
  hosts=$(echo $hosts | tr "$delim" ",")
  local pkg=$3
  local action=$4
  local hadoopConfDir=$5

  #check action
  checkAction $action
  
  #check pkg
  checkPkg $pkg

  echo "---------------------- `date` $action $pkg ------------------------"
  
  local set_conf_cmd
  local reset_node_cmd="sudo /usr/local/bin/yinst $action -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER} $pkg 2>&1"

  #if hadoopConfDir is not empty then set it to that value
  if [ -z "$hadoopConfDir" ]; then
    #set it to the default value of undefined
    set_conf_cmd="sudo /usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER} hadoop_qa_restart_config.HADOOP_CONF_DIR=undefined"
  else
    #set the setting to what the user provided
    set_conf_cmd="sudo /usr/local/bin/yinst set -root ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER} hadoop_qa_restart_config.HADOOP_CONF_DIR=${hadoopConfDir}"
  fi

  #start/stop the host
  local final_cmd="/home/y/bin/pdsh -w $hosts \"${set_conf_cmd} >> /dev/null;${reset_node_cmd}\""
  echo $final_cmd
  
  output=`eval $final_cmd`
  status=0
  #Do no echo the output until we format it better
  #echo $output
  echo -e $output | grep "yinst $action failed"
  #if the above command returns with exit code 0, then stop failed
  if [ 0 -eq $? ]; then
    status=1
  fi
  
  echo "----------------------------------------------------"

  # Wait for the daemon process(es) to start/stop
  local max_tries=1
  local max_tries_limit=10
  if [ $action == "stop" ] || [ $action == "start" ]; then
      # for host in `echo "$hosts"|sed 's/,/ /g'`; do
      # Wait for the daemon to start or stop
      is_daemon_in_target_state=0
      while [ $is_daemon_in_target_state -eq 0 ]; do
          if [ $action == "stop" ]; then
              local state="down"
          elif [ $action == "start" ]; then
              local state="up"
          fi 
          echo "CLUSTER=${CLUSTER} $WORKSPACE/utils/get_cluster status $pkg $hosts|grep -q \"daemon is $state\""
          CLUSTER=${CLUSTER} $WORKSPACE/utils/get_cluster status $pkg $hosts|grep -q "daemon is $state"
          found_match=$?
          if [ $found_match -eq 0 ]; then
              is_daemon_in_target_state=1
          fi
          sleep 6;
          max_tries=$((max_tries+1))
          if [ $max_tries -gt $max_tries_limit ]; then
              echo "ERROR: unable to $action daemon on host $host!!!"
              break
          fi
      done
      # done
  fi

  return $status
}

#########################################
# function to reset the namenodes
# $1 - action (start, stop)
# $2 - confDir
#########################################
function resetNameNodes
{
  #check the env vars
  checkEnvVars
  
  #get the action
  local action=$1
  local confDir=$2
  
  #get the namenode
  local nns=$(getNameNodes)
  resetNodes $nns ';' namenode $action $confDir
  rc="$?"

  return $rc
}

#########################################
# function to reset all the secondary namenodes
# $1 - action (start, stop)
# $2 - confDir
#########################################
function resetSecondaryNameNodes
{
  #check the env vars
  checkEnvVars
  
  #get the action
  local action=$1
  local confDir=$2
  
  #get the namenode
  local snns=$(getSecondaryNameNodes)
  resetNodes $snns ';' secondarynamenode $action $confDir
  rc="$?"
  return $rc
}


##########################################
# function to reset the datanodes
# $1 - action (start, stop)
# $2 - confDir
##########################################
function resetDataNodes
{
  #check the env vars
  checkEnvVars
 
  #get the action
  local action=$1

  local confDir=$2

  #get the datnodes
  local dns=$(getDataNodes)
  resetNodes $dns ';' datanode $action $confDir
  rc="$?"

  return $rc
}

##########################################
# function to reset the tasknodes
# $1 - action (start, stop)
# $2 - confDir
##########################################
function resetTaskTrackers
{
  #check the env vars
  checkEnvVars
 
  #get the action
  local action=$1
  local confDir=$2
  #get the tasktrackers
  local nms=$(getTaskTrackers)
  resetNodes $nms ';' nodemanager $action $confDir
  rc="$?"

  return $rc
}

#########################################
# function to reset the jobtracker
# $1 - action (start, stop)
# $2 - confDir
#########################################
function resetJobTracker
{
  #check the env vars
  checkEnvVars
  
  #get the action
  local action=$1
  local confDir=$2

  #get the namenode
  local rm=$(getResourceManager)

  resetNodes $rm '' resourcemanager $action $confDir
  local rc=$?
  return $rc
}

################################################
# function to stop the cluster
# $1 - confDirNN
# $2 - confDirJT
# $3 - confDirDN
################################################
function stopCluster
{
  #check the env vars
  checkEnvVars

  local confDirNN=$1
  local confDirJT=$2
  local confDirDN=$3

  echo "------------------ STOP CLUSTER $CLUSTER ---------------------------------"
  local rc=0
 

  #reset task trackers
  resetTaskTrackers stop $confDirDN
  rc=$[ $? + $rc ]

  #reset JobTracker
  resetJobTracker stop $confDirJT
  rc=$[ $? + $rc ]
  
  #reset datanodes 
  resetDataNodes stop $confDirDN
  rc=$[ $? + $rc ]
  
  #reset Namenode 
  resetNameNodes stop $confDirNN
  rc=$[ $? + $rc ]

  if [ "$rc" -ne 0 ]; then
    echo "SOMETHING FAILED TO STOP"
  else
    echo ""
    echo "EVERTHING STOPPED"
  fi

  echo "---------------------------------------------------"
  echo ""
  return $rc
}

################################################
# function to start the cluster
# $1 - confDirTT
# $2 - confDirJT
# $3 - confDirDN
################################################
function startCluster
{
  #check the env vars
  checkEnvVars
  
  local confDirNN=$1
  local confDirJT=$2
  local confDirDN=$3

  echo "------------------ START CLUSTER $CLUSTER ---------------------------------"
  local rc=0
  
  #reset Namenode 
  resetNameNodes start $confDirNN
  if [ "$?" -ne 0 ]; then
    echo "Failed to start namenodes"
  fi
  rc=$[ $? + $rc ]
  
  #reset datanodes 
  resetDataNodes start $confDirDN
  if [ "$?" -ne 0 ]; then
    echo "Failed to start datanodes"
  fi
  rc=$[ $? + $rc ]
  
  #reset JobTracker
  resetJobTracker start $confDirJT
  if [ "$?" -ne 0 ]; then
    echo "Failed to start resourcemanager"
  fi
  rc=$[ $? + $rc ]
  
  #reset task trackers
  resetTaskTrackers start $confDirDN
  if [ "$?" -ne 0 ]; then
    echo "Failed to start nodemanager"
  fi
  rc=$[ $? + $rc ]
  
  if [ "$rc" -ne 0 ]; then
    echo "SOMETHING FAILED TO START"
  else
    echo ""
    echo "EVERTHING STARTED"
  fi

  echo "---------------------------------------------------"
  echo ""
  return $rc
}

function restartCluster {
    TIME=`date`
    echo "=========== Restart the cluster: ($TIME)================"
    CR_START_IN_SEC=`date +%s`
    getAllUserTickets
    setKerberosTicketForUser $HADOOPQA_USER
    date
    stopCluster
    startCluster
    date
    takeNNOutOfSafemode
    setKerberosTicketForUser $HADOOPQA_USER
    date
    CR_END_IN_SEC=`date +%s`
    (( CR_TOTAL_EXECUTION_TIME_IN_SEC = $CR_END_IN_SEC - $CR_START_IN_SEC ))
    CR_TOTAL_EXECUTION_TIME=`convertTime $CR_TOTAL_EXECUTION_TIME_IN_SEC`
    echo "Cluster restart took: $CR_TOTAL_EXECUTION_TIME"
}

