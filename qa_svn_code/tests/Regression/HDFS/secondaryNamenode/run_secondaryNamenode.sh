#!/bin/bash
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh


###################
######## SETUP
###################
function setup
{
  TEST_USER=$HADOOPQA_USER
  NN=$(getDefaultNameNode)
  FS=$(getDefaultFS)
  NN_PREFIX=`echo $NN | cut -d'.' -f1`  
  SNN=$(getSecondaryNameNodeForNN $NN)
  #Conf where the SNN configs are stored
  CONF_DIR="/homes/${TEST_USER}/secondaryNamenodeTests_CONF_`date +%s`"
  CONF_DIR_SNN=${CONF_DIR}/SNN
  
  local snn_prefix="`echo $SNN | cut -d'.' -f1`"
  SNN_LOG_DIR="${HADOOP_LOG_DIR}/${HDFS_SUPER_USER}"
  #determine what the SNN_HOSTNAME_PREFIX SHOULD BE
  if [ "$snn_prefix" == "virtual" ]
  then
    SNN_LOG_FILE="hadoop-${HDFS_SUPER_USER}-secondarynamenode-`echo ${SNN} | cut -d'.' -f2-`.log"
    #then default is the vip, set the prefix to the acutal hostname
    SNN_HOSTNAME_PREFIX="`echo $SNN | cut -d'.' -f2`"
  else
    SNN_LOG_FILE="hadoop-${HDFS_SUPER_USER}-secondarynamenode-${SNN}.log"
    #set the prefix to 
    SNN_HOSTNAME_PREFIX="virtual"
  fi

  #Conf where the SNN configs are stored
  CONF_DIR="/homes/${TEST_USER}/secondaryNamenodeTests_CONF_`date +%s`"
  CONF_DIR_SNN=${CONF_DIR}/SNN
  
  #check point message
  CHECK_POINT_MSG="org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode: Checkpoint done"
}

######################
######## teardown
######################
function teardown
{
  #remove the conf dir
  local cmd="rm -rf $CONF_DIR"
  echo $cmd
  eval $cmd
}

function setNewPropertyValues
{
  	#get the old settings
	local snnHttpAdd=`getValueFromField "${confDirNN}/${CLUSTER}.namenodeconfigs.xml" "dfs.namenode.secondary.http-address.${NN_PREFIX}"`
	
	if [ "$SNN_HOSTNAME_PREFIX" == "virtual" ]
  	#if the new hostname will should have virtual in the host then do the following
	then
    	 #add virtual to the end of the hostname and that is your new add
    	NEW_SNN_HTTP_ADD=${SNN_HOSTNAME_PREFIX}.${snnHttpAdd}
  	#if it should have something else then do the following
	else
    	#remove virtual from the hostname and that is your new add
    	NEW_SNN_HTTP_ADD="`echo ${snnHttpAdd} | cut -d'.' -f2-`"
	fi
  	#the new keytab file
	NEW_SNN_KEYTAB_FILE="/etc/grid-keytabs/${SNN_HOSTNAME_PREFIX}.dev.service.keytab"
}

#Steps:
#Run SNN with kerberos principal which has virtual hostname as a part of it 
#
#Expected:
#Checkpoint should be completed
#Bug 4405348, Jira HADOOP-7215
function test_checkpoint_1
{
	TESTCASE_NAME="test_checkpoint1"
	echo "Running Test Case: $TESTCASE_NAME"
	TESTCASE_DESC="test to make sure that snn checkpoints correctly when connecting using the virtual host"
 
	local confDirNN=${CONF_DIR}/NN

  	#copy the NN config
	copyHadoopConfig $NN $confDirNN
	if [ "$?" -ne "0" ]
	then
    	REASONS="Config not copied from NN"
    	COMMAND_EXIT_CODE=1
    	displayTestCaseResult
    	return 1
	fi

  	#copy the snn config
	copyHadoopConfig $SNN $CONF_DIR_SNN
	if [ "$?" -ne "0" ]
	then
    	REASONS="Config not copied from SNN"
    	COMMAND_EXIT_CODE=1
    	displayTestCaseResult
    	return 1
	fi
  
  
  	##set the new properties
	setNewPropertyValues
	
	#modify hdfs-site.xml for the nn to the new valies
	modifyValueOfAField ${confDirNN}/${CLUSTER}.namenodeconfigs.xml "dfs.namenode.secondary.http-address.${NN_PREFIX}" "${NEW_SNN_HTTP_ADD}"
	
	#stop nn
	resetNode $NN 'namenode' 'stop' ''
  	#start the snn with the new config
	resetNode $NN 'namenode' 'start' $confDirNN
 	sleep 5

	#take the namenode out of safemode
	takeNNOutOfSafemode $FS
  
	#update the property dfs.namenode.checkpoint.period to 60 seconds
	modifyValueOfAField ${CONF_DIR_SNN}/hdfs-site.xml 'dfs.namenode.checkpoint.period' '60'
	modifyValueOfAField ${CONF_DIR_SNN}/${CLUSTER}.namenodeconfigs.xml "dfs.namenode.secondary.http-address.${NN_PREFIX}" ${NEW_SNN_HTTP_ADD}
	modifyValueOfAField ${CONF_DIR_SNN}/${CLUSTER}.namenodeconfigs.xml "dfs.secondary.namenode.keytab.file.${NN_PREFIX}" "${NEW_SNN_KEYTAB_FILE}"
	
	
  	#stop snn
	resetNode $SNN 'secondarynamenode' 'stop' ''
  	#start the snn with the new config	
	resetNode $SNN 'secondarynamenode' 'start' $CONF_DIR_SNN

	#less ${CONF_DIR_SNN}/hdfs-site.xml | grep "checkpoint.period" -2
	#less ${CONF_DIR_SNN}/core-site.xml | grep "checkpoint.period" -2
	
	
  	#get the current time, checkpoint should have been done after this time
	local currTime=`date +%s`

	local cmd="sleep 120"
	echo $cmd
	eval $cmd

  	#find the occurances of checkpoint
        cmd="ssh $SNN \"grep -A1 '${CHECK_POINT_MSG}' \"$SNN_LOG_DIR/$SNN_LOG_FILE\" | tail -1\""
	echo $cmd
	local snn_cp=`eval $cmd`
        echo $snn_cp
	local oldIFS=$IFS
	IFS=';'
	local ts
	local cpTime
	local status=1
        # should have the last checkpoint done msg
        echo "Working on message -> $snn_cp"
        #get the timestamp from the message
        ts=`expr match $snn_cp '.*\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\)'`
   	#get the time in seconds when checkpoint was done
	echo $ts
        cpTime=`date -d "$ts" +%s`
    	echo "Last checkpoint time is $cpTime"
    	echo "The current time is `date -d @$currTime`"
    	#now check if the checkpoint time is > currTime
    	if [ "$cpTime" -gt "$currTime" ]
    	  then
    		echo "Found checkpoint message in SNN Log -> $snn_cp"
    		status=0
    	fi
	IFS=$oldIFS
	COMMAND_EXIT_CODE=$status
	REASONS="Did not find message: ${CHECK_POINT_MSG} at the appropriate time"
	displayTestCaseResult

  	# Temporary work around Bugzilla Ticket 5436106
  	# Alexandria primary NN could not restart after restoreCheckpointStorage tests
	sleep 60;
  
  	#stop nn
	resetNode $NN 'namenode' 'stop' '$confDirNN'
  	#start the snn with the new config
	resetNode $NN 'namenode' 'start' ''
 
	sleep 5

  	#take the namenode out of safemode
	takeNNOutOfSafemode $FS
  
  	#stop snn
	resetNode $SNN 'secondarynamenode' 'stop' '$CONF_DIR_SNN'
  	#start the snn with the new config
	resetNode $SNN 'secondarynamenode' 'start' ''
  
	#rmKeytabFileOfHostFromLocal $SNN
	rm -rf $confDirNN
}

#Steps:
#Run SNN with kerberos principal which has actual hostname as a part of it 
#
#Expected:
#Checkpoint should be completed
#Bug 4405348, Jira HADOOP-7215
function test_checkpoint_2
{
  TESTCASE_NAME="test_checkpoint2"
  echo "Running Test Case: $TESTCASE_NAME"
  TESTCASE_DESC="test to make sure that snn checkpoints correctly when connecting using normal host name"
  
  #copy the snn config
  copyHadoopConfig $SNN $CONF_DIR_SNN
  if [ "$?" -ne "0" ]
  then
    REASONS="Config not copied from SNN"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    return 1
  fi

  #update the property dfs.namenode.checkpoint.period to 60 seconds
  addPropertyToXMLConf ${CONF_DIR_SNN}/hdfs-site.xml 'dfs.namenode.checkpoint.period' '60'

  #stop snn
  resetNode $SNN 'secondarynamenode' 'stop'
  #start the snn with the new config
  resetNode $SNN 'secondarynamenode' 'start' $CONF_DIR_SNN

  #get the current time, checkpoint should have been done after this time
  local currTime=`date +%s`

  local cmd="sleep 120"
  echo $cmd
  eval $cmd

  #find the occurances of checkpoint
  cmd="ssh $SNN \"grep -A1 '${CHECK_POINT_MSG}' \"$SNN_LOG_DIR/$SNN_LOG_FILE\" | tail -1\""
  echo $cmd
  local snn_cp=`eval $cmd`
  echo $snn_cp
  local oldIFS=$IFS
  IFS=';'
  local ts
  local cpTime
  local status=1
  #should have the last chkpt msg
  echo "Working on message -> $snn_cp"
  #get the timestamp from the message
  ts=`expr match $snn_cp '.*\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\)'`
  #get the time in seconds when checkpoint was done
  cpTime=`date -d "$ts" +%s`
  #now check if the checkpoint time is > currTime
  if [ "$cpTime" -gt "$currTime" ]
  then
      echo "Found checkpoint message in SNN Log -> $snn_cp"
      status=0
  fi
  IFS=$oldIFS
  COMMAND_EXIT_CODE=$status
  REASONS="Did not find message: ${CHECK_POINT_MSG} at the appropriate time"
  displayTestCaseResult
  
  #stop snn
  resetNode $SNN 'secondarynamenode' 'stop' $CONF_DIR_SNN
  sleep 5
  #start the snn with the new config
  resetNode $SNN 'secondarynamenode' 'start'
}

#Steps:
#Run SNN and make sure that the cluster id is part of the signatures
#
#Expected:
#Checkpoint should be completed
#Bug 4140688
function test_clusterId1
{
	TESTCASE_NAME="test_clusterId1"
	echo "Running Test Case: $TESTCASE_NAME"
	TESTCASE_DESC="test to make sure that snn uses cluster id in the check point. Bug 4140688"
  
  	#copy the snn config
	copyHadoopConfig $SNN $CONF_DIR_SNN
	if [ "$?" -ne "0" ]
	then
    	REASONS="Config not copied from SNN"
    	COMMAND_EXIT_CODE=1
    	displayTestCaseResult
    	return 1
	fi

  	#get the cluster id
	local clusterId=$(getClusterId)

  	#update the property dfs.namenode.checkpoint.period to 60 seconds
	addPropertyToXMLConf ${CONF_DIR_SNN}/hdfs-site.xml 'dfs.namenode.checkpoint.period' '60'

  	#stop snn
	resetNode $SNN 'secondarynamenode' 'stop'
  	#start the snn with the new config
	resetNode $SNN 'secondarynamenode' 'start' $CONF_DIR_SNN

	 
  	#get the current time, checkpoint should have been done after this time
	local currTime=`date +%s`

	local cmd="sleep 120"
	echo $cmd
	eval $cmd

	#local msg="INFO org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode: Posted URL.*:${clusterId}:"
	local msg="WARN org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode: Checkpoint done. New Image Size"
  	#find the occurances of checkpoint
        cmd="ssh $SNN \"grep -A1 '${msg}' \"$SNN_LOG_DIR/$SNN_LOG_FILE\" | tail -1\""
	echo $cmd
	local snn_cp=`eval $cmd`
	echo $snn_cp
	local oldIFS=$IFS
	IFS=';'
	local ts
	local cpTime
	local status=1
  	#should have the last chkpt msg
    	echo "Working on message -> $snn_cp"
    	#get the timestamp from the message
    	ts=`expr match $snn_cp '.*\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\)'`
    	#get the time in seconds when checkpoint was done
        cpTime=`date -d "$ts" +%s`
    	#now check if the checkpoint time is > currTime
    	if [ "$cpTime" -gt "$currTime" ]
    	then
          echo "Found checkpoint message in SNN Log -> $snn_cp"
    	  status=0
    	fi
	IFS=$oldIFS
	COMMAND_EXIT_CODE=$status
	REASONS="Did not find message: ${msg} at the appropriate time"
	displayTestCaseResult
  
  	#stop snn
	resetNode $SNN 'secondarynamenode' 'stop' $CONF_DIR_SNN
	sleep 5
  	#start the snn with the new config
	resetNode $SNN 'secondarynamenode' 'start'
}

############################################
# Test Cases to run
############################################
OWNER=arpitg
#setup
setup

#execute all tests that start with test_
executeTests

#teardown
teardown
