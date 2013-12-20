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
  SNN=$(getSecondaryNameNodeForNN $NN)

  VIRTUAL_HOST_PREFIX="virtual"
  LOCAL_HOST_PREFIX=`echo $SNN | cut -d'.' -f 1`
  VIRTUAL_HOST_KEYTAB="/etc/grid-keytabs/${VIRTUAL_HOST_PREFIX}.dev.service.keytab"
  LOCAL_HOST_KEYTAB="/etc/grid-keytabs/${LOCAL_HOST_PREFIX}.dev.service.keytab"
}

######################
######## teardown
######################
function teardown
{
  echo "teardown: cleanup stuff"
}


#Steps:
#Run Balancer with  principal which has virtual hostname as a part of it  
#
#Expected:
#Balancer should successfuly run
#Bug 4405348, Jira HADOOP-7215
function test_balancer_1
{
  TESTCASE_NAME="test_balancer_1"
  echo "Running Test Case: $TESTCASE_NAME"
  TESTCASE_DESC="test to make sure that balancer runs when using the virtual host keytab"

  #command for kinit
  local kinit_cmd="kinit -k -t ${VIRTUAL_HOST_KEYTAB} hdfs/${VIRTUAL_HOST_PREFIX}.${SNN}@DEV.YGRID.YAHOO.COM"
 
  #hadoop_common_cmd
  local hadoop_common_export="export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}"
  #balancer command
  local balancer_cmd="${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} balancer -fs $FS 2>&1"

  local exec_cmd="ssh -i ${HDFS_SUPER_USER_KEY_FILE} ${HDFS_SUPER_USER}@${SNN} \"${kinit_cmd};${hadoop_common_export};${balancer_cmd}\""

  echo $exec_cmd
  eval $exec_cmd | grep "The cluster is balanced."
  COMMAND_EXIT_CODE=$?
  REASONS="Did not see the message 'The cluster is balanced.' when balancer was run."
  displayTestCaseResult
}

#Steps:
#Run Balancer with  principal which has local hostname as a part of it  
#
#Expected:
#Balancer should successfuly run
#Bug 4405348, Jira HADOOP-7215
function test_balancer_2
{
  TESTCASE_NAME="test_balancer_2"
  echo "Running Test Case: $TESTCASE_NAME"
  TESTCASE_DESC="test to make sure that balancer runs when using the local host keytab"
  #command for kinit
  local kinit_cmd="kinit -k -t ${LOCAL_HOST_KEYTAB} hdfs/${SNN}@DEV.YGRID.YAHOO.COM"
  
  #hadoop_common_cmd
  local hadoop_common_export="export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}"
  #balancer command
  local balancer_cmd="${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} balancer -fs $FS 2>&1"

  local exec_cmd="ssh -i ${HDFS_SUPER_USER_KEY_FILE} ${HDFS_SUPER_USER}@${SNN} \"${kinit_cmd};${hadoop_common_export};${balancer_cmd}\""

  echo $exec_cmd
  eval $exec_cmd | grep "The cluster is balanced."
  COMMAND_EXIT_CODE=$?
  REASONS="Did not see the message 'The cluster is balanced.' when balancer was run."
  displayTestCaseResult
}

#Steps:
#Run Balancer with  principal which has hostname which is neither local nor the virtual as a part of it  
#
#Expected:
#Balancer should not run
#Bug 4405348, Jira HADOOP-7215
#bug 4550634
function test_balancer_4
{
  TESTCASE_NAME="test_balancer_4"
  echo "Running Test Case: $TESTCASE_NAME"
  TESTCASE_DESC="test to make sure that balancer does not run when using keytab with neither the local or the virutal host."
  
  #command for kinit
  local kinit_cmd="kinit -k -t ${KEYTAB_FILES_DIR}/hdfs${KEYTAB_FILE_NAME_SUFFIX} hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
  
  #hadoop_common_cmd
  local hadoop_common_export="export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}"
  #balancer command
  local balancer_cmd="${HADOOP_HDFS_CMD} --config ${HADOOP_CONF_DIR} balancer -fs $FS 2>&1"

  local exec_cmd="ssh -i ${HDFS_SUPER_USER_KEY_FILE} ${HDFS_SUPER_USER}@${SNN} \"${kinit_cmd};${hadoop_common_export};${balancer_cmd}\""

  echo $exec_cmd
  eval $exec_cmd | grep "AuthorizationException: User hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM (auth:KERBEROS)"
  COMMAND_EXIT_CODE=$?
  REASONS="See Bug 4550634. Did not see the message 'org.apache.hadoop.security.authorize.AuthorizationException: User hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM (auth:) is not authorized for protocol interface org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol, expected client  principal is hdfs/${SNN}@DEV.YGRID.YAHOO.COM.' when balancer was run."
  displayTestCaseResult
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
