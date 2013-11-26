#!/bin/sh

#load the library file
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

#############################################
#SETUP
#############################################
function setup
{
  NUM_TESTCASE=0
  NUM_TESTCASE_PASSED=0
  SCRIPT_EXIT_CODE=0;
  export  COMMAND_EXIT_CODE=0
  export TESTCASE_DESC="None"
  export REASONS=""
  TEST_USER=$HADOOPQA_USER
  FS=$(getDefaultFS)
  NN=$(getDefaultNameNode)

  timestamp=`date +%s`
  #File for the delegation token
  DELEGATION_TOKEN_FILE="${TMPDIR}DelegationToken_"$timestamp".file"
  #get the delegation tokens
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS $DELEGATION_TOKEN_FILE 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd

  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    echo "COULD NOT FETCH DELEGATION TOKEN, EXITING TEST SUITE"
    exit 1
  fi
  
  #file to store the kerberos ticket
  KERB_TICKET="${TMPDIR}DelegationTokenTestsKerbTicket_"$timestamp".tmp"

  #get the old delegation token value
  OLD_HADOOP_TOKEN_FILE_LOCATION=$HADOOP_TOKEN_FILE_LOCATION
   
  #get the old location for the kerb ticket 
  OLD_KRB5CCNAME=$KRB5CCNAME

  #dir for delefation token tests
  DELEGATION_TOKEN_TESTS_DIR="/user/${TEST_USER}/delegation_token_tests"
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $DELEGATION_TOKEN_TESTS_DIR $FS 
  #create the directory for tests on hdfs 
  createHdfsDir $DELEGATION_TOKEN_TESTS_DIR $FS

  #Dir for SF110_6
  SF110_6_DIR_INPUT="$DELEGATION_TOKEN_TESTS_DIR/SF110_6_Input"
  createHdfsDir $SF110_6_DIR_INPUT $FS
  SF110_6_DIR_OUTPUT="$DELEGATION_TOKEN_TESTS_DIR/SF110_6_Output"
}


##############################################
# teardown
#############################################
function teardown
{
  #unset the 2 vars that were set
  local cmd="unset HADOOP_TOKEN_FILE_LOCATION"
  echo $cmd
  eval $cmd
  
  #delete the kerb ticket and delegation token
  cmd="rm -rf $KERB_TICKET $DELEGATION_TOKEN_FILE"
  echo $cmd
  eval $cmd
  
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $DELEGATION_TOKEN_TESTS_DIR $FS 
}

####################################################
#create kerberos ticket for user in a specific location, move to lib later on
# $1 - User
# $2 - Location
# $3 - lifetime
####################################################
function createKerberosTicketWithLocation
{
  local user=$1
  local dest=$2
  local lifetime=$3
  
 if [ $user == 'hadoopqa' ]; then
      local keytab_file=${KEYTAB_FILES_HOMES_DIR}/${user}/${user}${KEYTAB_FILE_NAME_SUFFIX}
  else 
      local keytab_file=${KEYTAB_FILES_DIR}/${user}${KEYTAB_FILE_NAME_SUFFIX}
  fi

  #make sure the keytab file exists
  if [ ! -f "$keytab_file" ]; then
    echo "keytab file $keytab_file for user $user does not exist"
    exit 1
  fi
      
  local cmd
  if [ -z "$lifetime" ]; then
    cmd="kinit -c $dest -k -t $keytab_file $user"
    echo $cmd
    eval $cmd
  else
    cmd="kinit -l $lifetime -c $dest -k -t $keytab_file $user"
    echo $cmd
    eval $cmd
  fi
}

#Delegation tokens can be created and used to communicate with NameNode by headless users 
#Steps:
#Verify delegation tokens can be created for headless user and user can access HDFS using Delegation token even after kdestroy. 
#
#Expected:
#HDFS should be accessible even after kdestroy 
#TC ID=1172560
function test_SF110_1
{
  TESTCASE_DESC="SF110_1"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #get the kerberos ticket
  createKerberosTicketWithLocation $HADOOPQA_USER $KERB_TICKET
  
  #set the location of the kerberos ticket
  export KRB5CCNAME=$KERB_TICKET

  #Do a kdestroy
  kdestroy -c $KERB_TICKET
  
  #do bin/hadoop dfs -ls and make sure it failes
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
  echo $cmd
  eval $cmd
  status="$?"
  
  if [ "$status" -eq 0 ]
    #if the ls command was succesful that means that kdestroy did not work and thus fail the test  
    then
      REASONS="Kdestroy was not successful, $cmd command ran even after kdestroy"
      COMMAND_EXIT_CODE=1
      displayTestCaseResult
    
    #if it did fail run the command and it should pass
    else
      #set the env variable for delegation token file
      export HADOOP_TOKEN_FILE_LOCATION=$DELEGATION_TOKEN_FILE

      #do bin/hadoop dfs -ls and make sure it passes
      cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
      echo $cmd
      eval $cmd
      status="$?"
      COMMAND_EXIT_CODE=$status
      REASONS="$cmd command did not run successfully after the Delegation Token had been set."
      displayTestCaseResult
  fi
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
  
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#Delegation tokens can be created and used to communicate with NameNode by headless users
#Steps:
#Verify delegation tokens can be created for headless users and user can access HDFS using Delegation token even after lifetime of kerbrose ticket is expired.
#
#Expected:
#HDFS should be accessible even after kdestroy 
#TC ID=1172561
function test_SF110_2
{
  TESTCASE_DESC="SF110_2"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #get the kerberos ticket
  createKerberosTicketWithLocation $HADOOPQA_USER $KERB_TICKET 60s
  
  #set the location of the kerberos ticket
  export KRB5CCNAME=$KERB_TICKET

  #wait for longer than one minute
  local cmd="sleep 90"
  echo $cmd
  eval $cmd

  #do bin/hadoop dfs -ls and make sure it failes
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
  echo $cmd
  eval $cmd
  status="$?"
  
  if [ "$status" -eq 0 ]
    then
      REASONS="$cmd command ran even after kerberos ticket had exired"
      COMMAND_EXIT_CODE=1
      displayTestCaseResult
    
    #if it did fail run the command and it should pass
    else
      #set the env variable for delegation token file
      export HADOOP_TOKEN_FILE_LOCATION=$DELEGATION_TOKEN_FILE

      #do bin/hadoop dfs -ls and make sure it passes
      cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
      echo $cmd
      eval $cmd
      status="$?"
      COMMAND_EXIT_CODE=$status
      REASONS="$cmd command did not run successfully, even after delegation token was set"
      displayTestCaseResult
  fi
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#Delegation tokens can be created and used to communicate with NameNode by headless users 
#Steps:
#Verify delegation token cane be created for headless users and user can run Map reduce job.
#. 1. create a kerber ticket
#. 2. Run map reduce job. 
#
#Expected:
#Map reduce job should run sucessfuly. 
#Verify that the delegation token info is printed out

function test_SF110_6
{
  TESTCASE_DESC="SF110_6"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #create the file for wordcount
  local txt_file=" ${TMPDIR}/SF110_6.txt"
  createFile $txt_file
  local cmd="echo \"${TESTCASE_DESC}, this is a test for wordcount, just put some text in the file... wordcount is it how many times does wordcount appear. i dont know this is test ${TESTCASE_DESC}\" > $txt_file"
  echo $cmd
  eval $cmd
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -copyFromLocal $txt_file $SF110_6_DIR_INPUT"
  echo $cmd
  eval $cmd

  
  local cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR wordcount -fs $FS $SF110_6_DIR_INPUT $SF110_6_DIR_OUTPUT 2>&1 | grep \"Got dt for\""
  echo $cmd
  eval $cmd
  
  #Get the status of the previous command
  status="$?"
  COMMAND_EXIT_CODE=$status
  REASONS="While running MR wordcount job, did not find ouput that stated delegation token being used"
  displayTestCaseResult
}

#1) Set the following parameters in hdfs-site.xml on the test cluster:
#
#<property>
#<name>dfs.namenode.delegation.key.update-interval</name>
#<value>86400000</value>
#<description>The update interval for master key for delegation tokens in the namenode in milliseconds.</description>
#</property>
#
#<property>
#<name>dfs.namenode.delegation.token.max-lifetime</name>
#<value>600000</value>
#<description>The maximum lifetime in milliseconds for which a delegation token is valid.</description>
#</property>
#
#<property>
#<name>dfs.namenode.delegation.token.renew-interval</name>
#<value>300000</value>
#<description>The renewal interval for delegation token in milliseconds.</description>
#</property>
#
#
#2) Restart the cluster.
#3) Issue the renew command for the delegation token
#
#Expected:
#Renew command should not be successful. (This is a 0.22 test case)
#TC ID=1172566
#THIS TEST CASE WILL BE COMPLETED AS PART OF 0.22 AUTOMATION
#DO NOT ADD test_ as test node ready to run yet
function test_SF120_1
{
  TESTCASE_DESC="SF120_1"	
  echo "Running Test Case: $TESTCASE_DESC"

  #location of the new conf dir
  confDir="/homes/$TEST_USER/DelegationTokenTest_SF120_1_CONF_`date +%s`"
  confDirNN="$confDir/NN"
  confDirJT="$confDir/JT"
  confDirDN="$confDir/DN"

  #create the dir
  mkdir -p $confDirNN
  mkdir -p $confDirJT
  mkdir -p $confDirDN

  #copy the conf from the NN to the new location
  ssh $TEST_USER@$NN exec "cp -r $HADOOP_CONF_DIR/* $confDirNN"

  #copy the conf from the JT to the new location
  jt=$(getJobTracker_TMP)
  ssh $TEST_USER@$jt exec "cp -r $HADOOP_CONF_DIR/* $confDirJT"

  #get the datnodes
  dns=$(getDataNodes)

  dn=`echo $dns | cut -d ';' -f1`
  ssh $TEST_USER@$dn exec "cp -r $HADOOP_CONF_DIR/* $confDirDN"
 
  #update the property for NN
  addPropertyToXMLConf $confDirNN/hdfs-site.xml "dfs.namenode.delegation.key.update-interval" "86400000" "The update interval for master key for delegation tokens in the namenode in milliseconds."
  addPropertyToXMLConf $confDirNN/hdfs-site.xml "dfs.namenode.delegation.token.max-lifetime" "60000" "The maximum lifetime in milliseconds for which a delegation token is valid."
  addPropertyToXMLConf $confDirNN/hdfs-site.xml "dfs.namenode.delegation.token.renew-interval" "30000" "The renewal interval for delegation token in milliseconds."
  update_xi_include_in_file ${confDirNN}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${confDirNN}/${CLUSTER}.namenodeconfigs.xml"

  #update the property for DN/TT
  addPropertyToXMLConf $confDirDN/hdfs-site.xml "dfs.namenode.delegation.key.update-interval" "86400000" "The update interval for master key for delegation tokens in the namenode in milliseconds."
  addPropertyToXMLConf $confDirDN/hdfs-site.xml "dfs.namenode.delegation.token.max-lifetime" "60000" "The maximum lifetime in milliseconds for which a delegation token is valid."
  addPropertyToXMLConf $confDirDN/hdfs-site.xml "dfs.namenode.delegation.token.renew-interval" "30000" "The renewal interval for delegation token in milliseconds."
  update_xi_include_in_file ${confDirDN}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${confDirDN}/${CLUSTER}.namenodeconfigs.xml"
  
  #update the property for JT
  addPropertyToXMLConf $confDirJT/hdfs-site.xml "dfs.namenode.delegation.key.update-interval" "86400000" "The update interval for master key for delegation tokens in the namenode in milliseconds."
  addPropertyToXMLConf $confDirJT/hdfs-site.xml "dfs.namenode.delegation.token.max-lifetime" "60000" "The maximum lifetime in milliseconds for which a delegation token is valid."
  addPropertyToXMLConf $confDirJT/hdfs-site.xml "dfs.namenode.delegation.token.renew-interval" "30000" "The renewal interval for delegation token in milliseconds."
  update_xi_include_in_file ${confDirJT}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${confDirJT}/${CLUSTER}.namenodeconfigs.xml"
  
  #restart the cluster with the new conf
  stopCluster $confDirNN $confDirJT $confDirDN
  startCluster $confDirNN $confDirJT $confDirDN
  #wait a few secs
  sleep 5

  #take the nn out of safemode
  takeNNOutOfSafemode $FS '' $confDirNN

  #File for the delegation token
  local dt_file=${confDir}/DelegationToken.file
  #get the delegation tokens
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS --renewer $TEST_USER $dt_file 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not retrieved"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #restart the cluster with the default conf
    stopCluster
    startCluster

    sleep 5
    #take the nn out of safemode
    takeNNOutOfSafemode $FS
    
    return 1
  fi
  
  #set the env variable for delegation token file
  export HADOOP_TOKEN_FILE_LOCATION=$dt_file
  
  #wait for 2 mins to let the DT expire and try to renew the delegation token
  cmd="sleep 120"
  echo $cmd
  eval $cmd
      
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS --renew $dt_file 2>&1 | grep 'token.*is expired'"
  echo $cmd
  eval $cmd
  status="$?"
  COMMAND_EXIT_CODE=$status
  REASONS="$cmd  did not run succesfully, appropriate error message was not thrown."
  displayTestCaseResult
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER

  #restart the cluster with the default conf
  stopCluster
  startCluster

  #wait a few secs
  sleep 5
    
  #take the nn out of safemode
  takeNNOutOfSafemode $FS

  #remove the dir
  rm -rf $confDir
}

#Delegation tokens expire according to their expiration date
#Steps:
#Verify delegation tokens expire on expiration date/time.
#1.Create delegation tokens for user
#2. Forward clock/wait till delegation token expiration time is elapsed.
#3.After expiration time verify that delegation token is expired and it's not usable for Hadoop operations(HDFS commands and Map reduce job).
#
#You can use the following settings in the hdfs-site.xml to control the time for which the delegation tokens are valid
#
#<property>
#<name>dfs.namenode.delegation.key.update-interval</name>
#<value>120000</value>
#<description>The update interval for master key for delegation tokens in the namenode in milliseconds.</description>
#</property>
#
#<property>
#<name>dfs.namenode.delegation.token.max-lifetime</name>
#<value>60000</value>
#<description>The maximum lifetime in milliseconds for which a delegation token is valid.</description>
#</property>
#
#<property>
#<name>dfs.namenode.delegation.token.renew-interval</name>
#<value>30000</value>
#<description>The renewal interval for delegation token in milliseconds.</description>
#</property>
#
#
#Expected:
#Hadoop operations should fail with access control exception.
#With the settings above delegation token will be valid for 2 mins. wait 3 mins and run an ls command and it should fail
#TC ID=1172569
function test_SF160_1
{
  TESTCASE_DESC="SF160_1"	
  echo "Running Test Case: $TESTCASE_DESC"

  #location of the new conf dir
  local confDir
  confDir="/homes/$TEST_USER/DelegationTokenTest_"$TESTCASE_DESC"_CONF_`date +%s`"
  local confDirNN
  confDirNN=${confDir}/NN
  local confDirJT
  confDirJT=${confDir}/JT
  local confDirDN
  confDirDN=${confDir}/DN

  #create the dir
  mkdir -p $confDirNN
  mkdir -p $confDirJT
  mkdir -p $confDirDN

  #copy the conf from the NN to the new location
  ssh $TEST_USER@$NN exec "cp -r $HADOOP_CONF_DIR/* $confDirNN"

  #copy the conf from the JT to the new location
  jt=$(getJobTracker_TMP)
  ssh $TEST_USER@$jt exec "cp -r $HADOOP_CONF_DIR/* $confDirJT"

  #get the datnodes
  dns=$(getDataNodes)

  dn=`echo $dns | cut -d ';' -f1`
  ssh $TEST_USER@$dn exec "cp -r $HADOOP_CONF_DIR/* $confDirDN"

  updInterval_prop="dfs.namenode.delegation.key.update-interval"
  updInterval_prop_value="60000"
  updInterval_prop_desc="The update interval for master key for delegation tokens in the namenode in milliseconds."

  maxLife_prop="dfs.namenode.delegation.token.max-lifetime"
  maxLife_prop_value="30000"
  maxLife_prop_desc="The maximum lifetime in milliseconds for which a delegation token is valid."

  renewInterval_prop="dfs.namenode.delegation.token.renew-interval"
  renewInterval_prop_value="10000"
  renewInterval_prop_desc="The renewal interval for delegation token in milliseconds."

  #update the property for NN
  addPropertyToXMLConf ${confDirNN}/hdfs-site.xml $updInterval_prop $updInterval_prop_value "$updInterval_prop_desc"
  addPropertyToXMLConf ${confDirNN}/hdfs-site.xml $maxLife_prop $maxLife_prop_value "$maxLife_prop_desc"
  addPropertyToXMLConf ${confDirNN}/hdfs-site.xml $renewInterval_prop $renewInterval_prop_value "$renewInterval_prop_desc"
  update_xi_include_in_file ${confDirNN}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${confDirNN}/${CLUSTER}.namenodeconfigs.xml"
  
  #update the property for DN/TT
  addPropertyToXMLConf ${confDirDN}/hdfs-site.xml $updInterval_prop $updInterval_prop_value "$updInterval_prop_desc"
  addPropertyToXMLConf ${confDirDN}/hdfs-site.xml $maxLife_prop $maxLife_prop_value "$maxLife_prop_desc"
  addPropertyToXMLConf ${confDirDN}/hdfs-site.xml $renewInterval_prop $renewInterval_prop_value "$renewInterval_prop_desc"
  update_xi_include_in_file ${confDirDN}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${confDirDN}/${CLUSTER}.namenodeconfigs.xml"
  
  #update the property for JT
  addPropertyToXMLConf ${confDirJT}/hdfs-site.xml $updInterval_prop $updInterval_prop_value "$updInterval_prop_desc"
  addPropertyToXMLConf ${confDirJT}/hdfs-site.xml $maxLife_prop $maxLife_prop_value "$maxLife_prop_desc"
  addPropertyToXMLConf ${confDirJT}/hdfs-site.xml $renewInterval_prop $renewInterval_prop_value "$renewInterval_prop_desc"
  update_xi_include_in_file ${confDirJT}/hdfs-site.xml "${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/${CLUSTER}.namenodeconfigs.xml" "${confDirJT}/${CLUSTER}.namenodeconfigs.xml"
  
  #restart the cluster with the new conf
  stopCluster $confDirNN $confDirJT $confDirDN
  startCluster $confDirNN $confDirJT $confDirDN

  #wait a few secs
  sleep 5
  
  #take the nn out of safemode
  takeNNOutOfSafemode $FS '' $confDirNN

  #File for the delegation token
  local dt_file=${confDir}/DelegationToken.file
  #get the delegation tokens
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS $dt_file 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not retrieved"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #restart the cluster with the default conf
    stopCluster
    startCluster

    #take the nn out of safemode
    takeNNOutOfSafemode $FS
  
    return 1
  fi
      
  #set the env variable for delegation token file
  export HADOOP_TOKEN_FILE_LOCATION=$dt_file
  
  #get the kerberos ticket
  createKerberosTicketWithLocation $HADOOPQA_USER $KERB_TICKET
  
  #set the location of the kerberos ticket
  export KRB5CCNAME=$KERB_TICKET
  
  #Do a kdestroy
  kdestroy -c $KERB_TICKET
  
  #do bin/hadoop dfs -ls and make sure it passes
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
  echo $cmd
  eval $cmd
  status="$?"
  
  if [ "$status" -ne 0 ]
    #if the ls command was no succesful that means that kdestroy did not work and thus fail the test  
    then
      REASONS="$cmd command did not run with the delegation token"
      COMMAND_EXIT_CODE=1
      displayTestCaseResult
    #if it passed then wait for 2 mins and run ls command again and it should fail
    else
      cmd="sleep 120"
      echo $cmd
      eval $cmd
      
      #do bin/hadoop dfs -ls and make sure it fails as DT should have expired
      echo "$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls 2>&1 | grep \"token .* is expired\""
      $HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls 2>&1 | grep "token .* is expired"
      status="$?"
      COMMAND_EXIT_CODE=$status
      REASONS="dfs -ls command did not return the following message: token .* is expired"
      displayTestCaseResult
  fi
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER

  #restart the cluster with the default conf
  stopCluster
  startCluster

  #wait a few secs
  sleep 5
  
  #take the nn out of safemode
  takeNNOutOfSafemode $FS

  #remove the dir
  rm -rf $confDir
}


#For Headless users Verify that delegation token can be used in it's life time even after Name node restart.
#1.Create delegation token.
#2.Restart Name node.
#3.Verify that Hadoop operations(HDFS commands and Map reduce job) are allowed and there are no errors.
#Expected
#Hadoop operations should complete successfully. 
#TC ID=1172572
function test_SF170_2
{
  TESTCASE_DESC="SF170_2"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #get the kerberos ticket
  createKerberosTicketWithLocation $HADOOPQA_USER $KERB_TICKET
  
  #set the location of the kerberos ticket
  export KRB5CCNAME=$KERB_TICKET

  #Do a kdestroy
  kdestroy -c $KERB_TICKET
 
  #do bin/hadoop dfs -ls and make sure it failes
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
  echo $cmd
  eval $cmd
  status="$?"
  
  if [ "$status" -eq 0 ]
    #if the ls command was succesful that means that kdestroy did not work and thus fail the test  
    then
      REASONS="Kdestroy was not successful, $cmd command ran even after kdestroy"
      COMMAND_EXIT_CODE=1
      displayTestCaseResult
    
    #if it did fail run the command and it should pass
    else
      #set the env variable for delegation token file
      export HADOOP_TOKEN_FILE_LOCATION=$DELEGATION_TOKEN_FILE

      #restart the default namenode
      resetNode $NN namenode stop
      resetNode $NN namenode start
      
      #wait for a few secs to make sure NN can be connected to
      sleep 5
  
      #take the nn out of safemode
      takeNNOutOfSafemode $FS

      #do bin/hadoop dfs -ls and make sure it passes
      cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
      echo $cmd
      eval $cmd
      status="$?"
      COMMAND_EXIT_CODE=$status
      REASONS="$cmd command did not run successfully after delegation token was set and namenode was restarted"
      displayTestCaseResult
  fi
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
  
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#Delegation tokens are still valid after two or more NameNode restarts within delegation token persistence time on NameNode
#Steps:
#Verify that delegation token can be used in it's life time even after Name node restart is done 3 times.
#1.Create delegation token.
#2.Restart Name node.
#3.Verify that Hadoop operations(HDFS commands and Map reduce job) are allowed and there are no errors.
#
#Expected:
#Hadoop operations should complete successfully
#TC ID=1172574
function test_SF175_1
{
  TESTCASE_DESC="SF175_1"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #get the kerberos ticket
  createKerberosTicketWithLocation $HADOOPQA_USER $KERB_TICKET
  
  #set the location of the kerberos ticket
  export KRB5CCNAME=$KERB_TICKET

  #Do a kdestroy
  kdestroy -c $KERB_TICKET
 
  #do bin/hadoop dfs -ls and make sure it failes
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
  echo $cmd
  eval $cmd
  status="$?"
  
  if [ "$status" -eq 0 ]
    #if the ls command was succesful that means that kdestroy did not work and thus fail the test  
    then
      REASONS="Kdestroy was not successful, $cmd command ran even after kdestroy"
      COMMAND_EXIT_CODE=1
      displayTestCaseResult
    
    #if it did fail run the command and it should pass
    else
      #set the env variable for delegation token file
      export HADOOP_TOKEN_FILE_LOCATION=$DELEGATION_TOKEN_FILE

      #restart the default namenode
      resetNode $NN namenode stop
      resetNode $NN namenode start
      
      #wait for a few secs to make sure NN can be connected to
      sleep 5

      #restart the default namenode
      resetNode $NN namenode stop
      resetNode $NN namenode start
      
      sleep 5
      
      #restart the default namenode
      resetNode $NN namenode stop
      resetNode $NN namenode start
      
      #wait for a few secs to make sure NN can be connected to
      sleep 5
  
      #take the nn out of safemode
      takeNNOutOfSafemode $FS

      #do bin/hadoop dfs -ls and make sure it passes
      cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls"
      echo $cmd
      eval $cmd
      status="$?"
      COMMAND_EXIT_CODE=$status
      REASONS="$cmd command did not run successfully, after setting the delegation token and restarting namenode 3 times"
      displayTestCaseResult
  fi
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
  
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#Once delegation token is canceled, it cannot be used anymore
#(This is a 0.22 test case)
#Steps:
#Verify that after delegation token cancellation no hadoop operations are allowed using delegation token and without kerberos ticket.
#
#Expected:
#Hadoop operations using delegation token (without kerberos ticket) should fail with access control exception when delegation tokens are canceled.
function test_SF190_1
{
  TESTCASE_DESC="SF190_1"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #File for the delegation token
  local dt_file="${TMPDIR}DelegationToken_${TESTCASE_DESC}.file"
  
  #get the delegation tokens
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS $dt_file 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not retrieved"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the 2 vars that were set
    unset HADOOP_TOKEN_FILE_LOCATION 
 
    #set the user as $HADOOPQA_USER
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi
  
  #set the env variable for delegation token file
  export HADOOP_TOKEN_FILE_LOCATION=$dt_file
  
  #cancel the DT
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS --cancel $dt_file 2>&1 | grep 'Cancelling HDFS_DELEGATION_TOKEN'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not canceled"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the 2 vars that were set
    unset HADOOP_TOKEN_FILE_LOCATION 
 
    #set the user as $HADOOPQA_USER
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi
  
  #issue a dfs -ls command and it should fail with an exception
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls 2>&1 | grep \"token.*can't be found in cache\""
  echo $cmd
  eval $cmd
  status="$?"
  COMMAND_EXIT_CODE=$status
  REASONS="$cmd  did not run succesfully, appropriate error message was not thrown. (token.*can't be found in cache)"
  displayTestCaseResult
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#Delegation token from one namenode cannot be used on another namenode
# This is a 0.22 test case
#Steps:
#1. Get delegation token for namenode 1
#2. kdestroy the active kerberos ticket
#3. Use the delegation token to run dfs -ls command on namenode 2
#
#Expected:
#Command should fail with security exception
function test_SF210_1
{
  TESTCASE_DESC="SF210_1"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #File for the delegation token
  local dt_file="${TMPDIR}DelegationToken_${TESTCASE_DESC}.file"
  
  #get the delegation tokens
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS $dt_file 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not retrieved"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the 2 vars that were set
    unset HADOOP_TOKEN_FILE_LOCATION 
 
    #set the user as $HADOOPQA_USER
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi
  
  #set the env variable for delegation token file
  cmd="export HADOOP_TOKEN_FILE_LOCATION=$dt_file"
  echo $cmd
  eval $cmd
 
  #get the namenodes
  local nns=$(getNameNodes)
  local oldIFS=$IFS
  IFS=";"
  local arr_nns=($nns)
  num_of_nns=${#arr_nns[@]}
  #if # nns is not > 1 then fail the test case
  if [ "$num_of_nns" -le "1" ] ; then
    REASONS="Number of Namenodes in the cluster is ${num_of_nns}. There should be more that 1 namenode present for this test"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the env var and set the user to hadoopqa
    unset HADOOP_TOKEN_FILE_LOCATION
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi

  local NN2=${arr_nns[1]}
  IFS=$oldIFS
  
  #get the kerberos ticket
  createKerberosTicketWithLocation $HADOOPQA_USER $KERB_TICKET
  
  #set the location of the kerberos ticket
  export KRB5CCNAME=$KERB_TICKET

  #Do a kdestroy
  kdestroy -c $KERB_TICKET

  #issue a dfs -ls command  on the 2nd anmenode and it should fail with an exception
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs hdfs://${NN2}:8020 -ls 2>&1 | grep 'GSSException: No valid credentials provided'"
  echo $cmd
  eval $cmd
  status="$?"
  COMMAND_EXIT_CODE=$status
  REASONS="$cmd  did not run succesfully, appropriate error message was not thrown. (GSSException: No valid credentials provided)"
  displayTestCaseResult
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}


#Delegation token from one namenode cannot be used on another namenode
# This is a 0.22 test case
#Steps:
#1. Get delegation token for namenode 1
#2. Use the delegation token to run dfs -ls command on namenode 2
#
#Expected:
#Command should pass as it will implicitly use the active kerberos ticket
function test_SF210_2
{
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
  
  TESTCASE_DESC="SF210_2"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #File for the delegation token
  local dt_file="${TMPDIR}DelegationToken_${TESTCASE_DESC}.file"
  
  #get the delegation tokens
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS $dt_file 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not retrieved"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the 2 vars that were set
    unset HADOOP_TOKEN_FILE_LOCATION 
 
    #set the user as $HADOOPQA_USER
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi
  
  #set the env variable for delegation token file
  cmd="export HADOOP_TOKEN_FILE_LOCATION=$dt_file"
  echo $cmd
  eval $cmd
 
  #get the namenodes
  local nns=$(getNameNodes)
  local oldIFS=$IFS
  IFS=";"
  local arr_nns=($nns)
  num_of_nns=${#arr_nns[@]}
  #if # nns is not > 1 then fail the test case
  if [ "$num_of_nns" -le "1" ] ; then
    REASONS="Number of Namenodes in the cluster is ${num_of_nns}. There should be more that 1 namenode present for this test"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the env var and set the user to hadoopqa
    unset HADOOP_TOKEN_FILE_LOCATION
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi

  #get the 2nd namenode
  local NN2=${arr_nns[1]}
  IFS=$oldIFS
  
  #issue a dfs -ls command  on the 2nd anmenode and it should fail with an exception
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs hdfs://${NN2}:8020 -ls 2>&1"
  echo $cmd
  eval $cmd
  status="$?"
  COMMAND_EXIT_CODE=$status
  REASONS="$cmd  did not run succesfully"
  displayTestCaseResult
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#Delegation token from one namenode cannot be used on another namenode
# This is a 0.22 test case
#Steps:
#1. Get delegation token for namenode
#2. Update the delegation token file
#3. Use the delegation token to run dfs -ls
#
#Expected:
#Command should fail stating that checksum failed
function test_SF210_3
{
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
  
  TESTCASE_DESC="SF210_3"	
  echo "Running Test Case: $TESTCASE_DESC"
  
  #File for the delegation token
  local dt_file="${TMPDIR}DelegationToken_${TESTCASE_DESC}.file"
  
  #get the delegation tokens
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fetchdt -fs $FS $dt_file 2>&1 | grep 'Fetched token for'"
  echo $cmd
  eval $cmd
  #if the above command was not successfuly exit out of the suite
  if [ "$?" -ne "0" ]; then
    REASONS="ran $cmd, but delegation token was not retrieved"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the 2 vars that were set
    unset HADOOP_TOKEN_FILE_LOCATION 
 
    #set the user as $HADOOPQA_USER
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi
  
  #set the env variable for delegation token file
  cmd="export HADOOP_TOKEN_FILE_LOCATION=$dt_file"
  echo $cmd
  eval $cmd

  #corrupt the delegation token file
  cmd="sed -i \"s|HDFS_DELEGATION_TOKEN|${TESTCASE_DESC}_DELEGATION_TOKEN|\" $dt_file"
  echo $cmd
  eval $cmd
  if [ "$?" -ne "0" ] ; then
    REASONS="$cmd did not run successfully"
    COMMAND_EXIT_CODE=1
    displayTestCaseResult
    #unset the env var and set the user to hadoopqa
    unset HADOOP_TOKEN_FILE_LOCATION
    setKerberosTicketForUser $HADOOPQA_USER
    return 1
  fi

  #issue a dfs -ls command  on the 2nd anmenode and it should fail with an exception
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls 2>&1 | grep \"ls: Exception reading\""
  echo $cmd
  eval $cmd
  status="$?"
  COMMAND_EXIT_CODE=$status
  REASONS="$cmd  did not run succesfully. Did not get the appropriate message (Found checksum error)"
  displayTestCaseResult
  
  #unset the 2 vars that were set
  unset HADOOP_TOKEN_FILE_LOCATION 
 
  #set the user as $HADOOPQA_USER
  setKerberosTicketForUser $HADOOPQA_USER
}

#####################################################################################################
#MAIN 
#####################################################################################################
NUM_TESTCASE=0
NUM_TESTCASE_PASSED=0
SCRIPT_EXIT_CODE=0

OWNER=arpitg

#setup the tests
setup

#execute all tests that start with test_
executeTests

#teardown any unwanted info
teardown
