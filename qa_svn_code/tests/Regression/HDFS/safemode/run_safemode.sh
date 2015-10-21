#!/bin/sh

#load the library file
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

###################################
# SETUP
####################################
function setup {

  NUM_TESTCASE=0
  NUM_TESTCASE_PASSED=0
  SCRIPT_EXIT_CODE=0;
  export  COMMAND_EXIT_CODE=0
  export TESTCASE_DESC="None"
  export REASONS=""
  TEST_USER=$HADOOPQA_USER
  #select the first namenode as your default
  NN=$(getDefaultNameNode)

  #Get the address for the namenode
  NAME_NODE_SERVER="http://${NN}:50070/dfshealth.html"

  #get the default FS
  FS=$(getDefaultFS)
  #dir for safemode tests on hdfs
  SAFEMODE_TESTS_DIR="${FS}/user/${TEST_USER}/safemode_tests"
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $SAFEMODE_TESTS_DIR
  #create the directory for safemode tests on hdfs
  createHdfsDir $SAFEMODE_TESTS_DIR
}

############################################
# tear down
#############################################
function teardown
{
  #take the nn out of safemode
  takeNNOutOfSafemode $FS
  
  #delete directories on hdfs
  deleteHdfsDir $SAFEMODE_TESTS_DIR
}

#############################################
# function get leave safemode
############################################
function leaveSafemode
{
  #set the user as the super user
  setKerberosTicketForUser $HDFS_SUPER_USER
  
  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave"
  echo $cmd
  eval $cmd
  
  #take the nn out of safemode
  takeNNOutOfSafemode $FS
  
  #set the user as hdfs
  setKerberosTicketForUser $HADOOPQA_USER
}

#Test dfsadmin -safemode enter command when namenode is not in safe mode
# TC ID=1172545
function test_safemode_CLI_01
{
  TESTCASE_DESC="safemode_CLI_01"	
  displayTestCaseMessage $TESTCASE_DESC

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER
  
  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter"
  echo $cmd
  eval $cmd

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER
  
  #make a curl call and grep for 	
  cmd="curl $NAME_NODE_SERVER | grep \"Safe mode is ON.\""
  echo $cmd
  eval $cmd
  
  #Get the status of the previous command
  COMMAND_EXIT_CODE="$?"
  #reason if the test case fails
  REASONS="Web UI does not show that safe mode is on"
  displayTestCaseResult

  #leave safemode
  leaveSafemode
}

#Test dfsadmin -safemode enter command when namenode is in safe mode 
# TC ID=1172546
function test_safemode_CLI_02
{
  TESTCASE_DESC="safemode_CLI_02"	
  displayTestCaseMessage $TESTCASE_DESC
  
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER
  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  #if the namenode never went into safe mode throw and error
  if [ "$status" -ne 0 ]
    then
      #reason if the test case fails
      REASONS="!!!!!!!!!!!!!!!!!!!!NAME NODE NEVER WENT INTO SAFE MODE!!!!!!!!!!!!!!!!"
      COMMAND_EXIT_CODE=$status
      displayTestCaseResult
    else
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #try to enter the name node in safe mode again
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
      echo $cmd
      eval $cmd
      status="$?"
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      #Get the status of the previous command
      COMMAND_EXIT_CODE=$status
      #reason if the test case fails
      REASONS="Command did not print 'Safe mode is ON' when entering safemode"
      displayTestCaseResult
  fi
  
  #leave safemode
  leaveSafemode
}

#Test dfsadmin -safemode leave command when namenode is in safe mode
# TC ID=1172547
function test_safemode_CLI_03
{
  TESTCASE_DESC="safemode_CLI_03"	
  displayTestCaseMessage $TESTCASE_DESC

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  #0=pass, 1 = fail
  if [ "$status" -ne 0 ]
    then
      #reason if the test case fails
      REASONS="!!!!!!!!!!!!!!!!!!!!NAME NODE NEVER WENT INTO SAFE MODE!!!!!!!!!!!!!!!!"
      COMMAND_EXIT_CODE=$status
      displayTestCaseResult
    else
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave"
      echo $cmd
      eval $cmd
  
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      #make a curl call and grep for 	
      cmd="curl $NAME_NODE_SERVER | grep 'Safe mode is ON.'"
      echo $cmd
      eval $cmd
  
      #Get the status of the previous command
      status="$?"

      #1=pass, 0 = fail, as that message should not be there as its not suppose to be in safe mode.
      if [ "$status" -ne 0 ]
       then
         status=0
         #leave safemode
         leaveSafemode
       else
         status=1;   
      fi
 
      COMMAND_EXIT_CODE=$status
      #reason if the test case fails
      REASONS="Namenode is in safemode when it should not be"
      displayTestCaseResult
  fi
  
}

#Test dfsadmin -safemode leave command when namenode is not in safe mode.
# TC ID=1172548
function test_safemode_CLI_04
{
  TESTCASE_DESC="safemode_CLI_04"	
  displayTestCaseMessage $TESTCASE_DESC

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  #if the status is 1, then leave the safemode
  if [ "$status" -ne 0 ]
    then
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      if [ "$status" -ne 0 ]
        then
          #reason if the test case fails
          REASONS="Cannot leave safemode, thus test fails"
          COMMAND_EXIT_CODE=$status
          displayTestCaseResult
        else
          #set the user as hdfs
          setKerberosTicketForUser $HDFS_USER

          #take the namdemode out of safemode again
          cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
          echo $cmd
          eval $cmd
          #Get the status of the previous command
          status="$?"
          
          #set the user as hadoopqa
          setKerberosTicketForUser $HADOOPQA_USER

          COMMAND_EXIT_CODE=$status
          #reason if the test case fails
          REASONS="dfsadmin -safemode leave command is showing that safemode is on"
          displayTestCaseResult
      fi
    else
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode again
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      COMMAND_EXIT_CODE=$status
      #reason if the test case fails
      REASONS="dfsadmin -safemode leave command is showing that the safemode is on"
      displayTestCaseResult
  fi
  
  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}

#Test dfsadmin -safemode get command when namenode is in safe mode.
# TC ID=1172549
function test_safemode_CLI_05
{
  TESTCASE_DESC="safemode_CLI_05"	
  displayTestCaseMessage $TESTCASE_DESC

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  #if the status is 1, then enter the safemode
  if [ "$status" -ne 0 ]
    then
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode into safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      if [ "$status" -ne 0 ]
        then
          #reason if the test case fails
          REASONS="Cannot enter safemode, thus test fails"
          COMMAND_EXIT_CODE=$status
          displayTestCaseResult
      else
        #set the user as hdfs
        setKerberosTicketForUser $HDFS_USER

        #test the safemode get command
        cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is ON'"
        echo $cmd
        eval $cmd
        #Get the status of the previous command
        status="$?"

        #set the user as hadoopqa
        setKerberosTicketForUser $HADOOPQA_USER
        
        COMMAND_EXIT_CODE=$status
        #reason if the test case fails
        REASONS="dfsadmin -safemode get commands does not show that safemode is on"
        displayTestCaseResult
      fi
    else
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #test the safemode get command
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is ON'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      COMMAND_EXIT_CODE=$status
      #reason if the test case fails
      REASONS="dfsadmin -safemode get command does not show that safemode is on"
      displayTestCaseResult
  fi

  #leave safemode
  leaveSafemode
}

#Test dfsadmin -safemode get command when namenode is not in safe mode.
# TC ID=1172550
function test_safemode_CLI_06
{
  TESTCASE_DESC="safemode_CLI_06"	
  displayTestCaseMessage $TESTCASE_DESC

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #get the status namdemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  #if the status is 1, then leave the safemode
  if [ "$status" -ne 0 ]
    then
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      if [ "$status" -ne 0 ]
        then
          #reason if the test case fails
          REASONS="Still in safemode, thus test fails"
          COMMAND_EXIT_CODE=$status
          displayTestCaseResult
      else
        #set the user as hdfs
        setKerberosTicketForUser $HDFS_USER

        #test the safemode get command
        cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is OFF'"
        echo $cmd
        eval $cmd
        #Get the status of the previous command
        status="$?"

        #set the user as hadoopqa
        setKerberosTicketForUser $HADOOPQA_USER

        COMMAND_EXIT_CODE=$status
        #reason if the test case fails
        REASONS="dfsadmin -safemode get command does not show that safemode is off"
        displayTestCaseResult
      fi
    else
      COMMAND_EXIT_CODE=$status
      #reason if the test case fails
      REASONS="dfsadmin -safemode get command does not show that safemode is off"
      displayTestCaseResult
  fi

  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}

#Test dfsadmin -safemode wait command when namenode is not in safemode.
#1) take NameNode out of safe mode
#2) Open second terminal and execute command > dfsadmin -safemode wait 
#3) Expected: dfsadmin -safemode wait should exit immediately with status message safemode is off
# TC ID=1172551
function test_safemode_CLI_07
{
  TESTCASE_DESC="safemode_CLI_07"
  displayTestCaseMessage $TESTCASE_DESC
  
  #get the status namdemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  
  #Get the status of the previous command
  status="$?"
 
  #if the status is 1, then leave the safemode
  if [ "$status" -ne 0 ]
    then
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      
      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      if [ "$status" -ne 0 ]
        then
          #reason if the test case fails
          REASONS="Still in safemode, thus test fails"
          COMMAND_EXIT_CODE=$status
          displayTestCaseResult
      else
        #test the safemode wait command
        cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode wait | grep 'Safe mode is OFF'"
        echo $cmd
        eval $cmd
  
        #Get the status of the previous command
        status="$?"
        COMMAND_EXIT_CODE=$status
        #reason if the test case fails
        REASONS="dfsadmin -safemode wait command does not show that safemode is off"
        displayTestCaseResult
      fi
    else
      #test the safemode wait command
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode wait | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"
      COMMAND_EXIT_CODE=$status
      #reason if the test case fails
      REASONS="dsfadmn -safemode wait command does not show that safemode is off"
      displayTestCaseResult
  fi

  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}

#Test dfsadmin -safemode wait command when namenode is not in safemode.
#1) Enter NameNode in safe mode manually
#2) Open second terminal and execute command > dfsadmin -safemode wait.
#3) After a while exit namenode safe mode by executing dfsadmin safemode -leave in first terminal
#4) Expected: Wait command should wait until namenode is in safemode , once namenode is exited safemode manually wait command should print status message safemode is off and exit 
# TC ID=1172552
function test_safemode_CLI_08
{
  TESTCASE_DESC="safemode_CLI_08"
  displayTestCaseMessage $TESTCASE_DESC
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER
 
  #if the status is 1, then we never got into safe mode and fail the test
  if [ "$status" -ne 0 ]
    then
      #reason if the test case fails
      REASONS="Could not get into safemode, thus test fails"
      COMMAND_EXIT_CODE=$status
      displayTestCaseResult
    else
      #test the safemode wait command, run it in the background
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode wait | grep 'Safe mode is OFF' &"
      echo $cmd
      eval $cmd
      #store the process id
      pid=$!
      
      echo "Sleep for 20 secs, proccess $pid running"
      sleep 20
     
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      #Get the status of the previous command
      status="$?"

      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      if [ "$status" -ne 0 ]
        then
          #reason if the test case fails
          REASONS="Still in safemode, thus test fails"
          #kill the process
          cmd="kill -9 $pid"
          echo $cmd
          eval $cmd
          COMMAND_EXIT_CODE=$status
          displayTestCaseResult
       else
        #wait for the wait cmd to finish
        cmd="wait $pid"
        echo $cmd
        eval $cmd
        status="$?"
        COMMAND_EXIT_CODE=$status
        #reason if the test case fails
        REASONS="dfsadmin -safemode wait command does not show safemode is off"
        displayTestCaseResult
      fi
  fi

  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}

#Steps:
#Verify that if safe mode is manually entered, name-node does not come out of safe mode even after the startup safe mode conditions are met.
#
#1. Create 2 files on your cluster
#2. Restart your cluster
#3. Manually enter safe mode
#4. Make sure that the namenode remains in safemode even after meeting all the conditions. That is after the threshold is met and the dfs.safemode.extension (30 secs) has passed.
#
#Expected:
#Namenode should be in safe mode. The ui would first show a message like this
#
#Safe mode is ON. The reported blocks 0.0000 has not reached the threshold 1.0000
#
#Once threshold is met it will say
#
#Safe mode is ON. The reported blocks 1.0000 has reached
#the threshold 1.0000. Use "hdfs dfsadmin -safemode leave" to turn safe
#mode off.
#
#Note - The above numbers are just an example, use regex in the actual tests.
#TC ID=1172528
function test_safemode_01
{
  TESTCASE_DESC="safemode_01"
  displayTestCaseMessage $TESTCASE_DESC

  #DATA for safemode_01 test
  SAFEMODE_01_INPUT_DIR="$SAFEMODE_TESTS_DIR/safemode_01"
  #create the directory for safemode tests on hdfs
  createHdfsDir $SAFEMODE_01_INPUT_DIR
  
  #create 2 files with some text to copy to the dir SAFEMODE_01_INPUT_DIR on hdfs
  file1=${TMPDIR}/safemode_01_1.txt
  echo 'this the first file for the test' > $file1

  file2=${TMPDIR}/safemode_01_2.txt
  echo 'this the 2nd file for the test' > $file2

  #copy the files to hdfs
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -copyFromLocal $file1 $SAFEMODE_01_INPUT_DIR"
  echo $cmd
  eval $cmd

  COMMAND_EXIT_CODE="$?"
  if [ "$COMMAND_EXIT_CODE" -ne "0" ]; then
    REASONS="failed to copy $file1 to hdfs"
    displayTestCaseResult
    return
  fi
  
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -copyFromLocal $file2 $SAFEMODE_01_INPUT_DIR"
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  if [ "$COMMAND_EXIT_CODE" -ne "0" ]; then
    REASONS="failed to copy $file2 to hdfs"
    displayTestCaseResult
    return
  fi


  local rc=0
  #reset datanodes 
  resetDataNodes stop
  rc=$[ $? + $rc ]
  #reset Namenode 
  resetNameNodes stop
  rc=$[ $? + $rc ]

  if [ "$rc" -ne "0" ]; then
    echo "SOMETHING FAILED TO STOP" 
  fi
  
  #start the cluster in a different order
  rc=0
  #reset datanodes 
  resetDataNodes start
  rc=$[ $? + $rc ]
  #reset Namenode 
  resetNameNodes start
  rc=$[ $? + $rc ]

  if [ "$rc" -ne "0" ]; then
    echo "SOMETHING FAILED TO START" 
  fi

  #wait for 5 secs to make sure services are up
  sleep 5

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  #if the above command failed, then exit with a failure
  if [ "$COMMAND_EXIT_CODE" -ne "0" ]; then
     #reason if the test case fails
     REASONS="Enter safemode command failed, thus test fails"
     displayTestCaseResult
     return
  fi

  status=1
  local counter=0
  #every 10 secs make a curl call to the NN and see if it has reached the threshold
  while [ "$status" -ne "0" ]
  do
    echo "try $counter"
    echo "curl $NAME_NODE_SERVER | grep \"Safe mode is ON.*The reported blocks .* has reached the threshold .*.\""
    curl $NAME_NODE_SERVER | grep "Safe mode is ON.*The reported blocks .* has reached the threshold .*."
    status="$?"
    sleep 10
    counter=$[$counter + 1]

    #fail the tests after 30 tries (3 mins)
    if [ "$counter" -ge "30" ]; then
      COMMAND_EXIT_CODE=1
      REASONS="Waited for 3 mins for the message: Safe mode is ON.*The reported blocks .* has reached the threshold .*. to appear. Since it did not failing the test"
      displayTestCaseResult
      #set the user as hdfs
      setKerberosTicketForUser $HDFS_USER

      #take the namdemode out of safemode
      cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -fs $FS -safemode leave | grep 'Safe mode is OFF'"
      echo $cmd
      eval $cmd
      if [ "$?" -ne "0" ]; then
        echo "Namenode still in safemode, more tests might fail"
      fi

      #set the user as hadoopqa
      setKerberosTicketForUser $HADOOPQA_USER

      return 1
    fi
  done

  #once the threshold has reached wait for 30 secs and check the status again
  sleep 30
  #did not use eval as eval replaces * with ..
  echo "curl $NAME_NODE_SERVER | grep 'Safe mode is ON.*The reported blocks .* has reached the threshold .*. Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.'"
  curl $NAME_NODE_SERVER | grep 'Safe mode is ON.*The reported blocks .* has reached the threshold .*. Use "hdfs dfsadmin -safemode leave" to turn safe mode off.'
  COMMAND_EXIT_CODE="$?"
  REASONS='Namenode UI did not say, Safe mode is ON. The reported blocks .* has reached the threshold .*. Use "hdfs dfsadmin -safemode leave" to turn safe mode off.'
  displayTestCaseResult
  
  #leave safemode
  leaveSafemode
}

#Steps:
#Verify if namenode enters safe mode at startup
#
#1) Start cluster
#2) Verify namenode web ui shows that namenode is in safe mode and will exit in x.xx seconds
#
#Expected:
#For very few seconds after startup namenode should be in safe mode .Namenode should exit safe mode after few seconds.
#Since this depends on the condition of the cluster the message you might see is something like
#
#Safe mode is ON. The reported blocks 0.8571 has not reached the threshold 1.0000. Safe mode will be turned off automatically.
#
#or
#
#Safe mode is ON. The reported blocks 1.0000 has reached the threshold 1.0000. Safe mode will be turned off automatically in 20 seconds.
#
#So if either of the above messages are present the test passes
#TC ID=1172529
function test_safemode_02
{
  TESTCASE_DESC="safemode_02"
  displayTestCaseMessage $TESTCASE_DESC

  local rc=0
  #reset datanodes 
  resetDataNodes stop
  rc=$[ $? + $rc ]
  #reset Namenode 
  resetNameNodes stop
  rc=$[ $? + $rc ]

  if [ "$rc" -ne "0" ]; then
    echo "SOMETHING FAILED TO STOP" 
  fi
  
  #start the cluster in a different order
  rc=0
  #reset datanodes 
  resetDataNodes start
  rc=$[ $? + $rc ]
  #reset Namenode 
  resetNameNodes start
  rc=$[ $? + $rc ]

  if [ "$rc" -ne "0" ]; then
    echo "SOMETHING FAILED TO START" 
  fi

  #wait for 5 secs to make sure services are up
  sleep 5
  
  #make sure the appropriate message is printed on the UI
  echo "curl $NAME_NODE_SERVER | grep  -P 'Safe mode is ON.*Safe mode will be turned off automatically in .* seconds.|Safe mode is ON.*Safe mode will be turned off automatically.'"
  curl $NAME_NODE_SERVER | grep  -P 'Safe mode is ON.*Safe mode will be turned off automatically in .* seconds.|Safe mode is ON.*Safe mode will be turned off automatically.'
  COMMAND_EXIT_CODE="$?"
  REASONS='Namenode UI did not say, Safe mode is ON.*Safe mode will be turned off automatically in .* seconds. OR Safemode is ON.*Safe mode will be turned off automatically.'
  displayTestCaseResult
 
  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}



#Execute mapred wordcount job when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to execute example wordcount job when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to execute example wordcount job now when namenode is not in safe mode .
#Expected:
#a) At step 3 expect word count job to fail to start with SafeModeException.
#b) At step 5 word count job should start successfully .
# TC ID=1172532
function test_safemode_05
{
  TESTCASE_DESC="safemode_05"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
  
  #DATA for safemode_05 test
  SAFEMODE_05_INPUT_DIR="$SAFEMODE_TESTS_DIR/safemode_05_input"
  SAFEMODE_05_OUTPUT_DIR="$SAFEMODE_TESTS_DIR/safemode_05_output"
  SAFEMODE_05_OUTPUT_DIR2="$SAFEMODE_TESTS_DIR/safemode_05_output2"

  #create the directory for safemode tests on hdfs
  createHdfsDir $SAFEMODE_05_INPUT_DIR
  
  #create the file for wordcount
  SAFEMODE_05_FILE=" ${TMPDIR}/safemode_05.txt"
  createFile $SAFEMODE_05_FILE
  local cmd="echo \"${TESTCASE_DESC}, this is a test for wordcount, just put some text in the file... wordcount is it how many times does wordcount appear. i dont know this is test ${TESTCASE_DESC}\" > $SAFEMODE_05_FILE"
  echo $cmd
  eval $cmd
  cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -copyFromLocal $SAFEMODE_05_FILE $SAFEMODE_05_INPUT_DIR"
  echo $cmd
  eval $cmd

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     #reason if the test case fails
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #run the wordcount job, which should fail
     cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR wordcount $SAFEMODE_05_INPUT_DIR $SAFEMODE_05_OUTPUT_DIR"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -eq 0 ]
       then
         #reason if the test case fails
         REASONS="Wordcount job ran successfully even when safemode was on."
         result='fail'
     fi
  fi
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     #reason if the test case fails
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
 
     #take the nn out of safemode
     takeNNOutOfSafemode $FS
     
     return
   else
     #run the wordcount job
     cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR wordcount $SAFEMODE_05_INPUT_DIR $SAFEMODE_05_OUTPUT_DIR2"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         #reason if the test case fails
         REASONS="wordcount job did not run successfully when safe mode was off. "$REASONS
         echo "FAILED WHEN SAFEMODE OFF"
         result='fail'
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
 
  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}

#Execute delete , copy , rename when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to execute  delete, copy, rename job when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to execute delete, copy, rename now when namenode is not in safe mode . 
#
#Expected:
#a) At step 3 expect delete, copy, rename to fail to start with SafeModeException.
#b) At step 5 delete, copy, rename should start successfully .
# TC ID=1172533
function test_safemode_06
{
  TESTCASE_DESC="safemode_06"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
  
  #create the file for safemode_06
  SAFEMODE_06_DEL_FILE="safemode_06_delete.txt"
  createFileOnHDFS ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_DEL_FILE}

  #create the file for safemode_06
  SAFEMODE_06_RENAME_FILE="safemode_06_rename.txt"
  createFileOnHDFS ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_RENAME_FILE}
  #create the file for safemode_06
  SAFEMODE_06_COPY_FILE="safemode_06_copy.txt"
  createFileOnHDFS ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_COPY_FILE}
  
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     #reason if the test case fails
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #try to delete a file
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -rm ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_DEL_FILE}"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -eq 0 ]
       then
         #reason if the test case fails
         REASONS="Able to delete a file when safemode is on"
         result='fail'
     fi
     
     #try to rename a file, mv
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -mv ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_RENAME_FILE} ${SAFEMODE_TESTS_DIR}/safemode_06_renamed.txt"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -eq 0 ]
       then
         REASONS="Able to move a file when safemode is on "$REASONS
         result='fail'
     fi

     #try to copy the file
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -cp ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_COPY_FILE} ${SAFEMODE_TESTS_DIR}/safemode_06_copied.txt"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -eq 0 ]
       then
         result='fail'
         REASONS="Able to copy a file when safemode is on "$REASONS
     fi
  
  fi
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
  
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     #reason if the test case fails
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
 
     #take the nn out of safemode
     takeNNOutOfSafemode $FS

     return
   else
     #try to delete a file
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -rm ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_DEL_FILE}"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Not able to remove a file when safemode is off $REASONS"
     fi
     
     #try to rename a file, mv
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -mv ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_RENAME_FILE} ${SAFEMODE_TESTS_DIR}/safemode_06_renamed.txt"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Not able to move a file when safemode is off $REASONS"
     fi
     
     #try to copy the file
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -cp ${SAFEMODE_TESTS_DIR}/${SAFEMODE_06_COPY_FILE} ${SAFEMODE_TESTS_DIR}/safemode_06_copied.txt"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Not able to copy a file when safemode is on $REASONS"
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
}

#Execute chown when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to chown of an existing file/directory when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to chown of an existing file/dire when namenode is in safemode 
#Expected:
#a) At step 3 expect chown operation to fail with SafeModeException.
#b) At step 5 chown operation should be successful successfully . 
# TC ID=1172534
function test_safemode_07
{
  TESTCASE_DESC="safemode_07"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
  
  #create the file for safemode_07
  SAFEMODE_07_CHOWN_FILE="safemode_07_chown.txt"
  createFileOnHDFS ${SAFEMODE_TESTS_DIR}/${SAFEMODE_07_CHOWN_FILE}
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to chown of the file, run it as hdfs
     echo "${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chown hdfs ${SAFEMODE_TESTS_DIR}/${SAFEMODE_07_CHOWN_FILE} 2>&1 | grep 'Cannot set owner for.*Name node is in safe mode.'"
     ${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chown hdfs ${SAFEMODE_TESTS_DIR}/${SAFEMODE_07_CHOWN_FILE} 2>&1 | grep 'Cannot set owner for.*Name node is in safe mode.'
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Able to  run chown when in safemode $REASONS"
     fi
  
  fi
  
  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
  
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
 
     #take the nn out of safemode
     takeNNOutOfSafemode $FS

     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to chown of the file, as user hdfs
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chown hdfs ${SAFEMODE_TESTS_DIR}/${SAFEMODE_07_CHOWN_FILE}"
     echo $cmd
     eval $cmd
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     #make an ls command and check for the owner is hdfs and group is hdfs
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -ls ${SAFEMODE_TESTS_DIR}/${SAFEMODE_07_CHOWN_FILE} | grep 'hdfs hdfs'"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Not able to  run chown when not in safemode $REASONS"
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
}


#Execute chmod when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to chmod of an existing file/directory when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to chmod of an existing file/dire when namenode is in safemode 
#Expected:
#a) At step 3 expect chmod operation to fail with SafeModeException.
#b) At step 5 chmod operation should be successful successfully . 
# TC ID=1172535
function test_safemode_08
{
  TESTCASE_DESC="safemode_08"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
  
  #create the file for safemode_08
  SAFEMODE_08_CHMOD_FILE="safemode_08_chmod.txt"
  createFileOnHDFS $SAFEMODE_TESTS_DIR/$SAFEMODE_08_CHMOD_FILE

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to chown of the file, run it as hdfs
     echo "${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chmod 664 ${SAFEMODE_TESTS_DIR}/${SAFEMODE_08_CHMOD_FILE} 2>&1 | grep 'Cannot set permission for .* Name node is in safe mode.'"
     ${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chmod 664 ${SAFEMODE_TESTS_DIR}/${SAFEMODE_08_CHMOD_FILE} 2>&1 | grep 'Cannot set permission for .* Name node is in safe mode.'
     #Get the status of the previous command
     status="$?"
    
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Able to run chmod when in safemode $REASONS"
     fi
  
  fi

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
 
     #take the nn out of safemode
     takeNNOutOfSafemode $FS

     return
   else
    
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to chmod of the file, as user hdfs
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chmod 664 ${SAFEMODE_TESTS_DIR}/${SAFEMODE_08_CHMOD_FILE}"
     echo $cmd
     eval $cmd
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     #make an ls command and check for the permission which whould be -rw-rw-r--, escape the - with \
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -ls ${SAFEMODE_TESTS_DIR}/${SAFEMODE_08_CHMOD_FILE} | grep '\-rw\-rw\-r\-\-'"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Not able to run chmod when not in safemode $REASONS"
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
}

#Execute chgrp when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to chgrp of an existing file/directory when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to chgrp of an existing file/dire when namenode is in safemode 
#Expected:
#a) At step 3 expect chgrp operation to fail with SafeModeException.
#b) At step 5 chgrp operation should be successful successfully . 
# TC ID=1172536
function test_safemode_09
{
  TESTCASE_DESC="safemode_09"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
  
  #create the file for safemode_09
  SAFEMODE_09_CHGRP_FILE="safemode_09_chmod.txt"
  #copy the file to hdfs
  createFileOnHDFS ${SAFEMODE_TESTS_DIR}/${SAFEMODE_09_CHGRP_FILE}
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
  
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to chgrp of the file, run it as hdfs
     echo "${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chgrp hadoop ${SAFEMODE_TESTS_DIR}/${SAFEMODE_09_CHGRP_FILE} 2>&1 | grep 'Cannot set owner for .* Name node is in safe mode.'"
     ${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chgrp hadoop ${SAFEMODE_TESTS_DIR}/${SAFEMODE_09_CHGRP_FILE} 2>&1 | grep 'Cannot set owner for .* Name node is in safe mode.'
     #Get the status of the previous command
     status="$?"

     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Able to run chgrp when in safemode $REASONS"
     fi
  
  fi

  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     #take the nn out of safemode
     takeNNOutOfSafemode $FS
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to chgrp of the file, run it as hdfs
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -chgrp hadoop ${SAFEMODE_TESTS_DIR}/${SAFEMODE_09_CHGRP_FILE}"
     echo $cmd
     eval $cmd
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER
     
     #make an ls command and check grop is hdfs
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -ls ${SAFEMODE_TESTS_DIR}/${SAFEMODE_09_CHGRP_FILE} | grep 'hadoopqa hadoop'"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="Not able to run chgrp when not in safemode $REASONS"
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
}

#Execute -setSpaceQuota when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to -setSpaceQuota of an existing file/directory when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to --setSpaceQuota of an existing file/dire when namenode is in safemode 
#Expected:
#a) At step 3 expect -setQuota operation to fail with SafeModeException.
#b) At step 5 -setQuota operation should be successful successfully . 
# TC ID=1172537
function test_safemode_10
{
  TESTCASE_DESC="safemode_10"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
 
  #DATA for safemode_10 test
  SAFEMODE_10_DIR="$SAFEMODE_TESTS_DIR/safemode_10_dir"
  #create the directory for safemode tests on hdfs
  createHdfsDir $SAFEMODE_10_DIR
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to do dfsadmin --setSpaceQuota
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -setSpaceQuota 10 ${SAFEMODE_10_DIR}"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     if [ "$status" -eq 0 ]
       then
         result='fail'
         REASONS="able to run dfsadmin -setSpaceQuota when in safemode $REASONS"
     fi
  
  fi
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"

  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     #take the nn out of safemode
     takeNNOutOfSafemode $FS
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to do dfsadmin --setSpaceQuota
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -setSpaceQuota 10 ${SAFEMODE_10_DIR}"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="not able to run dfsadmin -setSpaceQuota when not in safemode $REASONS"
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
}

#Execute -setQuota when namenode is in safemode
#1) Start cluster
#2) Enter namenode in safe mode manually
#3) Try to -setSpaceQuota of an existing file/directory when namenode is in safemode
#4) Exit namenode from safemode manually
#5) Once again try to -setSpaceQuota of an existing file/dire when namenode is in safemode
#Expected:
#a) At step 3 expect -setSpaceQuota operation to fail with SafeModeException.
#b) At step 5 -setSpaceQuota operation should be successful successfully .
# TC ID=1172538
function test_safemode_11
{
  TESTCASE_DESC="safemode_11"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
 
  #DATA for safemode_11 test
  SAFEMODE_11_DIR="$SAFEMODE_TESTS_DIR/safemode_11_dir"
  #create the directory for safemode tests on hdfs
  createHdfsDir $SAFEMODE_11_DIR
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="NOT in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to do dfsadmin -setQuota
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -setQuota 10 ${SAFEMODE_11_DIR}"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
      
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     if [ "$status" -eq 0 ]
       then
         result='fail'
         REASONS="able to run dfsadmin -setQuota when in safemode $REASONS"
     fi
  
  fi
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode out of safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
  
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="Still in safemode, thus test fails"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     #take the nn out of safemode
     takeNNOutOfSafemode $FS
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to do dfsadmin -setQuota
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -setQuota 10 ${SAFEMODE_11_DIR}"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     if [ "$status" -ne 0 ]
       then
         result='fail'
         REASONS="not able to run dfsadmin -setQuota when not in safemode $REASONS"
     fi
  fi

  #if result is fail then the test case fails
  if [ $result = "fail" ]
    then
      status=1
    else
      status=0
  fi
  COMMAND_EXIT_CODE=$status
  displayTestCaseResult
}

#Execute -saveNamespace when namenode is not in safemode 
#Expected:
#saveNamespace operation should fail with exception message - saveNamespace: java.io.IOException: Safe mode should be turned ON in order to create namespace image. 
# TC ID=1172539
function test_safemode_12
{
  TESTCASE_DESC="safemode_12"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER

  #take the namdemode into safemode
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave | grep 'Safe mode is OFF'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="STILL IN SAFE MODE THUS TEST FAILS"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to do dfsadmin -saveNamespace
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -saveNamespace 2>&1 | grep 'saveNamespace: Safe mode should be turned ON in order to create namespace image.'"
     echo $cmd
     eval $cmd
     #Get the status of the previous command
     status="$?"
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     COMMAND_EXIT_CODE=$status
     REASONS="dfsadmin -saveNamespace command ran successfully when not in safemode"
     displayTestCaseResult
  fi
}

#Execute -saveNamespace when namenode is in safemode 
#Expected:
#Check -saveNamespace executed successfully by comparing exit value (0 for success ).
# TC ID=1172540
function test_safemode_13
{
  TESTCASE_DESC="safemode_13"
  displayTestCaseMessage $TESTCASE_DESC
  result='pass'
 
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER
  
  #take the namdemode into safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode enter | grep 'Safe mode is ON'"
  echo $cmd
  eval $cmd
  #Get the status of the previous command
  status="$?"
 
  #set the user as hadoopqa
  setKerberosTicketForUser $HADOOPQA_USER

  if [ "$status" -ne 0 ]
   then
     REASONS="NOT IN SAFE MODE THUS TEST FAILS"
     COMMAND_EXIT_CODE=$status
     displayTestCaseResult
     return
   else
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER

     #try to do dfsadmin -saveNamespace
     cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -saveNamespace 2>&1"
     echo $cmd
     eval $cmd

     #Get the status of the previous command
     status="$?"
     
     #set the user as hadoopqa
     setKerberosTicketForUser $HADOOPQA_USER

     COMMAND_EXIT_CODE=$status
     REASONS="$cmd command was not successfuly run when in safemode"
     displayTestCaseResult
     #set the user as hdfs
     setKerberosTicketForUser $HDFS_USER
  
     #enter the namdemode in safemode
     local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave"
     echo $cmd
     eval $cmd
     #take the nn out of safemode
     takeNNOutOfSafemode $FS
     return
  fi
  #set the user as hdfs
  setKerberosTicketForUser $HDFS_USER
  
  #enter the namdemode in safemode
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfsadmin -safemode leave"
  echo $cmd
  eval $cmd
  #take the nn out of safemode
  takeNNOutOfSafemode $FS
}



#################################################################
#MAIN 
#################################################################

OWNER=arpitg

#setup the tests
setup

#execute all tests that start with test_
executeTests

#tear down
teardown
