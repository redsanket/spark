#!/bin/sh

#load the library file
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh

##########################################################################################
# Note: in 23 job history server do list operation every 3 mins
# therefore the filelisting count will increase even without client listing operation
# so we have to stop jh server otherwise the tests may fail intermittently
##########################################################################################

#############################################
#SETUP
#############################################
function setup
{
  REASONS=""
  TEST_USER=$HADOOPQA_USER
  PROPERTY="FilesInGetListingOps"
  SERVICE="Hadoop:name=NameNodeActivity,service=NameNode"
  METRICS_PROP_FILE="${HADOOP_CONF_DIR}/hadoop-metrics2.properties"
  METRICS_PROP="*.period"

  #get the default FS
  DEFAULT_FS=$(getDefaultFS)
  #get the defatul namenode
  DEFAULT_NN=$(getDefaultNameNode)
	
	# the following code kill job history server in resource manger (see ticket #)
	# I assume that hadoopqa can access RM as mapredqa (id_dsa.pub of hadoopqa has to be in authorized_keys of mapredqa
	RESOURCE_MN=$(getResourceManager)	
	
	cmd="ssh mapredqa@$RESOURCE_MN '$HADOOP_COMMON_HOME/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR stop historyserver'"	
	echo $cmd
	eval $cmd
	
	if [ $? -eq 1 ]; then 
		echo "Cannot kill job history server. See ticket 5595712 for more details. The tests may fail because of this"		
	fi
	
	
  #dir for delefation token tests
  DATA_DIR="/user/${TEST_USER}/FilesInGetListingOps"
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $DATA_DIR ${DEFAULT_FS} "0"
  #create the directory for tests on hdfs
  createHdfsDir $DATA_DIR ${DEFAULT_FS}

  #create the test data under DATA_DIR
  createTestData ${DEFAULT_FS}${DATA_DIR}

  NN_PID=''
  CURRENT_VALUE=''
  PROP_REFRESH_TIME=$(getValueFromPropertiesFile "$METRICS_PROP_FILE" "$METRICS_PROP")

  #wait to make sure the property value is updated
  waitForRefresh

  #get the latest info
  resetInfo $DEFAULT_NN

  #get the namenodes
  local nns=$(getNameNodes)
  local oldIFS=$IFS
  IFS=";" 
  local arr_nns=($nns)
  num_of_nns=${#arr_nns[@]}
  #if # nns is not > 1 then fail the test case
  if [ "$num_of_nns" -le "1" ]
  then
    echo "INFO: ONLY 1 OR LESS NN's IN THE CLUSTER. SOME TESTS WILL NOT RUN"
    NN2=''
  else
    #get the 2nd namenode
    NN2=${arr_nns[1]} 
    FS2="hdfs://${NN2}:8020"
  
    #delete directories on hdfs, make sure no old data is there
    deleteHdfsDir $DATA_DIR ${FS2} "0"
    #create the directory for tests on hdfs
    createHdfsDir $DATA_DIR ${FS2}
    #create the test data under DATA_DIR
    createTestData ${FS2}${DATA_DIR}
  fi
  IFS=$oldIFS
}


##############################################
# teardown
#############################################
function teardown
{
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $DATA_DIR ${DEFAULT_FS} "0"
 
  #if NN2 exists then delete its data
  if [ -n "$NN2" ]
  then
    deleteHdfsDir $DATA_DIR ${FS2} "0" 
  fi
	
	#need to restartCluster because job history server has been killed
	ssh mapredqa@$RESOURCE_MN "$HADOOP_COMMON_HOME/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver"
}

###############################################
# resetInfo
# this function will recompute the NN_PID and the value for the PROPERTY for the given namenode
# $1 - namenode host
###############################################
function resetInfo
{
  local nn=$1
  if [ -z "$nn" ]
  then
    echo "FATAL: NO VALUE SENT FOR: nn"
    exit 1
  fi

  NN_PID=$(getHadoopProcessId "$nn" "namenode")
  CURRENT_VALUE=$(getJMXPropertyValue "$nn" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "NN PID(${nn}): $NN_PID"
  echo "CURRENT VALUE(${nn}): $CURRENT_VALUE"
}

#function to create data in a dir, it will create a total of 31 items
function createTestData
{
  local parentDir=$1
  if [ -z "$parentDir" ]
  then
    echo "FATAL: NO VALUE SENT FOR parentDir"
    exit 1
  fi

  #create 3 files and 2 dir's under the parent -> 6 items
  local dir00=${parentDir}/dir00
  local dir01=${parentDir}/dir01
  local dir_empty=${parentDir}/dir_empty
  createHdfsDir $dir00
  createHdfsDir $dir01
  createHdfsDir $dir_empty
  createFileOnHDFS ${parentDir}/file00.txt
  createFileOnHDFS ${parentDir}/file01.txt
  createFileOnHDFS ${parentDir}/file02.txt

  #create 10 files under dir00 -> 10 items
  createFileOnHDFS ${dir00}/file10.txt
  createFileOnHDFS ${dir00}/file11.txt
  createFileOnHDFS ${dir00}/file12.txt
  createFileOnHDFS ${dir00}/file13.txt
  createFileOnHDFS ${dir00}/file14.txt
  createFileOnHDFS ${dir00}/file15.txt
  createFileOnHDFS ${dir00}/file16.txt
  createFileOnHDFS ${dir00}/file17.txt
  createFileOnHDFS ${dir00}/file18.txt
  createFileOnHDFS ${dir00}/file19.txt


  #create 10 dirs, 5 files under dir01 -> 15 items
  createFileOnHDFS ${dir01}/file10.txt
  createFileOnHDFS ${dir01}/file11.txt
  createFileOnHDFS ${dir01}/file12.txt
  createFileOnHDFS ${dir01}/file13.txt
  createFileOnHDFS ${dir01}/file14.txt
  createHdfsDir ${dir01}/dir10
  createHdfsDir ${dir01}/dir11
  createHdfsDir ${dir01}/dir12
  createHdfsDir ${dir01}/dir13
  createHdfsDir ${dir01}/dir14
  createHdfsDir ${dir01}/dir15
  createHdfsDir ${dir01}/dir16
  createHdfsDir ${dir01}/dir17
  createHdfsDir ${dir01}/dir18
  createHdfsDir ${dir01}/dir19
}

#function to wait for property to be refreshed
function waitForRefresh
{
	
	echo "SLEEP for 3*${PROP_REFRESH_TIME}(refresh time)"
  	#wait few secs longer than the refresh time and check the value
	sleep $PROP_REFRESH_TIME
	sleep $PROP_REFRESH_TIME
	sleep $PROP_REFRESH_TIME
}

#Issus dfs -ls command and make sure the metric shows the updated value
#Steps:
#
#Expected:
#TC ID=1313848
function test_FilesInGetListingOps1
{
  TESTCASENAME="FilesInGetListingOps1"
  echo "Running Test Case: $TESTCASENAME"
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the latest info
  resetInfo $DEFAULT_NN

  #issue a dfs -ls command on dir00, it has 10 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}/dir00"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  change=$[$new_val - $CURRENT_VALUE]
  COMMAND_EXIT_CODE=0
  if [ "$change" -ne "10" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${CURRENT_VALUE}, and the new value: $new_val for property: $PROPERTY is not 10"
  fi

  #display the test case result
  displayTestCaseResult
  
  #set the current value to new_val
  CURRENT_VALUE=$new_val
}

#Issus dfs -ls -R command and make sure the metric shows the updated value
#Steps:
#
#Expected:
#TC ID=1313849
function test_FilesInGetListingOps2
{
  TESTCASENAME="FilesInGetListingOps2"
  echo "Running Test Case: $TESTCASENAME"
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the latest info
  resetInfo $DEFAULT_NN

  #issue a dfs -ls command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  change=$[$new_val - $CURRENT_VALUE]
  COMMAND_EXIT_CODE=0
  if [ "$change" -ne "31" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${CURRENT_VALUE}, and the new value: $new_val for property: $PROPERTY is not 31"
  fi

  #display the test case result
  displayTestCaseResult
  
  #set the current value to new_val
  CURRENT_VALUE=$new_val
}

#Issue dfs -ls command when viewfs is configured. Make sure that the metric is updated
#Steps:
#
#Expected:
#TC ID=1313850
function test_FilesInGetListingOps3
{
  TESTCASENAME="FilesInGetListingOps3"
  echo "Running Test Case: $TESTCASENAME"
 
  #create the new conf dir
  local viewfs_conf_dir="${TMPDIR}/run_metric_FilesInGetListingOps_conf_`date +%s`"
  local cmd="mkdir -p $viewfs_conf_dir"
  echo $cmd
  eval $cmd
  if [ "$?" -ne 0 ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="Could not create the dir for viewfs conf $viewfs_conf_dir"
    #display the test case result
    displayTestCaseResult
    return
  fi

  #copy the conf from HADOOP_CONF_DIR to viewfs_conf_dir
  cmd="cp -r ${HADOOP_CONF_DIR}/* ${viewfs_conf_dir}/"
  echo $cmd
  eval $cmd
  if [ "$?" -ne 0 ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="Could not copy the contents for $HADOOP_CONF_DIR to $viewfs_conf_dir"
    #display the test case result
    displayTestCaseResult
    return
  fi

  #update the properties
  #change fs.defaultFS to viewfs
  cmd="sed -i \"/fs.defaultFS/ { N; N; N; s#<value>[a-zA-Z0-9/\:\.]*</value>#<value>viewfs:///</value># ; }\" ${viewfs_conf_dir}/core-site.xml"
  echo $cmd
  eval $cmd
  #set fs.viewfs.impl
  addPropertyToXMLConf ${viewfs_conf_dir}/core-site.xml "fs.viewfs.impl" "org.apache.hadoop.fs.viewfs.ViewFileSystem" 
  #add the different paths for viewfs
  addPropertyToXMLConf ${viewfs_conf_dir}/core-site.xml "fs.viewfs.mounttable.default.link./jobtracker" "${DEFAULT_FS}/jobtracker" '' '' 
  addPropertyToXMLConf ${viewfs_conf_dir}/core-site.xml "fs.viewfs.mounttable.default.link./mapred" "${DEFAULT_FS}/mapred" '' '' 
  addPropertyToXMLConf ${viewfs_conf_dir}/core-site.xml "fs.viewfs.mounttable.default.link./mapredsystem" "${DEFAULT_FS}/mapredsystem" '' ''
  addPropertyToXMLConf ${viewfs_conf_dir}/core-site.xml "fs.viewfs.mounttable.default.link./FilesInGetListingOps" "${DEFAULT_FS}${DATA_DIR}" '' '' 
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh

  #get the latest info
  resetInfo $DEFAULT_NN

  #issue a dfs -ls command on DATA_DIR using viewfs path. it should list 6 items
  cmd="$HADOOP_HDFS_CMD --config $viewfs_conf_dir dfs -ls /FilesInGetListingOps"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  change=$[$new_val - $CURRENT_VALUE]
  COMMAND_EXIT_CODE=0
  if [ "$change" -ne "6" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${CURRENT_VALUE}, and the new value: $new_val for property: $PROPERTY is not 6"
  fi

  #display the test case result
  displayTestCaseResult
}

#Restart the NN and make sure that the metric gets reset to 0
#Steps:
#
#Expected:
#TC ID=1313851
function test_FilesInGetListingOps4
{
  TESTCASENAME="FilesInGetListingOps4"
  echo "Running Test Case: $TESTCASENAME"
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the latest info
  resetInfo $DEFAULT_NN

  #issue a dfs -ls command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  COMMAND_EXIT_CODE=0
  if [ "$new_val" -le "0" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The current value for property $PROPERTY is 0. Cannot continue with the test"
    
    #set the current value to new_val
    CURRENT_VALUE=$new_val
  else
    #restart the NN to reset everything
    resetNode $DEFAULT_NN "namenode" "stop"
    resetNode $DEFAULT_NN "namenode" "start"

    #wait for 5 secs
    sleep 5

    #take the nn out of safemode
    takeNNOutOfSafemode $DEFAULT_FS

    resetInfo $DEFAULT_NN

    #check to see if current value is =0
    if [ "$CURRENT_VALUE" -ne "0" ]
    then
      COMMAND_EXIT_CODE=1
      REASONS="The current value for property $PROPERTY does not reset to 0 after NN restart. See Bug 4626670"
    fi
  fi

  #display the test case result
  displayTestCaseResult
}

#Issus multiple dfs -ls and dfs -ls -R commands and make sure that the metric has the correct value
#Steps:
#
#Expected:
#TC ID=1313852
function test_FilesInGetListingOps5
{
  TESTCASENAME="FilesInGetListingOps5"
  echo "Running Test Case: $TESTCASENAME"
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the latest info
  resetInfo $DEFAULT_NN

  #issue a dfs -ls -R command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls command on DATA_DIR, it has 6 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls command on dir01, it has 15 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}/dir01"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls -R command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  change=$[$new_val - $CURRENT_VALUE]
  COMMAND_EXIT_CODE=0
  if [ "$change" -ne "83" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${CURRENT_VALUE}, and the new value: $new_val for property: $PROPERTY is not 83"
  fi

  #display the test case result
  displayTestCaseResult
  
  #set the current value to new_val
  CURRENT_VALUE=$new_val
}

#Issus dfs -ls command on an empty dir and make sure it does not change the value for the property
#Steps:
#
#Expected:
#TC ID=1313853
function test_FilesInGetListingOps6
{
  TESTCASENAME="FilesInGetListingOps6"
  echo "Running Test Case: $TESTCASENAME"
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the latest info
  resetInfo $DEFAULT_NN

  #issue a dfs -ls command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}/dir_empty"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  change=$[$new_val - $CURRENT_VALUE]
  COMMAND_EXIT_CODE=0
  if [ "$change" -ne "0" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${CURRENT_VALUE}, and the new value: $new_val for property: $PROPERTY are not the same. They should be"
  fi

  #display the test case result
  displayTestCaseResult
  
  #set the current value to new_val
  CURRENT_VALUE=$new_val
}

#THIS TEST REQUIRES MULTIPLE NN SETUP
#Issue a dfs -ls command on NN1. Make sure the metric is updated for NN1 but remains unchanged for NN2
#Steps:
#
#Expected:
#TC ID=1313854
function test_FilesInGetListingOps7
{
  TESTCASENAME="FilesInGetListingOps7"
  echo "Running Test Case: $TESTCASENAME"

  #if NN2 is empty then do not run this test
  if [ -z "$NN2" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The cluster $CLUSTER only has 1 NN. This test requires more than 1 NN."
    #display the test case result
    displayTestCaseResult
    return
  fi
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh

  #get the info for NN2
  resetInfo $NN2

  #issue a dfs -ls command on DATA_DIR, it has 6 items on NN1
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  new_val=$(getJMXPropertyValue "$NN2" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  COMMAND_EXIT_CODE=0
  if [ "$new_val" -ne "$CURRENT_VALUE" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The value for property: $PROPERTY on namenode $NN2 changed when it should not have"
  fi

  #display the test case result
  displayTestCaseResult
}
#THIS TEST REQUIRES MULTIPLE NN SETUP
#Restart NN1 and make sure that the metric is reset for NN1 but remains the same for NN2
#Steps:
#
#Expected:
#TC ID=1313854
function test_FilesInGetListingOps8
{
  TESTCASENAME="FilesInGetListingOps8"
  echo "Running Test Case: $TESTCASENAME"

  #if NN2 is empty then do not run this test
  if [ -z "$NN2" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The cluster $CLUSTER only has 1 NN. This test requires more than 1 NN."
    #display the test case result
    displayTestCaseResult
    return
  fi
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the info for NN2
  resetInfo $NN2

  #issue a dfs -ls -R command on DATA_DIR, it has 31 items on NN2
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${FS2}${DATA_DIR}"
  echo $cmd
  eval $cmd

  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
    
  #restart the NN to reset everything
  resetNode $DEFAULT_NN "namenode" "stop"
  resetNode $DEFAULT_NN "namenode" "start"

  #wait for 5 secs
  sleep 5

  #take the nn out of safemode
  takeNNOutOfSafemode $DEFAULT_FS
  
  new_val=$(getJMXPropertyValue "$NN2" "$NN_PID" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value: $new_val"
  COMMAND_EXIT_CODE=0
  local change=$[$new_val - $CURRENT_VALUE]
  if [ "$change" -ne "31" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference between the new value: $new_val and the old value: $CURRENT_VALUE on namenode $NN2 is not 31 after restart of namenode $DEFAULT_NN"
  fi

  #display the test case result
  displayTestCaseResult
}

#THIS TEST REQUIRES MULTIPLE NN SETUP
#Issue multiple dfs -ls and dfs -ls -R commands on multiple NNs. Make sure that the metric is updated for all appropriate NNs with the correct value
#Steps:
#
#Expected:
#TC ID=1313856
function test_FilesInGetListingOps9
{
  TESTCASENAME="FilesInGetListingOps9"
  echo "Running Test Case: $TESTCASENAME"
  
  #if NN2 is empty then do not run this test
  if [ -z "$NN2" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The cluster $CLUSTER only has 1 NN. This test requires more than 1 NN."
    #display the test case result
    displayTestCaseResult
    return
  fi
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
  
  #get the latest info for default NN
  resetInfo $DEFAULT_NN

  local nn1_pid=$NN_PID
  local nn1_prop_value=$CURRENT_VALUE
  
  #get the latest info for NN2
  resetInfo $NN2

  local nn2_pid=$NN_PID
  local nn2_prop_value=$CURRENT_VALUE

  #issue a dfs -ls -R command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls command on DATA_DIR, it has 6 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls command on dir01, it has 15 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${DEFAULT_FS}${DATA_DIR}/dir01"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls -R command on DATA_DIR, it has 31 items
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${DEFAULT_FS}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls -R command on DATA_DIR, it has 31 items on NN2
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls -R ${FS2}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls command on DATA_DIR, it has 6 items on NN2
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${FS2}${DATA_DIR}"
  echo $cmd
  eval $cmd
  
  #issue a dfs -ls command on dir01, it has 15 items on NN2
  local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls ${FS2}${DATA_DIR}/dir01"
  echo $cmd
  eval $cmd
  
  #wait 10sec longer than the refresh time and check the value
  waitForRefresh
 
  #get the new value for NN1, it should have 83 more
  nn1_new_val=$(getJMXPropertyValue "$DEFAULT_NN" "$nn1_pid" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value(${DEFAULT_NN}): $nn1_new_val"
  
  #get the new value for NN2, it should have 52 more
  nn2_new_val=$(getJMXPropertyValue "$NN2" "$nn2_pid" "$SERVICE" "$PROPERTY" "$HDFS_SUPER_USER" "$HDFS_SUPER_USER_KEY_FILE")
  echo "New value(${NN2}): $nn2_new_val"
  
  local nn1_change=$[$nn1_new_val - $nn1_prop_value]
  local nn2_change=$[$nn2_new_val - $nn2_prop_value]
  COMMAND_EXIT_CODE=0
  REASONS=''
  #check the change for nn1
  if [ "$nn1_change" -ne "83" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${nn1_prop_value}, and the new value: $nn1_new_val for property: $PROPERTY is not 83 on namenode $DEFAULT_NN"
    COMMAND_EXIT_CODE=1
    REASONS=''
  fi
  #check the change for nn2
  if [ "$nn2_change" -ne "52" ]
  then
    COMMAND_EXIT_CODE=1
    REASONS="The difference in the old value: ${nn2_prop_value}, and the new value: $nn2_new_val for property: $PROPERTY is not 52 on namenode $NN2.${REASONS}"
    COMMAND_EXIT_CODE=1
  fi

  #display the test case result
  displayTestCaseResult
}

#####################################################################################################
#MAIN 
#####################################################################################################
OWNER=arpitg

#setup the tests
setup

#execute all the tests that start with test_
executeTests

#teardown any unwanted info
teardown

