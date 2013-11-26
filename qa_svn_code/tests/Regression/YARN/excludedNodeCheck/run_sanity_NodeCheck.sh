#!/bin/bash 

. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/locallibrary.sh
. $WORKSPACE/lib/user_kerb_lib.sh

#################################################################
# This test case tests the exclude node functionality
# Get the list of active trackers and add a few to mapred.exclude file 
# Now refresh nodes from gateway and go to the jobtracker UI to see the effect
#################################################################

# Environment variables
export JOBTRACKER="None"
export USER_ID=`whoami`
export NUM_TESTCASE=0
export NUM_TESTPASSED=0

# set the no of nodes to be excluded
export EXCLUDED_NODES=4
# config file location for JT 
myconfDir=/homes/$USER_ID/customconf
customSetUp="DEFAULT"
excludeSuccess="FALSE"

customConfDir="/homes/$USER_ID/tmp/CustomConfDir"
customNMConfDir="/homes/$USER_ID/tmp/CustomNMConfDir"


##################################################################
# This is a prerequisite funtion that makes sure all  set is done
##################################################################

function preRequisites {
    # Create a file to record the out of jobtracker starting
    createFile $ARTIFACTS_DIR/TT_hosts
}

function getActiveNodeCount {
    $HADOOP_MAPRED_CMD job -list-active-trackers |grep tracker_ |wc -l
}

######################################################################################################
######################################################################################################
#Main Test Functions
######################################################################################################
#########################################################
# Test case to exclude nodes
#########################################################
function test_ExcludeActiveNodes {
    echo "*************************************************************"
    echo "*  TESTCASE:  Check the exclude Node functionality by populating mapred.exclude file on JT "
    echo "***************************************************************"
    TESTCASE_DESC="Exclude active nodes"
    displayTestCaseMessage $TESTCASE_DESC
    TESTCASE_ID="Exclude01"
    displayHeader $TESTCASE_ID - $TESTCASE_DESC test case started
    # Initialize variables
    EXCLUDED_NODES=4
    NEW_EXCLUDED_NODES=0            
    preRequisites
    # Check if the env is pointing to customconfig as expected
    if [ $customSetUp == "DEFAULT" ] ; then
        setRMandNMTestEnv $customConfDir  
    fi
    local initialActiveNodeCount=`getActiveNodeCount`
    # The following command will get the list of active trackers to be excuded and save it in a file
    get_TT_to_exclude
    if [ $EXCLUDED_NODES -lt 1 ] ; then
        dumpmsg "*** $TESTCASE_ID test case failed"  
        setFailCase "No TaskTracker host available to be excluded"
        echo "No TaskTracker host available to be excluded"
        windUp
        return;
    fi

    # Getting ResourceManager and NodeManagers
    local rmHost=`getResourceManagerHost` 
    local nmHost=`getActiveTaskTrackers | tr '\n' ','`
    if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no name node"
    	exit 1
	fi
	
    getFileFromRemoteServer $rmHost $customConfDir/yarn-site.xml $ARTIFACTS_DIR/yarn-site.xml
    addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.resourcemanager.nodes.exclude-path" "$customConfDir/mapred.exclude"
    # Moving the mapred file to JT host
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/yarn-site.xml $customConfDir/yarn-site.xml
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/TT_hosts $customConfDir/mapred.exclude
    
    #restart only RM
	setUpRMWithConfig $rmHost $customConfDir 
    
    local finalActiveNodeCount=`getActiveNodeCount`
    if [ $finalActiveNodeCount -lt $initialActiveNodeCount ] ; then
        echo " Exclusion of nodes reflected correctly in the check and so passing the tests "
        COMMAND_EXIT_CODE=0
        excludeSuccess="TRUE"
    else
        echo " The check for excluded nodes did not account for and so failing the test case "
        setFailCase "UI did not reflect the excluded node count and so failed "
    fi
    
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/empty_mapred.exclude $customConfDir/mapred.exclude
    setUpRMandNMWithConfig $rmHost $nmHost "NONE" "NONE"
    customSetUp="DEFAULT"
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#########################################################
# Test case to include excluded nodes
#########################################################
# This test should be removed due to 4634445
function toremove_test_IncludeOfExcludedNodes {
    echo "*************************************************************"
    echo "* TESTCASE: test the include of excluded nodes * "
    echo "*************************************************************"
    TESTCASE_DESC="Included Exclude active nodes"
    displayTestCaseMessage $TESTCASE_DESC
    TESTCASE_ID="IncludeOfExcludedNodes01"
    local rmHost=`getResourceManagerHost`
    local initialActiveNodeCount=`getActiveNodeCount`
    # This test case depends on the previous test case execution
    # A flag is set in previous test so this test can continue based on the state of the cluster
    if [ $excludeSuccess == "FALSE" ] ; then
        echo " The env is not in that state based on previous test case and so marking it as failed "
        setFailCase " Unable to set the env in a state with excluded nodes and so cannont proceed with include functionality and so marking the test as failed "
        windUp
        return
    fi
    
    local nmHost=`getActiveTaskTrackers | tr '\n' ','`
    if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no name node"
    	exit 1
	fi
	
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/empty_mapred.exclude $customConfDir/mapred.exclude   
    $HADOOP_MAPRED_HOME/bin/yarn rmadmin -refreshNodes             
	#restart only RM
	#setUpRMWithConfig $rmHost $customConfDir 
    
    local finalActiveNodeCount=`getActiveNodeCount`               
    if [ $finalActiveNodeCount -gt $initialActiveNodeCount ] ; then
        echo " Inclusion of nodes reflected correctly in the check and so passing the tests "
        COMMAND_EXIT_CODE=0
    else
        echo " The check for excluded nodes did not account for and so failing the test case "
        setFailCase "UI did not reflect the excluded node count and so failed "
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function get_all_TT {
    for (( i=0; i < 5; i++ )); do
        echo "Looping in to get the active trackers $i times "
        getActiveTaskTrackers > $ARTIFACTS_DIR/TT_host_temp
        cat $ARTIFACTS_DIR/TT_host_temp |grep yahoo.com >$ARTIFACTS_DIR/TT_host_fulllist
        cat $ARTIFACTS_DIR/TT_host_fulllist
        local mycount=`cat $ARTIFACTS_DIR/TT_host_fulllist |wc -l`
        if [ $mycount -gt 0 ] ; then
            break
        else
            sleep 10
        fi
    done        
    return $mycount
}

function get_TT_to_exclude {
    get_all_TT
    local mycount=$?
    if [ $mycount -lt 1 ] ; then
        echo "ERROR: Number of active task tracker is less than 1: $mycount"
        EXCLUDED_NODES=$mycount
        return 
    elif [ $mycount -lt 5 ] ; then
        EXCLUDED_NODES=$mycount
    else
        EXCLUDED_NODES=4
    fi

    cat $ARTIFACTS_DIR/TT_host_fulllist |head -$EXCLUDED_NODES > $ARTIFACTS_DIR/TT_hosts
    echo "*************************************"
    echo "Number of trackers excluded is as follows "
    cat $ARTIFACTS_DIR/TT_hosts
    echo "*************************************"
}

function test_ExcludeActiveNodesUsingNodeRefresh {
    echo "*************************************************************"
    echo "*  TESTCASE:  Check the exclude Node functionality by populating mapred.exclude file on JT "
    echo "***************************************************************"
    TESTCASE_DESC="testExcludeActiveNodesUsingNodeRefresh"
    displayTestCaseMessage $TESTCASE_DESC
    TESTCASE_ID="Exclude01"
    displayHeader $TESTCASE_ID - $TESTCASE_DESC test case started
    # Initialize variables
    preRequisites
    # Check if the env is pointing to customconfig   as expected
    if [ $customSetUp == "DEFAULT" ] ; then
        setRMandNMTestEnv $customConfDir
    fi
    local initialActiveNodeCount=`getActiveNodeCount`
    # The following command will get the list of active trackers to be excuded and save it in a file
    get_TT_to_exclude
    if [ $EXCLUDED_NODES -lt 1 ] ; then
        dumpmsg "*** $TESTCASE_ID test case failed"  
        setFailCase "No TaskTracker host available to be excluded"
        echo "No TaskTracker host available to be excluded"
        windUp
        return;
    fi

    # Getting JobTracker hostname
    local rmHost=`getResourceManagerHost`
    getFileFromRemoteServer $rmHost $customConfDir/yarn-site.xml $ARTIFACTS_DIR/yarn-site.xml
    addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.resourcemanager.nodes.exclude-path" "$customConfDir/mapred.exclude"
    # Moving the mapred file to JT host
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/yarn-site.xml $customConfDir/yarn-site.xml
    
    local nmHost=`getActiveTaskTrackers | tr '\n' ','`
    if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no name node"
    	exit 1
	fi
    setUpRMandNMWithConfig $rmHost $nmHost $customConfDir $customConfDir
    
    sleep 10        
    echo "###########################################"
    cat $customConfDir/mapred.exclude
    echo "###########################################"
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/TT_hosts $customConfDir/mapred.exclude
    $HADOOP_MAPRED_HOME/bin/yarn rmadmin -refreshNodes
    sleep 5
    local finalActiveNodeCount=`getActiveNodeCount`
    if [ $finalActiveNodeCount -lt $initialActiveNodeCount ] ; then
        echo " Exclusion of nodes reflected correctly in the check and so passing the tests "
        COMMAND_EXIT_CODE=0
    else
        echo " The check for excluded nodes did not account for and so failing the test case "
        setFailCase "UI did not reflect the excluded node count and so failed "
    fi
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/empty_mapred.exclude $customConfDir/mapred.exclude
    setUpRMandNMWithConfig $rmHost $nmHost "NONE" "NONE"
    customSetUp="DEFAULT"
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


#########################################################
# Test case to check if only authorized folks can do noderefresh
#########################################################
function test_NonPrivilegedUserExcludedNodes {
    echo "*************************************************************"
    echo "*  TESTCASE:  Check the privilege of a user requesting node refresh "
    echo "***************************************************************"
    TESTCASE_DESC="testNonPrivilegedUserExcludedNodes"
    displayTestCaseMessage "$TESTCASE_DESC"
    EXCLUDED_NODES=4
    preRequisites
    local initialActiveNodeCount=`getActiveNodeCount`
    # Check if the env is pointing to customconfig   as expected
    if [ $customSetUp == "DEFAULT" ] ; then
        setRMandNMTestEnv $customConfDir
    fi
    # The following command will get the list of active trackers to be excuded and save it in a file
    for (( i=0; i < 5; i++ )); do
        echo "Looping in to get the active trackers $i times "
        #getActiveTaskTrackers > $ARTIFACTS_DIR/TT_host_temp
        #cat $ARTIFACTS_DIR/TT_host_temp |grep '^[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' >$ARTIFACTS_DIR/TT_host_fulllist
		getActiveTaskTrackers > $ARTIFACTS_DIR/TT_host_fulllist
        local mycount=`cat $ARTIFACTS_DIR/TT_host_fulllist |wc -l`
        if [ $mycount -gt 0 ] ; then
            cat $ARTIFACTS_DIR/TT_host_fulllist
            break
        else
            sleep 10
        fi
    done
    #cat $ARTIFACTS_DIR/TT_host_fulllist |grep '^[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' |awk '{if (NR <= 4 ) {print}}'> $ARTIFACTS_DIR/TT_hosts
	cat $ARTIFACTS_DIR/TT_host_fulllist | awk '{if (NR <= 4 ) {print}}'> $ARTIFACTS_DIR/TT_hosts
    echo "*************************************"
    echo " No of trackers excluded is  as follows "
    cat $ARTIFACTS_DIR/TT_hosts
    echo "*************************************"
    # Getting JobTracker hostname
    local rmHost=`getResourceManagerHost`
    getFileFromRemoteServer $rmHost $customConfDir/yarn-site.xml $ARTIFACTS_DIR/yarn-site.xml
    addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.resourcemanager.nodes.exclude-path" "$customConfDir/mapred.exclude" 
    addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.admin.acl" "hadoop1"
    # Moving the mapred file to RM host
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/yarn-site.xml $customConfDir/yarn-site.xml
    
	local nmHost=`getActiveTaskTrackers | tr '\n' ','`
	if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no name node"
    	exit 1
	fi
    setUpRMandNMWithConfig $rmHost $nmHost $customConfDir $customConfDir
    
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/TT_hosts $customConfDir/mapred.exclude
    # Setting the user has hdfsqa user 
    setKerberosTicketForUser $HDFSQA_USER
    $HADOOP_MAPRED_HOME/bin/yarn rmadmin -refreshNodes
    sleep 5
    local finalActiveNodeCount=`getActiveNodeCount`
    if [ $finalActiveNodeCount -lt $initialActiveNodeCount ] ; then
        echo " The check for excluded nodes did not account for and so failing the test case "
        setFailCase "UI did not reflect the excluded node count and so failed "        
    else
        echo " Exclusion of nodes reflected correctly in the check and so passing the tests "
        COMMAND_EXIT_CODE=0
    fi
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/empty_mapred.exclude $customConfDir/mapred.exclude    
    setUpRMandNMWithConfig $rmHost $nmHost "NONE" "NONE"    
    customSetUp="DEFAULT"
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


#########################################################
# test case to check if only authorized folks can do noderefresh
#########################################################
function test_MapredQaUserExcludedNodes {
    echo "*************************************************************"
    echo "*  TESTCASE:  Check the privilege of a user requesting node refresh "
    echo "***************************************************************"
    TESTCASE_DESC="testNonPrivilegedUserExcludedNodes"
    displayTestCaseMessage "$TESTCASE_DESC"
    EXCLUDED_NODES=4
    preRequisites
    local initialActiveNodeCount=`getActiveNodeCount`
    # Check if the env is pointing to customconfig   as expected
    if [ $customSetUp == "DEFAULT" ] ; then
        setRMandNMTestEnv $customConfDir
    fi
    # The following command will get the list of active trackers to be excuded and save it in a file
    # The following command will get the list of active trackers to be excuded and save it in a file
    get_TT_to_exclude
    if [ $EXCLUDED_NODES -lt 1 ] ; then
        dumpmsg "*** $TESTCASE_ID test case failed"  
        setFailCase "No TaskTracker host available to be excluded"
        echo "No TaskTracker host available to be excluded"
        windUp
        return;
    fi

    # Getting JobTracker hostname
    local rmHost=`getResourceManagerHost`
    getFileFromRemoteServer $rmHost $customConfDir/yarn-site.xml $ARTIFACTS_DIR/yarn-site.xml
    addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.resourcemanager.nodes.exclude-path" "$customConfDir/mapred.exclude"
    #addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.server.resourcemanager.admin.acls" "hadoop1"
	addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.admin.acl" "hadoop"
    # Moving the mapred file to RM host
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/yarn-site.xml $customConfDir/yarn-site.xml
    
    local nmHost=`getActiveTaskTrackers | tr '\n' ','`
    if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no name node"
    	exit 1
	fi
    setUpRMandNMWithConfig $rmHost $nmHost $customConfDir $customConfDir
    
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/TT_hosts $customConfDir/mapred.exclude
    # Setting the user has hdfsqa user 
    setKerberosTicketForUser $MAPREDQA_USER
    $HADOOP_MAPRED_HOME/bin/yarn rmadmin -refreshNodes
    sleep 5
    local finalActiveNodeCount=`getActiveNodeCount`
    if [ $finalActiveNodeCount == $initialActiveNodeCount ] ; then
        echo " Exclusion of nodes reflected correctly in the check and so passing the tests "
        COMMAND_EXIT_CODE=0
    else
        echo " The check for excluded nodes did not account for and so failing the test case "
        setFailCase "UI did not reflect the excluded node count and so failed "
    fi
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/empty_mapred.exclude $customConfDir/mapred.exclude
    setUpRMandNMWithConfig $rmHost $nmHost "NONE" "NONE"
    customSetUp="DEFAULT"
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

# This test should be removed due to 4634445
function toremove_test_AllotmentOfTasksToRecentlyIncludedNodes {
    echo "*************************************************************"
    echo "*  TESTCASE:  Check the privilege of a user requesting node refresh "
    echo "***************************************************************"
    TESTCASE_DESC="testNonPrivilegedUserExcludedNodes bug : 4634445"
    local captureFile="/tmp/t30.out"
    if [ $customSetUp == "DEFAULT" ] ; then
        setUpTestEnv "capacity-scheduler.xml" 
    fi
    # exclude  a lot of nodes and submit a job with lot of maps and reduces
    # now include the excluded nodes and look tasks being assigned to the included nodes.
    # The following command will get the list of active trackers to be excuded and save it in a file
    get_TT_to_exclude
    if [ $EXCLUDED_NODES -lt 1 ] ; then
        dumpmsg "*** $TESTCASE_ID test case failed"  
        setFailCase "No TaskTracker host available to be excluded"
        echo "No TaskTracker host available to be excluded"
        windUp
        return;
    fi

    # Getting JobTracker hostname
    local rmHost=`getResourceManagerHost`
    getFileFromRemoteServer $rmHost $customConfDir/yarn-site.xml $ARTIFACTS_DIR/yarn-site.xml
    addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.resourcemanager.nodes.exclude-path" "$customConfDir/mapred.exclude"
    #  addPropertyToXMLConf "$ARTIFACTS_DIR/yarn-site.xml" "yarn.server.resourcemanager.admin.acls" "hadoop1"
    # Moving the mapred file to RM host
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/yarn-site.xml $customConfDir/yarn-site.xml
    
    local nmHost=`getActiveTaskTrackers | tr '\n' ','`
    if [ "X${nmHost}X" == "XX" ]; then
    	echo "There is no name node"
    	exit 1
	fi
	
    setUpRMandNMWithConfig $rmHost $nmHost $customConfDir $customNMConfDir
    
    
    putFileToRemoteServer $rmHost $ARTIFACTS_DIR/TT_hosts $customConfDir/mapred.exclude
    # Setting the user has hadoopqa user 
    setKerberosTicketForUser $HADOOPQA_USER
    $HADOOP_MAPRED_HOME/bin/yarn rmadmin -refreshNodes
    sleep 5
    # Now submit a job to the cluster with reduced node
    submitSleepJobToQueue "default" "$captureFile" 100 100 50000 50000
    # get the job id of the job submitted
    local myjobId=`getJobId $captureFile`
    
    putFileToRemoteServer $rmHost ${JOB_SCRIPTS_DIR_NAME}/data/empty_mapred.exclude $customConfDir/mapred.exclude
    $HADOOP_MAPRED_HOME/bin/yarn rmadmin -refreshNodes
    sleep 20
    # Need to get the the NM host that was recently included 
    # Navigate to log folder to see if there is any task for this app assigned
    # Till then mark this test case as failed 
    #setFailCase " Unable to include nodes via noderefresh and so failing the test case " 
    customSetUp="DEFAULT"
    displayTestCaseResult
    return $COMMAND_EXIT_CODE    
}                       

#execute all tests that start with test_
executeTests

#test_ExcludeActiveNodes
#test_IncludeOfExcludedNodes
#test_ExcludeActiveNodesUsingNodeRefresh
#test_MapredQaUserExcludedNodes
#test_AllotmentOfTasksToRecentlyIncludedNodes
#test_NonPrivilegedUserExcludedNodes

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
