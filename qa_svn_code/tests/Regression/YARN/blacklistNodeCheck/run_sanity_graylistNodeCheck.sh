#!/bin/bash

. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/locallibrary.sh

#################################################################
# This test case tests the exclude node functionality
# Get the list of active trackers and add a few to mapred.exclude file 
# Now refresh nodes from gateway and go to the jobtracker UI to see the effect
#################################################################

export JOBTRACKER="None"
export EXCLUDENODESTEST=0
export JOBCOUNT=1
export GRAYLISTED_NODES=1

# Setting Owner of this TestSuite
OWNER="vmotilal"

#This is the location of config file for JT
myconfDir="/tmp/customconf"
customSetUp="default"

WAIT_TIME=60

###########################################
# Function to display a message on success of a test
###########################################
function displayJobSummaryFailure {
    displayHeader "$TESTCASE_DESC FAILED"
}

###########################################
# Function to display a message on failure of a test
###########################################
function displayJobSummarySuccess {
    displayHeader "$TESTCASE_DESC PASSED"
}


###########################################
# Function to  cleanup the settings
###########################################
function cleanUp {
    updateMaxTrackerBlacklistsCount 16
    getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    modifyValueOfAField $ARTIFACTS_DIR/mapred-site.xml mapred.jobtracker.blacklist.fault-timeout-window "180"
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
    startJobTracker $JOBTRACKER
    sleep 15
    startJobTracker $JOBTRACKER
    sleep 15
}

################################################################
# This function creates all the prerequisite files required 
# for the test run
################################################################
function preRequisites {
    # Create a file to record the out of jobtracker starting
    createFile $ARTIFACTS_DIR/jtStart.log
    createFile $ARTIFACTS_DIR/jtStop.log
    createFile $ARTIFACTS_DIR/mapred-site.xml
}


#####################################################################################
# This function checks if the job is complete or is still running
# This function takes the job id
#####################################################################################
function checkForJobCompletion {
    response=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -status $1 | grep reduce | grep completion |grep 1.0 `
    if [ -z "$response" ] ; then
        echo " The job $1 is not completed yet "
        return 1
    else
        echo " The job $1 is completed "
        return 0
    fi
}


######################################################################################
# This function returns the JOBID of the task submitted
######################################################################################
function checkClusterForNoJobs {
    local match=$($HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR job -list | grep "0 jobs currently running")
    if [ -z "$match" ]; then    
        JOBCOUNT=1
    else
        JOBCOUNT=0
    fi
}


######################################################################################
# This function returns the JOBID of the task submitted
# This function takes host name as a param
######################################################################################
function stopTaskTracker {
    ssh -i /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$1 exec  $HADOOP_HOME/bin/hadoop-daemon.sh stop tasktracker
}

######################################################################################
# This function returns the JOBID of the task submitted
# This function takes host name as a param
######################################################################################
function startTaskTracker {
    ssh -i /home/y/var/pubkeys/mapred/.ssh/flubber_mapred_dsa mapred@$1 exec  $HADOOP_HOME/bin/hadoop-daemon.sh start tasktracker
}


######################################################################################
# This function  updates the timeout value for graylisting
# This function takes timeout value as a param $1
# $2 will take the path to config dir
######################################################################################
function updateGraylistTimeoutWindow {
    local mytimeOut=$1
    local myconfDir=$2
    echo " Now setting $mytimeOut value to $myconfDir "
    if [ "$JOBTRACKER" == "None" ] ; then
        echo "Empty job tracker host and so getting the hostname "
        getJobTracker
        echo " Got the job tracker host name  and it is  $JOBTRACKER "
    fi

    # Get the mapred-site.xml locally
    getFileFromRemoteServer $JOBTRACKER $myconfDir/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml
    timeout=`getValueFromField $ARTIFACTS_DIR/mapred-site.xml mapred.jobtracker.blacklist.fault-timeout-window`
    echo " the time out value set in mapred-site.xml is set to $timeout"
    if [ -z "$timeout" ] ; then
        echo " unable to read the value set in the mapred xml site for the timeout "
        return 1
    fi
    if [ "$timeout" -eq "$1" ] ; then
        echo "The value of timout in mapred-site.xml is already set to 5 and so we are good to proceed "       
        return 0
    fi
    #Modify the mapred-site to xml
    modifyValueOfAField $ARTIFACTS_DIR/mapred-site.xml mapred.jobtracker.blacklist.fault-timeout-window $1
    putFileToRemoteServer $JOBTRACKER $ARTIFACTS_DIR/mapred-site.xml $myconfDir/mapred-site.xml
    startJobTracker $JOBTRACKER
    sleep 15
    startJobTracker $JOBTRACKER
    sleep 15
    return 0
}


# Get the mapred-site.xml locally
getFileFromRemoteServer $JOBTRACKER $HADOOP_CONF_DIR/mapred-site.xml $ARTIFACTS_DIR/mapred-site.xml


#####################################################################################
# This function logs on to the task tracker host and kills the task
# This function requires host name of task trackers
######################################################################################
function graylistTTNode {
    myconfDir=$1
    triggerSleepJob 200 3 50000 1000 1

    # Wait for the job id
    myJobId=`waitForJobId $WAIT_TIME`
    if [ $? -eq 1 ] ; then
        echo " Unable to get the job id and so failing "
        dumpmsg "*** $TESTCASE_ID test case failed"
        setFailCase " unable to get the job id and so cannot proceed and faling the test case "
        windUp
        return -1;
    fi

    # Wait for the attempt id for the job
    myattemptId=`waitForAttemptIdsForJobId $myJobId map $WAIT_TIME`;
    if [ $? -eq 1 ] ; then
        echo " Unable to get the attempt id "
        dumpmsg "*** $TESTCASE_ID test case failed"        
        setFailCase "Unable to get the attempt id and so failing "
        windUp 
        return -1;
    fi
    echo " The attempt id interested is $myattemptId"

    # Wait for the task to get started on a TT host and fetch the TT hostname
    taskTracker=`waitForTTHostForAttemptId $myattemptId MAP $WAIT_TIME`

    x=0
    while [ 1 ]; do
        ssh $taskTracker  ps -ef |grep hadoopqa |grep attempt_  |while read data; do killProcessOnTT $taskTracker $data; done
        sleep 10
        x=`expr $x + 1`

        getCountOfGrayListedNodes 1
        if [ $NEW_GRAYLISTED_NODES -gt 0 ] ; then
            break
        fi

        # checkClusterForNoJobs           
        # if [ $JOBCOUNT -eq 0 ] ; then
        #     break
        # fi      

        if [ $x -ge 50 ]; then
            echo " Exiting out of make Node graylisted based on try count and not based on confirmation "
            break
        fi
    done

    return $NEW_GRAYLISTED_NODES
}


#####################################################################################
# This function gets the count of GrayListedNodes from the UI
#####################################################################################
function getCountOfGrayListedNodes {
    getJobTracker
    y=0
    while [ $y -le $1 ]; do
        wget --quiet "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jt_content
        NEW_GRAYLISTED_NODES=`grep -A 1 "graylisted" $ARTIFACTS_DIR/jt_content |awk -F'graylisted' '{print $2}' |cut -d '>' -f2 |cut -d '<' -f1`  
        y=`expr $y + 1 `
        sleep 10
        echo " count of graylisted nodes got from UI is $NEW_GRAYLISTED_NODES "
        checkClusterForNoJobs           
        if [ $JOBCOUNT -eq 0 ] ; then
            break
        fi      
        if [ $NEW_GRAYLISTED_NODES -gt 0 ] ; then
            break
        fi
    done
}

function getCountOfGrayListededNodesOnUI {
    getJobTracker
    wget --quiet "http://$JOBTRACKER:50030/jobtracker.jsp" -O $ARTIFACTS_DIR/jt_content
    grep -A 1 "graylisted" $ARTIFACTS_DIR/jt_content |awk -F'graylisted' '{print $2}' |cut -d '>' -f2 |cut -d '<' -f1
}

function restartJobTracker {
    getJobTracker
    stopJobTracker $JOBTRACKER
    sleep 15
    startJobTracker $JOBTRACKER
    sleep 15
}


#####################################################################################
#####################################################################################
# Testcases
#####################################################################################
#####################################################################################

#########################################################
#  Test case to check the graylisting of node functionality
#########################################################
function testGrayListNode {
    echo "*************************************************************"
    echo "* TESTCASE : Test the graylisting of Node functionality "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=0
    TESTCASE_DESC="checkgrayListNode"
    TESTCASE_ID="gray01"
    displayTestCaseMessage "$TESTCASE_DESC"

    # Check if the env is as expected
    if [ $customSetUp == "default" ] ; then
        setTestEnv $myconfDir
    fi      

    getCountOfGrayListedNodes 1
    if [ "$NEW_GRAYLISTED_NODES" -gt 0 ] ; then
        restartJobTracker
    fi

    # Set the blacklist count of job in mapred-site.xml to 1
    getJobTracker
    getCountOfGrayListedNodes 1

    updateMaxTrackerBlacklistsCount 1 $myconfDir
    if [ $? -eq 1 ] ; then
        echo " Unable to update the max tracker blacklist to 1 so $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed" 
        setFailCase " Unable to update the blacklist count threshold  in mapredsite.xml  and so failing "
        windUp
        return
    fi      

    graylistTTNode $myconfDir
    NEW_GRAYLISTED_NODES=$?

    if [ $GRAYLISTED_NODES -le $NEW_GRAYLISTED_NODES ]; then
        echo " GrayListed  Nodes test cases  PASSED ....."
        dumpmsg "*** $TESTCASE_ID test case passed"
    else
        echo " graylisted nodes = $GRAYLISTED_NODES, new graylisted nodes = $NEW_GRAYLISTED_NODES"
        echo " Did not get the expected nodes count in the JT UI and marking the test case failed .. "
        dumpmsg "*** $TESTCASE_ID test case failed"
        setFailCase " Did not get the expected nodes count in the JT UI and marking the testcases failed"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
    # cleanup the state
    # updateMaxTrackerBlacklistsCount 16
    # EXCLUDENODESTEST=1
}


function testGrayListNodeAutoCure {
    echo "*************************************************************"
    echo "* TESTCASE : Test the graylisting of Node that cures automatically "
    echo "* after certain threshold is met functionality "
    echo "*************************************************************"
    COMMAND_EXIT_CODE=0 
    TESTCASE_DESC="checkgrayListNodeAutoCure"
    TESTCASE_ID="gray01"
    displayTestCaseMessage "$TESTCASE_DESC"

    # Check if the env is as expected
    if [ $customSetUp == "default" ] ; then
        setTestEnv $myconfDir
    fi

    checkClusterForNoJobs
    if [ $JOBCOUNT -ge 1 ] ; then
        echo " The job count is not empty looks like some job is running "
        setFailCase " The job count is not empty looks like some job is running "
        windUp
        return  
    fi

    getJobTracker

    updateGraylistTimeoutWindow 5 $myconfDir
    if [ $? -eq 1 ] ; then
        echo "there has been an issue in setting the graylist time limit and so failing "
        setFailCase "there has been an issue in setting the graylist time limit and so failing "
        # updateGraylistTimeoutWindow 180
        windUp
        return
    fi    
    
    getCountOfGrayListedNodes 1
    if [ "$NEW_GRAYLISTED_NODES" -gt 0 ] ; then
        restartJobTracker
    fi

    # Set the blacklist count of job in mapred-site.xml to 1
    # getJobTracker
    getCountOfGrayListedNodes 1

    updateMaxTrackerBlacklistsCount 1 $myconfDir
    if [ $? -eq 1 ] ; then
        echo " Unable to udpate the max tracker blacklist  to 1  so  $TESTCASE_DESC FAILED "
        setFailCase " Unable to udpate the max tracker blacklist  to 1  so  $TESTCASE_DESC FAILED "
        dumpmsg "*** $TESTCASE_ID test case failed"
        # updateGraylistTimeoutWindow 180
        windUp
        return
    fi

    graylistTTNode $myconfDir
    NEW_GRAYLISTED_NODES=$?    

    if [ $NEW_GRAYLISTED_NODES -eq 0 ] ; then
        echo "Unable to get the env with graylisted nodes " 
        setFailCase "Unable to get the env with graylisted nodes " 
        # updateGraylistTimeoutWindow 180  
        windUp  
        return
    fi

    echo " Now that we have the graylisted nodes proceeding to test autocure "
    triggerSleepJob 200 3 50000 1000 1
    sleep 20
    myJobId=`getJobId`    
    
    count=0
    while [ $count -le 30 ]; do
        getCountOfGrayListededNodesOnUI
        if [ $? -eq 0 ] ; then
            NEW_GRAYLISTED_NODES=0
            break
        fi
        count=`expr $count +1`
        sleep 10
    done

    if [ $NEW_GRAYLISTED_NODES -eq 0 ]; then
        echo  " Auto cure of graylist nodes tests passed "
        # updateGraylistTimeoutWindow 180
    else
        setFailCase "Auto cure of gralist nodes test failed "
        # updateGraylistTimeoutWindow180
        windUp
        return
    fi
    displayTestCaseResult
    return $COMMAND_EXIT_CODE 
}


#################################################
# This testcase checks to see that restarting the JT clears off the 
# graylisting of node
#################################################
function testClearGrayListAfterRestartOfJT {
    echo "*************************************************************"
    echo "* TESTCASE : Test the undoing of graylisting of Node functionality "
    echo "*************************************************************"
    TESTCASE_DESC="checkUndograyListNode"
    COMMAND_EXIT_CODE=0
    TESTCASE_ID="gray01"
    displayTestCaseMessage "$TESTCASE_DESC"
    # Check if the env is as expected
    if [ $customSetUp == "default" ] ; then
        setTestEnv $myconfDir
    fi    
    getJobTracker
    getCountOfGrayListedNodes 5
    if [ $NEW_GRAYLISTED_NODES -gt 0 ]; then
        echo " There are nodes already gray listed and we can continue with the test"
    else
        echo " There are no nodes in gray listed stated  triggering grayListNode function"
        setFailCase "Please execute the graylist node testcase  and so marking this as failed "
        windUp
        return
    fi
    stopJobTracker $JOBTRACKER
    sleep 15
    startJobTracker $JOBTRACKER
    sleep 15
    getCountOfGrayListedNodes 5
    if [ $NEW_GRAYLISTED_NODES -eq 0 ]; then
        echo "****************** Graylisted nodes have been cleared**************** ..."
        dumpmsg "*** $TESTCASE_ID test case passed"
    else
        echo "****************** Graylisted nodes have not been cleared******************** "
        dumpmsg "*** $TESTCASE_ID test case failed"
        setFailCase "Graylisted nodes have not been cleared and so failing the test "
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function testClearGrayListAfterRestartOfTT {
    echo "*************************************************************"
    echo "* TESTCASE : Test the clearing of graylisting of Node functionality with the restart of faulty TaskTracker "
    echo "*************************************************************"
    TESTCASE_DESC="checkgrayListNodeAfterRestartOfFaultyTT"
    displayTestCaseMessage $TESTCASE_DESC
    # Check if the env is as expected
    if [ $customSetUp == "default" ] ; then
        setTestEnv $myconfDir
    fi
    # Set the blacklist count of job in mapred-site.xml to 1
    getJobTracker
    getCountOfGrayListedNodes 3
    if [ $NEW_GRAYLISTED_NODES -ne 0  ]; then
        echo " Restarting JT to clear any exisiting graylisted node" 
        stopJobTracker
        sleep 20
        startJobTracker
        sleep 20
    fi
    
    updateMaxTrackerBlacklistsCount 1 $myconfDir

    graylistTTNode $myconfDir
    NEW_GRAYLISTED_NODES=$?    

    local expGLCount=`expr $NEW_GRAYLISTED_NODES - 1 `
    if [ $NEW_GRAYLISTED_NODES -ge 0  ]; then
        echo " GrayListed  Nodes are available in the cluster .$taskTracker...."
        #  COMMAND_EXIT_CODE=0
            
        stopTaskTracker $taskTracker
        sleep 10
        startTaskTracker $taskTracker
        sleep 10
    else
        echo " The cluster does not have any blacklisted node to continue with this testing "
        setFailCase "The cluster does not hae any nodes in blacklisted state to continue and so failing"
        windUp
        return
    fi

    getCountOfGrayListedNodes 3
    # Clusters can have graylisted nodes not because of our tests and causes the count to be messedup
    #  This test triggered a node to be graylisted and so restarting that TT will reduce the count by one
    #       and that is what I am looking for to mark this test pass or fail
    if [ $NEW_GRAYLISTED_NODES -le $expGLCount  ]; then
        echo "*****************Testcase for clearGrayList after restart PASSED **************** "
    else
        echo "*****************Testcase for clearGrayList after restart failed  **************** "
        setFailCase "Testcase for clearGrayList after restart failed "  
    fi
    # checkJobSummaryResult $COMMAND_EXIT_CODE
    displayTestCaseResult
    return $COMMAND_EXIT_CODE

}

testGrayListNode
testClearGrayListAfterRestartOfJT
testClearGrayListAfterRestartOfTT
testGrayListNodeAutoCure

resetEnv $myconfDir
# cleanUp

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}
