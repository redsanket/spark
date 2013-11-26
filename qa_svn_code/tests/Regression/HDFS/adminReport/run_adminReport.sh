#!/bin/bash
#Uses bash features. Cannot be used with sh

# This is an automated test for
# - https://issues.apache.org/jira/browse/HADOOP-5094 (Show dead nodes information in dfsadmin -report)

# Load the library files
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

OWNER="raviprak"		# Email: raviprak@yahoo-inc.com hadoop-qe@yahoo-inc.com


#####################################
# PARAMETERS TO CUSTOMIZE TESTING
#####################################
MAX_WAIT=60				# The maximum amount of time to wait to see that dfsadmin report has been updated for 1 node
POLL_INTERVAL=1			# The interval at which namenode is queried for dfsadmin report while waiting
NUM_NODES_TO_KILL=2		# The number of nodes to first kill (checkDeadNodesReported) and then bring back up (checkAliveNodesReported)
USER_ID=hadoopqa		# User ID to use for sshing into namenodes while modifying configuration


############################################
# GLOBAL VARIABLES SHARED BETWEEN FUNCTIONS
############################################
KILLED_NODES=""		# Used to share the list of nodes brought down in checkDeadNodesReported() with checkAliveNodesReported()
ADMINREPORT_NN_CONFIG_DIR=""		# Used to remember which was the last namenode config directory used.
ipToHostnameMap=""		# Used to remember the mappings already found between ip addresses and hostnames

#################################
# Functions to help with setup
#################################

# Function to add property values to hdfs-site.xml. Can add new properties. Renames pre-existing properties.
# e.g. if property A exists, then it is renamed to test_old_A
# $1 The filename of the hdfs-site.xml. This file is modified as well
# $2 The filename of the modified hdfs-site.xml. Write permission must be available on this location
# $3,$4 Property Name to add / change, Value of the property name
# $5,$6 .... Pairs of property names and values can follow
function addPropertyToHdfsSite
{
	if [[ $# -lt 4 || -z $1 || -z $2 || -z $3 || $(( $# % 2 )) -ne 0 ]]; then
		echo "Usage: addPropertyToHdfsSite hdfs-site.xml hdfs-site.xml.new property value property value ...."
		echo "e.g. addPropertyToHdfsSite tmp/adminReport_conf/hdfs-site.xml tmp/adminReport_conf/hdfs-site.xml.new dfs.exclude /tmp/excludeFile.txt"
		return 1
	fi
	if [ "$1" == "$2" ]; then
		echo "Input file cannot be same as output file. Please specify another output file."
	fi

	# If the property already existed, rename it
	for (( i=3; i<$#; i=i+2 )); do
		eval name=\${$i}
		local cmd="sed -i 's:<name>\s*${name}\s*</name>:<name> test_old_${name} </name>:g' $1"
		echo "Running $cmd"
		eval $cmd
	done

	local FLAG=0
	echo -n '' > $2		# Important to not include a new line at the top of XML otherwise it'll be invalid
	while read; do
		if [[ $FLAG -eq 0 && "`echo \"$REPLY\" | grep '<property>'`" != "" ]] ; then
			for (( i=3; i<$#; i=i+2 )); do
				eval name=\${$i}
				eval value=\${`expr $i + 1`}
				echo -e "<property>\n\t<name>$name</name>\n\t<value>$value</value>\n\t<description>Some description</description>\n</property>" >> $2
				echo -e "Added\n<property>\n\t<name>$name</name>\n\t<value>$value</value>\n\t<description>Some description</description>\n</property>"
			done
			FLAG=1
		fi
		echo "$REPLY" >> $2
	done < $1
}


# On node $2, modify $3 (assuming it is hdfs-site.xml) to include (parameter,value)
# values ($4,$5) ($6,$7) etc
# $1 - Username to use to ssh into the namenode. Should have write access to $3
# $2 - Namenode address. Can be IP address as well as hostname
# $3 - Location of writable hdfs-site.xml. Note the filename is to be included as well
# $4,$5 - Property Name to add / change, Value of the property name
# $6,$7 .... Pairs of property names and values can follow
function changeHdfsSiteOnNode
{
	if [[ $# -lt 5 || -z $1 || -z $2 || -z $3 || $(( $# % 2 )) -eq 0 ]]; then
		echo -n -e "Usage: changeHdfsSiteOnNode username node path property value\ne.g. changeHdfsSiteOnNode "
		echo "hadoopqa gsbl90629.blue.ygrid.yahoo.com $HADOOP_CONF_DIR/hdfs-site.xml dfs.exclude /tmp/excludeFile.txt"
		return 1
	fi

	scp ${1}@$2:${3} .									#Copy remote hdfs-site.xml to local directory so it can be modified easily
	local HDFS_SITE=${3##*/}

	local cmd="addPropertyToHdfsSite ./$HDFS_SITE ./${HDFS_SITE}.new"
	for (( i=4; i<=$#; i++ )); do
		eval parameter=\$${i}
		cmd="$cmd \"$parameter\""
	done

	echo "Running $cmd"
	eval $cmd

	ssh ${1}@$2 mv ${3} ${3}.original		#Backed up old hdfs-site.xml files as hdfs-site.xml.original
	scp ./${HDFS_SITE}.new ${1}@$2:${3}		#Copy modified hdfs-site.xml to remote NN
	rm ./${HDFS_SITE}.new ./${HDFS_SITE}
}


# Changes hdfs-site.xml configuration to include dfs.namenode.heartbeat.recheck-interval and dfs.heartbeat.interval
# $1 - Username to use to ssh into the namenode. Should have write access to $3
# $2 - Namenode address. Can be IP address as well as hostname
# $3 - Path where a temporary hadoop configuration directory will be created
function changeHeartbeatInterval
{
	if [[ $# -ne 3 || -z $1 || -z $2 || -z $3 ]]; then
		echo -n -e "Usage: changeHeartbeatInterval username namenode path\ne.g. changeHeartbeatInterval "
		echo "hadoopqa gsbl90629.blue.ygrid.yahoo.com $HADOOP_CONF_DIR/"
		return 1
	fi

	echo "Changing heartbeat interval to 1 in namenode $2 configuration"
	changeHdfsSiteOnNode $1 $2 ${3}/hdfs-site.xml dfs.namenode.heartbeat.recheck-interval 1 dfs.heartbeat.interval 1
	echo "Changed heartbeat interval to 1 in namenode $2 configuration"
}


# Removes all slaves so that HDFS has 0 data nodes
# $1 - Username to use to ssh into the namenode. Should have write access to $3
# $2 - Namenode address. Can be IP address as well as hostname
# $3 - Path where a temporary hadoop configuration directory will be created
function removeAllSlaves
{
	if [[ $# -ne 3 || -z $1 || -z $2 || -z $3 ]]; then
		echo "Usage: removeAllSlaves username namenode path"
		echo "e.g. removeAllSlaves hadoopqa gsbl90629.blue.ygrid.yahoo.com $HADOOP_CONF_DIR/"
		return 1
	fi
	ssh ${1}@$2 mv ${3}/slaves ${3}/slaves.original						#Backed up old slaves file as slaves.original
	ssh ${1}@$2 touch ${3}/slaves
	echo "Removed all slaves from namenode $2 configuration"
}


# Adds datanodes to dfs.exclude and stops them
# $1 - Username to use to ssh into the namenode. Should have write access to $3
# $2 - Namenode address. Can be IP address as well as hostname
# $3 - Path where a temporary hadoop configuration directory will be created
# $4 - The \n separated list of nodes to add to dfs.exclude 
function addExcludedNodes
{
	if [[ $# -ne 4 || -z $1 || -z $2 || -z $3 ]]; then
		echo "Usage: addExcludedNodes username namenode path nodelist"
		echo "e.g. addExcludedNodes hadoopqa gsbl90629.blue.ygrid.yahoo.com /tmp/adminReport_conf gsbl90633.blue.ygrid.yahoo.com"
		return 1
	fi

	echo "Putting" $4 "in dfs.exclude on namenode $2"
	ssh ${1}@$2 rm ${3}/dfs.exclude				# This removes the symlink so that the original dfs.exclude isn't modified
	echo "$4" > dfs.exclude
	scp dfs.exclude ${1}@$2:${3}/dfs.exclude
	rm dfs.exclude
	echo "Put" $4 "in dfs.exclude on namenode $2"

	changeHdfsSiteOnNode $1 $2 ${3}/hdfs-site.xml dfs.hosts.exclude ${3}/dfs.exclude
}


# Setup HDFS configuration so that we can run tests.
# $1 - Username to use to ssh into the namenode. Should have write access to $2
# $2 - Path where a temporary hadoop configuration directory will be created
# $3 - if it is "heartbeat" then the dead nodes are marked dead (approx 30s) after being killed so that this test is not long running
#    - if it is "zeroNodes", then all the slave nodes are removed so that we can test admin report with 0 data nodes
#    - if it is "addExcludedNodes" then $4 is added to dfs.exclude
# $4 - The \n separated list of nodes to add to dfs.exclude
function setupNewAdminReportConfFile
{
	if [[ $# -lt 3 || $# -gt 4 || -z $1 || -z $2 || -z $3 || -z $HADOOP_CONF_DIR ]]; then
		echo "Usage: setupNewAdminReportConfFile username path nodelist"
		echo "e.g. setupNewAdminReportConfFile hadoopqa /tmp/adminReport_conf gsbl90633.blue.ygrid.yahoo.com"
		return 1
	fi

	local USER_ID=$1
	local NAMENODES=`getNameNodes`				# Get list of namenodes
	NAMENODES=`echo $NAMENODES | tr ';' '\n'`
	echo -e "On cluster $CLUSTER, Name Nodes: \n$NAMENODES"
	
	if [ "$3" == "heartbeat" ] ; then
		echo "Stopping NameNodes so that heartbeat interval configuration can be changed"
		resetNameNodes stop $ADMINREPORT_NN_CONFIG_DIR
		echo "Stopped NameNodes so that heartbeat interval configuration can be changed"
	elif [ "$3" == "zeroNodes" ]; then
		echo "Going to stop cluster so I can remove all slaves"
		stopCluster $ADMINREPORT_NN_CONFIG_DIR
	elif [ "$3" == "addExcludedNodes" ]; then
		echo "Stopping namenodes so that nodes may be added to dfs.exclude"
		resetNameNodes stop $ADMINREPORT_NN_CONFIG_DIR
		echo "Stopped namenodes so that nodes may be added to dfs.exclude"
		echo "Stopping nodes :" $4
		for line in `echo "$4"`; do
			echo "Stopping: \"${line}\""
			resetNode $line datanode stop
		done
		echo "Stopped nodes :" $4
	fi
	
	# Setup new config location on NNs
	local NEW_CONFIG_LOCATION=$2
	for NN in `echo "$NAMENODES"`; do 
		if [ -z `ssh ${USER_ID}@$NN ls -d "$NEW_CONFIG_LOCATION"` ] ; then
			echo "Creating new temporary directory ssh ${USER_ID}@$NN mkdir $NEW_CONFIG_LOCATION"
			ssh ${USER_ID}@$NN mkdir "$NEW_CONFIG_LOCATION"
		else
			echo "Cleaning out temporary directory ssh ${USER_ID}@$NN rm -rf \"$NEW_CONFIG_LOCATION/*\""
			ssh ${USER_ID}@$NN rm -rf "$NEW_CONFIG_LOCATION/*"
		fi
		ssh ${USER_ID}@$NN cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"	# Copy all the conf files to new location in NN host
		ssh ${USER_ID}@$NN chmod -R 755 "$NEW_CONFIG_LOCATION"

		if [ "$3" == "heartbeat" ] ; then
			changeHeartbeatInterval $USER_ID $NN $NEW_CONFIG_LOCATION
		elif [ "$3" == "zeroNodes" ]; then
			removeAllSlaves $USER_ID $NN $NEW_CONFIG_LOCATION
		elif [ "$3" == "addExcludedNodes" ]; then
			addExcludedNodes $USER_ID $NN $NEW_CONFIG_LOCATION "$4"
		fi
	done
	
	if [ "$3" == "heartbeat" ] ; then
		echo "Starting NameNodes after heartbeat interval configuration has been changed"
		resetNameNodes start "$2"
		ADMINREPORT_NN_CONFIG_DIR=$2
		echo "Started NameNodes after heartbeat interval configuration has been changed"
	elif [ "$3" == "zeroNodes" ]; then
		echo "Restarting NameNode so the changes in configuration take effect"
		resetNameNodes start "$2"
		ADMINREPORT_NN_CONFIG_DIR=$2
		echo "Restarted NameNode so the changes in configuration take effect."
	elif [ "$3" == "addExcludedNodes" ]; then
		echo "Starting namenode after adding" $4 "to dfs.exclude and modifying hdfs-site.xml to point to new dfs.exclude"
		resetNameNodes start "$2"
		ADMINREPORT_NN_CONFIG_DIR=$2
		echo "Started namenode after adding" $4 "to dfs.exclude and modifying hdfs-site.xml to point to new dfs.exclude"
	fi	
}


# Setup environment to start testing
# $1 - if it is "heartbeat" then Test Case 1
#    - if is is "addExcludedNodes" then Test Case 3
#    - if it is "zeroNodes" then Test Case 4
# $2 - The \n separated list of nodes to add to dfs.exclude
function setup
{
	displayHeader "Setting up $1"
	setKerberosTicketForUser $HDFS_USER

	if [ "$1" == "heartbeat" ] ; then 
		setupNewAdminReportConfFile $USER_ID "/tmp/adminReport_conf/" heartbeat
	elif [ "$1" == "zeroNodes" ]; then 
		setupNewAdminReportConfFile $USER_ID "/tmp/adminReport_conf/" zeroNodes
	elif [ "$1" == "addExcludedNodes" ]; then
		setupNewAdminReportConfFile $USER_ID "/tmp/adminReport_conf/" addExcludedNodes "$2"
	fi
}


################################
# Functions to help in teardown
################################

# Reset Namenode to the original configuration (before testing began) for tearing down
# $1 - Path to temporary configuration.
# $2 - Username . Should have write access to $1
function resetNNToDefault
{
	if [[ $# -ne 2 || -z $1 || -z $2 ]]; then
		echo -e "Usage: resetNNToDefault path username\ne.g. resetNNToDefault /tmp/adminReport_conf hadoopqa"
		return 1
	fi

	echo "Resetting Namenode to original configuration"
	local NEW_CONFIG_LOCATION=$1
	local NAMENODES=`getNameNodes`
	NAMENODES=`echo $NAMENODES | tr ';' '\n'`
	for NN in `echo "$NAMENODES"`; do
		ssh ${2}@$NN rm -R $NEW_CONFIG_LOCATION
	done
	echo "Reset Namenodes to original configuration"
}


# Depricate: Use library.sh takeNNOutOfSafemode function instead. This current
# function can potentially hang since the wait command does not have a timeout.
#Function to wait for the cluster to come out of safemode
function waitToComeOutOfSafemode
{
	$HADOOP_COMMON_CMD dfsadmin -safemode wait
}


# Tear down test environment and return to original environment (before testing began)
function teardown
{
	displayHeader "Tearing down"
	echo "Stopping namenodes so that it can be reset to original configuration"
	resetNameNodes stop $ADMINREPORT_NN_CONFIG_DIR
	echo "Stopped namenodes so that it can be reset to original configuration"
	resetNNToDefault  "/tmp/adminReport_conf/" $USER_ID
	echo "Starting cluster in original configuration."
	startCluster
	echo "Started cluster in original configuration."
	echo "Waiting for cluster to come out of safemode"
	# waitToComeOutOfSafemode
        takeNNOutOfSafemode
	echo "Cluster is now out of safemode"
}


###################################
# Functions used during test cases
###################################

# Return hostname when an IP is given
# $1 the IP address to lookup
# Output echoes the hostname if found. Otherwise $1
function getHostName
{
	if [ -z $1 ]; then
		return 1;
	fi
	local ip=$1
	hostname=`echo -n -e "$ipToHostnameMap" | grep "$ip" | cut -d' ' -f 2`
	if [ -z $hostname ]; then
		hostname=`nslookup $ip | grep "name =" | cut -d' ' -f 3 | sed 's/\(.*\)./\1/'`
		if [ ! -z $hostname ]; then
			ipToHostnameMap="${ipToHostnameMap}\n$ip ${hostname}"
		else
			ipToHostnameMap="${ipToHostnameMap}\n$ip $ip"
			hostname=$ip
		fi
	fi

	echo -n $hostname
}

#Get the adminReport using the command "hadoop dfsadmin -report"
function getAdminReport
{
	$HADOOP_COMMON_CMD dfsadmin -report
}

# Parse the admin report and get list of live nodes
# $1 - The complete hadoop dfsadmin -report output. If not given, the dfsadmin report is obtained automatically.
# The sorted list of live nodes is output to STDOUT
function getLiveNodes
{
	local ADMIN_REPORT="$1"
	if [ -z "$ADMIN_REPORT" ]; then
		ADMIN_REPORT=`getAdminReport`
	fi
	local FLAG=0
	local LIVE_NODES=""
	while read; do
		if [ "$REPLY" == "Live datanodes:" ] ; then
			FLAG=1
		fi
		if [ "$REPLY" == "Dead datanodes:" ] ; then
			break
		fi
		local TEMP=`echo $REPLY | cut -d: -f1`
		if [[ $FLAG == 1 && $TEMP == "Name" ]] ; then
			local IP=`echo "$REPLY" | cut -d' ' -f2 | cut -d: -f1`
			local HOSTNAME=`getHostName $IP`
			local LIVE_NODES="${LIVE_NODES}\n$HOSTNAME"
		fi
	done< <(echo "$ADMIN_REPORT")
	LIVE_NODES="`echo -e $LIVE_NODES | sort | tr '\n' ' '`"
	echo $LIVE_NODES
}

#Parse the admin report and get list of dead nodes
# $1 - The complete hadoop dfsadmin -report output. If not given, the dfsadmin report is obtained automatically.
# The sorted list of dead nodes is output to STDOUT
function getDeadNodes
{
	local ADMIN_REPORT="$1"
	if [ -z "$ADMIN_REPORT" ]; then
		ADMIN_REPORT=`getAdminReport`
	fi
	local FLAG=0
	local DEAD_NODES=""
	while read; do
		if [ "$REPLY" == "Dead datanodes:" ] ; then
			FLAG=1
		fi
		local TEMP=`echo $REPLY | cut -d: -f1`
		if [[ $FLAG == 1 && $TEMP == "Name" ]] ; then
			local IP=`echo "$REPLY" | cut -d' ' -f2 | cut -d: -f1`
			local HOSTNAME=`getHostName $IP`
			local DEAD_NODES="${DEAD_NODES}\n$HOSTNAME"
		fi
	done< <(echo "$ADMIN_REPORT")		# For this to work, bash must be used. Doesn't work in sh
	DEAD_NODES="`echo -e $DEAD_NODES | sort | tr '\n' ' '`"
	echo $DEAD_NODES
}


# Kill data node and wait for it to show up in dead list.
# $1 Node to kill
# $2 Maximum amount of time to wait to kill the node (in seconds)
# $3 Time between polls (in seconds)
# $4 if "false" then don't stop the node (just check that the node has been killed) 
# Returns 0 if killed. 1 if node didn't die in $2 seconds
function killDataNode
{
	if [[ $# -ne 3 || -z $1 || -z $2 || -z $3 ]]; then
		echo -e "Usage: killDataNode nodeToKill maxWait pollInterval\ne.g. killDataNode gsbl90633.blue.ygrid.yahoo.com 60 5"
		return 1
	fi

	if [ "$4" != "false" ]; then
		resetNode $1 datanode stop
	fi

	#Wait until MAX_WAIT
	local START=$(date +%s)
	while [ `expr $(date +%s) - $START` -lt $2 ]; do
		#Check Node I just killed is in DEAD_NODES and not in LIVE_NODES
		local ADMIN_REPORT=`getAdminReport`
		local LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
		local DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
		echo "Live Nodes: $LIVE_NODES. Dead nodes: $DEAD_NODES"
		if [[ "`echo $DEAD_NODES | grep $1`" != "" && "`echo $LIVE_NODES | grep $1`" == "" ]]; then
			echo "Verified node $1 is in dead nodes and not live nodes list after `expr $(date +%s) - $START` seconds"
			return 0
		fi
		echo "Sleeping for $3 seconds. $1 not reported dead yet"
		sleep $3
	done
	echo "Node $1 did not show up in dead nodes list / showed up in live nodes list after $2 seconds"
	return 1
}

# Revive data node and wait for it to show up in live list.
# $1 Node to revive
# $2 Maximum amount of time to wait to revive the node
# $3 Time between polls
# $4 if "false" then don't start the node (just check that the node has been revived)
# Returns 0 if revived. 1 if node wasn't revived in $2 seconds
function reviveDataNode
{
	if [[ $# -lt 3 || $# -gt 4 || -z $1 || -z $2 || -z $3 ]]; then
		echo -e "Usage: reviveDataNode nodeToRevive maxWait pollInterval [false]\ne.g. reviveDataNode gsbl90633.blue.ygrid.yahoo.com 60 5"
		return 1
	fi

	if [ "$4" != "false" ]; then
		resetNode $1 datanode start
	fi

	#Wait until MAX_WAIT
	local START=$(date +%s)
	while [ `expr $(date +%s) - $START` -lt $2 ]; do
		#Check Node I just revived is in LIVE_NODES and not in DEAD_NODES
		local ADMIN_REPORT=`getAdminReport`
		local LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
		local DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
		echo "Live Nodes: $LIVE_NODES. Dead nodes: $DEAD_NODES"
		if [[ "`echo $LIVE_NODES | grep $1`" != "" && "`echo $DEAD_NODES | grep $1`" == "" ]]; then
			echo "Verified node $1 is in live nodes and not dead nodes list after `expr $(date +%s) - $START` seconds"
			return 0
		fi
		echo "Sleeping for $3 seconds. $1 not reported live yet"
		sleep $3
	done
	echo "Node $1 did not show up in live nodes list  / showed up in dead nodes list after $2 seconds"
	return 1
}


########################
# Test Case functions
########################

# Test Case 1. Checks nodes which are killed show up in the dead nodes section of the hadoop dfsadmin -report
function checkDeadNodesReported
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check nodes which are killed show up in the dead nodes section of the hadoop dfsadmin -report"
	displayTestCaseMessage $TESTCASE_DESC

	KILLED_NODES=""

	local COUNT=0
	local LIVE_NODES=`getLiveNodes`
	if [ `echo -n $LIVE_NODES | tr ' ' '\n' | wc -l` -lt $NUM_NODES_TO_KILL ]; then
		COMMAND_EXIT_CODE=1
		REASONS="Not enough nodes `echo -n $LIVE_NODES | tr ' ' '\n' | wc -l` to kill. I need at least $NUM_NODES_TO_KILL for this test"
	else
		while [ $COUNT -lt $NUM_NODES_TO_KILL ]; do
			#Kill the first node in LIVE_NODES
			echo -e "\n\nLive nodes: $LIVE_NODES. Killed nodes: $KILLED_NODES"
			local NODE_TO_KILL=`echo $LIVE_NODES | cut -d" " -f1`
			echo "Killing node #$COUNT: killDataNode $NODE_TO_KILL. MAX_WAIT=$MAX_WAIT POLL_INTERVAL=$POLL_INTERVAL"
			killDataNode $NODE_TO_KILL $MAX_WAIT $POLL_INTERVAL

			if [ $? = 0 ]; then
				KILLED_NODES="$KILLED_NODES $NODE_TO_KILL"
			else
				echo "FAIL!!! Killing a node ( $NODE_TO_KILL ) did not update the hadoop dfsadmin -report as expectedFailed to kill node . Going to fail test. Continuing in the meantime"
				COMMAND_EXIT_CODE=1
			fi
			LIVE_NODES=`getLiveNodes`
			let COUNT++
			REASONS="Killing a node did not update the hadoop dfsadmin -report as expected"
		done
	fi

	displayTestCaseResult
}

# Test Case 2. Checks nodes which are revived show up in the live nodes section of the hadoop dfsadmin -report
function checkLiveNodesReported
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check nodes which are revived show up in the live nodes section of the hadoop dfsadmin -report"
	displayTestCaseMessage $TESTCASE_DESC

	local NODES_TO_BRING_BACK="$KILLED_NODES"
	while [[ "$NODES_TO_BRING_BACK" != "" ]]; do
		echo -e "\n\nNodes to revive: $NODES_TO_BRING_BACK."
		local NODE_TO_REVIVE=`echo $NODES_TO_BRING_BACK | cut -d" " -f1`
		echo "Reviving node $NODE_TO_REVIVE. MAX_WAIT=$MAX_WAIT POLL_INTERVAL=$POLL_INTERVAL"
		reviveDataNode $NODE_TO_REVIVE $MAX_WAIT $POLL_INTERVAL

		if [ $? = 0 ]; then
			NODES_TO_BRING_BACK=`echo $NODES_TO_BRING_BACK | tr ' ' '\n' | grep -v "$NODE_TO_REVIVE" | tr '\n' ' '`
		else
			COMMAND_EXIT_CODE=1
			echo "FAIL!!! Reviving a node ( $NODE_TO_REVIVE ) did not update the hadoop dfsadmin -report as expectedFailed to kill node . Going to fail test. Continuing in the meantime"
		fi
	done

	REASONS="Reviving a node did not update the hadoop dfsadmin -report as expected"
	displayTestCaseResult
}

#Test Case 3. Check the nodes reported dead and alive are the same after restarting the namenodes as before restarting.
function checkRestart
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that reported nodes after restarting namenode is the same as before restarting"
	displayTestCaseMessage $TESTCASE_DESC

	local ADMIN_REPORT=`getAdminReport`
	local LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
	local DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
	echo -e "\nBefore restarting: \nLive nodes : \"$LIVE_NODES\"\nDead Nodes : \"$DEAD_NODES\""
	resetNameNodes stop $ADMINREPORT_NN_CONFIG_DIR
	resetNameNodes start $ADMINREPORT_NN_CONFIG_DIR
	sleep 10
	ADMIN_REPORT=`getAdminReport`
	local AFTER_LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
	local AFTER_DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
	echo -e "\nAfter restarting: \nLive nodes : \"$AFTER_LIVE_NODES\"\nDead Nodes : \"$AFTER_DEAD_NODES\""
	if [[ "$AFTER_LIVE_NODES" == "$LIVE_NODES" && "$AFTER_DEAD_NODES" == "$DEAD_NODES" ]]; then
		echo "Verified the reported nodes after restarting namenode is the same as before restarting"
	else
		REASONS="FAIL!!! Reported nodes after restarting namenode is NOT the same as before restarting"
		COMMAND_EXIT_CODE=1
	fi

	displayTestCaseResult
}

#Test Case 4. Check that nodes which at cluster start off are in dfs.exclude, show up as live when revived
function checkRevivalOfExcludedNodes
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that nodes which at cluster start off are in dfs.exclude, show up as live when revived"
	displayTestCaseMessage $TESTCASE_DESC

	local LIVE_NODES=`getLiveNodes`
	if [ `echo $LIVE_NODES | tr ' ' '\n' | wc -l` -lt $NUM_NODES_TO_KILL ] ; then
		COMMAND_EXIT_CODE=1
		REASONS="Not enough nodes ( `echo $LIVE_NODES | tr ' ' '\n' | wc -l`: $LIVE_NODES ) to kill. I need at least $NUM_NODES_TO_KILL for this test"
	else
		local NODES_TO_EXCLUDE=`echo $LIVE_NODES | cut -d' ' -f1-$NUM_NODES_TO_KILL | tr ' ' '\n'`
		setup addExcludedNodes "$NODES_TO_EXCLUDE"
		local ADMIN_REPORT=`getAdminReport`
		local BEFORE_LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
		local BEFORE_DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
		while read; do
			if [[ "`echo $BEFORE_LIVE_NODES | grep $REPLY`" != "" || "`echo $BEFORE_DEAD_NODES | grep $REPLY`" == "" ]]; then
# 			Uncomment when dfsadmin -report is fixed . Checkout https://issues.apache.org/jira/browse/HADOOP-5094?focusedCommentId=13003427&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13003427
#			if [ "`echo $BEFORE_LIVE_NODES | grep $REPLY`" != "" ]; then
				COMMAND_EXIT_CODE=1
				echo -n "$REPLY showed up as live / didn't show up as dead even though it was in dfs.exclude."
				echo -n " LIVE_NODES=${BEFORE_LIVE_NODES}. DEAD_NODES=${BEFORE_DEAD_NODES}. Going to fail test. Continuing in the meantime. "
				REASONS="$REASONS. $REPLY showed up as live / didn't show up as dead even though it was in dfs.exclude"
			else
				echo "$REPLY correctly did not show up as live and showed up as dead since it was in dfs.exclude"
			fi
		done< <(echo "$NODES_TO_EXCLUDE")

		echo "\nGoing to empty dfs.exclude file and making sure nodes are revived."
		# Revive nodes in dfs.exclude
		local NAMENODES=`getNameNodes`
		NAMENODES=`echo $NAMENODES | tr ';' '\n'`
		while read; do
			ssh ${USER_ID}@$REPLY "echo '' > /tmp/adminReport_conf/dfs.exclude"
		done< <(echo "$NAMENODES")
		for line in `echo "$NODES_TO_EXCLUDE"`; do 
			resetNode $line datanode start
		done
		$HADOOP_COMMON_CMD dfsadmin -refreshNodes

		#Check that they have been revived
		while read; do
			reviveDataNode $REPLY $MAX_WAIT $POLL_INTERVAL false
			if [ $? -eq 0 ]; then
				echo "Verified $REPLY was revived"
			else
				echo "Node $REPLY was not revived despite being removed from dfs.exclude. Going to fail test. Continuing in the meantime."
				COMMAND_EXIT_CODE=1
				REASONS="$REASONS. Node $REPLY was not revived despite being removed from dfs.exclude."
			fi
		done< <(echo "$NODES_TO_EXCLUDE")
	fi

	displayTestCaseResult
}


# Test Case 5. Check that when the slaves file is empty, reported nodes are 0 
function checkZeroDataNodes
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that when the slaves file is empty, reported nodes are 0 "
	displayTestCaseMessage $TESTCASE_DESC

	local ADMIN_REPORT=`getAdminReport`
	local LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
	local DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
	if [[ "$LIVENODES" != "" && "$DEADNODES" != "" ]]; then
		COMMAND_EXIT_CODE=1
		REASONS="FAIL!!! : liveNodes ( $LIVENODES ) or deadNodes ( $DEADNODES ) was not empty"
	else
		echo "Verified that when there are no data nodes, no live nodes / dead nodes are reported"
	fi
	displayTestCaseResult
}


# Test Case 1
# 1. Check list of live and dead nodes
# 2. Kill nodes one by one (up to a maximum of NUM_NODES_TO_KILL)
# 3. Check list is updated as expected
# Test Case 2
# 4. Start bringing back dead nodes that _I_ had killed
# 5. Check list is updated as expected
setup heartbeat
checkDeadNodesReported
checkLiveNodesReported

# Test Case 3
# 1. Note down list of live and dead nodes before restarting namenode
# 2. Restart namenode
# 3. Get list of live and dead nodes. This should be same as before
checkRestart

# Test Case 4
# 1. Stop the namenode. Add $NUM_NODES_TO_KILL nodes into dfs.exclude
# 2. Restart the namenode. Check that the excluded nodes are not live
# 3. Remove those nodes from dfs.exclude. Call hadoop dfsadmin -refreshNodes
# 4. Check that the nodes are live.
# setup happens within this function for this test
checkRevivalOfExcludedNodes

# Test Case 5
# 1. Rename HADOOP_CONF_DIR/slaves. Touch HADOOP_CONF_DIR/slaves.
# 2. Start the cluster
# 3. Check 0 live and 0 dead nodes
setup zeroNodes
checkZeroDataNodes

teardown

SCRIPT_EXIT_CODE=0
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
