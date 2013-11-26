#!/bin/bash
#Uses bash features. Cannot be used with sh


# This is an automated test for
# - https://issues.apache.org/jira/browse/HADOOP-5258 (Provide dfsadmin functionality to report on namenode's view of network topology)
# Since this depends on the actual physical topology of the cluster the test is running on, I can only test that all the slaves show up in the report
# ALL nodes, irrespective of wether they are dead / decommissioned / live are shown in the topology report.

# Load the library files
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

OWNER="raviprak"                # Email : raviprak@yahoo-inc.com hadoop-qe@yahoo-inc.com


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


#####################################
# PARAMETERS TO CUSTOMIZE TESTING
#####################################
USER_ID=hadoopqa		# User ID to use for sshing into namenodes for reading slaves file

# Steps in the test:
# 1. Get the dfsadmin -printTopology output
# 2. Get the slaves file from each namenode
# 3. Make sure all the slaves are in the topology report.
function testTopologyReport
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Checking dfsadmin topology report contains all the nodes in the slaves file"
	displayTestCaseMessage $TESTCASE_DESC

	setKerberosTicketForUser $HDFS_USER
	local TOPOLOGYREPORT=`$HADOOP_COMMON_CMD dfsadmin -printTopology`
	echo -e "Topology report : \n$TOPOLOGYREPORT"

	local NAMENODES=`getNameNodes`
	NAMENODES=`echo $NAMENODES | tr ';' '\n'`
	echo -e "On cluster $CLUSTER, Name Nodes: \n$NAMENODES\n\n"

	REASONS="These nodes :"
	local ADMIN_REPORT=`getAdminReport`
	local LIVE_NODES=`getLiveNodes "$ADMIN_REPORT"`
	local DEAD_NODES=`getDeadNodes "$ADMIN_REPORT"`
	slaves="$LIVE_NODES $DEAD_NODES"
	echo "Checking for : $slaves"
	slaves=`echo $slaves | tr ' ' '\n'`
	while read; do
		if [ "`echo $TOPOLOGYREPORT | grep \"$REPLY\"`" == "" ]; then
			COMMAND_EXIT_CODE=1
			REASONS="$REASONS $REPLY"
			echo "Did not find $REPLY in topology report!!!"
		else
			echo "Found $REPLY in topology report"
		fi
	done< <(echo "$slaves")

	REASONS="$REASONS were found in the slaves file but not in the topologyReport"
    displayTestCaseResult
}

testTopologyReport
SCRIPT_EXIT_CODE=0
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE