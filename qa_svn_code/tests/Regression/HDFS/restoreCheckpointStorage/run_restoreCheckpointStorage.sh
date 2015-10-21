#!/bin/sh

# This is an automated test for
# - https://issues.apache.org/jira/browse/HADOOP-4885 (Try to restore failed replicas of Name Node storage (at checkpoint time))
# - https://issues.apache.org/jira/browse/HADOOP-5144 (manual way of turning on restore of failed storage replicas for namenode)

# Load the library files
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

OWNER="raviprak"		# Email: raviprak@yahoo-inc.com hadoop-qe@yahoo-inc.com


#####################################
# PARAMETERS TO CUSTOMIZE TESTING
#####################################
USER_ID=hadoopqa		# User ID for sshing into secondary namenodes while modifying hdfs-site configuration
POLL_INTERVAL=120			# The interval at which checkpoints will be made (By default it is 3600*24 seconds)
NN_DFSDIR=/grid/2/hadoop/var/hdfs/name/* # The directory from which the checkpoints will be copied during setup
#NN_DFSDIR=$WORKSPACE/tests/Regression/HDFS/restoreCheckpointStorage/namespaces/nn1ns/name/* # The directory from which the checkpoints will be copied during setup
SNN_DFSDIR=/grid/0/tmp/hadoop-$HDFS_SUPER_USER/dfs/namesecondary/current/ #the dir on secondary namenode where it store checkpoint imgs

LOOPBACKBLOCKSIZE=1024
LOOPBACKBLOCKS=5120		# number of blocks copy to loopback device

############################################
# GLOBAL VARIABLES SHARED BETWEEN FUNCTIONS
############################################
RESTORECHECKPOINT_SNN_CONFIG_DIR=$HADOOP_CONF_DIR		# Used to remember the last secondary namenode config directory.
RESTORECHECKPOINT_NN_CONFIG_DIR=$HADOOP_CONF_DIR		# Used to remember the last namenode config directory.

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
		echo "hadoopqa 98.137.97.97 $HADOOP_CONF_DIR/hdfs-site.xml dfs.exclude /tmp/excludeFile.txt"
		return 1
	fi
	scp ${1}@$2:${3} /tmp/									#Copy remote hdfs-site.xml to local directory so it can be modified easily
	local HDFS_SITE=${3##*/}

	local cmd="addPropertyToHdfsSite /tmp/$HDFS_SITE /tmp/${HDFS_SITE}.new"
	for (( i=4; i<=$#; i++ )); do
		eval parameter=\$${i}
		cmd="$cmd \"$parameter\""
	done

	echo "Running $cmd"
	eval $cmd

	ssh ${1}@$2 mv ${3} ${3}.original		#Backed up old hdfs-site.xml files as hdfs-site.xml.original
	scp /tmp/${HDFS_SITE}.new ${1}@$2:${3}		#Copy modified hdfs-site.xml to remote node
	rm /tmp/${HDFS_SITE}.new /tmp/${HDFS_SITE}	
}


##########################
# Setup
##########################

# Setup a loopback filesystem on a node. This is used to make write operations fail
# $1 Namenode to setup the loopback filesystem on
# $2 The file system image to create
# $3 The directory on which the the loopback filesystem will be mounted
function setupLoopbackFSOnNN
{
	local NN=$1
	echo "Creating the loopback file system on $NN"
	echo "dd creating file"
	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER $HDFS_SUPER_USER@$NN "dd if=/dev/zero of=$2 bs=$LOOPBACKBLOCKSIZE count=$LOOPBACKBLOCKS"
	#echo "Running losetup : $1 /sbin/losetup -f $2"
	#runasroot $1 losetup -f $2
	echo "running mkfs"
	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER $HDFS_SUPER_USER@$NN "/sbin/mkfs -t ext3 -q $2 -F; mkdir -p $3"
	echo "mounting"	
	runasroot $NN mount "-o loop=/dev/loop0 $2 $3"
	runasroot $NN losetup "-a"
	echo "Complete creating the loopback file system on $NN"
	runasroot $NN df | grep /tmp/testImage
}

function saveNamespace {
	echo "Going to save namespace"
	execHDFSAdminCmd "-safemode enter"
	execHDFSAdminCmd "-saveNamespace"
	execHDFSAdminCmd "-saveNamespace"
	execHDFSAdminCmd "-safemode leave"
	#for NN in `getNameNodes | tr ';' '\n'`; do
	#	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "$HADOOP_HDFS_CMD dfsadmin -safemode enter"
	#	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "$HADOOP_HDFS_CMD dfsadmin -saveNamespace"
	#	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "$HADOOP_HDFS_CMD dfsadmin -saveNamespace"
	#	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "$HADOOP_HDFS_CMD dfsadmin -safemode leave"
	#done
}

# Setup the hdfs-site.xml on namenode, secondary namenode and create whatever's necessary for testing
# On the namenode change the checkpoint storage directories and set restoration to false by default
# On the secondary namenode change the frequency of checkpointing to $POLL_INTERVAL
# $1 - Username to use to ssh into the namenode. Should have write access to $2
# $2 - Path where a temporary hadoop configuration directory will be created
function setupCheckpointDirsAndInterval
{
	displayHeader "Setting up cluster for testing restoration of checkpoint storage directories"
	if [[ $# -lt 2 || -z $1 || -z $2 ]]; then
		echo -n -e "Usage: setupCheckPointDirsAndInterval username path \ne.g. setupCheckPointDirsAndInterval "
		echo "hadoopqa /tmp/restoreCheckpointStorage_config"
		return 1
	fi
	
	
	setKerberosTicketForUser $HDFS_SUPER_USER
	local USER_ID=$1
	local NEW_CONFIG_LOCATION=$2

	local NAMENODES=`getNameNodes`				# Get list of namenodes
	NAMENODES=`echo $NAMENODES | tr ';' '\n'`
	echo -e "On cluster $CLUSTER, Name Nodes: \n$NAMENODES"
	
	#save namespaces to make it fit into the loopback device
	#we assume that the test cluster has small name space
	saveNamespace
	resetNameNodes stop $RESTORECHECKPOINT_NN_CONFIG_DIR
	
	for NN in `echo "$NAMENODES"`; do
		#make sure /tmp/testImage is not mounted to a loop back device yet
		runasroot $NN umount "-d /tmp/testImage"
		runasroot $NN losetup "-d /dev/loop0" 
		
		echo "Creating new temporary directory ssh ${USER_ID}@$NN mkdir -p $NEW_CONFIG_LOCATION"
		ssh ${USER_ID}@$NN "mkdir -p $NEW_CONFIG_LOCATION; rm -rf $NEW_CONFIG_LOCATION/*"
		ssh ${USER_ID}@$NN cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"	# Copy all the conf files to new location in SNN host
		ssh ${USER_ID}@$NN chmod -R 755 "$NEW_CONFIG_LOCATION"

		setupLoopbackFSOnNN $NN /tmp/testImage /tmp/checkpointDir
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "mkdir -p /tmp/checkpointDir/checkPoint_test1 /tmp/checkPoint_test2 /tmp/checkPoint_test3; rm -rf /tmp/checkpointDir/checkPoint_test1/* /tmp/checkPoint_test2/* /tmp/checkPoint_test3/*"
		#print the file stats for debugging, if the namespace is bigger than the size of the loopback device, 
		#the test will not run correctly
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "ls -l $NN_DFSDIR"
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "cp -R $NN_DFSDIR /tmp/checkpointDir/checkPoint_test1; cp -R $NN_DFSDIR /tmp/checkPoint_test2; cp -R $NN_DFSDIR /tmp/checkPoint_test3"
		if [ $? -ne 0 ]; then
			REASONS="There is not enough space in loopback device to copy the hdfs file system"
			COMMAND_EXIT_CODE=1
			displayTestCaseResult
			exit
		fi
		echo "Complete copy files to the new hdfs location on $NN"
		#echo "The new disk status is:"
		#runasroot $NN df | grep /tmp/testImage
		changeHdfsSiteOnNode $USER_ID $NN ${NEW_CONFIG_LOCATION}/hdfs-site.xml dfs.namenode.name.dir file:////tmp/checkpointDir/checkPoint_test1,file:///tmp/checkPoint_test2,file:///tmp/checkPoint_test3 dfs.namenode.name.dir.restore false
	done
	
	resetNameNodes start $NEW_CONFIG_LOCATION
	
	RESTORECHECKPOINT_NN_CONFIG_DIR=$NEW_CONFIG_LOCATION
	#echo "Go get the conf files at $NEW_CONFIG_LOCATION"
	sleep 120
	
	local SECONDARY_NAMENODES=`getSecondaryNameNodes`				# Get list of secondary namenodes
	SECONDARY_NAMENODES=`echo $SECONDARY_NAMENODES | tr ';' '\n'`
	echo -e "On cluster $CLUSTER, Secondary Name Nodes: \n$SECONDARY_NAMENODES"

	resetSecondaryNameNodes stop $RESTORECHECKPOINT_SNN_CONFIG_DIR
	# Setup new config location on SNNs
	
	for SNN in `echo "$SECONDARY_NAMENODES"`; do
		echo "Creating new temporary directory ssh ${USER_ID}@$SNN mkdir -p $NEW_CONFIG_LOCATION"
		ssh ${USER_ID}@$SNN "mkdir -p $NEW_CONFIG_LOCATION; rm -rf $NEW_CONFIG_LOCATION/*"
		ssh ${USER_ID}@$SNN cp -r $HADOOP_CONF_DIR/* "$NEW_CONFIG_LOCATION"	# Copy all the conf files to new location in SNN host
		ssh ${USER_ID}@$SNN chmod -R 755 "$NEW_CONFIG_LOCATION"
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_${HDFS_SUPER_USER} ${HDFS_SUPER_USER}@$SNN "rm -rf ${SNN_DFSDIR}/*"
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_${HDFS_SUPER_USER} ${HDFS_SUPER_USER}@$SNN "ls -l ${SNN_DFSDIR}"
		changeHdfsSiteOnNode $USER_ID $SNN ${NEW_CONFIG_LOCATION}/hdfs-site.xml dfs.namenode.checkpoint.period $POLL_INTERVAL
	done
			
	resetSecondaryNameNodes start $NEW_CONFIG_LOCATION
	RESTORECHECKPOINT_SNN_CONFIG_DIR=$NEW_CONFIG_LOCATION
}


##########################
# Teardown
##########################

# Reset Secondary Namenodes and Namenodes to the original configuration (before testing began) for tearing down
# $1 - Username . Should have write access to $2
# $2 - Path to temporary configuration.
function resetNNsToDefault
{
	if [[ $# -ne 2 || -z $1 || -z $2 ]]; then
		echo -e "Usage: resetNNsToDefault path username\ne.g. resetNNsToDefault /tmp/adminReport_conf hadoopqa"
		return 1
	fi

	local USER_ID=$1
	local NEW_CONFIG_LOCATION=$2

	echo "Resetting Secondary Namenodes to original configuration"
	local SECONDARY_NAMENODES=`getSecondaryNameNodes`
	SECONDARY_NAMENODES=`echo $SECONDARY_NAMENODES | tr ';' '\n'`
	echo "Stopping SNN with conf $RESTORECHECKPOINT_SNN_CONFIG_DIR"
	resetSecondaryNameNodes stop $RESTORECHECKPOINT_SNN_CONFIG_DIR
	for SNN in `echo "$SECONDARY_NAMENODES"`; do
		ssh ${USER_ID}@$SNN rm -R $NEW_CONFIG_LOCATION 
	done
	echo "Starting SNN with conf $HADOOP_CONF_DIR"
	resetSecondaryNameNodes start $HADOOP_CONF_DIR
	RESTORECHECKPOINT_SNN_CONFIG_DIR=$HADOOP_CONF_DIR
	echo "Reset Secondary Namenodes to original configuration"

	echo "Resetting Namenodes to original configuration"
	local NAMENODES=`getNameNodes`				# Get list of namenodes
	NAMENODES=`echo $NAMENODES | tr ';' '\n'`
	resetNameNodes stop $RESTORECHECKPOINT_NN_CONFIG_DIR
	for NN in `echo "$NAMENODES"`; do
		echo "On namenode $NN removing $NEW_CONFIG_LOCATION" 
		ssh ${1}@$NN rm -R $NEW_CONFIG_LOCATION
		runasroot $NN umount "-d /tmp/testImage"
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN rm -rf /tmp/testImage /tmp/checkpointDir /tmp/checkPoint_test1 /tmp/checkPoint_test2 /tmp/checkPoint_test3 
	done
	resetNameNodes start $HADOOP_CONF_DIR
	RESTORECHECKPOINT_NN_CONFIG_DIR=$HADOOP_CONF_DIR
	echo "Reset Namenodes to original configuration"
}


#Function to wait for the cluster to come out of safemode
function waitToComeOutOfSafemode
{
	echo "Waiting for cluster to come out of safemode"
	$HADOOP_HDFS_CMD dfsadmin -safemode wait
	echo "Cluster is now out of safemode"
}


# Tear down test environment and return to original environment (before testing began)
# $1 - The path where the temporary configuration directory was created
function teardown
{
	displayHeader "Tearing down"
	resetNNsToDefault $USER_ID $1 
	waitToComeOutOfSafemode
}


#################################
# Functions to help with testing
#################################

# Verifies that on namenode $1 the directory $2 is a valid checkpoint storage directory
#
# A directory is valid if it contain only one current/edits_inprogress, 
# and current/fsimage and current/edits are both newer than $5
#
# The epoch ($5) can be set to 0 if it doesn't need to be used.
# Also dfshealth.html is parsed and the directory is grepped to be IMAGE_AND_EDITS and "Active"
# $1 - the user id to log on to the namenode
# $2 - the namenode to check on
# $3 - the directory to check
# $4 - the Storage Directory section from dfshealth.html curl'ed from $1
# $5 - epoch after which last change should have come  
function isValidDirectory
{
	echo "Checking isValidDirectory $1 $2 \"${3}\" <storage section> $5"
	if [[ $# -lt 5 || -z $1 || -z $2 || -z $3 || -z $4 || -z $5 ]]; then
		echo -e "Usage: isValidDirectory username namenode path <storage section> epoch"
		echo -e "e.g. isValidDirectory hadoopqa gsbl90629.blue.ygrid.yahoo.com /tmp/checkPoint_test3 <storage section> 1300642536"
		return 1
	fi
	
	local returnCode=0;
	
	local dirStats=`ssh ${1}@$2 "find $3 -name '*' | xargs stat --printf=\"%n %Z\n\""`
	#echo "Dir $3 status:"
	#echo "$dirStats"
	
	local editFile=`echo "$dirStats" | grep current/edits_inprogress`
	if [ -z "$editFile" ]; then	 
		echo "Fail!! The current/edits did not stat. Failing test."		
		return 1
	fi	
	echo "editFile: $editFile"
	local numberOfFiles=`echo "$editFile" | wc -l`
	echo "number of inprogress files: $numberOfFiles" 
	if [ $numberOfFiles -gt 1 ]; then
		echo -e "There are more than 1 edit_inprogress"
		return 1
	fi		
	local editEpoch=`echo "$editFile" | cut -d' ' -f2`
	echo "editEpoch: $editEpoch"
	
	local fsimageFile=`echo "$dirStats" | egrep 'current/fsimage_[0-9]+ [0-9]+' |  tr '\n' ' ' | cut -d' ' -f1-2`
	local fsimageEpoch=`echo "$fsimageFile" | cut -d' ' -f2`
	echo "fsimageEpoch: "$fsimageEpoch
	#Check current/edits and current/fsimage exists and is after $5
	echo "Epoch of current/edits (${editEpoch}) is going to be compared with $5"
	if [[ $editEpoch -lt $5 ]]; then 
		echo "Fail!! The edits file $editFile was older than $5. Failing test."
		returnCode=1
	fi
	 
	#if [ -z "`echo \"$dirStats\" | grep current/fsimage`" ]; then 
	#	echo "Fail!! The current/fsimage did not stat. Failing test."
	#	returnCode=1
	echo "Epoch of fsimage (${fsimageEpoch}) is going to be compared with $5"
	if [[ $fsimageEpoch -lt $5 ]]; then 
		echo "Fail!! The fsimage file \""`echo "$dirStats" | grep current/fsimage`"\" was older than $5. Failing test."
		returnCode=1
	fi
	
	local storageDirs=$4
	echo "storageDirs: "$storageDirs
	#if [ -z "`echo \"$storageDirs\" | grep $3 | grep IMAGE_AND_EDITS | grep Active`" ]; then
		#	echo -n "Fail!!! On namenode $2 the directory $3 either wasn't IMAGE_AND_EDITS or wasn't Active in dfshealth.html. The grepped line was:"
		#echo "$storageDirs" | grep $3
		#echo ""
		#returnCode=1
	#fi
	return $returnCode
}

# Verifies that on namenode $1 the directory $2 is an invalid checkpoint storage directory
# A directory is deemed invalid if the current/edits and current/fsimage files are older than epoch.
# Also dfshealth.html is parsed to check that the directory is either not IMAGE_AND_EDITS or not "Active"
# $1 - the user id to log on to the namenode
# $2 - the namenode to check on
# $3 - the directory to check
# $4 - the Storage Directory section from dfshealth.html curl'ed from $1
# $5 - epoch after which last change should have come  
function isInvalidDirectory
{
	echo "Checking isInvalidDirectory $1 $2 \"${3}\" <storage section> $5"
	if [[ $# -lt 5 || -z $1 || -z $2 || -z $3 || -z $4 || -z $5 ]]; then 
		echo -e "Usage: isInvalidDirectory username namenode path <storage section> epoch"
		echo -e "e.g. isInvalidDirectory hadoopqa gsbl90629.blue.ygrid.yahoo.com /tmp/checkPoint_test3 <storage section> 1300642536"
		return 1
	fi
	local returnCode=0;

	# Check DIRSTAT directory is before $5
	local dirStats=`ssh ${1}@$2 "find $3 -name '*' | xargs stat --printf=\"%n %Z\n\""`
	#echo "Dir $3 status:"
	#echo "$dirStats"
	
	#local editFile=`echo "$dirStats" | grep current/edits_inprogress`
	#echo "Current edit_inprogress:"
	#echo $editFile
	#local numberOfFiles=`echo "$editFile" | wc -l`
	#echo "number of inprogress files: $numberOfFiles"
	
	#if [ $numberOfFiles -gt 1 ]; then
	#	echo -e "There are more than 1 edit_inprogress"
	#	return 1
	#fi		
	#local editEpoch=`echo "$editFile" | cut -d' ' -f2`
	#echo "editEpoch: $editEpoch"
	
	local fsimageFile=`echo "$dirStats" | egrep "fsimage_[0-9]+ [0-9]+"` 
	echo "Current fsimageFile:"
	echo $fsimageFile
	local numberOfFSFiles=`echo "$fsimageFile" | grep fsimage | wc -l`
	echo "number of fsimageFile: $numberOfFSFiles"
	 
	local fsimageEpochs=`echo "$fsimageFile" | cut -d' ' -f2`
	#get the max fsimageEpochs
	local fsimageEpoch=`echo $fsimageEpochs | cut -d' ' -f1`
	echo "fsimageEpoch: $fsimageEpoch"
	
	echo "Epoch of current/fsimage (${fsimageEpoch}) is going to be compared with $5"
	if [[ $fsimageEpoch -gt $5 ]]; then
		echo -e "Fail!!! Epoch of current/fsimage (${fsimageEpoch}) was greater than $5" 
		returnCode=1		
	fi
	 

	#local storageDirs=$4
	#if [ -n "`echo "$storageDirs" | grep $3 | grep IMAGE_AND_EDITS | grep Active`" ]; then
		#	echo -n "Fail!!! On namenode $2 the directory $3 was IMAGE_AND_EDITS and Active: "
		#returnCode=1
	#	fi
	#echo "$storageDirs" | grep $3
	return $returnCode
}

# Verifies that the directories expected to be active are indeed so and that directories expected to be invalid are exactly that.
# $1 - the user id to log on to the namenode
# $2 - comma separated list of namenodes to check
# $3 - comma separated list of storage directories which should be valid
# $4 - comma separated list of storage directories which should be invalid
# $5 - epoch after which last change should have come
# return - 0 if $2 are valid and $3 are invalid. 1 otherwise
# Notes : Assumes webpage is at http://${NN}:50070/dfshealth.html
function verifyCheckpointStorageState
{
	if [[ $# -lt 5 || -z $1 || -z $2 ]]; then
		echo -e "Usage: verifyCheckpointStorageState username namenode,namenode path1,path2 path3,path4,path5 epoch"
		echo -e "e.g. verifyCheckpointStorageState hadoopqa gsbl90629.blue.ygrid.yahoo.com /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2 /tmp/checkPoint_test3 1300642536"
		return 1
	fi
	
	echo "Trying to verify that on $2, the valid directories are: \"$3\" and the invalid directories are: \"$4\"" 
	local epoch=$5
	if [ -z $5 ]; then 
		epoch=0
	fi
	local returnCode=0
	for NN in `echo $2 | tr ',' ' '`; do
		echo "Trying curl http://${NN}:50070/dfshealth.html"
		local dfsHealth=`curl http://${NN}:50070/dfshealth.html`
		local storageDirs=`echo "$dfsHealth" | grep "Storage Directory" | sed 's:<tr>:\n:g'`
		if [ -z "$dfsHealth" ]; then
			echo "Fail! Curl did not return anything. Failing test."
			return 1
		fi 
		
		for storDir in `echo $3 | tr ',' ' '`; do
			isValidDirectory $1 $NN "$storDir" "$storageDirs" $epoch 
			if [ $? -ne 0 ]; then
				echo -e "\nDirectory $storDir on $NN did not seem to be a valid checkpoint storage directory"
				returnCode=1
			fi
		done

		for storDir in `echo $4 | tr ',' ' '`; do
			isInvalidDirectory $1 $NN "$storDir" "$storageDirs" $epoch
			if [ $? -ne 0 ]; then
				echo -e "\nDirectory $storDir on $NN did not seem to be an invalid checkpoint storage directory"
				returnCode=1
			fi
		done

	done
	
	return $returnCode
}


# This function "knocks off" the filesystem so that write operations to it fail. It does so by filling up the 
# FS so that no space is left.
# $1 - User used to login to the node. Should have access to $3
# $2 - Node The node on which to create the file
# $3 - Path The file path of the file.
# $4 - Size in bytes of the file
function knockOffFS
{
	
	echo "Creating $3 on $2 of size $4"
	local fillPercent="13%"
	local count=0
	while [[ "$fillPercent" != "100%" && $count -lt 20 ]]; do
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$1 ${1}@$2 dd if=/dev/zero of=$3 bs=1024 count=$4
		echo "Sleeping for " `expr 2 '*' $POLL_INTERVAL ` "seconds" 
		sleep `expr 2 '*' $POLL_INTERVAL `
		fillPercent=`ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$1 ${1}@$2 df | grep /tmp/checkpointDir | awk '{print $5}'`
		echo "Loopback file system was fillPercent = $fillPercent"
		let count++
	done
}

function knockOffFS2 {
	# umount -l is needed to unmount when the device is almost always busy because of the Namenode activities
	# I have to detach the loop device outside of umount command because umount does not work with -l and -d together

	#lazy umount
	runasroot $2 umount "-l $3"
	
	#detach loop device
	echo "Run losetup -a on $2"
	local res=`runasroot $2 losetup "-a"`
	local loopdev=`echo $res | grep /tmp/testImage | cut -d' ' -f6 | cut -d':' -f1`
	echo "Return message: $res"
	local stat=0
	local attempt=0
	while [[ "$stat" == "0" ]];do
		echo "Run: runasroot $2 losetup -d $loopdev"
		res=`runasroot $2 losetup "-d $loopdev"`		
		echo $res
		echo $res | grep "Device or resource busy"
		stat=$?
		sleep 1s
		let "attempt += 1"
		if [ $attempt -gt "24" ] && [ "$stat" == "0" ]; then
			COMMAND_EXIT_CODE=1;
			stat="1"
		fi				
	done
	echo "Attempted to take down the loopback device ${attempt} times"
	if [[ "$COMMAND_EXIT_CODE" != 0 ]]; then
		REASONS="Too many attempts to take down the node."
		echo -e "Failed! Too many attempts to take down the node."
		displayTestCaseResult
		#exit
	fi
}

# This function brings back the FS so that write operations to it succeed once more. It does so by deleting the 
# file used to create the file system
# $1 - User used to login to the node. Should have access to $3
# $2 - Node The node on which to create the file
# $3 - Path The file path of the file
function bringBackFS
{
	echo "Removing $3 on $2"
	ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$1 ${1}@$2 rm $3
	fillPercent=`ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$1 ${1}@$2 df | grep /tmp/checkpointDir | awk '{print $5}'`
	echo "Loopback file system was fillPercent = $fillPercent"
}

function bringBackFS2 {
	echo "Bringback file system"
	runasroot $1 mount "-o loop $2 $3"
}

function execHDFSAdminCmd {
	cmd=$1
	local NAMENODES=`getNameNodes`			
	NAMENODES=`echo $NAMENODES | tr ';' '\n'`
	setKerberosTicketForUser $HDFS_SUPER_USER
	for NN in $NAMENODES; do
		$HADOOP_HDFS_CMD dfsadmin -fs hdfs://${NN} ${cmd}
	done
}
################################
# Test Function
################################

# Check checkpoint storage directory is not restored when dfs.namenode.name.dir.restore is false
function testCheckpointStorageNotRestored
{
	setTestCaseDesc "Check checkpoint storage directory is not restored when dfs.namenode.name.dir.restore is false" 
	displayTestCaseMessage $TESTCASE_DESC

	COMMAND_EXIT_CODE=0
	REASONS="If errors exist, please see http://bug.corp.yahoo.com/show_bug.cgi?id=4422879 . "
	
	#cmd="$HADOOP_HDFS_CMD dfsadmin -restoreFailedStorage false"
	#echo $cmd
	#eval $cmd
	
	execHDFSAdminCmd "-restoreFailedStorage false"
	
	local NAMENODES=`getNameNodes`
	
	# Check right after startup that all 3 checkpoint storage directories are valid
	verifyCheckpointStorageState $USER_ID `echo $NAMENODES | tr ';' ','` /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 '' 0
	if [ $? -ne 0 ]; then 
		COMMAND_EXIT_CODE=1;
		REASONS="After setup could not verify that /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid. "
	else
		echo "After setup verified that /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid"
	fi
	
	$HADOOP_HDFS_CMD dfs -rm -r /tmp/fileOne

	# Disable write operations on one of the storage directories on all the namenodes	
	echo -e "\nKnocking off storage directory"
	# Knock one storage directory off on every namenode
	for NN in `echo $NAMENODES | tr ';' '\n'`; do
		#echo "On namenode $NN filling up /tmp/checkpointDir/checkPoint_test1"
		#knockOffFS $HDFS_SUPER_USER $NN /tmp/checkpointDir/fillerFile $LOOPBACKSIZE
		knockOffFS2 $HDFS_SUPER_USER $NN /tmp/testImage				
	done
	echo -e "Knocked off storage directory\n"
	
	# Wait for a little while to let the namenode remove the disabled directory and for another checkpoint to happen
    local START=$(date +%s)
	echo "Setting START to $START. Time before sleeping" `expr 2 '*' $POLL_INTERVAL ` "seconds is $(date +%s) . This should be enough time to trigger a checkpoint. Valid directories should thus have something newer than START and invalid should have older than START" 
	sleep `expr 2 '*' $POLL_INTERVAL`
	echo "Time after sleeping is $(date +%s)" 
	
	# Verify that the disabled directory is invalid and the unmodified directories are valid
	verifyCheckpointStorageState $USER_ID `echo $NAMENODES | tr ';' ','` /tmp/checkPoint_test2,/tmp/checkPoint_test3 /tmp/checkpointDir/checkPoint_test1 $START
	if [ $? -ne 0 ]; then 
		COMMAND_EXIT_CODE=1;
		REASONS="${REASONS}After knocking off one directory, could not verify that /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid. "
		echo -e "Fail! Could not verify that after knocking off one directory /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid\n\n"
	else
		echo -e "Verified that after knocking off one directory /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid\n\n"
	fi

	# Touch a file on the HDFS FS. 
	$HADOOP_HDFS_CMD dfs -touchz /tmp/fileOne
	
	# Bring back the directory which was knocked off and verify it wasn't restored because dfs.namenode.name.dir.restore is false
	for NN in `echo $NAMENODES | tr ';' '\n'`; do		 
		bringBackFS2 $NN /tmp/testImage /tmp/checkpointDir
	done
	START=$(date +%s)
	echo "Setting START to $START. Time before sleeping" `expr 2 '*' $POLL_INTERVAL` "seconds is $(date +%s) . This should be enough time to trigger a checkpoint. Valid directories should thus have something newer than START and invalid should have older than START" 
	sleep `expr 2 '*' $POLL_INTERVAL`
	echo "Time after sleeping is $(date +%s)" 
	verifyCheckpointStorageState $USER_ID `echo $NAMENODES | tr ';' ','` /tmp/checkPoint_test2,/tmp/checkPoint_test3 /tmp/checkpointDir/checkPoint_test1 $START
	if [ $? -ne 0 ]; then 
		COMMAND_EXIT_CODE=1;
		REASONS="${REASONS}After restoring the storage directory could not verify that /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid because dfs.namenode.name.dir.restore is false. "
		echo -e "Fail! Could not verify that after restoring the storage directory /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid because dfs.namenode.name.dir.restore is false\n\n"
	else
		echo "Verified that after restoring the renamed directory /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid because dfs.namenode.name.dir.restore is false"
	fi

	displayTestCaseResult
}


# Check checkpoint storage directory is restored after hadoop dfsadmin -restoreFailedStorage true (HADOOP-5144)
function testCheckpointStorageRestored
{
	setTestCaseDesc "Check checkpoint storage directory is restored after hadoop dfsadmin -restoreFailedStorage true"
	displayTestCaseMessage $TESTCASE_DESC

	COMMAND_EXIT_CODE=0
	REASONS="If errors exist, please see http://bug.corp.yahoo.com/show_bug.cgi?id=4422879 . "
	
	execHDFSAdminCmd "-restoreFailedStorage true"
		
	local NAMENODES=`getNameNodes`

	# Check right after startup that all 3 checkpoint storage directories are valid
	verifyCheckpointStorageState $USER_ID `echo $NAMENODES | tr ';' ','` /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 '' 0
	if [ $? -ne 0 ]; then 
		COMMAND_EXIT_CODE=1;
		REASONS="After setup could not verify that /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid. "
	else
		echo "After setup verified that /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid"
	fi
	
	$HADOOP_HDFS_CMD dfs -rm -r /tmp/fileOne

	# Disable write operations on one of the storage directories on all the namenodes	
	echo -e "\nKnocking off storage directory"
	# Knock one storage directory off on every namenode
	for NN in `echo $NAMENODES | tr ';' '\n'`; do
		#echo "On namenode $NN filling up /tmp/checkpointDir/checkPoint_test1"
		#knockOffFS $HDFS_SUPER_USER $NN /tmp/checkpointDir/fillerFile $LOOPBACKSIZE
		knockOffFS2 $HDFS_SUPER_USER $NN /tmp/testImage				
	done
	echo -e "Knocked off storage directory\n"
	
	# Wait for a little while to let the namenode remove the disabled directory and for another checkpoint to happen
    local START=$(date +%s)
    DATE_STR=`date -d @$START +%H:%M:%S`
	echo "Setting START to $DATE_STR. Time before sleeping" `expr 2 '*' $POLL_INTERVAL` "seconds is $DATE_STR . This should be enough time to trigger a checkpoint. Valid directories should thus have something newer than START and invalid should have older than START" 
	sleep `expr 2 '*' $POLL_INTERVAL`
	echo "Time after sleeping is $(date +%s)" 
	 
	# Verify that the disabled directory is invalid and the unmodified directories are valid
	verifyCheckpointStorageState $USER_ID `echo $NAMENODES | tr ';' ','` /tmp/checkPoint_test2,/tmp/checkPoint_test3 /tmp/checkpointDir/checkPoint_test1 $START
	if [ $? -ne 0 ]; then 
		COMMAND_EXIT_CODE=1;
		REASONS="${REASONS}After knocking off one directory, could not verify that /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid. "
		echo -e "Fail! Could not verify that after knocking off one directory /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid\n\n"
	else
		echo -e "Verified that after knocking off one directory /tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid and /tmp/checkpointDir/checkPoint_test1 was invalid\n\n"
	fi

	# Touch a file on the HDFS FS. 
	$HADOOP_HDFS_CMD dfs -touchz /tmp/fileOne
	
	# Bring back the directory which was knocked off and verify it was restored because dfs.namenode.name.dir.restore is true
	for NN in `echo $NAMENODES | tr ';' '\n'`; do		 
		bringBackFS2 $NN /tmp/testImage /tmp/checkpointDir
	done
	START=$(date +%s)
	DATE_STR=`date -d @$START +%H:%M:%S`
	echo "Setting START to $DATE_STR. Time before sleeping" `expr 2 '*' $POLL_INTERVAL` "seconds is $DATE_STR . This should be enough time to trigger a checkpoint. Valid directories should thus have something newer than START and invalid should have older than START" 
	sleep `expr 2 '*' $POLL_INTERVAL`
	echo "Time after sleeping is $(date +%s)" 
	
	# Verify that all directories are valid
	verifyCheckpointStorageState $USER_ID `echo $NAMENODES | tr ';' ','` /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 '' $START
	if [ $? -ne 0 ]; then
		COMMAND_EXIT_CODE=1;
		REASONS="${REASONS}After turning on restore could not verify that /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid"
	else
		echo "Verified that after turning on restore /tmp/checkpointDir/checkPoint_test1,/tmp/checkPoint_test2,/tmp/checkPoint_test3 were valid"
	fi 
	displayTestCaseResult
}


# Check the edits logs and fsimage after restoration were correct
# This is done as follows. By the time this test is run, /tmp/fileOne had been touched in HDFS when the storage directory was invalid
# The storage directory was then activated again so that the checkpoint in there is one from recovery.
# The same storage directory is again knocked offline and /tmp/fileTwo is touched.
# The namenode is then restarted with the checkpoint from the directory which was knocked off.
# The assertion is made that in the newly restarted state the namenode knows of /tmp/fileOne but not of /tmp/fileTwo
function testRestoredCheckpointStorageDirectoryWasCorrect
{
	setTestCaseDesc "Check that the checkpoint (edits logs and fsimage) after restoration was correct"
	displayTestCaseMessage $TESTCASE_DESC

	COMMAND_EXIT_CODE=0
	REASONS="If errors exist, please see http://bug.corp.yahoo.com/show_bug.cgi?id=4422879 . "
	local NAMENODES=`getNameNodes`
		
	#$HADOOP_HDFS_CMD dfs -touchz /tmp/fileOne
		
	#Copy /tmp/checkpointDir/checkPoint_test1 to safe location
	for NN in `echo $NAMENODES | tr ';' '\n'`; do
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "rm -rf /tmp/checkPoint_test1; cp -R /tmp/checkpointDir/checkPoint_test1 /tmp" 
		#knockOffFS $HDFS_SUPER_USER $NN /tmp/checkpointDir/fillerFile $LOOPBACKSIZE
		knockOffFS2 $HDFS_SUPER_USER $NN /tmp/testImage
	done
	
	$HADOOP_HDFS_CMD dfs -touchz /tmp/fileTwo
	
	# Start namenodes with only /tmp/checkpointDir/checkPoint_test1
	resetNameNodes stop $RESTORECHECKPOINT_NN_CONFIG_DIR
	local NEW_CONFIG_LOCATION=$RESTORECHECKPOINT_NN_CONFIG_DIR	
	for NN in `echo $NAMENODES | tr ';' '\n'`; do
		#bringBackFS $HDFS_SUPER_USER $NN /tmp/checkpointDir/fillerFile
		bringBackFS2 $NN /tmp/testImage /tmp/checkpointDir
		changeHdfsSiteOnNode $USER_ID $NN ${NEW_CONFIG_LOCATION}/hdfs-site.xml dfs.namenode.name.dir file:////tmp/checkpointDir/checkPoint_test1 dfs.namenode.name.dir.restore true
		ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN "rm -rf /tmp/checkpointDir/checkPoint_test1; mv /tmp/checkPoint_test1 /tmp/checkpointDir" 
	done
	echo $NEW_CONFIG_LOCATION
	
	resetNameNodes start $NEW_CONFIG_LOCATION
	$HADOOP_HDFS_CMD dfs -touchz /tmp/fileThree
	sleep 120	
	listing=`$HADOOP_HDFS_CMD dfs -ls /tmp`
	echo "Listing: $listing"
	if [ -z "`echo \"$listing\" | grep /tmp/fileOne`" ]; then
		COMMAND_EXIT_CODE=1
		REASONS="Could not find /tmp/fileOne in HDFS which should have been restored. "
	fi 
	
	if [ -n "`echo \"$listing\" | grep /tmp/fileTwo`" ]; then
		COMMAND_EXIT_CODE=1
		REASONS="${REASONS}Found /tmp/fileTwo in HDFS which should not have been found. "
	fi 

	displayTestCaseResult
}

# Check the namenode shutsdown when all the storage directories are unwritable.
# This is done by knocking offline once again the single storage directory setup in the 
# test just before this one testRestoredCheckpointStorageDirectoryWasCorrect
function testNamenodesShutDownWhenNoStorage
{
	setTestCaseDesc " Check namenodes shutdown when all directories are invalid" 
	displayTestCaseMessage $TESTCASE_DESC

	waitToComeOutOfSafemode
	
	COMMAND_EXIT_CODE=0
	REASONS="If errors exist, please see http://bug.corp.yahoo.com/show_bug.cgi?id=4422879 . "
	local NAMENODES=`getNameNodes`
	
	#Knock off the one and only checkpoint storage directory
	for NN in `echo $NAMENODES | tr ';' '\n'`; do
		#knockOffFS2 $HDFS_SUPER_USER $NN /tmp/checkpointDir/fillerFile $LOOPBACKSIZE
		knockOffFS2 $NN /tmp/testImage
	done

	echo "Time before sleeping" `expr 2 '*' $POLL_INTERVAL` "seconds is $(date +%s) . This should be enough time to trigger a checkpoint. If all storage directories are invalid, namenode should have shutdown" 
	sleep `expr 2 '*' $POLL_INTERVAL`
	echo "Time after sleeping is $(date +%s)" 
	
	#Verify that the namenodes shutdown
	for NN in `echo $NAMENODES | tr ';' '\n'`; do
		local jpsOutput=`ssh -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_$HDFS_SUPER_USER ${HDFS_SUPER_USER}@$NN jps`
		echo -e "jps Output on $NN is \n$jpsOutput"
		if [ -n "`echo $jpsOutput | grep NameNode`" ]; then
			COMMAND_EXIT_CODE=1
			REASONS="${REASONS}On namenode $NN the java process NameNode was still running despite all checkpoint storage directories being knocked off."
		fi
	done

	displayTestCaseResult
}


setupCheckpointDirsAndInterval hadoopqa /tmp/restoreCheckpointStorage_config
waitToComeOutOfSafemode
testCheckpointStorageNotRestored

#need to run setup again because the previous tests may create tale files in designated directories.
setupCheckpointDirsAndInterval hadoopqa /tmp/restoreCheckpointStorage_config
waitToComeOutOfSafemode
testCheckpointStorageRestored
#testRestoredCheckpointStorageDirectoryWasCorrect
#testNamenodesShutDownWhenNoStorage

teardown /tmp/restoreCheckpointStorage_config

SCRIPT_EXIT_CODE=0
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
