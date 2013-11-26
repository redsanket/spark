#!/bin/bash

# This is an automated test for
# - https://issues.apache.org/jira/browse/HADOOP-1298 - Implement permissions for HDFS
# - https://issues.apache.org/jira/browse/HADOOP-2381 - Support permission information in FileStatus
# - https://issues.apache.org/jira/browse/HADOOP-2288 - Change FileSystem API to support access control
# - https://issues.apache.org/jira/browse/HADOOP-2431 - Test HDFS File Permissions
# - https://issues.apache.org/jira/browse/HADOOP-2371 - Candidate user guide for permissions feature of Hadoop DFS
# - https://issues.apache.org/jira/browse/HADOOP-2463 - Test permissions related shell commands with DFS
# - https://issues.apache.org/jira/browse/HADOOP-4077 - Access permissions for setting access times and modification times for files
# - https://issues.apache.org/jira/browse/HADOOP-3953 - Sticky bit for directories

# Load the library files
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

OWNER="raviprak"		# Email: raviprak@yahoo-inc.com hadoop-qe@yahoo-inc.com

#####################################
# PARAMETERS TO CUSTOMIZE TESTING
#####################################
hdfsFilesDirectory="/tmp/permissionsTestFiles"
hdfsFoldersDirectory="/tmp/permissionsTestFolders"

############################################
# GLOBAL VARIABLES SHARED BETWEEN FUNCTIONS
############################################

#################################
# Functions to help with setup
#################################

# Create a file for each of the permissions in HDFS. Only rw are used (x is not used)
# $1 - The HDFS directory to create the files in
# $2 - User to kinit as
function setupFiles
{
	
	displayHeader "Setting up files for testing the permissions feature"
	if [[ -z $1 || -z $2  ]]; then
		echo -e "Usage: setupFiles Directory User \ne.g. setupFiles /tmp/filesDirectory hadoopqa"
		return 1
	fi

	setKerberosTicketForUser $2
	
	$HADOOP_HDFS_CMD dfs -rmr $1
	$HADOOP_HDFS_CMD dfs -mkdir -p $1
	
	local count=0
	local procArr[$count]=0
	# Create files
	local hdfsFiles=""
	for i in $(seq 0 2 6); do
		for j in $(seq 0 2 6); do
			for k in $(seq 0 2 6); do
				hdfsFiles="$hdfsFiles ${1}/file-${i}${j}${k}"
			done
		done
	done
	echo "Touching in HDFS $hdfsFiles"
	$HADOOP_HDFS_CMD dfs -touchz $hdfsFiles
	
	#Set appropriate permissions
	runCmdOnDirectoryContents $1 "$HADOOP_HDFS_CMD dfs -chmod \`echo fileSentinel | awk -F\"file-\" '{print \$2}'\` fileSentinel" fileSentinel false
}


# Create a folder for each of the permissions in HDFS. So there'll be 511 (777octal) folders
# $1 - The HDFS directory to create the folders in
# $2 - User to kinit as
function setupFolders
{
	setKerberosTicketForUser $2
	
	$HADOOP_HDFS_CMD dfs -rmr $1
	$HADOOP_HDFS_CMD dfs -mkdir -p $1
	
	local count=0
	local procArr[$count]=0
	local hdfsFolders=""
	# Create files
	for i in $(seq 0 7); do
		for j in $(seq 0 7); do
			for k in $(seq 0 7); do
				hdfsFolders="$hdfsFolders ${1}/folder-${i}${j}${k}"
			done
		done
	done
	echo "mkdir in HDFS $hdfsFolders"
	$HADOOP_HDFS_CMD dfs -mkdir -p $hdfsFolders
	
	#Set appropriate permissions
	runCmdOnDirectoryContents $1 "$HADOOP_HDFS_CMD dfs -chmod \`echo fileSentinel | awk -F\"folder-\" '{print \$2}'\` fileSentinel" fileSentinel false
}


################################
# Functions to help in teardown
################################

# $1 - HDFS directories to delete
function teardown
{
	$HADOOP_HDFS_CMD dfs -rmr $1
}
 

###################################
# Functions used during test cases
###################################

# $1 - The command to run
# $2 - The expected result.
# $3 - The file to write to output to 
# $4 - Whether to check command executed conformed with Linux's permission model
function runCmd
{
	local cmd="$1"
	local msgToDump=""
	local output=`eval $cmd 2>&1; echo -e "\nPermissionTestReturnCode: $?"`
	local retCode=`echo "$output" | grep PermissionTestReturnCode | cut -d' ' -f2`
	
	msgToDump="==============================="
	if [[ $4 = "false" || ( $2 -eq 0 && $retCode -eq 0 ) || ( $2 -ne 0 && $retCode -ne 0 ) ]]; then
		msgToDump="$msgToDump\nPermissions Test Passed for $1"
	else
		msgToDump="$msgToDump\nPermissions Test Failed for $1"
	fi
	msgToDump="$msgToDump\nOutput (stdout+stderr):\n`echo "$output"`\nReturnCode: $retCode\nExpected result: $2\n==============================="
	echo -e $msgToDump >> $3 		# Writing all at once to file so that file is locked (hopefully) 
}


# $1 - hdfs dfs -ls line of HDFS file
# $2 - isOwner
# $3 - isInGroup
# $4 - The command being run on HDFS
function getCalculatedExpectedCode
{
	local isOwner=$2
	local isInGroup=$3
	
	local perm=`echo $1 | awk '{print $1}'`
	local REtoCheck="^."
	if [ "$isOwner" != "1" ]; then REtoCheck="${REtoCheck}..."; fi
	if [[ "$isOwner" != "1" && "$isInGroup" != "1" ]]; then REtoCheck="${REtoCheck}..."; fi
	if [ -n "`echo $perm | grep '^d'`" ]; then
		if [[ -n "`echo $4 | grep '\-cat\|\-tail\|\-copyToLocal\|\-get'`" ]]; then    			# Read permission
			REtoCheck="${REtoCheck}r";
		elif [[ -n "`echo $4 | grep '\-ls\|\-cp'`" ]]; then										# List permission
			REtoCheck="${REtoCheck}r.x";
		elif [[ -n "`echo $4 | grep '\-touchz'`" ]]; then								   		# Write permission
			REtoCheck="${REtoCheck}.wx";
		fi
		
		if [ -n "`echo $perm | grep $REtoCheck`" ]; then
			return 0;
		fi
		return 1;
	else
		if [[ -n "`echo $4 | grep '\-cat\|\-cp\|\-tail\|\-copyToLocal\|\-get'`" ]]; then    	# Read permission
			REtoCheck="${REtoCheck}r";
		elif [[ -n "`echo $4 | grep '\-ls'`" ]]; then											# List permission
			REtoCheck="${REtoCheck}r.x";
		elif [[ -n "`echo $4 | grep '\-touchz'`" ]]; then								   		# Write permission
			REtoCheck="${REtoCheck}.w";
		fi
		
		#echo "getCalculatedExpectedCode REtoCheck='$REtoCheck', perm='$perm', dirLine='$1'"
		
		if [ -n "`echo $perm | grep $REtoCheck`" ]; then
			return 0;
		fi
		return 1;
	fi
		
}


# Run command on each file read from the HDFS directory specified. Forks processes to run them in parallel
# $1 - The directory in HDFS on whose members to iterate over
# $2 - The command to run
# $3 - The string in $2 to replace with the name of the directory member
# $4 - Check command executed conformed with Linux's permission model
# $5 - isOwner
# $6 - isInGroup
# return - 0 if all commands worked as expected (AccessControlException or success). 1 otherwise  
function runCmdOnDirectoryContents
{
	if [[ -z $1 || -z $2 || -z $3 ]]; then
		echo -e "Usage: runCmdOnDirectoryContents Directory Command Sentinel toCheck isOwner isInGroup\ne.g. runCmdOnDirectoryContents /tmp/filesDirectory \"hdfs dfs -cat fileSentinel\" fileSentinel false 0 1"
		return 1
	fi

	#Get list of files
	local dirList=`$HADOOP_HDFS_CMD dfs -ls $1`
	local files=`echo "$dirList" | awk '{print $8}'`
	
	local user=`/usr/kerberos/bin/klist | grep "Default principal" | cut -d' ' -f 3 | cut -d'@' -f1`;
	
	rm -f ./commandsOutput.txt
	touch ./commandsOutput.txt
	local count=0
	local procArr[$count]=0
	for file in `echo "$files"`; do
		local cmd=`echo $2 | sed s:$3:$file:g`
		echo "Executing on file $file the command $cmd"
		local dirListLine=`echo "$dirList" | grep $file`
		getCalculatedExpectedCode "$dirListLine" $5 $6 "$cmd"
		local expectedCode=$?
		
		runCmd "$cmd" $expectedCode ./commandsOutput.txt $4&
		procArr[$count]=$!
		let count++
	done
	let count--
	
	for i in $(seq 0 $count); do
		wait ${procArr[$i]}
	done
	
	local retCode=0
	local flag=0
	while read line; do
		if [ -n "`echo $line | grep "Permissions Test Failed"`" ]; then
			retCode=1
			flag=1
		fi
		if [ $flag -eq 1 ]; then
			echo $line
		fi
		if [[ $flag -eq 1 && -n "`echo $line | grep ======`" ]]; then
			flag=0
		fi  
	done < ./commandsOutput.txt
	rm -f ./commandsOutput.txt
	return $retCode
}

# Checks that the two HDFS directories have the same permissions
# $1 - The source HDFS directory
# $2 - The target HDFS directory
# $3 - "true" if $2 is already a list 
function checkSamePermissions
{
	local srcFiles=`$HADOOP_HDFS_CMD dfs -ls $1`
	local tgtFiles="$2"
	if [ "$3" != "true" ]; then
		tgtFiles=`$HADOOP_HDFS_CMD dfs -ls $2`
	fi
	if [ `echo "$tgtFiles" | wc -l` -lt 4  ]; then		# If not enough files exist in tgt
		return 1
	fi 

	while read; do
		local line=$REPLY
		local filename=`echo $line | awk '{print $8}'`

		if [ -n "$filename" ]; then
			filename=`basename $filename`
			local srcLine=`echo "$srcFiles" | grep "$filename"`
			local srcPerm=`echo $srcLine | awk '{print $1}'`
			local tgtPerm=`echo $line | awk '{print $1}'`
			if [ "$srcPerm" != "$tgtPerm" ]; then 
				return 1
			fi
		fi
	done< <(echo "$tgtFiles")
	return 0
}

########################
# Test Case functions
########################

# Permission_05 and Permission_10
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFileReadAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 file read access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC

	#For each of the files in the directory try a read.
	local cmd="runCmdOnDirectoryContents $hdfsFilesDirectory \"$HADOOP_HDFS_CMD dfs -cat fileSentinel\" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	
	if [ $retCode -ne 0 ]; then 
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode
	fi

	displayTestCaseResult
}

# Even though you CANNOT write a pre-existing file
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFileWriteAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 file write access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC
	
	local tgtFiles=`$HADOOP_HDFS_CMD dfs -ls $hdfsFilesDirectory`

	#For each of the files in the directory try a read.
	local cmd="runCmdOnDirectoryContents $hdfsFilesDirectory \"$HADOOP_HDFS_CMD dfs -touchz fileSentinel\" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	
	if [ $retCode -ne 0 ]; then 
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode 
	fi

	checkSamePermissions $hdfsFilesDirectory "$tgtFiles" true
	retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="$REASONS The permissions on files in $hdfsFilesDirectory changed after the touch command. Please refer to http://bug.corp.yahoo.com/show_bug.cgi?id=4513561"
		COMMAND_EXIT_CODE=`expr $retCode + $COMMAND_EXIT_CODE`
	fi
	
	displayTestCaseResult
}

#Permission_06
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFolderWriteAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 folder write access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC

	#For each of the folders in the directory try to write a file into them.
	local cmd="runCmdOnDirectoryContents $hdfsFoldersDirectory \"$HADOOP_HDFS_CMD dfs -touchz fileSentinel/file \" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	
	if [ $retCode -ne 0 ]; then 
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode
	fi

	displayTestCaseResult
}

#Permission_07
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFolderListAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 folder list access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC

	#For each of the folders in the directory try to list its contents
	local cmd="runCmdOnDirectoryContents $hdfsFoldersDirectory \"$HADOOP_HDFS_CMD dfs -ls fileSentinel \" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	
	if [ $retCode -ne 0 ]; then 
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode
	fi

	displayTestCaseResult
}


## Permission_08
#function testDistCpSetsCorrectPermissions
#{
#	$HADOOP_COMMON_CMD distcp 
#}

# Permission_09
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testCpSetsCorrectPermissions
{
	
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 cp sets correct permissions on file/directory"
	displayTestCaseMessage $TESTCASE_DESC
	
	$HADOOP_HDFS_CMD dfs -rmr /tmp/checkCpMaintainsFilePermissions /tmp/checkCpMaintainsFolderPermissions
	$HADOOP_HDFS_CMD dfs -mkdir -p /tmp/checkCpMaintainsFilePermissions
	#For each of the folders in the directory try to copy its contents
	local cmd="runCmdOnDirectoryContents $hdfsFilesDirectory \"$HADOOP_HDFS_CMD dfs -cp fileSentinel /tmp/checkCpMaintainsFilePermissions \" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	
	if [ $retCode -ne 0 ]; then 
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux."
		COMMAND_EXIT_CODE=$retCode
	fi

	checkSamePermissions $hdfsFilesDirectory /tmp/checkCpMaintainsFilePermissions
	retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="$REASONS The permissions on $hdfsFilesDirectory and copied folder (/tmp/checkCpMaintainsFilePermissions) were different. Please refer to http://bug.corp.yahoo.com/show_bug.cgi?id=4513561"
		COMMAND_EXIT_CODE=$retCode
	fi

	# Check folders 	
	$HADOOP_HDFS_CMD dfs -mkdir -p /tmp/checkCpMaintainsFolderPermissions
	cmd="runCmdOnDirectoryContents $hdfsFoldersDirectory \"$HADOOP_HDFS_CMD dfs -cp fileSentinel /tmp/checkCpMaintainsFolderPermissions \" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	if [ $retCode -ne 0 ]; then 
		REASONS="$REASONS The execution of $cmd yielded different return codes on HDFS and Linux."
		COMMAND_EXIT_CODE=$retCode
	fi
	checkSamePermissions $hdfsFoldersDirectory /tmp/checkCpMaintainsFolderPermissions
	retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="$REASONS The permissions on $hdfsFoldersDirectory and copied folder (/tmp/checkCpMaintainsFolderPermissions) were different. Please refer to http://bug.corp.yahoo.com/show_bug.cgi?id=4513561"
		COMMAND_EXIT_CODE=$retCode
	fi
	$HADOOP_HDFS_CMD dfs -rmr /tmp/checkCpMaintainsFilePermissions /tmp/checkCpMaintainsFolderPermissions

	displayTestCaseResult	
}

# Permission_11
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFileTailAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 file tail access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC

	#For each of the files in the directory try a read.
	local cmd="runCmdOnDirectoryContents $hdfsFilesDirectory \"$HADOOP_HDFS_CMD dfs -tail fileSentinel\" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	if [ $retCode -ne 0 ]; then 
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode
	fi

	displayTestCaseResult
}

# Permission_13
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFileCopyToLocalAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 file CopyToLocal's read access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC

	#For each of the files in the directory try a read.
	mkdir /tmp/permissionTestFileCopyToLocalAccess
	local cmd="runCmdOnDirectoryContents $hdfsFilesDirectory \"$HADOOP_HDFS_CMD dfs -copyToLocal fileSentinel /tmp/permissionTestFileCopyToLocalAccess \" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode
	fi

	rm -rf /tmp/permissionTestFileCopyToLocalAccess

	displayTestCaseResult
}


# Permission_16
# $1 - owner / group or other permissions
# $2 - isOwner
# $3 - isInGroup 
function testFileGetAccess
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check that $1 file get's read access is same as Linux's model"
	displayTestCaseMessage $TESTCASE_DESC

	#For each of the files in the directory try a read.
	mkdir -p /tmp/permissionTestGetFileAccess/
	local cmd="runCmdOnDirectoryContents $hdfsFilesDirectory \"$HADOOP_HDFS_CMD dfs -get fileSentinel /tmp/permissionTestGetFileAccess/ \" fileSentinel true $2 $3"
	eval $cmd
	local retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="The execution of $cmd yielded different return codes on HDFS and Linux"
		COMMAND_EXIT_CODE=$retCode
	fi

	rm -rf /tmp/permissionTestGetFileAccess/

	displayTestCaseResult
}


function testPermissionsStaySameAfterRestart
{
	COMMAND_EXIT_CODE=0
	setTestCaseDesc "Check permissions stay the same after restarting the namenode"
	displayTestCaseMessage $TESTCASE_DESC

	local tgtFiles=`$HADOOP_HDFS_CMD dfs -ls $hdfsFilesDirectory`
	local tgtFolders=`$HADOOP_HDFS_CMD dfs -ls $hdfsFoldersDirectory`

	resetNameNodes stop
	resetNameNodes start
	
	checkSamePermissions $hdfsFilesDirectory "$tgtFiles" true
	retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="$REASONS The permissions on $hdfsFilesDirectory changed after restarting the namenodes"
		COMMAND_EXIT_CODE=$retCode
	fi
	
	checkSamePermissions $hdfsFoldersDirectory "$tgtFolders" true
	retCode=$?
	if [ $retCode -ne 0 ]; then
		REASONS="$REASONS The permissions on $hdfsFoldersDirectory changed after restarting the namenodes"
		COMMAND_EXIT_CODE=$retCode
	fi
	
	displayTestCaseResult
}

setupFiles $hdfsFilesDirectory hadoopqa
setupFolders $hdfsFoldersDirectory hadoopqa

#setKerberosTicketForUser $HDFS_SUPER_USER
$HADOOP_HDFS_CMD dfs -chmod 777 $hdfsFilesDirectory
$HADOOP_HDFS_CMD dfs -chmod 777 $hdfsFoldersDirectory
setKerberosTicketForUser $HADOOPQA_USER

testFileReadAccess owner 1 1
testFileTailAccess owner 1 1
testFileGetAccess owner 1 1
testFileCopyToLocalAccess owner 1 1
testCpSetsCorrectPermissions owner 1 1 # This test is failing because cp is not maintaining permissions
testFolderWriteAccess owner 1 1
testFolderListAccess  owner 1 1
testPermissionsStaySameAfterRestart
$HADOOP_HDFS_CMD dfsadmin -safemode wait
testFileWriteAccess owner 1 1 # This is changing permissions of the file. (Because touchz changes permissions of the file)

# Remove setupFiles after http://bug.corp.yahoo.com/show_bug.cgi?id=4513561 gets fixed 
setupFiles $hdfsFilesDirectory hadoopqa

setKerberosTicketForUser $HDFS_SUPER_USER
$HADOOP_HDFS_CMD dfs -chmod 777 $hdfsFilesDirectory
$HADOOP_HDFS_CMD dfs -chmod 777 $hdfsFoldersDirectory
$HADOOP_HDFS_CMD dfs -rm $hdfsFoldersDirectory/folder*/*
$HADOOP_HDFS_CMD dfs -chown -R $HDFS_SUPER_USER:hadoop $hdfsFilesDirectory
$HADOOP_HDFS_CMD dfs -chown -R $HDFS_SUPER_USER:hadoop $hdfsFoldersDirectory
setKerberosTicketForUser $HADOOPQA_USER
testFileReadAccess group 0 1
testFileTailAccess group 0 1
testFileGetAccess group 0 1
testFileCopyToLocalAccess group 0 1
testCpSetsCorrectPermissions group 0 1 # This test is failing because cp is not maintaining permissions
testFolderWriteAccess group 0 1
testFolderListAccess group 0 1
testFileWriteAccess group 0 1 # This is changing permissions of the file. (Because touchz changes permissions of the file)

# Remove setupFiles after http://bug.corp.yahoo.com/show_bug.cgi?id=4513561 gets fixed 
setupFiles $hdfsFilesDirectory hadoopqa

setKerberosTicketForUser $HDFS_SUPER_USER
$HADOOP_HDFS_CMD dfs -chmod 777 $hdfsFilesDirectory
$HADOOP_HDFS_CMD dfs -chmod 777 $hdfsFoldersDirectory
$HADOOP_HDFS_CMD dfs -rm $hdfsFoldersDirectory/folder*/*
$HADOOP_HDFS_CMD dfs -chown -R ${HDFS_SUPER_USER}:someGroup $hdfsFilesDirectory
$HADOOP_HDFS_CMD dfs -chown -R ${HDFS_SUPER_USER}:someGroup $hdfsFoldersDirectory
setKerberosTicketForUser $HADOOPQA_USER

testFileReadAccess other 0 0
testFileTailAccess other 0 0
testFileGetAccess other 0 0
testFileCopyToLocalAccess other 0 0
testCpSetsCorrectPermissions other 0 0 # This test is failing because cp is not maintaining permissions
testFolderWriteAccess other 0 0
testFolderListAccess other 0 0
testFileWriteAccess other 0 0 # This is changing permissions of the file. (Because touchz changes permissions of the file)


teardown "$hdfsFilesDirectory $hdfsFoldersDirectory"

SCRIPT_EXIT_CODE=0
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
