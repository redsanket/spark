#!/bin/sh

## Import Libraries
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh


# Setting Owner of this TestSuite
OWNER="rajsaha"


function localSetup {
	### Regularly used script level GLOBAL variables
	SUITESTART=`date +%s`
	HDFSCMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR "

	TMPRES=$TMPDIR/result
	TMPEXP=$TMPDIR/expected
	setKerberosTicketForUser $HDFSQA_USER
}

function localCleanup {
	### Clean Up
	setKerberosTicketForUser $HDFSQA_USER
	execcmdlocal "$HDFSCMD dfsadmin -clrQuota /user/$HDFSQA_USER"
	execcmdlocal "$HDFSCMD dfsadmin -clrSpaceQuota /user/$HDFSQA_USER"
	execcmdlocal "$HDFSCMD dfs -rm -R -skipTrash $SUITESTART"
	execcmdlocal "$HDFSCMD dfs -rm -R -skipTrash /user/$HDFSQA_USER/.Trash"
        setKerberosTicketForUser $HADOOPQA_USER
	rm -rf $TMPRES $TMPEXP
}

function validateQuotaQueryResult {
	local ExQUOTA=$1
	local ExREMAINING_QUATA=$2
	local ExSPACE_QUOTA=$3
	local ExREMAINING_SPACE_QUOTA=$4
	local ExDIR_COUNT=$5
	local ExFILE_COUNT=$6
	local ExCONTENT_SIZE=$7
	local ExFILE_NAME=$8

	cmd="$HDFSCMD dfs -count -q $ExFILE_NAME"
	echo "Command Executed > $cmd"
	$cmd 2> $TMPDIR/tmpErr1 > $TMPDIR/tmpOut
	cat $TMPDIR/tmpErr1
	cat $TMPDIR/tmpOut
	cat $TMPDIR/tmpErr1 | grep -v WARN > $TMPDIR/tmpErr
	
	if [ -s $TMPDIR/tmpErr ];then
		COMMAND_EXIT_CODE=1
		REASONS=`cat $TMPDIR/tmpErr`
		echo $REASONS
		return 1
	fi
	
	local ActQUOTA=`cat $TMPDIR/tmpOut		| awk '{print $1}'`
	local ActREMAINING_QUATA=`cat $TMPDIR/tmpOut	| awk '{print $2}'`
	local ActSPACE_QUOTA=`cat $TMPDIR/tmpOut	| awk '{print $3}'`
	local ActREMAINING_SPACE_QUOTA=`cat $TMPDIR/tmpOut| awk '{print $4}'`
	local ActDIR_COUNT=`cat $TMPDIR/tmpOut          | awk '{print $5}'`
	local ActFILE_COUNT=`cat $TMPDIR/tmpOut		| awk '{print $6}'`
	local ActCONTENT_SIZE=`cat $TMPDIR/tmpOut	| awk '{print $7}'`
	local ActFILE_NAME=`cat $TMPDIR/tmpOut		| awk '{print $8}'`

	assertEqual $ExQUOTA $ActQUOTA "NameQuota Mismatch"
	assertEqual $ExREMAINING_QUATA $ActREMAINING_QUATA "Remaining NameQuota Mismatch"
	assertEqual $ExSPACE_QUOTA $ActSPACE_QUOTA "SpaceQuota Mismatch"
	assertEqual $ExREMAINING_SPACE_QUOTA $ActREMAINING_SPACE_QUOTA 
	assertEqual $ExDIR_COUNT $ActDIR_COUNT "Directory Count Mismatch"
	assertEqual $ExFILE_COUNT $ActFILE_COUNT "File Counts Mismatch"
	assertEqual $ExCONTENT_SIZE $ActCONTENT_SIZE "[ Bug - 3961742 ] Content Size mistmatch due to partial blocks allows quotas to be violated"
}



######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
function test_NameQuota {
	localSetup

	TESTCASE_DESC="validate -count -q when quota has not been set"
	execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART"
	validateQuotaQueryResult "none" "inf" "none" "inf" 1 0 0 $SUITESTART
	displayTestCaseResult "withnoquota"

	TESTCASE_DESC="-setQuota on directory"
	execcmdlocal "$HDFSCMD dfsadmin -setQuota 5 $SUITESTART"
	validateQuotaQueryResult 5 4 "none" "inf" 1 0 0 $SUITESTART
	displayTestCaseResult "setQuota_dir"
	
	TESTCASE_DESC="Check quota on sub-directory when only parent has quota set"
	execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART/L1"
	validateQuotaQueryResult  "none" "inf" "none" "inf" 1 0 0 $SUITESTART/L1
	displayTestCaseResult "no_setQuota_subDir" 

	TESTCASE_DESC="Check quota on sub-directory"
	execcmdlocal "$HDFSCMD dfsadmin -setQuota 10 $SUITESTART/L1"
	validateQuotaQueryResult 10 9 "none" "inf" 1 0 0 $SUITESTART/L1
	validateQuotaQueryResult 5 3 "none" "inf" 2 0 0 $SUITESTART
	displayTestCaseResult "setQuota_subDir"

	TESTCASE_DESC="Validate Name quota while sub-directory has name quota set"
	execcmdlocal "$HDFSCMD dfs -touchz $SUITESTART/L1/a $SUITESTART/L1/b $SUITESTART/L1/c"
	execcmdlocal "$HDFSCMD dfs -ls -R $SUITESTART/L1"
	validateQuotaQueryResult 10 6 "none" "inf" 1 3 0 $SUITESTART/L1
	validateQuotaQueryResult 5 0 "none" "inf" 2 3 0 $SUITESTART
	displayTestCaseResult "validate_quota_subDir"
	
	TESTCASE_DESC="Create more file than quota in parentdir"
        cat > $TMPEXP<<EOF
touchz: The NameSpace quota (directories and files) of directory /user/$HDFSQA_USER/$SUITESTART is exceeded: quota=5 file count=6
EOF
	$HDFSCMD dfs -touchz $SUITESTART/L1/d 2>&1 | grep -v 'WARN conf.Configuration: mapred.task.id is deprecated' > $TMPRES
	assertEqualTwoFiles $TMPEXP $TMPRES "Parent Quota is overwritten by sub-directory quota"
	execcmdlocal "$HDFSCMD dfs -count -q $SUITESTART"
	execcmdlocal "$HDFSCMD dfs -ls -R $SUITESTART"
	displayTestCaseResult "subDir_negative_1" 
	
	TESTCASE_DESC="Create more directory than quota in parentdir"
	assertEqualCmdFailStatus "$HDFSCMD dfs -mkdir -p $SUITESTART/L1/d" "Parent Quota is overwritten by sub-directory quota"
	execcmdlocal "$HDFSCMD dfs -count -q $SUITESTART"
	execcmdlocal "$HDFSCMD dfs -ls -R $SUITESTART"
	displayTestCaseResult "subDir_negative_2" 

	TESTCASE_DESC="Validate -clrQuota in parent directory"
	execcmdlocal "$HDFSCMD dfsadmin -clrQuota $SUITESTART"
	validateQuotaQueryResult "none" "inf" "none" "inf" 2 3 0 $SUITESTART
	displayTestCaseResult "clrQuota_dir"

	TESTCASE_DESC="Check Quota in sub-directory when parent quota is cleared"
	validateQuotaQueryResult 10 6 "none" "inf" 1 3 0 $SUITESTART/L1
	displayTestCaseResult "clrQuota_dir_check_subDir"

	TESTCASE_DESC="Validate Sub Dir quota with parent not quota"
	execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART/L1/L2 $SUITESTART/L1/L3"
	execcmdlocal "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART/L1/L3"
	execcmdlocal "$HDFSCMD dfs -cp $SUITESTART/L1/L3 $SUITESTART/L1/L5"
	dd if=/dev/zero of=$TMPDIR/5GData bs=1000000000 count=5
	execcmdlocal "$HDFSCMD dfs -copyFromLocal $TMPDIR/5GData $SUITESTART/L1/L5"
	validateQuotaQueryResult 10 0 "none" "inf" 4 6 5000015518 $SUITESTART/L1
	displayTestCaseResult "subDir_validate"

	TESTCASE_DESC="Check copy after exceeding quota"
	assertEqualCmdFailStatus "$HDFSCMD dfs -cp $SUITESTART/L1/L3/passwd $SUITESTART/L1" "Copy successful even after exceeding quota"
	validateQuotaQueryResult 10 0 "none" "inf" 4 6 5000015518 $SUITESTART/L1
	displayTestCaseResult "copy_negative_quota"

	TESTCASE_DESC="Check move after exceeding quota"
	assertEqualCmdPassStatus "$HDFSCMD dfs -mv $SUITESTART/L1/L5/5GData $SUITESTART/L1"
	validateQuotaQueryResult 10 0 "none" "inf" 4 6 5000015518 $SUITESTART/L1
	displayTestCaseResult "move_withFullQuota"
	
	TESTCASE_DESC="Check quota free after remove"
	execcmdlocal "$HDFSCMD dfs -rm $SUITESTART/L1/5GData"
	assertEqualCmdPassStatus "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART/L1/"
	validateQuotaQueryResult 10 0 "none" "inf" 4 6 23277 $SUITESTART/L1
	displayTestCaseResult "validate_quotaFree_after_remove"

	TESTCASE_DESC="Remove and recreate same directory; check quota"
	execcmdlocal "$HDFSCMD dfs -rm -R $SUITESTART/L1"
	execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART/L1"
	validateQuotaQueryResult "none" "inf" "none" "inf" 1 0 0 $SUITESTART/L1
	displayTestCaseResult "recreate_same_dir"
	
	TESTCASE_DESC="Check Name quota with trash"
	execcmdlocal "$HDFSCMD dfs -rm -R -skipTrash /user/$HDFSQA_USER"
	execcmdlocal "$HDFSCMD dfs -mkdir -p /user/$HDFSQA_USER"
	execcmdlocal "$HDFSCMD dfsadmin -setQuota 7 /user/$HDFSQA_USER"
	execcmdlocal "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd /user/$HDFSQA_USER"
	execcmdlocal "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd /user/$HDFSQA_USER/passwd1"
	execcmdlocal "$HDFSCMD dfs -rm /user/$HDFSQA_USER/passwd"
	validateQuotaQueryResult 7 0 "none" "inf" 5 2 15518 /user/$HDFSQA_USER
	assertEqualCmdPassStatus "$HDFSCMD dfs -rm /user/$HDFSQA_USER/passwd1"
	validateQuotaQueryResult 7 0 "none" "inf" 5 2 15518 /user/$HDFSQA_USER
	displayTestCaseResult "Trash"
		
	TESTCASE_DESC="Check default nameQuota of HDFS Root (/)"
	execcmdlocal "$HDFSCMD dfs -count -q /"
	rootQuota=`$HDFSCMD dfs -count -q / | awk '{print $1}'`
	assertEqual 2147483647 $rootQuota "HDFS Root (/) nameQuota id not equal to 2147483647"
	displayTestCaseResult "root_default_nameQuota"

	localCleanup
}

function test_spaceQuota {
	localSetup
	
	echo "Getting the minimum space quota: minimum spacequota = dfs.replication * dfs.blocksize" 
	local REPL=`getValueFromField $HADOOP_CONF_DIR/hdfs-site.xml dfs.replication`
	if [ "x$REPL" = "x" ];then
		REPL=3
	fi
	local BLOCKSIZE=`getValueFromField $HADOOP_CONF_DIR/hdfs-site.xml dfs.blocksize`
	if [ "x$BLOCKSIZE" = "x" ];then
		BLOCKSIZE=268435456
	fi
	
	(( MIN_SPACE_QUOTA_MB = $REPL*$BLOCKSIZE/1024/1024 ))
	(( MIN_SPACE_QUOTA = MIN_SPACE_QUOTA_MB*1024*1024 ))
	
	TESTCASE_DESC="Setting space less than dfs.replication * dfs.blocksize"
	(( quota_MB = $MIN_SPACE_QUOTA_MB-1 ))
	(( quota = $quota_MB*1024*1024 ))
	
	execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART"
	execcmdlocal "$HDFSCMD dfsadmin -setSpaceQuota ${quota_MB}M  $SUITESTART"
	validateQuotaQueryResult "none" "inf" ${quota} ${quota} 1 0 0 $SUITESTART
	assertEqualCmdFailStatus "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART" "[ Bug-4524822 ] Putting file is possible even with setting space quota less than 3 times block size"
	displayTestCaseResult "spaceQuota_less_dfs.replication*dfs.blocksize"

	TESTCASE_DESC="Setting space dfs.replication * dfs.blocksize"
	execcmdlocal "$HDFSCMD dfs -rm -R -skipTrash $SUITESTART"
	execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART"
	execcmdlocal "$HDFSCMD dfsadmin -setSpaceQuota ${MIN_SPACE_QUOTA_MB}M  $SUITESTART"
	validateQuotaQueryResult "none" "inf" ${MIN_SPACE_QUOTA} ${MIN_SPACE_QUOTA} 1 0 0 $SUITESTART
	assertEqualCmdPassStatus "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART"
	$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART
	filesize=`ls -l $JOB_SCRIPTS_DIR_NAME/passwd | awk '{ print $5 }'`
	(( REMAIN_SPACE_QUOTA = $MIN_SPACE_QUOTA-$filesize*$REPL ))
	validateQuotaQueryResult "none" "inf" $MIN_SPACE_QUOTA $REMAIN_SPACE_QUOTA 1 1 $filesize $SUITESTART
	displayTestCaseResult "spaceQuota_dfs.replication*dfs.blocksize"

	TESTCASE_DESC="Putting first more data with space 3X64M"
	assertEqualCmdFailStatus "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd  $SUITESTART/1"
	validateQuotaQueryResult "none" "inf" $MIN_SPACE_QUOTA $REMAIN_SPACE_QUOTA 1 1 $filesize $SUITESTART 
	displayTestCaseResult "fit_more_dfs.replication*dfs.blocksize"

	localCleanup
}

function test_HDFS_1258_clrSpaceQuota_root {
	localSetup
	TESTCASE_DESC="[ HDFS -1258 ] Clearing namespace quota on \"/\" corrupts FS image"
	ver=`getHadoopVersion`
	echo $ver	
	if [ "$ver" == "2.0" ]; then
		`cat > $TMPEXP<<EOF	
clrQuota: Cannot clear namespace quota on root.
EOF`
	else
		
		`cat > $TMPEXP<<EOF	
clrQuota: java.lang.IllegalArgumentException: Cannot clear namespace quota on root.
EOF`	
	fi
	
	$HDFSCMD dfsadmin -clrQuota / 2>&1 | grep -v 'WARN conf.Configuration: mapred.task.id is deprecated' > $TMPRES
	assertEqualTwoFiles $TMPEXP $TMPRES "Clearing root namequota has become possible again"	
	
	setKerberosTicketForUser $HADOOPQA_USER
	resetNameNodes stop
	resetNameNodes start
	takeNNOutOfSafemode
	displayTestCaseResult
	
	localCleanup
}

function test_bug3758240_N_4416674_HDFS1189_quotaBetweenClrAndSet {
	localSetup
	
        execcmdlocal "$HDFSCMD dfs  -mkdir -p $SUITESTART/nameQuota"
        execcmdlocal "$HDFSCMD dfs  -mkdir -p $SUITESTART/spaceQuota"

        echo "Setting up Name and Space quota as \"$HDFSQA_USER\" user"
        execcmdlocal "$HDFSCMD dfsadmin -setQuota 5 $SUITESTART/nameQuota"
        
        echo "Getting the minimum space quota: minimum spacequota = dfs.replication * dfs.blocksize" 
		local REPL=`getValueFromField $HADOOP_CONF_DIR/hdfs-site.xml dfs.replication`
		if [ "x$REPL" = "x" ];then
			REPL=3
		fi
		local BLOCKSIZE=`getValueFromField $HADOOP_CONF_DIR/hdfs-site.xml dfs.blocksize`
		if [ "x$BLOCKSIZE" = "x" ];then
			BLOCKSIZE=268435456
		fi
				
		(( MIN_SPACE_QUOTA_MB = $REPL* $BLOCKSIZE/ 1024/ 1024 ))
				
        execcmdlocal "$HDFSCMD dfsadmin -setSpaceQuota ${MIN_SPACE_QUOTA_MB}m  $SUITESTART/spaceQuota"

        echo "Loading Files and Directories in HDFS /user/$HDFSQA_USER/$SUITESTART"
        execcmdlocal "$HDFSCMD dfs -mkdir -p $SUITESTART/nameQuota/L1"
        execcmdlocal "$HDFSCMD dfs -touchz $SUITESTART/nameQuota/A"
        execcmdlocal "$HDFSCMD dfs -touchz $SUITESTART/nameQuota/B"
        execcmdlocal "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART/spaceQuota/1"

        echo "Clearing up Name and Space quota as \"$HDFSQA_USER\" user"
        execcmdlocal "$HDFSCMD dfsadmin -clrQuota $SUITESTART/nameQuota"
        execcmdlocal "$HDFSCMD dfsadmin -clrSpaceQuota $SUITESTART/spaceQuota"

        echo "Putting Files again while there is no quota"
        execcmdlocal "$HDFSCMD dfs -touchz $SUITESTART/nameQuota/C"
        echo "Creating a 900M  sized file -> $TMPDIR/900mdata"
        dd if=/dev/zero of=$TMPDIR/900mdata bs=100000000 count=9
        execcmdlocal "$HDFSCMD dfs -put $TMPDIR/900mdata $SUITESTART/spaceQuota/2"

        echo "Re-Setting up Name and Space quota as \"$HDFSQA_USER\" user" 
        execcmdlocal "$HDFSCMD dfsadmin -setQuota 5 $SUITESTART/nameQuota"
        execcmdlocal "$HDFSCMD dfsadmin -setSpaceQuota ${MIN_SPACE_QUOTA_MB}m  $SUITESTART/spaceQuota"

        echo "FINAL VALIDATIONs"
        execcmdlocal "$HDFSCMD dfs -count -q $SUITESTART/spaceQuota $SUITESTART/nameQuota"

        TESTCASE_DESC="[ Bug-3758240/Bug-4416674/HDFS:1189 ] Name Quota counts missed between clear quota and set quota"
        cmd="$HDFSCMD dfs -touchz $SUITESTART/nameQuota/D"
        $cmd 2>&1 | grep -v 'WARN conf.Configuration: mapred.task.id is deprecated' > $TMPRES 
        echo "$cmd"
        cat $TMPRES
        cat > $TMPEXP<<EOF
touchz: The NameSpace quota (directories and files) of directory /user/$HDFSQA_USER/$SUITESTART/nameQuota is exceeded: quota=5 file count=6
EOF
        assertEqualTwoFiles $TMPRES $TMPEXP "Ticket - 3758240; Name  Quota counts missed between clear quota and set quota"
        displayTestCaseResult "nameQuota"

        TESTCASE_DESC="[ Bug-3758240/Bug-4416674/HDFS:1189 ] Space Quota counts missed between clear quota and set quota"
        assertEqualCmdFailStatus "$HDFSCMD dfs -put $JOB_SCRIPTS_DIR_NAME/passwd $SUITESTART/spaceQuota/3"
        displayTestCaseResult "spaceQuota"

	localCleanup
}

function test_BUG_4567876_quota_with_Trash {
        localSetup
        setTestCaseDesc "[ BUG 2971228 ] HADOOP-6203 Improve error message when moving to trash fails due to quota issue"
        execcmdlocal "$HDFSCMD dfs  -rm -R -skipTrash /user/$HADOOPQA_USER"
        execcmdlocal "$HDFSCMD dfs  -mkdir -p /user/$HADOOPQA_USER"
        execcmdlocal "$HDFSCMD dfs  -chown -R $HADOOPQA_USER /user/$HADOOPQA_USER"
        execcmdlocal "$HDFSCMD dfsadmin -setQuota 5 /user/$HADOOPQA_USER"
        setKerberosTicketForUser $HADOOPQA_USER
        execcmdlocal "$HDFSCMD dfs  -mkdir -p /user/$HADOOPQA_USER/.Trash"
        execcmdlocal "$HDFSCMD dfs  -mkdir -p /user/$HADOOPQA_USER/a/b/c"
        validateQuotaQueryResult 5 0 "none" "inf" 5 0 0 /user/$HADOOPQA_USER
        assertEqualCmdFailStatus "$HDFSCMD dfs -rm -R /user/$HADOOPQA_USER/a/b/c"
	$HDFSCMD dfs -rm -R /user/$HADOOPQA_USER/a/b/c 2>  $TMPRES
	$HDFSCMD dfs -ls -R /user/$HADOOPQA_USER
	echo ">>>>>>>>>>>>>>>"
	cat $TMPRES
	echo ">>>>>>>>>>>>>>>"
	#cat $TMPRES | grep 'Problem with Trash.The NameSpace quota (directories and files) of directory /user/hadoopqa is exceeded: quota=5 file count=6'
	cat $TMPRES | grep "WARN fs.TrashPolicyDefault: Can't create trash directory"
	local stat=$?
	assertEqual 0 $stat "[ bug-4586423 ] HADOOP-6203 comes back "
	cat $TMPRES | grep 'Consider using -skipTrash option'
	local stat=$?
	assertEqual 0 $stat "[ bug-4586423 ] HADOOP-6203 comes back "
	cat $TMPRES | grep 'rm: Failed to move to trash'
	local stat=$?
        assertEqual 0 $stat "[ bug-4586423 ] HADOOP-6203 comes back "
        displayTestCaseResult 
        setKerberosTicketForUser $HDFSQA_USER
        execcmdlocal "$HDFSCMD dfsadmin -clrQuota /user/$HADOOPQA_USER"
        setKerberosTicketForUser $HADOOPQA_USER
        execcmdlocal "$HDFSCMD dfs  -count -q /user/$HADOOPQA_USER"
        execcmdlocal "$HDFSCMD dfs  -rm -R -skipTrash /user/$HADOOPQA_USER/a"
	
        localCleanup
}

function test_BUG_3961742_HDFS_1377 {
	localSetup
	setTestCaseDesc "[ BUG 3961742/HDFS-1377 ] Quota bug for partial blocks allows quotas to be violated"
	execcmdlocal "$HDFSCMD dfs -rm -R -skipTrash /user/$HDFSQA_USER/$SUITESTART"
	execcmdlocal "$HDFSCMD dfs -mkdir -p /user/$HDFSQA_USER/$SUITESTART"
	#execcmdlocal "$HDFSCMD dfs -put /etc/passwd /user/$HDFSQA_USER/$SUITESTART"
	#echo "$HDFSCMD dfs -stat \"%o\" /user/$HDFSQA_USER/$SUITESTART/passwd 2>&1 | grep -v WARN | sed -e 's/\"//g'"
	#local blocksize=`$HDFSCMD dfs -stat \"%o\" /user/$HDFSQA_USER/$SUITESTART/passwd 2>&1 | grep -v WARN | sed -e 's/"//g'`
	local fileCount=0
	local quota=0
	
	echo "Getting the minimum space quota: minimum spacequota = dfs.replication * dfs.blocksize" 
	local REPL=`getValueFromField $HADOOP_CONF_DIR/hdfs-site.xml dfs.replication`
	if [ "x$REPL" = "x" ];then
		REPL=3
	fi
	local BLOCKSIZE=`getValueFromField $HADOOP_CONF_DIR/hdfs-site.xml dfs.blocksize`
	if [ "x$BLOCKSIZE" = "x" ];then
		BLOCKSIZE=268435456
	fi
		
	(( fileCount = $BLOCKSIZE/ 1000000 ))
	(( quota = $BLOCKSIZE * $REPL ))
	echo ">>>>>>>>> BLOCKSIZE = $BLOCKSIZE >>>>>>>>>>>>>>>>"
	echo ">>>>>>>>> QUOTA     = $quota     >>>>>>>>>>>>>>>>"
	mkdir $TMPDIR/dir
	#execcmdlocal "$HDFSCMD dfs -rm -R -skipTrash /user/$HDFSQA_USER/$SUITESTART/passwd"
	execcmdlocal "dd if=/dev/zero of=$TMPDIR/dir/file1 bs=1000000 count=$fileCount"
	execcmdlocal "dd if=/dev/zero of=$TMPDIR/dir/file2 bs=1000000 count=$fileCount"
	ls -l $TMPDIR/dir/file1
	execcmdlocal "$HDFSCMD dfsadmin -setSpaceQuota $quota /user/$HDFSQA_USER/$SUITESTART"
	execcmdlocal "$HDFSCMD dfs -count -q /user/$HDFSQA_USER/$SUITESTART"
	assertEqualCmdFailStatus "$HDFSCMD dfs -put $TMPDIR/dir /user/$HDFSQA_USER/$SUITESTART"
	(( filesize = 1000000* $fileCount ))
	(( remain = $quota- $filesize* $REPL ))
	
	echo $filesize
	echo $remain
	 
	validateQuotaQueryResult "none" "inf" $quota  $remain  2   1  $filesize /user/$HDFSQA_USER/$SUITESTART
		
	displayTestCaseResult

	localCleanup
}

## Execute Tests
executeTests
