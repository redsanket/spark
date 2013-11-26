#!/bin/sh

## Import Libraries
. $WORKSPACE/lib/hdfs_dfip.sh

# Setting Owner of this TestSuite
OWNER="rajsaha"

function testSteps_HDFSCMDEXEC {
	local timing=$1
	local diskFailOpt=$2
	local diskFailamt=$3
	local TESTCASENAME=$4
	local cmd=$5
	local asserttype=$6
	local dstFile=$7
	local diskFailpercent=""
	local dns=""
	if [ "$diskFailamt" == "moreVolTol" ];then
		dns=${DN[3]}
		diskFailpercent="50"
	else
		dns="${DN[1]},${DN[2]}"
		diskFailpercent="25"
	fi
	setTestCaseDesc "HDFS DFIP :: $TESTCASENAME"
	TakeLogSnapShot hdfs namenode $NN
	TakeLogSnapShot hdfs datanode $dns
	local command="$HADOOP_HDFS_CMD $cmd"
	if [ "$timing" == "during" ];then
		echo "command :: $command 2>&1 &"
		$command 2>&1 &
		local pid=$!
		DiskFailInMultiNodes $diskFailOpt $diskFailpercent $dns
		echo "Waiting for $pid to finish"
		wait $pid
	else
		execcmdlocal "$command"
		DiskFailInMultiNodes $diskFailOpt $diskFailpercent $dns
	
	fi
	echo "Sleeping for 10s"
	sleep 10
	$asserttype $dstFile
	fscktest
	ExtractDifferentialLog hdfs namenode $NN "4851866"
	ExtractDifferentialLog hdfs datanode $dns "4851866"
	displayTestCaseResult
	RecoverFailedDisks $diskFailOpt
	if [ "$diskFailamt" == "moreVolTol" ] || [ "$diskFailOpt" == "chmod" ];then
		resetNodes $dns "," datanode stop 
		resetNodes $dns "," datanode start $SHARETMPDIR/${DN[0]}/hadoop
	fi
	echo "Sleeping for NN and DN to realign blocks across cluster"
	sleep 30
}

######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
function test_DFIP_NNRestart {
	### Feed DATA to HDFS for all 8 tests
        execcmdlocal "$HADOOP_HDFS_CMD dfs -put $DATAFILE /user/$HADOOPQA_USER/data1"

        setTestCaseDesc "01. Namenode startup : Disks Failed(in DN) Before NN startup : Disk Failed = VolTolerated : umount -l"
        DiskFailInMultiNodes "umount" 25 "${DN[1]},${DN[3]}"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        checkNNstate
        displayTestCaseResult "before-umount-lessVolTol"
        RecoverFailedDisks "umount"

        setTestCaseDesc "03. Namenode startup : Disks Failed(in DN) Before NN startup : Disk Failed > VolTolerated : umount -l"
	DiskFailInMultiNodes "umount" 75 "${DN[1]}"
	resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
	checkNNstate
	displayTestCaseResult "before-umount-moreVolTol"
	RecoverFailedDisks "umount"
	resetNode ${DN[1]} datanode start $SHARETMPDIR/${DN[1]}/hadoop
	
        setTestCaseDesc "05. Namenode startup : Disks Failed(in DN) During NN startup : Disk Failed = VolTolerated : umount -l"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        DiskFailInMultiNodes "umount" 25 "${DN[1]},${DN[3]}"
        checkNNstate
        displayTestCaseResult "during-umount-EqVolTol"
        RecoverFailedDisks "umount"

        setTestCaseDesc "07. Namenode startup : Disks Failed(in DN) During NN startup : Disk Failed > VolTolerated : umount -l"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        DiskFailInMultiNodes "umount" 75 "${DN[1]}"
        checkNNstate
        displayTestCaseResult "during-umount-moreVolTol"
        RecoverFailedDisks "umount"
	resetNode ${DN[1]} datanode start $SHARETMPDIR/${DN[1]}/hadoop

        setTestCaseDesc "02. Namenode startup : Disks Failed(in DN) Before NN startup : Disk Failed = VolTolerated : chmod 000 Datadir"
        DiskFailInMultiNodes "chmod" 25 "${DN[1]},${DN[3]}"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        checkNNstate
        displayTestCaseResult "before-chmod-lessVolTol"
        RecoverFailedDisks "chmod"
        resetNodes "${DN[1]},${DN[3]}" ","  datanode start $SHARETMPDIR/${DN[1]}/hadoop

        setTestCaseDesc "04. Namenode startup : Disks Failed(in DN) Before NN startup : Disk Failed > VolTolerated : chmod 000 Datadir"
        DiskFailInMultiNodes "chmod" 75 "${DN[1]}"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        checkNNstate
        displayTestCaseResult "before-chmod-moreVolTol"
        RecoverFailedDisks "chmod"
        resetNode ${DN[1]} datanode start $SHARETMPDIR/${DN[1]}/hadoop

        setTestCaseDesc "06. Namenode startup : Disks Failed(in DN) During NN startup : Disk Failed = VolTolerated : chmod 000 Datadir"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        DiskFailInMultiNodes "chmod" 25 "${DN[1]},${DN[3]}"
        checkNNstate
        displayTestCaseResult "during-chmod-EqVolTol"
        RecoverFailedDisks "chmod"
        resetNodes "${DN[1]},${DN[3]}" ","  datanode start $SHARETMPDIR/${DN[1]}/hadoop

        setTestCaseDesc "08. Namenode startup : Disks Failed(in DN) During NN startup : Disk Failed > VolTolerated : chmod 000 Datadir"
        resetNode $NN namenode stop
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
        DiskFailInMultiNodes "chmod" 75 "${DN[1]}"
        checkNNstate
        displayTestCaseResult "during-chmod-moreVolTol"
        RecoverFailedDisks "chmod"
        resetNode ${DN[1]} datanode start $SHARETMPDIR/${DN[1]}/hadoop

	cleanHDFS
}

function test_DFIP_BUG_4445034_NNSafemode {
	local diskFailOpt="umount"
	setTestCaseDesc "[ Bug 4445034 ] Failing disks (by umount -l )less than VolTolerated in one datanode during NN Startup Safemode. When NN is out of safemode fail disks more than VolTolerated in another DN "
	execcmdlocal "$HADOOP_HDFS_CMD dfs -put $DATAFILE /user/$HADOOPQA_USER/data1"
	sleep 10
	resetNode $NN namenode stop
	TakeLogSnapShot hdfs datanode ${DN[1]}
	TakeLogSnapShot hdfs datanode ${DN[3]}
	TakeLogSnapShot hdfs namenode $NN
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
	DiskFailInMultiNodes $diskFailOpt 25 ${DN[1]}
	takeNNOutOfSafemode
	DiskFailInMultiNodes $diskFailOpt 75 ${DN[3]}
	pidExists=`ssh ${DN[3]} "ps aux | grep -v grep | grep java | grep -c datanode"`
	local stat=1
	local count=10
        while [ $count -gt 0 ];do
		if  [ "$pidExists" == "0" ];then
			stat=0
			break;
		else
                	echo "Waiting for the datanode process to finish"
                	pidExists=`ssh ${DN[3]} "ps aux | grep -v grep | grep java | grep -c datanode"`
               	 	echo "$pidExists processes are still running"
                	execcmdlocal "$HDFSCMD dfs -rmr /user/$USER/data1"
                	execcmdlocal "$HDFSCMD dfs -put ${TMPDIR}/DATA /user/$USER/data1"
		fi
		(( count = $count - 1 ))
        done
	ExtractDifferentialLog hdfs datanode ${DN[1]} "4851866"
	ExtractDifferentialLog hdfs datanode ${DN[3]} "4851866"
	ExtractDifferentialLog hdfs namenode $NN "4851866"
	assertEqual $stat 0 "Datanode is still not shutdown"
	assertEqual `grep -c 'SHUTDOWN_MSG: Shutting down DataNode' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${DN[3]}.log.diff` 1

	displayTestCaseResult
	RecoverFailedDisks $diskFailOpt
	resetNode ${DN[3]} datanode start $SHARETMPDIR/${DN[2]}/hadoop
}



function DEPRECATED_test_DFIP_BUG_4512169_chmod_allDN {
        local diskFailOpt="chmod"
        local diskFailpercent=25
        local dns="${DN[0]},${DN[1]},${DN[2]},${DN[3]}"
	setTestCaseDesc "[ Bug 4512169 ] HDFS DFIP putting a file and fail disks less than volTolerated with \"$diskFailOpt\""

        TakeLogSnapShot hdfs datanode $dns
        local cmd="$HADOOP_HDFS_CMD dfs -copyFromLocal $DATAFILE /user/$HADOOPQA_USER/data1"
        echo "command :: $cmd 2>&1 &"
        $cmd 2>&1 &
        DiskFailInMultiNodes $diskFailOpt $diskFailpercent "${DN[0]},${DN[1]},${DN[2]}"
        DiskFailInMultiNodes $diskFailOpt 50 ${DN[3]}
        local pid=$!
        echo "Waiting for $pid to finish"
        wait $pid
        echo "Sleeping for 10s"
        sleep 10
        $HADOOP_HDFS_CMD dfs -ls /user/$HADOOPQA_USER/
        local newSize=`$HADOOP_HDFS_CMD dfs -dus /user/$HADOOPQA_USER/data1 | awk '{print $2}'`
        assertEqual $FILESIZE $newSize "File Size is not matched after Fault Injection"

	fscktest

        ExtractDifferentialLog hdfs datanode $dns "4851866"
        displayTestCaseResult
        RecoverDiskNRestartMultiDN $diskFailOpt $dns
}

function DEPRECATED_test_DFIP_BUG_4512360_umount_allDN {
	local diskFailOpt="umount"
	local diskFailpercent=25
	local dns="${DN[0]},${DN[1]},${DN[2]},${DN[3]}"

	setTestCaseDesc "[ Bug 4512360 ] Fail Disks (less than VolTolerated limit by UMOUNT ) in all the datanodes in a cluster BEFORE NN StartUp"

	execcmdlocal "$HADOOP_HDFS_CMD dfs -copyFromLocal $DATAFILE /user/$HADOOPQA_USER/data1"
	DiskFailInMultiNodes $diskFailOpt $diskFailpercent $dns
	resetNode $NN namenode stop
	TakeLogSnapShot hdfs namenode $NN
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
	takeNNOutOfSafemode
	$HADOOP_HDFS_CMD dfs -ls /user/$HADOOPQA_USER/
	local newSize=`$HADOOP_HDFS_CMD dfs -dus /user/$HADOOPQA_USER/data1 | awk '{print $2}'`
	assertEqual $FILESIZE  $newSize "File Size is not matched after Fault Injection"
	
	#fscktest
	setKerberosTicketForUser $HDFS_USER
        $HADOOP_HDFS_CMD fsck / 2>&1
	setKerberosTicketForUser $HADOOPQA_USER

        ExtractDifferentialLog hdfs namenode $NN "4851866"
        displayTestCaseResult
        RecoverDiskNRestartMultiDN $diskFailOpt $dns
	cleanHDFS
}


function test_DFIP_HDFS_1592_HonorVolTol {
	TESTACASEDES_BASE="[ HDFS-1592 ] 1. When Datanode is started, it checks if volumes tolerated is honored. 2. Also, volumes required is calculated correctly at the startup."
	datanode=${DN[0]}
	execcmdlocal "$HDFSCMD dfs -put $DATAFILE /user/$USER/1"

	#for diskFailOpt in "mountRO" "chmodCurrent" "umount" "chmod"; do
	for diskFailOpt in "mountRO" "umount" "chmod"; do
		for diskFailamt in "lessVolTol" "EqVolTol" "moreVolTol"; do
			if [ "$diskFailamt" == "lessVolTol" ];then
				DiskFailureInPercentage=0
				datanodeProcCount=2
				shutDownMsg=0
			elif [ "$diskFailamt" == "EqVolTol" ];then
				DiskFailureInPercentage=25
				datanodeProcCount=2
				shutDownMsg=0
			else
				DiskFailureInPercentage=50
				datanodeProcCount=0
				shutDownMsg=1
			fi
			desc="${diskFailOpt}-${diskFailamt}-before"
			setTestCaseDesc "${TESTACASEDES_BASE} :: $desc"
			FailDisksNdatanodeRestart $datanode $DiskFailureInPercentage $diskFailOpt
			echo "Waiting for 20s"
			sleep 20
			assertEqual `getDatanodeProcCount $datanode` $datanodeProcCount "$desc - Datanode processes are not equal to $datanodeProcCount"
			assertEqual `grep -c 'SHUTDOWN_MSG: Shutting down DataNode' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` $shutDownMsg
			displayTestCaseResult $desc
			RecoverDiskNRestartMultiDN $diskFailOpt $datanode
		done
	done
	cleanHDFS
}

function test_DFIP_BUG_4299952_datanodeProc {
	setTestCaseDesc "[ Bug-4299952 ] Datanode process hangs after shutdown is called; Volume Tolerated = 0 and failing 1 disk in datanode by \"umount -l\" while dataloading is happening in HDFS"
	datanode=${DN[0]}
	DiskFailureInPercentage=75
	DiskFailOption="umount"

	#### Take Log from the datanode
	TakeLogSnapShot hdfs datanode $datanode

	### Feeding to HDFS
	echo ">>>>>> Putting $DATAFILE in HDFS ( Job is in Backgroud )"
	$HDFSCMD dfs -put $DATAFILE /user/$USER/1
	pid=$!
	echo "Disk Fail will start after 5 sec"
	sleep 10

	#### Fault Injection
        DiskFail $datanode $DiskFailureInPercentage $DiskFailOption
	echo "Waiting for $pid to finish"
	wait $pid
	echo "Waiting for Datanode to be shutdown"
	pidExists=`ssh $datanode "ps aux | grep -v grep | grep java | grep -c datanode"`
	local count=0
	while [ $pidExists -gt 0 ];do
		echo "Waiting for the datanode process to finish"
		execcmdlocal "$HDFSCMD dfs -rmr /user/$USER/1"
		execcmdlocal "$HDFSCMD dfs -put $DATAFILE /user/$USER/1"
		pidExists=`ssh $datanode "ps aux | grep -v grep | grep java | grep -c datanode"`
		echo "$pidExists processes are still running"
		(( count = $count + 1 ))
		if [ "$count" == "30" ];then
			assertEqual 0 1 "INTERMITTENT :: Lazy DATANODE shutdown not triggered even after 3attempts"
			break;
		fi
	done
	sleep 10
        ExtractDifferentialLog hdfs datanode $datanode "4851866"
	assertEqual `getDatanodeProcCount $datanode` 0 "Still Datanode processes are hanging, after exceeding volume tolerated"
	assertEqual `grep -c 'SHUTDOWN_MSG: Shutting down DataNode' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
	displayTestCaseResult

	RecoverDiskNRestartMultiDN $DiskFailOption $datanode

	cleanHDFS
}

function test_DFIP_BUG_4318740_validVol {
	TESTACASEDES_BASE="[ Bug 4318740 ] At Startup, Valid volumes required has a bug"
	setTestCaseDesc "${TESTACASEDES_BASE}; volTolerated=25%  volumeFailed=50%"

        datanode=${DN[0]} ### First Datanode in the LIVE datanodes list
        DiskFailureInPercentage=50
	(( tolerated = ( $VolToleratedInPercentage * $DISKCOUNT_IN_DATANODE ) / 100 ))
	(( failed = ( $DiskFailureInPercentage * $DISKCOUNT_IN_DATANODE ) / 100 ))
        DiskFailOption="umount"

	execcmdlocal "$HDFSCMD dfs -put $DATAFILE /user/$USER/4G_1"

        FailDisksNdatanodeRestart $datanode $DiskFailureInPercentage $DiskFailOption
	assertEqual `grep -c "ERROR.*volsFailed : $failed.*Volumes tolerated : $tolerated" ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
        assertEqual `grep -c 'SHUTDOWN_MSG: Shutting down DataNode' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
        displayTestCaseResult 

	RecoverDiskNRestartMultiDN $DiskFailOption $datanode

	cleanHDFS
}

function test_DFIP_BUG_4132253_nonaccessibleDataDir {
	setTestCaseDesc "[ Bug-4132253/HDFS-7040 ] Datanode can't start if disk drive doesn't exist or on failed drive"
	datanode=${DN[0]}
	DiskFailureInPercentage=50
	DiskFailOption="chmod"
	execcmdlocal "$HDFSCMD dfs -put $DATAFILE  /user/$USER/4G_1"
	FailDisksNdatanodeRestart $datanode $DiskFailureInPercentage $DiskFailOption
	assertEqual `grep -c 'Incorrect permission for /grid/0/hadoop/var/hdfs/data, expected: rwx------, while actual: ---------' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
	assertEqual `grep -c 'Incorrect permission for /grid/1/hadoop/var/hdfs/data, expected: rwx------, while actual: ---------' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
	assertEqual `grep -c "ERROR.*volsFailed : $failed.*Volumes tolerated : $tolerated" ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
	assertEqual `grep -c 'SHUTDOWN_MSG: Shutting down DataNode' ${ARTIFACTS_DIR}/hadoop-hdfs-datanode-${datanode}.log.diff` 1
	assertEqual `getDatanodeProcCount $datanode` 0 "Still Datanode processes are hanging, after exceeding volume tolerated"
	displayTestCaseResult

	RecoverDiskNRestartMultiDN $DiskFailOption $datanode

        cleanHDFS
}

function test_DFIP_BUG_4299900_logdir {
	setTestCaseDesc "[ Bug-4299900 ] Datanode can't start if disk on which logs dir failed; Indirect Approach: Making LogDir not writable"
	datanode=${DN[0]}
	resetNode $datanode datanode stop
	sleep 2
	assertEqual `getDatanodeProcCount $datanode` 0 "Datanode not killed"
	resetNode $datanode datanode start
	sleep 2
	assertEqual `getDatanodeProcCount $datanode` 2 "Datanode is not up"
	sleep 2
	runasroot $datanode mv "$HADOOP_LOG_DIR ${HADOOP_LOG_DIR}.bak"
	ssh $datanode "touch /tmp/empty"
	runasroot $datanode cp "/tmp/empty $HADOOP_LOG_DIR"
	sleep 2
	resetNode $datanode datanode stop
	sleep 2
	assertEqual `getDatanodeProcCount $datanode` 0 "Datanode not killed - 2nd time"
	resetNode $datanode datanode start
	sleep 2
	assertEqual `getDatanodeProcCount $datanode` 0 "Datanode is still up after logdir not writable"

	runasroot $datanode mv "$HADOOP_LOG_DIR /tmp/empty.1"
	runasroot $datanode mv "$HADOOP_LOG_DIR.bak $HADOOP_LOG_DIR"
	resetNode $datanode datanode stop
	sleep 2
	assertEqual `getDatanodeProcCount $datanode` 0 "Datanode not killed - 2nd time"
	resetNode $datanode datanode start
	sleep 2
	assertEqual `getDatanodeProcCount $datanode` 2 "Datanode is still up after logdir not writable"
	displayTestCaseResult

        cleanHDFS

}

function test_DFIP_HDFSCMDEXEC_after_umount_EqVolTol {
	bunchofTests "after" "umount" "EqVolTol"
}
function test_DFIP_HDFSCMDEXEC_after_umount_moreVolTol {
	bunchofTests "after" "umount" "moreVolTol"
}
function test_DFIP_HDFSCMDEXEC_after_chmod_EqVolTol {
	bunchofTests "after" "chmod" "EqVolTol"
}
function test_DFIP_HDFSCMDEXEC_after_chmod_moreVolTol {
	bunchofTests "after" "chmod" "moreVolTol"
}

function test_DFIP_HDFSCMDEXEC_during_umount_EqVolTol {       
        bunchofTests "during" "umount" "EqVolTol"
}

function test_DFIP_HDFSCMDEXEC_during_umount_moreVolTol {
        bunchofTests "during" "umount" "moreVolTol"
}

function test_DFIP_HDFSCMDEXEC_during_chmod_EqVolTol {
        bunchofTests "during" "chmod" "EqVolTol"
}

function test_DFIP_HDFSCMDEXEC_during_chmod_moreVolTol {
        bunchofTests "during" "chmod" "moreVolTol"
}

function bunchofTests {
	CALLER=`caller 0|awk '{print $2}'`
	timing=$1
	diskFailOpt=$2
	diskFailamt=$3
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-copyFromLocal-file" "dfs -copyFromLocal $DATAFILE /user/$HADOOPQA_USER/data1" "filesizetest" "data1"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-HDFScopy-file" "dfs -cp /user/$HADOOPQA_USER/data1 /user/$HADOOPQA_USER/data2" "filesizetest" "data2"
       	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-HDFSmove-filr" "dfs -mv /user/$HADOOPQA_USER/data2 /user/$HADOOPQA_USER/.data.crc" "filesizetest" ".data.crc"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-distcpHDFStoHDFS-file" "distcp  /user/$HADOOPQA_USER/data1 /user/$HADOOPQA_USER/data3" "filesizetest" "data3"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-distcpHFTPtoHDFS-file" "distcp  hftp://$NN:50070/user/$HADOOPQA_USER/.data.crc hdfs://$NN/user/$HADOOPQA_USER/data4" "filesizetest" "data4"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-rmr-file" "dfs -rmr /user/$HADOOPQA_USER/data4" "filesizetest" ".Trash/Current/user/$HADOOPQA_USER/data4"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-put-Dir" "dfs -put $DATADIR /user/$HADOOPQA_USER" "dirsizetest" "L2_0Dir"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-cp-Dir" "dfs -cp /user/$HADOOPQA_USER/L2_0Dir /user/$HADOOPQA_USER/L3" "dirsizetest" "L3"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-mv-Dir" "dfs -mv /user/$HADOOPQA_USER/L3 /user/$HADOOPQA_USER/L4" "dirsizetest" "L4"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-dus-Dir" "dfs -dus /user/$HADOOPQA_USER/L4" "dustest" "L4"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-rmr-Dir" "dfs -rmr /user/$HADOOPQA_USER/L4" "dirsizetest" ".Trash/Current/user/$HADOOPQA_USER/L4"
	testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt "$CALLER-chmod-Dir" "dfs -chmod -R 766 /user/$HADOOPQA_USER/L2_0Dir" "chmodtest" "L2_0Dir"

	### Clean All the data in HDFS
        cleanHDFS
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HADOOP_HDFS_CMD dfs -mkdir -p /user/$HADOOPQA_USER"
        execcmdlocal "$HADOOP_HDFS_CMD dfs -chown hadoopqa /user/$HADOOPQA_USER"
        setKerberosTicketForUser $HADOOPQA_USER
}

function test_DFIP_archive_dir_during {
	ARCHIVE_JAR=/home/y/lib/jars/hadoop_archive.jar
	if [ -f "$ARCHIVE_JAR" ];then 
		echo $HADOOP_CLASSPATH | grep $ARCHIVE_JAR
		if [ "$?" -ne "0" ];then
			export HADOOP_CLASSPATH=${ARCHIVE_JAR}:${HADOOP_CLASSPATH}
		fi
	else
		echo "$ARCHIVE_JAR NOT FOUND, CANNOT CONTINUTE ARCHIVE TESTS" 
		exit 1
	fi
	dfipArchiveBase="/user/$HADOOPQA_USER/DFIP_ARCHIVE"
	execcmdlocal "$HADOOP_HDFS_CMD dfs -mkdir -p $dfipArchiveBase/SRC"
	execcmdlocal "$HADOOP_HDFS_CMD dfs -put $TMPDIR/DATA $dfipArchiveBase/SRC"

	timing="during"
	for diskFailOpt in "umount" "chmod"; do
		for diskFailamt in "EqVolTol" "moreVolTol"; do
			dfipArchiveName="$CALLER-archive-dir"
			testSteps_HDFSCMDEXEC $timing $diskFailOpt $diskFailamt $dfipArchiveName "archive -archiveName $dfipArchiveName.har -p $dfipArchiveBase SRC ${dfipArchiveBase}/DST" "archivetest" "DFIP_ARCHIVE/DST/${dfipArchiveName}.har"	
			execcmdlocal "$HADOOP_HDFS_CMD dfs -lsr ${dfipArchiveBase}"
			execcmdlocal "$HADOOP_HDFS_CMD dfs -rmr -skipTrash ${dfipArchiveBase}/DST"
		done

	done

}

function test_DFIP_fsck_during {
	setTestCaseDesc "HDFS DFIP :: Disk Fails more than volTolerated by chmod during FSCK"
	execcmdlocal "$HADOOP_HDFS_CMD dfs -put $TMPDIR/DATA /user/$HADOOPQ_USER"
	echo "Without Disk Fail :: $HADOOP_HDFS_CMD fsck / -files -blocks -locations"
	$HADOOP_HDFS_CMD fsck / -files -blocks -locations 2>&1 | egrep -v 'FSCK started by|FSCK ended at' > $TMPEXP
	echo "With Disk Fail BG :: $HADOOP_HDFS_CMD fsck / -files -blocks -locations"
	$HADOOP_HDFS_CMD fsck / -files -blocks -locations 2>&1 | egrep -v 'FSCK started by|FSCK ended at' > $TMPRES &
	local pid=$!
	DiskFailInMultiNodes "umount" 75 "${DN[1]}"
	wait $pid
	assertEqualTwoFiles $TMPEXP $TMPRES
	displayTestCaseResult
	RecoverDiskNRestartMultiDN "umount" "${DN[1]}"
	cleanHdfs
}

## Setting up Test Environment
testSuiteSetup

## Execute Tests
executeTests

## Cleaning the cluster and give back in its orinigal shape
testSuiteCleanUp

