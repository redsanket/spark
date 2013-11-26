#!/bin/sh

## Import Libraries
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh


function init {
	DATANODECOUT_IN_CLUSTER=$1
	DISKCOUNT_IN_DATANODE=$2
	if [ $DATANODECOUT_IN_CLUSTER -lt 3 ];then
		echo "FATAL : You should have atleast 3 datanodes in the cluster when replication factor is 3"
		exit 1
	fi
	### Regularly used script level GLOBAL variables
	SUITESTART=`date +%s`
	HDFSCMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR "
	ORIG_HDFS_SITE="$HADOOP_CONF_DIR/hdfs-site.xml"
	### Temporary files
	TMPRES=$TMPDIR/result
	TMPEXP=$TMPDIR/expected

	### This variable says if "DecommisionDatanodes" is called, then it sets to "1"
	DECOMMSTAT=0

	### Get the nodes hostname and their custom config directory
	NN=`getNameNode`
	SNN=`getSecondaryNameNode`
	JT=`getJobTracker_TMP`
	ACTIVEDATANODES=""

	ssh $NN "cat $HADOOP_CONF_DIR/slaves | grep -v ^\s*$ | sort" > $TMPEXP
	ORIGDNS=`cat $TMPEXP | tr '\n' ' '`
	LIVEDNS=`getLiveDatanodes`
	echo $LIVEDNS | tr ' ' '\n' > $TMPRES
	EXCLUDEDATANODES=`diff $TMPEXP $TMPRES | egrep  "^<|^>" | awk '{print $2}' | tr '\n' ' '`
	echo ">>>>>> NN => $NN"
	echo ">>>>>> JT => $JT"
	echo ">>>>>> SNN=> $SNN"
	echo ">>>>>> DATANODES ::"
	echo ">>>>>> 	DEPLOYED 	=> $ORIGDNS"
	echo ">>>>>> 	LIVE		=> $LIVEDNS"
	echo ">>>>>> 	EXCLUDE		=> $EXCLUDEDATANODES"
	if [ `echo $LIVEDNS | awk '{print NF}'` -lt $DATANODECOUT_IN_CLUSTER ]; then
		echo "FATAL : Not enough LIVE nodes to proceed"
		exit 1
	else 
		dindex=0
		for dn in $LIVEDNS ; do
			### We want only 4 nodes for DFIP Functional Tests
			if [ $dindex -lt $DATANODECOUT_IN_CLUSTER ];then
				DN[$dindex]=$dn
				ACTIVEDATANODES="$ACTIVEDATANODES $dn"
			else
				EXCLUDEDATANODES="$EXCLUDEDATANODES $dn"
			fi
			let dindex++
		done
	fi
}

#########################################################
# Steps 1. Shrink the cluster with 4 Datanodes
#       2. Restart Cluster with VolTolerated 25% of total disks
#       3. Create Test Data
#########################################################
function testSuiteSetup {
        TESTDATANODECOUNT=4
        VolToleratedInPercentage=25

        NN=`getNameNode`
        if [ "$NN" == "" ];then
                echo "FATAL :: NAMENODE not found"
        fi
        LIVENODES=`getLiveDatanodes`
        LIVENODE_COUNT=`echo $LIVENODES | awk '{print NF}'`
        if [ $LIVENODE_COUNT -lt $TESTDATANODECOUNT ];then
                echo "FATAL :: Tests can not be continued as LIVE DN count at starting < 4"
        fi
        DISKCOUNT=`ssh $NN "df -l | grep -c '/grid/'"`

        #### Start Init 
        init $TESTDATANODECOUNT $DISKCOUNT
        ### Clean All the data in HDFS
        cleanHDFS
        ### Create Clusster with $TESTDATANODECOUNT datanodes
        DecommisionDatanodes
        #### Check all the disks in the cluster
        CheckVolumesAllDatanodes ${DN[0]} ${DN[1]} ${DN[2]} ${DN[3]}
        displayTestCaseResult "volumeCheck"
        #### Create /user/hadoopqa in HDFS
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HADOOP_HDFS_CMD dfs -mkdir /user/$HADOOPQA_USER"
        execcmdlocal "$HADOOP_HDFS_CMD dfs -chown hadoopqa /user/$HADOOPQA_USER"
        setKerberosTicketForUser $HADOOPQA_USER


        ### Setting Vol.Tolerated as 25% of total disks in each Datanode               
        setVolToleratedInPercent $VolToleratedInPercentage

        ### Create and define data for tests
        DATAFILE=$TMPDIR/DATA
        DATADIR="/homes/hdfsqa/hdfsSpecialData/BigTreeData.0/L3_0Dir/L2_0Dir"

	echo ">>>>>>>>>>> Creating $DATAFILE >>>>>>>>>>>"
        execcmdlocal "dd if=/dev/zero of=$DATAFILE bs=1000000000 count=$DISKCOUNT"
        (( FILESIZE = 1000000000 * $DISKCOUNT ))
	echo ">>>>>>>>>>> Defining DATADIR => $DATADIR >>>>>>>>>>>"
        DIRCOUNT=18
        FILECOUNT=15282
        SIZE=183384
	echo "DIRCOUNT=$DIRCOUNT   FILECOUNT=$FILECOUNT   SIZE=$SIZE"
}

function CheckRefreshNodeStatus {
	echo ">>>>>> Polling for Decommissioning status >>>>>>>>"
        for i in {1..20}; do
		local decomDatanodes=`getDecommDatanodes | sed -e 's/^\(.*\)\s\+$/\1/g'`
		local livedatanodeCount=`getLiveDatanodes | awk '{print NF}'` 
                 if [ "X${decomDatanodes}X" == "XX" ] &&  [ "$livedatanodeCount" == "$DATANODECOUT_IN_CLUSTER" ];then
			echo "	Iteration : $i"
                        echo "	 LIVE DATANODES  :: `getLiveDatanodes`"
                        echo "	 DEAD DATANODES  :: `getDeadDatanodes`"
                        echo "	 DECOMMISSIONING :: `getDecommDatanodes`"
			echo "============================================================================================================="
                	echo "Decommissioning Completed"
			echo "============================================================================================================="
			return 0
                else
			echo "	Iteration : $i"
                        echo "	 LIVE DATANODES  :: `getLiveDatanodes`"
                        echo "	 DEAD DATANODES  :: `getDeadDatanodes`"
                        echo "	 DECOMMISSIONING :: `getDecommDatanodes`"
                        echo "	Sleeping for 10s ...."
                        sleep 30
                fi
        done
	echo "============================================================================================================="
        echo "FATAL :: Decommissioning could not be completed; Cluster Should be reverted back to default state"
	echo "============================================================================================================="
	return 1
}

function RecommisionDatanodes {
	echo "============================================================================================================="
	echo "Starting Bringing Back DATANODES=$EXCLUDEDATANODES"
	echo "============================================================================================================="
        runasroot $NN cp "$SHARETMPDIR/dfs.exclude.$SUITESTART $HADOOP_QA_ROOT/gs/conf/local/dfs.exclude"
	setKerberosTicketForUser $HDFS_USER
	execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
	setKerberosTicketForUser $HADOOPQA_USER
	### Restarting back the datanodes
	for d in $EXCLUDEDATANODES; do
		resetNode $d datanode start
	done
}

function DecommisionDatanodes {
	echo "============================================================================================================="
	echo "Starting Decommissioning DATANODES=$EXCLUDEDATANODES"
	echo "============================================================================================================="
        execcmdlocal "rm -rf $SHARETMPDIR/excludeDN"
        echo $EXCLUDEDATANODES | tr ' ' '\n' > $SHARETMPDIR/excludeDN
        runasroot $NN cp "$HADOOP_QA_ROOT/gs/conf/local/dfs.exclude $SHARETMPDIR/dfs.exclude.$SUITESTART"
        runasroot $NN cp "$SHARETMPDIR/excludeDN $HADOOP_QA_ROOT/gs/conf/local/dfs.exclude"
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
        CheckRefreshNodeStatus
	local decomStat=$?
	if [ "$decomStat" == "0" ] ;then
		echo ">>>>>>> DECOMMISSION SUCCESSFUL >>>>>>>>>"
	else
		echo ">>>>>>> DECOMMISSION FAILED; Cluster needs to be reverted back >>>>>>>>>"	
		RecommisionDatanodes
		exit 1
	fi
}

function pushConfig {
	local nodes=$1
	local configFile=$2
	local param=$3
	local value=$4
	
	echo "#### Pushing new configs at $SHARETMPDIR"
	if [ "X${nodes}X" == "XallX" ];then
		nodes="$NN $SNN $JT $ACTIVEDATANODES"
	fi
	for n in $nodes; do
		if [ -d "$SHARETMPDIR/$n" ];then
			rm -rf $SHARETMPDIR/$n
		fi
		mkdir $SHARETMPDIR/$n
		ssh $n "cp -r $HADOOP_CONF_DIR $SHARETMPDIR/$n"
		ssh $n "sed -i \"s|</configuration>|<property>\n<name>${param}</name>\n<value>${value}</value>\n</property>\n</configuration>|\" ${SHARETMPDIR}/${n}/hadoop/${configFile}"
	done

	echo "#### Stopping Cluster"
	stopCluster
	## Starting the cluster 
	echo "#### Start Namenode"
	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop
	echo "#### Start Datanodes"
	for n in $ACTIVEDATANODES; do 
		resetNode $n datanode start $SHARETMPDIR/$n/hadoop
	done
	echo "#### Start JobTracker"
	resetNode $JT jobtracker start $SHARETMPDIR/$JT/hadoop
	echo "#### Start TaskTrackers"
	for n in $ACTIVEDATANODES; do 
		resetNode $n tasktracker start $SHARETMPDIR/$n/hadoop
	done
	takeNNOutOfSafemode
}

function setVolToleratedInPercent {
	local volPercent=$1
	(( vocount = ( $DISKCOUNT_IN_DATANODE * $volPercent ) / 100 ))
	echo "============================================================================================================="
	echo ">>>>> Total Disks in DN = $DISKCOUNT_IN_DATANODE ; VolTolerableLimit in % = $volPercent; VolTolerableLimit in Count = $vocount >>>>>>>>"
	echo "============================================================================================================="
	pushConfig "all" "hdfs-site.xml" "dfs.datanode.failed.volumes.tolerated" $vocount
}

function DiskFail {
	local datanode=$1
	local DiskFailureInPercentage=$2
	local DiskFailOption=$3
	(( failDiskCount = ( $DISKCOUNT_IN_DATANODE * $DiskFailureInPercentage ) / 100 ))
	
	echo ">>>>>>>> DISK FAIL ERROR INJECTION >>>>>>>>>>"
	echo "DN::$datanode FAILED_DISK_COUNT::$failDiskCount DISK_FAIL_OPTION::$DiskFailOption"
	if [ "X${DiskFailOption}X" == "XumountX" ];then
		dCount=1
		while [ $dCount -le $failDiskCount ] ; do
			echo ">>>>>>>>> Failing /grid/${dCount} at $datanode >>>>>>>>>>>>>"
			runasroot $datanode umount "-l /grid/${dCount}"
			echo "$datanode:/grid/${dCount}" >> $TMPDIR/unmountedDisks
			let dCount++
		done
	elif  [ "X${DiskFailOption}X" == "XchmodX" ];then
		dCount=0
		while [ $dCount -lt $failDiskCount ] ; do
			echo ">>>>>>>>> chmod 000 /grid/${dCount}/hadoop/var/hdfs/data at $datanode >>>>>>>>>>>>>"
			runasroot $datanode chmod "000 /grid/${dCount}/hadoop/var/hdfs/data"
			echo "$datanode:/grid/${dCount}/hadoop/var/hdfs/data" >> $TMPDIR/chmod000Disks
			let dCount++
		done
        elif  [ "X${DiskFailOption}X" == "XchmodCurrentX" ];then
                dCount=0
                while [ $dCount -lt $failDiskCount ] ; do
                        echo ">>>>>>>>> chmod 000 /grid/${dCount}/hadoop/var/hdfs/data/current at $datanode >>>>>>>>>>>>>"
                        runasroot $datanode chmod "000 /grid/${dCount}/hadoop/var/hdfs/data/current"
                        echo "$datanode:/grid/${dCount}/hadoop/var/hdfs/data" >> $TMPDIR/chmod000DisksCurrent
                        let dCount++
                done
	elif [ "X${DiskFailOption}X" == "XmountROX" ];then
		dCount=1
		while [ $dCount -le $failDiskCount ] ; do
			echo ">>>>>>>>> mount -o ro /grid/${dCount} at $datanode >>>>>>>>>>>>>"
			resetNode $datanode datanode stop
			runasroot $datanode umount "/grid/${dCount}"
			runasroot $datanode mount " -o ro /grid/${dCount}"
			echo "$datanode:/grid/${dCount}" >> $TMPDIR/ROmountedDisks
			let dCount++
		done	
	fi
}	

function DiskFailInMultiNodes {
        local diskFailOpt=$1
        local diskFailpercent=$2
        local dns=$3
        for d in `echo $dns | tr ',' ' '`; do
                DiskFail $d $diskFailpercent $diskFailOpt
        done
}

function RecoverFailedDisks {
	local DiskFailOption=$1
	if [ "X${DiskFailOption}X" == "XumountX" ];then
		for i in `cat $TMPDIR/unmountedDisks`;do
			local node=`echo $i | cut -d':' -f1`
			local disk=`echo $i | cut -d':' -f2`
			runasroot $node mount $disk
		done
		cat > $TMPDIR/unmountedDisks < /dev/null
	elif [ "X${DiskFailOption}X" == "XchmodX" ];then
		for i in `cat $TMPDIR/chmod000Disks`;do
			local node=`echo $i | cut -d':' -f1`
			local datadir=`echo $i | cut -d':' -f2`
			runasroot $node chmod "700 $datadir"
		done
		cat > $TMPDIR/chmod000Disks < /dev/null
        elif [ "X${DiskFailOption}X" == "XchmodCurrentX" ];then
                for i in `cat $TMPDIR/chmod000DisksCurrent`;do
                        local node=`echo $i | cut -d':' -f1`
                        local datadir=`echo $i | cut -d':' -f2`
                        runasroot $node chmod "755 $datadir/current"
                done
                cat > $TMPDIR/chmod000Disks < /dev/null
	elif [ "X${DiskFailOption}X" == "XmountROX" ];then
                for i in `cat $TMPDIR/ROmountedDisks`;do
                        local node=`echo $i | cut -d':' -f1`
                        local disk=`echo $i | cut -d':' -f2`
			resetNode $node datanode stop
			runasroot $node umount $disk
			runasroot $node mount $disk
			resetNode $node datanode start 
                done
                cat > $TMPDIR/ROmountedDisks < /dev/null
	fi
}

function BringBackDatanode {
	local dn=$1
	local config=$2
	resetNode $dn datanode stop
	resetNode $dn datanode start $config
}

function RecoverDiskNRestartMultiDN {
        local diskFailOpt=$1
        local bringBackNode=$2
        ### Recover failed disks either by "mount" or "chmod"
        RecoverFailedDisks $diskFailOpt
        cleanHDFS
        resetNodes $bringBackNode ',' datanode stop
        resetNodes $bringBackNode ',' datanode start $SHARETMPDIR/`echo $bringBackNode | cut -d',' -f1`/hadoop
}

function ResetClusterToOrigSize {
        echo "============================================================================================================="
        echo "Starting Bringing back Cluster in its original state"
        echo "============================================================================================================="
        runasroot $NN cp "$SHARETMPDIR/dfs.exclude.$SUITESTART $HADOOP_QA_ROOT/gs/conf/local/dfs.exclude"
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
}

function giveBackClusterInDefaultState {
        stopCluster
        startCluster
        takeNNOutOfSafemode
}

function cleanHDFS {
        setKerberosTicketForUser $HDFS_USER
        newDirsAtHDFSRoot=`$HDFSCMD dfs -ls / | egrep -v '/data|/mapred|/mapredsystem|/tmp|/user|^Found' | awk '{print $8}' | tr '\n' ' '`
        dirs="/mapred/history/done/version-1/\* /user/\* /tmp/\* /data/\* $newDirsAtHDFSRoot"
        execcmdlocal "$HDFSCMD dfs -rmr -skipTrash $dirs"
        execcmdlocal "$HADOOP_HDFS_CMD dfs -mkdir /user/$HADOOPQA_USER"
        execcmdlocal "$HADOOP_HDFS_CMD dfs -chown hadoopqa /user/$HADOOPQA_USER"
        execcmdlocal "$HDFSCMD dfs -lsr /"
        setKerberosTicketForUser $HADOOPQA_USER
}

function cleanUpAndExit {
        cleanHDFS
        ### Removing unwanted test directories and files
        rm -rf $TMPRES $TMPEXP $SHARETMPDIR
}

#########################################################
# Steps 1. Bring back all the decommissioned datanodes 
#	2. Restart cluster with default config
#	3. Remove all the files/Dir created by tests 
#	   Both in HDFS and UNIX
#########################################################
function testSuiteCleanUp {
        ### Revert back originial dfs.exclude
        ResetClusterToOrigSize
        ### Restart cluster with default config
        giveBackClusterInDefaultState
        ### Local cleanup
        cleanUpAndExit
}


#########################################################
# steps 1. Stop a Datanode, 
#	2. Fail Disk with given option and given number 
#	3. Start the datanode with vol.Tolerated
#       4. Extract log
#########################################################
function FailDisksNdatanodeRestart {
        datanode=$1
        DiskFailureInPercentage=$2
        DiskFailOption=$3

        resetNode $datanode datanode stop
        TakeLogSnapShot hdfs datanode $datanode
        DiskFail $datanode $DiskFailureInPercentage $DiskFailOption
        resetNode $datanode datanode start $SHARETMPDIR/$datanode/hadoop
        sleep 20
        ExtractDifferentialLog hdfs datanode $datanode
}

######################################################
# Check two things -
#   1. All the disks are mounted in all the datanodes
#   2. All the data dir are with 700 permissions
######################################################
function CheckVolumesAllDatanodes {
        local nodes=$*
        if [ "$nodes" == "" ];then
                nodes=$ACTIVEDATANODES
        fi
        for i in $nodes; do
                echo ">>>>> Check Volumes in DN : $i"
		runasroot $i "cp" "/etc/fstab $SHARETMPDIR"
		local disks=`cat $SHARETMPDIR/fstab | grep ^LABEL=/grid | awk '{print $1}' | cut -d'=' -f2 | sort | tr '\n' ' '`
		for dsk in $disks ; do 
			ssh $i "df -l | grep $dsk"
			assertEqual 0 $? "DN:$i $dsk not mounted"
			if [ "$?" != 0 ];then
				echo ">>>>>>>>>>> Mounting back Volume $dsk in $i >>>>>>>>>>>"
				runasroot $i "mount" "$dsk"
			fi
			runasroot $i ls "-l $dsk/hadoop/var/hdfs/" | grep data | egrep "drwx------"
			local permStat=$?
			assertEqual 0 $permStat "DN:$i $dsk/hadoop/var/hdfs/data does not have permission drwx------"
                        if [ "$?" != 0 ];then
                                echo ">>>>>>>>>>> Changing Permission of dataDir $dsk/hadoop/var/hdfs/data in $i >>>>>>>>>>>"
                                runasroot $i "chmod" "700 $dsk/hadoop/var/hdfs/data"
                        fi
			runasroot $i ls "-l $dsk/hadoop/var/hdfs/data" | grep current | egrep "drwxr-xr-x" 
			local permStat=$?
                        assertEqual 0 $permStat "DN:$i $dsk/hadoop/var/hdfs/data/current does not have permission drwxr-xr-x"
                        if [ "$?" != 0 ];then
                                echo ">>>>>>>>>>> Changing Permission of dataDir $dsk/hadoop/var/hdfs/data/cuurent in $i >>>>>>>>>>>"
                                runasroot $i "chmod" "755 $dsk/hadoop/var/hdfs/data/current"
                        fi
		done
        done
}

######################################################
# Check for datanode processes in each Datanode
######################################################
function getDatanodeProcCount {
	local datanode=$1
	ssh $datanode "ps aux | grep -v grep | grep datanode | grep -c java"
}

######################################################
# Wait for 5 mins to NN come out of safemode. If not
# comining out fail test and call takeNNOutOfSafemode
######################################################
function checkNNstate {
        local count_max=20
        local count=0
        local stat=""
        setKerberosTicketForUser $HDFS_USER
        TakeLogSnapShot hdfs namenode $NN
        while [ $count -lt $count_max ]; do
                (( count = $count + 1 ))
                $HADOOP_HDFS_CMD dfsadmin -safemode get | grep OFF
                stat=$?
                if [ "$stat" == "0" ];then
                        echo "NN is out of safemode nicely"     
                        break ;
                else
                        echo "$count - Sleeping for 15 sec, as NN is in safemode"
                        stat=1
                        sleep 15
                fi
        done
        ExtractDifferentialLog hdfs namenode $NN
        setKerberosTicketForUser $HADOOPQA_USER
        assertEqual 0 $stat "$msg :: NN could not came out of safemode normally"
        if [ "$stat" != "0" ];then
                takeNNOutOfSafemode
        fi
}

######################################################
# Test the HDFS root '/' filesystem HEALTH by FSCK
# and also check for existance of CORRUPT file/Block
######################################################
function fscktest {
        setKerberosTicketForUser $HDFS_USER
        $HADOOP_HDFS_CMD fsck / 2>&1  > $TMPDIR/FSCK
        echo ">>>>>>>>> FSCK >>>>>>>>>>>"
        cat $TMPDIR/FSCK
        setKerberosTicketForUser $HADOOPQA_USER
        assertEqualCmdPassStatus "grep HEALTHY $TMPDIR/FSCK" "HDFS not Healthy"
        assertEqualCmdFailStatus "grep CORRUPT $TMPDIR/FSCK" "HDFS have corrupt block and/or file"
}

######################################################
#  Test the correctness of Size of the filr
#
######################################################
function filesizetest {
        local file=$1
        execcmdlocal "$HADOOP_HDFS_CMD dfs -ls /user/$HADOOPQA_USER/"
        local newSize=`$HADOOP_HDFS_CMD dfs -dus /user/$HADOOPQA_USER/$file | awk '{print $2}'`
        assertEqual $FILESIZE  $newSize "File Size is not matched after Fault Injection"
}

######################################################
# Test the correctness of Size, count of files and 
# count of directories
######################################################
function dirsizetest {
        local dir=$1
        execcmdlocal "$HADOOP_HDFS_CMD dfs -count -q /user/$HADOOPQA_USER/$dir"
        local dirC=`$HADOOP_HDFS_CMD dfs -count -q /user/$HADOOPQA_USER/$dir | awk '{print $5}'`
        local fileC=`$HADOOP_HDFS_CMD dfs -count -q /user/$HADOOPQA_USER/$dir | awk '{print $6}'`
        local size=`$HADOOP_HDFS_CMD dfs -count -q /user/$HADOOPQA_USER/$dir | awk '{print $7}'`
        echo "DIR Count => $dirC; FILE Count => $fileC; SIZE => $size"
        assertEqual $DIRCOUNT  $dirC "Dir Count does not march"
        assertEqual $FILECOUNT  $fileC "File Count does not match"
        assertEqual $SIZE $size "size" "Size does not match"
}

######################################################
#  Test the correctness of Size of the filr
#
######################################################
function archivetest {
        local file=$1
        execcmdlocal "$HADOOP_HDFS_CMD dfs -ls /user/$HADOOPQA_USER/$file"
        local newSize=`$HADOOP_HDFS_CMD dfs -dus /user/$HADOOPQA_USER/$file/part-0 | awk '{print $2}'`
        assertEqual $FILESIZE  $newSize "Archived file size is not matched after Fault Injection"
	local indexSize=`$HADOOP_HDFS_CMD dfs -dus /user/$HADOOPQA_USER/$file/_index | awk '{print $2}'`
	local masterindexsize=`$HADOOP_HDFS_CMD dfs -dus /user/$HADOOPQA_USER/$file/_masterindex | awk '{print $2}'`
        assertEqual $FILESIZE  $newSize "File Size is not matched after Fault Injection"
        assertEqual 80  $indexSize "Index Size is not matched after Fault Injection"
        assertEqual 22  $masterindexsize "MasterIndex Size is not matched after Fault Injection"
}

######################################################
# Test the correctness of Permission in Directories
# and Files
######################################################
function chmodtest {
        local dir=$1
        $HADOOP_HDFS_CMD dfs -lsr /user/$HADOOPQA_USER/$dir | grep -v 'file_' | egrep "drwxrw\-rw\-"
        local stat=$?
        assertEqual 0 $stat "All the directories are not having permission 766"
        $HADOOP_HDFS_CMD dfs -lsr /user/$HADOOPQA_USER/$dir | grep 'file_' | egrep  "\-rw\-rw\-rw\- *3"
        local stat=$?
        assertEqual 0 $stat "All the files are not having permission 666 or replication is not 3"
}

