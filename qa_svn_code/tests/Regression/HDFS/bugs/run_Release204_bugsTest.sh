#!/bin/sh

## Import Libraries
. $WORKSPACE/lib/hdfs_dfip.sh
. $WORKSPACE/lib/user_kerb_lib.sh


# Setting Owner of this TestSuite
OWNER="rajsaha"


function localSetup {
	### Regularly used script level GLOBAL variables
	SUITESTART=`date +%s`
	HDFSCMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR "
	NNHTTP="http://`getNameNode`:50070"
	NNHFTP="hftp://`getNameNode`:50070"
	URLPREFIX="${NNHTTP}/dfsnodelist.jsp?whatNodes="
	WGETCMD='wget --random-wait --no-cache --timeout=20 --tries=2 -b'

	LIVEURL="$TMPDIR/DEAD.url"
	DEADURL="$TMPDIR/LIVE.url"
	DECOMURL="$TMPDIR/DECOM.url"
	
	LIVELOG="$TMPDIR/DEAD.log"
	DEADLOG="$TMPDIR/LIVE.log"
	DECOMLOG="$TMPDIR/DECOM.log"

	LIVEOUT="$TMPDIR/LIVE.out"
	DEADOUT="$TMPDIR/DEAD.out"
	DECOMOUT="$TMPDIR/DECOM.out"

	TMPRES=$TMPDIR/result
	TMPEXP=$TMPDIR/expected
}

######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
function test_bug4270294_dfsnodelist_jsp_whatNodes {
	setTestCaseDesc "[Bug:4270294 ]:50070/dfsnodelist.jsp?whatNodes=DEAD link showing LIVE nodes sometimes"

	for i in {1..20}
	do
		echo "${URLPREFIX}LIVE" >> $LIVEURL
		echo "${URLPREFIX}DEAD" >> $DEADURL
		echo "${URLPREFIX}DECOMMISSIONING" >> $DECOMURL
	done
	
	### Running wget command to fetch LIVE,DEAD and DECOMISSIONING jsp pages in parallel
	$WGETCMD --input-file=$LIVEURL  -O $LIVEOUT  -o $LIVELOG
	$WGETCMD --input-file=$DEADURL  -O $DEADOUT  -o $DEADLOG
	$WGETCMD --input-file=$DECOMURL -O $DECOMOUT -o $DECOMLOG

	echo "Sleeping for 3 mins"
	sleep 60
	
	### Check the correctness of the Out put files
	assertEqual `grep -c 'Datanodes :' $LIVEOUT` "20"
	assertEqual `grep -c 'Datanodes :' $DEADOUT` "20"
	assertEqual `grep -c 'Datanodes :' $DECOMOUT` "20"

	### Check for any Dead or Decommissioning page while querying Live page
	assertEqual `grep -c 'Live Datanodes :' $LIVEOUT` "20"
	assertEqual `grep -c 'Dead Datanodes :' $LIVEOUT` "0"
	assertEqual `grep -c 'Decommissioning Datanodes :' $LIVEOUT` "0"

	### Check for any Live or Decommissioning page while querying Dead page
	assertEqual `grep -c 'Live Datanodes :' $DEADOUT` "0"
	assertEqual `grep -c 'Dead Datanodes :' $DEADOUT` "20"
	assertEqual `grep -c 'Decommissioning Datanodes :' $DEADOUT` "0"


	### Check for any Dead or Live page while querying Decommissioning page
	assertEqual `grep -c 'Live Datanodes :' $DECOMOUT` "0"
	assertEqual `grep -c 'Dead Datanodes :' $DECOMOUT` "0"
	assertEqual `grep -c 'Decommissioning Datanodes :' $DECOMOUT` "20"

	displayTestCaseResult 
}

function test_HDFS_1773_removeDeadnode {
	TESTCASE_DESC_BASE="[ HDFS 1773 ] Remove a datanode from cluster if include list is not empty and this datanode is removed from both include and exclude lists"
	NN=`getNameNode`
	SHAREDTMP="/homes/hadoopqa/hadoopqaTest_${SUITESTART}_bugs"
	NEWCONF=$SHAREDTMP/conf; mkdir -p $NEWCONF
	
	LIVENODESORIG=`getLiveDatanodes`
	DEADNODESORIG=`getDeadDatanodes`
	DECMNODESORIG=`getDecommDatanodes`
	DN=`echo $LIVENODESORIG | awk '{print $1}'`

	echo "LIVE DNs at START : $LIVENODESORIG"
	echo "DEAD DNs at SATRT : $DEADNODESORIG"
	echo "DN for test => $DN"
	### Take backUp of dfs.include and dfs.exclude
	ssh $NN "cp /home/gs/conf/local/dfs.include $SHAREDTMP/dfs.include.ORIG"
	ssh $NN "cp /home/gs/conf/local/dfs.exclude $SHAREDTMP/dfs.exclude.ORIG"
	echo "At Start NN:$NN /home/gs/conf/local/"
	ssh $NN "ls -l /home/gs/conf/local/"
	### Create New dfs.include and dfs.exclude
	rm -rf $SHAREDTMP/dfs.exclude $SHAREDTMP/dfs.include 2>&1 > /dev/null
	for d in $LIVENODESORIG; do
		echo $d >> $SHAREDTMP/dfs.include.1
	done
	for d in $DEADNODESORIG; do
                echo $d >> $SHAREDTMP/dfs.exclude.1
        done
	runasroot $NN cp "$SHAREDTMP/dfs.include.1 /home/gs/conf/local/dfs.include"
	runasroot $NN cp "$SHAREDTMP/dfs.exclude.1 /home/gs/conf/local/dfs.exclude"
	echo "At 1st Phase NN:$NN /home/gs/conf/local/"
	ssh $NN "ls -l /home/gs/conf/local/"

	ssh $NN "cp -r $HADOOP_CONF_DIR $NEWCONF"
	ssh $NN "sed -i \"s|</configuration>|<property>\n<name>heartbeat.recheck.interval</name>\n<value>1</value>\n</property>\n</configuration>|\" ${NEWCONF}/hadoop/hdfs-site.xml"
	echo "New config added"
	echo "================"
	tail -5 ${NEWCONF}/hadoop/hdfs-site.xml
	### Restart Cluster with new config and dfs.include/exclude file
	stopCluster
	resetNode $NN namenode start  ${NEWCONF}/hadoop
	resetNodes `echo $LIVENODESORIG | sed -e 's/ /,/g'` ',' datanode start

	echo "Sleeping for 1 min"
	sleep 60
	LIVENODESORIG=`getLiveDatanodes`
	DEADNODESORIG=`getDeadDatanodes`
	echo "LIVE DNs at Test Setup Phase : $LIVENODESORIG"
	echo "DEAD DNs at Test Setup Phase : $DEADNODESORIG"
	
	### Stop Datanode
	resetNode $DN datanode stop

	####### TESTS #############
	setTestCaseDesc "$TESTCASE_DESC_BASE; Deadnode is not in dfs.exclude and not in non-blank dfs.include"
	
	cat $SHAREDTMP/dfs.include.1 | grep -v $DN > $SHAREDTMP/dfs.include.2 
	runasroot $NN cp "$SHAREDTMP/dfs.include.2 /home/gs/conf/local/dfs.include"
	setKerberosTicketForUser $HDFS_USER
	execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
	setKerberosTicketForUser $HADOOPQA_USER
	echo "sleepimg for 2 mins"
	sleep 120
	
	LIVENODES=`getLiveDatanodes`
	DEADNODES=`getDeadDatanodes`
	echo "LIVE DNs at Testcase1 : $LIVENODES"
	echo "DEAD DNs at Testcase1 : $DEADNODES"
	assertEqual "X${DEADNODESORIG}X" "X${DEADNODES}X" "[ HDFS-1773 ] DEADNODE still appears in Deadnode list"
	echo "Check presense of $DN in LIVEnodes"
	echo $LIVENODES | grep -v $DN
	local stat=$?
	assertEqual 0 $stat "[ HDFS-1773 ] DEADNODE still appears in Livenode list"
	displayTestCaseResult "notInExcludeNInclude_nonBlankInclude"

	
	setTestCaseDesc "$TESTCASE_DESC_BASE; Deadnode is in dfs.exclue and in dfs.include"
	echo ">>>>>>>>> $TESTCASE_DESC >>>>>>>>>>>>"
	runasroot $NN cp "$SHAREDTMP/dfs.include.1 /home/gs/conf/local/dfs.include"
	echo $DN >> $SHAREDTMP/dfs.exclude.2
	runasroot $NN cp "$SHAREDTMP/dfs.exclude.2 /home/gs/conf/local/dfs.exclude"
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
        echo "sleepimg for 10s"
        sleep 10

        LIVENODES=`getLiveDatanodes`
        DEADNODES=`getDeadDatanodes`
        echo "LIVE DNs at Testcase1 : $LIVENODES"
	echo "DEAD DNs at Testcase1 : $DEADNODES"
        echo $LIVENODES | grep -v $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] DEADNODE still appears in Livenode list"
	echo $DEADNODES | grep $DN
	local stat=$?
	assertEqual 0 $stat "[ HDFS-1773 ] is not appearing in Deadnode list"
	displayTestCaseResult "InExcludeNInInclude"

	setTestCaseDesc "$TESTCASE_DESC_BASE; Deadnode is in dfs.exclude and dfs.include is blank"
	echo ">>>>>>>>> $TESTCASE_DESC >>>>>>>>>>>>"
	touch $SHAREDTMP/dfs.include.3
	runasroot $NN cp "$SHAREDTMP/dfs.include.3 /home/gs/conf/local/dfs.include"
	runasroot $NN cp "$SHAREDTMP/dfs.exclude.2 /home/gs/conf/local/dfs.exclude"
	setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
        echo "sleepimg for 10s"
        sleep 10

        LIVENODES=`getLiveDatanodes`
        DEADNODES=`getDeadDatanodes`
        echo "LIVE DNs at Testcase1 : $LIVENODES"
        echo "DEAD DNs at Testcase1 : $DEADNODES"
        echo $LIVENODES | grep -v $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] DEADNODE still appears in Livenode list"
        echo $DEADNODES | grep $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] is not appearing in Deadnode list"
        displayTestCaseResult "InExcludeNBlankInclude"
	
	setTestCaseDesc "$TESTCASE_DESC_BASE; both dfs.exclude and dfs.include is blank"
	touch $SHAREDTMP/dfs.exclude.3
        runasroot $NN cp "$SHAREDTMP/dfs.include.3 /home/gs/conf/local/dfs.include"
        runasroot $NN cp "$SHAREDTMP/dfs.exclude.2 /home/gs/conf/local/dfs.exclude"
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
        echo "sleepimg for 10s"
        sleep 10

        LIVENODES=`getLiveDatanodes`
        DEADNODES=`getDeadDatanodes`
        echo "LIVE DNs at Testcase1 : $LIVENODES"
        echo "DEAD DNs at Testcase1 : $DEADNODES"
        echo $LIVENODES | grep -v $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] DEADNODE still appears in Livenode list"
        echo $DEADNODES | grep $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] is not appearing in Deadnode list"
        displayTestCaseResult "blankExcludeNBlankInclude"

        setTestCaseDesc "$TESTCASE_DESC_BASE; blank dfs.exclude and dfs.include is not blank and having the deadnode"
        runasroot $NN cp "$SHAREDTMP/dfs.include.1 /home/gs/conf/local/dfs.include"
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
        echo "sleepimg for 10s"
        sleep 10

        LIVENODES=`getLiveDatanodes`
        DEADNODES=`getDeadDatanodes`
        echo "LIVE DNs at Testcase1 : $LIVENODES"
        echo "DEAD DNs at Testcase1 : $DEADNODES"
        echo $LIVENODES | grep -v $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] DEADNODE still appears in Livenode list"
        echo $DEADNODES | grep $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] is not appearing in Deadnode list"
        displayTestCaseResult "blankExcludeNNonBlankIncludeWithDN"

        setTestCaseDesc "$TESTCASE_DESC_BASE; blank dfs.exclude and dfs.include is not blank and not having the deadnode"
        runasroot $NN cp "$SHAREDTMP/dfs.include.2 /home/gs/conf/local/dfs.include"
        setKerberosTicketForUser $HDFS_USER
        execcmdlocal "$HDFSCMD dfsadmin -refreshNodes"
        setKerberosTicketForUser $HADOOPQA_USER
        echo "sleepimg for 10s"
        sleep 10
        
        LIVENODES=`getLiveDatanodes`
        DEADNODES=`getDeadDatanodes`
        echo "LIVE DNs at Testcase1 : $LIVENODES"        echo "DEAD DNs at Testcase1 : $DEADNODES"
        echo $LIVENODES | grep -v $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] DEADNODE still appears in Livenode list"
        echo $DEADNODES | grep $DN
        local stat=$?
        assertEqual 0 $stat "[ HDFS-1773 ] is not appearing in Deadnode list"
        displayTestCaseResult "blankExcludeNNonBlankIncludeWithoutDN"

	#### Cleanup
	runasroot $NN cp "$SHAREDTMP/dfs.include.ORIG /home/gs/conf/local/dfs.include"
	runasroot $NN cp "$SHAREDTMP/dfs.exclude.ORIG /home/gs/conf/local/dfs.exclude"
        setKerberosTicketForUser $HADOOPQA_USER
        giveBackClusterInDefaultState
	ssh $NN "ls -l /home/gs/conf/local/dfs.*"
	rm -rf $SHAREDTMP
}

function test_HDFS_1541_detectDeadnode_StartUpSafemode {
	setTestCaseDesc "[ HDFS 1541 ] Not marking datanodes dead When namenode in safemode; Check in StartUp Safemode"
	dns=`getLiveDatanodes | sed 's/;/ /g'`
	dnsCount=`echo $dns | awk '{print NF}'`
	NN=`getNameNode`
	disks=`ssh $NN "df -l | grep -c '/grid/'"`
	init $dnsCount $disks ## init <# datanode> <#disks>
	datanode=${DN[0]}

	execcmdlocal "dd if=/dev/zero of=$TMPDIR/4G bs=1000000000 count=4"
	execcmdlocal "$HDFSCMD dfs -put $TMPDIR/4G /user/$USER/4G_1"
	pushConfig "all" "hdfs-site.xml" "heartbeat.recheck.interval" 1
	
	resetNode $NN namenode stop
	TakeLogSnapShot hdfs namenode $NN

	resetNode $NN namenode start $SHARETMPDIR/$NN/hadoop	
	resetNode $datanode datanode stop

	local counter=30
	local testStat=0
	local msg=""
	start=`date`
	while  [ $counter -gt "0" ]
  	do
		echo "DEADNODE list -"
		getDeadDatanodes | grep $datanode
		local DeadNodeStat=$?
		${HADOOP_HOME}/bin/hadoop --config $HADOOP_CONF_DIR dfsadmin -safemode get | grep 'Safe mode is OFF'
		local safemodeStat=$?
		if [ "$DeadNodeStat" == "0" ] && [ "$safemodeStat" != "0" ];then
			msg="[ Ticket-4493752 ] $datanode is marked as DEADNODE and NN is in Safemode"
			testStat=1
			break;
		elif [ "$DeadNodeStat" != "0" ] && [ "$safemodeStat" == "0" ];then
			msg="$datanode is not marked as DEADNODE and NN is out of Safemode"
			echo "NN went to safemode at $start and came out of safemode at `date`"
			testStat=2
			break;
		elif [ "$DeadNodeStat" == "0" ] && [ "$safemodeStat" == "0" ];then
			msg="$datanode is marked as DEADNODE and NN is also out of Safemode; more precision needed in polling"
			testStat=3
			break;
		else
			msg="$datanode is not marked as DEADNODE and NN is not out of Safemode; need more polling"
			testStat=4
		fi
		sleep 10
		(( counter = $counter - 1 ))
	done
	assertEqual $testStat 2 "${msg}"
	displayTestCaseResult
	
        ### CleanUp
	ExtractDifferentialLog hdfs namenode $NN
        setKerberosTicketForUser $HADOOPQA_USER
        giveBackClusterInDefaultState
	rm -rf $NEWCONF
}

function test_HDFS_1541_detectDeadnode_manualSafemode {
	dns=`getLiveDatanodes | sed 's/;/ /g'`
	dnsCount=`echo $dns | awk '{print NF}'`
	NN=`getNameNode`
	disks=`ssh $NN "df -l | grep -c '/grid/'"`
	init $dnsCount $disks ## init <# datanode> <#disks>
	datanode=${DN[0]}

        execcmdlocal "dd if=/dev/zero of=$TMPDIR/4G bs=1000000000 count=4"
        execcmdlocal "$HDFSCMD dfs -put $TMPDIR/4G /user/$USER/4G_1"

	#### TestCase 1
	setTestCaseDesc "[ HDFS 1541 ] Not marking datanodes dead When namenode in safemode; Check normally deadnode"
	
	pushConfig "all" "hdfs-site.xml" "heartbeat.recheck.interval" 1
	resetNode $datanode  datanode stop
	sleep 180
	local deadNodeCount=`getDeadDatanodes | awk '{print NF}'`
	assertEqual $deadNodeCount 1 "Not marking stopped datanode as DEAD even after 3 mins , when heartbeat.recheck.interval is set to 1"
	resetNode $datanode  datanode start
	displayTestCaseResult "NotInSafemode"

	#### TestCase 2
	setTestCaseDesc "[ HDFS 1541 ] Not marking datanodes dead When namenode in safemode; Check deadnode in manual safemode"
	
	setKerberosTicketForUser $HDFS_USER
	$HDFSCMD dfsadmin -safemode enter
	resetNode $datanode  datanode stop
	sleep 180
	local deadNodeCount=`getDeadDatanodes | awk '{print NF}'`
	assertEqual $deadNodeCount 0 "Ticket-4493752 :: marking datanodes dead When namenode in manual safemode"
	resetNode $datanode  datanode start
	$HDFSCMD dfsadmin -safemode leave
	displayTestCaseResult "InSafemode"

	### CleanUp
	setKerberosTicketForUser $HADOOPQA_USER
	giveBackClusterInDefaultState
}

function test_HDFS_1750_distcp_crcfile {
	setKerberosTicketForUser $HADOOPQA_USER

        setTestCaseDesc "[HDFS 1750 ] Copy filename.crc files in distcp"
        execcmdlocal "$HDFSCMD dfs -mkdir -p /user/$USER/$SUITESTART/DISTCP1"
        execcmdlocal "$HDFSCMD dfs -mkdir -p /user/$USER/$SUITESTART/DISTCP2"
        execcmdlocal "$HDFSCMD dfs -mkdir -p /user/$USER/$SUITESTART/DISTCP3"
        execcmdlocal "$HDFSCMD dfs -mkdir -p /user/$USER/$SUITESTART/DISTCP4"
	execcmdlocal "$HDFSCMD dfs -put /etc/passwd /user/$USER/$SUITESTART/.abc.crc"
        execcmdlocal "$HDFSCMD distcp $NNHFTP/user/$USER/$SUITESTART/.abc.crc /user/$USER/$SUITESTART/DISTCP1/"
        execcmdlocal "$HDFSCMD distcp $NNHFTP/user/$USER/$SUITESTART/.abc.crc $NNHDFS/user/$USER/$SUITESTART/DISTCP2"
        execcmdlocal "$HDFSCMD distcp $NNHDFS/user/$USER/$SUITESTART/.abc.crc /user/$USER/$SUITESTART/DISTCP3/"
        execcmdlocal "$HDFSCMD distcp $NNHDFS/user/$USER/$SUITESTART/.abc.crc $NNHDFS/user/$USER/$SUITESTART/DISTCP4"
	$HDFSCMD dfs -lsr $NNHFTP/user/$USER/$SUITESTART/ 2>&1 | grep '.abc.crc' | awk '{print $8}' | sort> $TMPRES
        cat > $TMPEXP<<EOF
/user/$USER/$SUITESTART/.abc.crc
/user/$USER/$SUITESTART/DISTCP1/.abc.crc
/user/$USER/$SUITESTART/DISTCP2/.abc.crc
/user/$USER/$SUITESTART/DISTCP3/.abc.crc
/user/$USER/$SUITESTART/DISTCP4/.abc.crc
EOF
        assertEqualTwoFiles $TMPEXP $TMPRES "HDFS-1750 Distcp .crc failure"
        displayTestCaseResult

}

### Setup 
localSetup

## Execute Tests
executeTests
