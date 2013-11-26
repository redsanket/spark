#!/bin/sh

###########################  Test Suite TRASH #############################
#   Testplan => http://twiki.corp.yahoo.com/view/Grid/HadoopTrashAutomation


## Import Libraries
. $WORKSPACE/lib/restart_cluster_lib.sh

# Setting Owner of this TestSuite
OWNER="rajsaha"


# Function to copy files or directory to remote nods
function remoteCopy {
	local host=$1
	local src=$2
	local dst=$3
	echo "[`date +%D-%T`] $USER@`hostname | cut -d'.' -f1` > ssh $host \"mkdir -p $dst\""
	ssh $host "mkdir -p $dst" > /dev/null 
	echo "[`date +%D-%T`] $USER@`hostname | cut -d'.' -f1` > scp -r $src $host:$dst"
	scp -r $src $host:$dst > /dev/null 
}

# The function will run before execution any testcase
# So that testcase execution does not get impacted 
# with previous testcase run
function localSetup {
	SUITESTART=`date +%s`
	FS=$(getDefaultFS)
	HDFSCMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS "
	NNHOSTNAME=`getDefaultNameNode`
	TMPDIREXPECTED="$TMPDIR/expected"
	TMPDIRRESULT="$TMPDIR/result"
	echo "**** Copy Hadoop Config to new location"
	remoteCopy `hostname` $HADOOP_CONF_DIR $TMPDIR
	echo "**** Create base location in HDFS for this test"
	execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART"
	echo "**** Check HDFS before starting testcase"
	execcmdlocal "$HDFSCMD -ls -R $SUITESTART"
}

# This will clean up the test environment after every
# testcase execution
function localTeardown {
	execcmdlocal "$HDFSCMD -rm -R -skipTrash $SUITESTART"
	execcmdlocal "$HDFSCMD -rm -R .Trash/Current/user/$USER/${SUITESTART}*"
}

######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
function test_FsTrashIntervalNONZERO_Filedelete {
	TESTCASE_DESC="[Trash-1]Set fs.trash.interval to non zero, value, Create a file , delete it, verify it was moved to the trash"
	
	## Call Setup
	localSetup    
	echo "*** Copy /etc/passwd at HDFS"
	execcmdlocal "$HDFSCMD -copyFromLocal /etc/passwd $SUITESTART/"
	### Assertion 1
	echo "*** Remove passwd from HDFS"
	cmd="$HDFSCMD -rm $SUITESTART/passwd"
        echo "Moved to trash: hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/passwd" > $TMPDIREXPECTED
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
	### Assertion 2
        $HDFSCMD -ls -R .Trash | egrep "$SUITESTART/passwd" | awk '{print $8}'> $TMPDIRRESULT 2>&1
        echo ".Trash/Current/user/$USER/$SUITESTART/passwd" > $TMPDIREXPECTED
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	
	
	
	displayTestCaseResult
	### Clean Up
	localTeardown 
	
	
}

function test_FsTrashIntervalNONZERO_FileDirDelete {
	
	
        TESTCASE_DESC="[Trash-2] Set fs.trash.interval to non zero value, Create a file named A in a directory named B, delete file A, verify it was moved to the trash, delete directory B, verify it was moved to the trash"
        
        ## Call Setup
        localSetup
        echo "*** Copy /etc/passwd at HDFS"
        execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/B"
        execcmdlocal "$HDFSCMD -touchz $SUITESTART/B/A"
        ### Assertion 1
        echo "*** Remove $SUITESTART/B/A from HDFS"
        cmd="$HDFSCMD -rm $SUITESTART/B/A"
        echo "Moved to trash: hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/B/A" > $TMPDIREXPECTED
        assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
        ### Assertion 2
        $HDFSCMD -ls -R .Trash | egrep "$SUITESTART/B/A" | awk '{print $8}'> $TMPDIRRESULT 2>&1
        echo ".Trash/Current/user/$USER/$SUITESTART/B/A" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	### Assertion 3
	echo "*** Remove $SUITESTART/B"
	cmd="$HDFSCMD -rm -R $SUITESTART/B"
	echo "Moved to trash: hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/B" > $TMPDIREXPECTED
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
	### Assertion 4
	$HDFSCMD -ls -R .Trash | egrep "$SUITESTART/B1" | awk '{print $8}'> $TMPDIRRESULT 2>&1
	echo ".Trash/Current/user/$USER/$SUITESTART/B1" > $TMPDIREXPECTED
        assertLikeCmdPattern "$HDFSCMD -ls -R .Trash" ".Trash/Current/user/$USER/$SUITESTART/B1"
	
        
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}

function test_FsTrashIntervalNONZERO_DirWithFiles {
	
	
        TESTCASE_DESC="[Trash-5] Set fs.trash.interval to non zero value, create a directory(with only files) and delete it"
        
        ## Call Setup
        localSetup

	execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/A"
	execcmdlocal "$HDFSCMD -touchz $SUITESTART/A/B"
	execcmdlocal "$HDFSCMD -copyFromLocal /etc/passwd $SUITESTART/A"
	### Assertion 1
	cmd="$HDFSCMD -rm -R $SUITESTART/A"
	echo "Moved to trash: hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/A" > $TMPDIREXPECTED
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`

	### Assertion 2
	cat > $TMPDIREXPECTED<<EOF
.Trash/Current/user/$USER/$SUITESTART/A
.Trash/Current/user/$USER/$SUITESTART/A/B
.Trash/Current/user/$USER/$SUITESTART/A/passwd
EOF
	$HDFSCMD -ls -R .Trash | egrep "$SUITESTART/A" | awk '{print $8}'> $TMPDIRRESULT 2>&1
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

        
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}

function test_FsTrashIntervalNONZERO_DirWithSubDir {
	
        
        TESTCASE_DESC="[Trash-6] Set fs.trash.interval to non zero value, create a directory(with sub-directories) and delete it"
        
        ## Call Setup
        localSetup

	execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/A/B"
	execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/A/C"

	### Assertion 1
	cmd="$HDFSCMD -rm -R $SUITESTART/A"
	echo "Moved to trash: hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/A" > $TMPDIREXPECTED
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`

        ### Assertion 2
        cat > $TMPDIREXPECTED<<EOF
.Trash/Current/user/$USER/$SUITESTART/A
.Trash/Current/user/$USER/$SUITESTART/A/B
.Trash/Current/user/$USER/$SUITESTART/A/C
EOF
        $HDFSCMD -ls -R .Trash | egrep "$SUITESTART/A" | awk '{print $8}'> $TMPDIRRESULT 2>&1
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

        
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}


function test_FsTrashIntervalZERO_Filedelete {
	
	
        TESTCASE_DESC="[Trash-7] Set fs.trash.interval to zero value, Touch a file, Delete it"
        
        ## Call Setup
        localSetup
	### New Client Side Config
	cp $HADOOP_CONF_DIR/core-site.xml $TMPDIR/core-site.xml.temp
	sed '/fs.trash.interval/ {N;s|\(fs.trash.interval.*\)360|\10|;}'  $TMPDIR/core-site.xml.temp > $TMPDIR/hadoop/core-site.xml
	rm -rf $TMPDIR/core-site.xml.temp
	echo "Sleeping for 10 sec ..."
	sleep 10  ## This sleep was required, otherwise new config was not visible
	echo "*** Touch a file at HDFS"
	TMPDIRHDFSCMD="$HADOOP_HDFS_CMD --config $TMPDIR/hadoop dfs -fs $FS "
	execcmdlocal "$TMPDIRHDFSCMD -touchz $SUITESTART/new"
        ### Assertion 1
	echo "*** Remove \"new\" from HDFS"
        cmd="$TMPDIRHDFSCMD -rm $SUITESTART/new"
	echo "Deleted hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/new" > $TMPDIREXPECTED
        assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
        ### Assertion 2
	$TMPDIRHDFSCMD -ls -R .Trash | egrep "$SUITESTART/new" > $TMPDIRRESULT 2>&1
	rm -rf $TMPDIREXPECTED
	touch $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
        
        
	
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}

function test_FsTrashIntervalNONZERO_Del_Trash {
	
	
        TESTCASE_DESC="[Trash-8] Set fs.trash.interval to non zero value, Delete the Trash"
	
        ## Call Setup
        localSetup 

	execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/{A,B}"
	execcmdlocal "$HDFSCMD -copyFromLocal /etc/passwd $SUITESTART/A"
	execcmdlocal "$HDFSCMD -rm -R $SUITESTART"

	### Assertion 1
        cmd="$HDFSCMD -rm -R .Trash"
	echo "Deleted hdfs://$NNHOSTNAME:8020/user/$USER/.Trash" >> $TMPDIREXPECTED
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`

	### Assertion 2
	cat > $TMPDIREXPECTED<<EOF
ls.*.Trash': No such file or directory
EOF
        cmd="$HDFSCMD -ls -R .Trash"
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}

function test_FsTrashIntervalNONZERO_FilesInTrash {
	
        
        TESTCASE_DESC="[Trash-9] Delete a file in the Trash"
	
        ## Call Setup
        localSetup 
	
	execcmdlocal "$HDFSCMD -touchz $SUITESTART/A"
	execcmdlocal "$HDFSCMD -touchz $SUITESTART/B"
	execcmdlocal "$HDFSCMD -copyFromLocal /etc/passwd $SUITESTART"
	execcmdlocal "$HDFSCMD -rm $SUITESTART/*"

	### Assertion 1
        cmd="$HDFSCMD -rm .Trash/Current/user/$USER/$SUITESTART/A .Trash/Current/user/$USER/$SUITESTART/*"
	cat >$TMPDIREXPECTED<<EOF
Deleted hdfs://$NNHOSTNAME:8020/user/$USER/.Trash/Current/user/$USER/$SUITESTART/A
Deleted hdfs://$NNHOSTNAME:8020/user/$USER/.Trash/Current/user/$USER/$SUITESTART/B
Deleted hdfs://$NNHOSTNAME:8020/user/$USER/.Trash/Current/user/$USER/$SUITESTART/passwd
EOF
	assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
	### Assertion 2
        cat > $TMPDIREXPECTED<<EOF
.Trash/Current/user/$USER/$SUITESTART
EOF
        $HDFSCMD -ls -R .Trash | grep $SUITESTART | awk '{print $8}'> $TMPDIRRESULT 2>&1
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

	
        
        
        displayTestCaseResult
        ### Clean Up
        localTeardown
        
        

}

function test_FsTrashIntervalNONZERO_DirWithFilesInTrash {
	
        
        TESTCASE_DESC="[Trash-10] Delete a directory(with files only) in the Trash"
	
        ## Call Setup
        localSetup	

        execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/A"
        execcmdlocal "$HDFSCMD -touchz $SUITESTART/A/B"
        execcmdlocal "$HDFSCMD -copyFromLocal /etc/passwd $SUITESTART/A"
	execcmdlocal "$HDFSCMD -rm -R $SUITESTART/A"
        ### Assertion 1
        cmd="$HDFSCMD -rm -R .Trash/Current/user/$USER/$SUITESTART/A"
        echo "Deleted hdfs://$NNHOSTNAME:8020/user/$USER/.Trash/Current/user/$USER/$SUITESTART/A" > $TMPDIREXPECTED
        assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`

        ### Assertion 2
        cat > $TMPDIREXPECTED<<EOF
.Trash/Current/user/$USER/$SUITESTART
EOF
        $HDFSCMD -ls -R .Trash | egrep "$SUITESTART" | awk '{print $8}'> $TMPDIRRESULT 2>&1
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

	

	
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        

}

function test_FsTrashIntervalNONZERO_DirWithSubDirInTrash {
        
        
        TESTCASE_DESC="[Trash-11] Delete a directory(with sub-directories) in the Trash"
        
        ## Call Setup
        localSetup

        execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/A/B"
        execcmdlocal "$HDFSCMD -mkdir -p $SUITESTART/A/C"
        execcmdlocal "$HDFSCMD -rm -R $SUITESTART/A"

        ### Assertion 1
        cmd="$HDFSCMD -rm -R .Trash/Current/user/$USER/$SUITESTART/A"
        echo "Deleted hdfs://$NNHOSTNAME:8020/user/$USER/.Trash/Current/user/$USER/$SUITESTART/A" > $TMPDIREXPECTED
        assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`

        ### Assertion 2
        cat > $TMPDIREXPECTED<<EOF
.Trash/Current/user/$USER/$SUITESTART
EOF
        $HDFSCMD -ls -R .Trash | egrep "$SUITESTART" | awk '{print $8}'> $TMPDIRRESULT 2>&1
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

        
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}


function test_FsTrashIntervalNONZERO_Filedelete_skipTrash {
	
        
        TESTCASE_DESC="[Trash-12] Set fs.trash.interval to non zero, value, Create a file, delete it with -skipTrash, verify it is not moved to the trash"
        
        ## Call Setup
        localSetup
        echo "*** Copy /etc/passwd at HDFS"
        execcmdlocal "$HDFSCMD -copyFromLocal /etc/passwd $SUITESTART/"
        ### Assertion 1
        echo "*** Remove passwd from HDFS"
        cmd="$HDFSCMD -rm -skipTrash $SUITESTART/passwd"
        echo "Deleted hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/passwd" > $TMPDIREXPECTED
        assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`
        ### Assertion 2
        rm -rf $TMPDIREXPECTED
        touch $TMPDIREXPECTED
        $HDFSCMD -ls -R .Trash | egrep "$SUITESTART/passwd" > $TMPDIRRESULT 2>&1
	cat $TMPDIRRESULT
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
        
        
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}

function test_FsTrashIntervalNONZERO_FileDirDel_Namenode {
        TESTCASE_DESC="[Trash-13] Set fs.trash.interval to non zero value in Namenode, Create a file, delete, verify it is moved from the trash after give time"
        
        ## Call Setup
	localSetup
	resetNameNodes stop
	NEWTMPDIR="/homes/hadoopqa/`date +%s`_Trash/"
	# Test
	NEWCONF="$NEWTMPDIR/NN/conf"
	mkdir -p $NEWCONF
	ssh $NNHOSTNAME "cp -r $HADOOP_CONF_DIR $NEWCONF"
	cp $NEWCONF/hadoop/core-site.xml $NEWTMPDIR/core-site.xml
        sed '/fs.trash.interval/ {N;s|\(fs.trash.interval.*\)360|\12|;}'  $NEWTMPDIR/core-site.xml  > $NEWCONF/hadoop/core-site.xml

	resetNameNodes start $NEWCONF/hadoop
	TMPDIRHDFSCMD="$HADOOP_HDFS_CMD --config $NEWCONF/hadoop"
	echo "Waiting for Safe Mode to be OFF "
	setKerberosTicketForUser $HADOOPQA_USER
    	takeNNOutOfSafemode
    	setKerberosTicketForUser $HADOOPQA_USER

	#$TMPDIRHDFSCMD dfsadmin -safemode wait

	execcmdlocal "$TMPDIRHDFSCMD dfs -fs $FS -put /etc/passwd $SUITESTART/passwd"
	execcmdlocal "$TMPDIRHDFSCMD dfs -fs $FS -rm $SUITESTART/passwd"

	echo "sleeping for 300 sec"
	sleep 300
	$TMPDIRHDFSCMD dfs -fs $FS -ls -R .Trash | egrep "$SUITESTART/passwd" | awk '{print $8}' > $TMPDIRRESULT 2>&1
	rm -rf $TMPDIREXPECTED; touch $TMPDIREXPECTED
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

	displayTestCaseResult
        ### Clean Up
	resetNameNodes stop
	resetNameNodes start
	#execcmdlocal "$TMPDIRHDFSCMD dfsadmin -safemode wait"
	setKerberosTicketForUser $HADOOPQA_USER
    	takeNNOutOfSafemode
    	setKerberosTicketForUser $HADOOPQA_USER

        localTeardown
	rm -rf $NEWTMPDIR

}

function test_FsTrashIntervalNONZERO_JIRA_HADOOP1665 {
        TESTCASE_DESC="[JIRA_HADOOP-1665] https://issues.apache.org/jira/browse/HADOOP-1665; Remove same file more than one time"
        
        ## Call Setup
        localSetup
        echo "*** Copy /etc/passwd at HDFS"
        execcmdlocal "$HDFSCMD -put /etc/passwd $SUITESTART/passwd"
        echo "*** Remove passwd from HDFS"
        execcmdlocal "$HDFSCMD -rm $SUITESTART/passwd"
        echo "*** Copy again /etc/passwd at HDFS"
        execcmdlocal "$HDFSCMD -put /etc/passwd $SUITESTART/passwd"
        ### Assertion 1
        echo "*** Remove again passwd from HDFS"
        cmd="$HDFSCMD -rm $SUITESTART/passwd"
        echo "Moved to trash: hdfs://$NNHOSTNAME:8020/user/$USER/$SUITESTART/passwd" > $TMPDIREXPECTED
        assertLikeCmdPattern "$cmd" `cat $TMPDIREXPECTED`

	$HDFSCMD -ls -R .Trash | egrep -c "$SUITESTART/passwd" >  $TMPDIRRESULT
        ## Assertion 2
	assertEqualCmdString "cat $TMPDIRRESULT" "2"
        
	displayTestCaseResult
        ### Clean Up
        localTeardown
        
        
}

function test_FsTrashIntervalNONZERO_JIRA_HADOOP3561 {
        TESTCASE_DESC="[JIRA_HADOOP-3561] https://issues.apache.org/jira/browse/HADOOP-3561; With trash enabled, Do 'hadoop fs -rm -R .' Check working directory is not removed"
        
        ## Call Setup
        localSetup

        ### Assertion 1
        echo "Remove Working Directory"
	$HDFSCMD -ls -R /user/hadoopqa/ | wc -l > $TMPDIR/tempExpected
        $HDFSCMD -rm -R . > $TMPDIR/temp.txt  2>&1
	cat $TMPDIR/temp.txt | grep -v WARN > $TMPDIRRESULT
	rm -rf $TMPDIR/temp.txt
	cat > $TMPDIREXPECTED<<EOF
rm: Cannot move "hdfs://$NNHOSTNAME:8020/user/$USER" to the trash, as it contains the trash. Consider using -skipTrash option
EOF
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED "[ Ticket 4586423 ] not promptimg  -skipTrash"

        ### Assertion 2
	$HDFSCMD -ls -R /user/hadoopqa/ | wc -l > $TMPDIRRESULT 2>&1
	cp $TMPDIR/tempExpected $TMPDIREXPECTED
	rm -rf $TMPDIR/tempExpected
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	displayTestCaseResult
        ### Clean Up
        localTeardown
}

function test_BUG_4563905 {
	setTestCaseDesc "[ BUG 4563905/HDFS-1967 ] Check .Trash handling after creating some subdirectories inside .trash" 
        ## Call Setup
        localSetup
	execcmdlocal "$HDFSCMD -mkdir -p /user/$HADOOPQA_USER/.Trash/1111"
	execcmdlocal "$HDFSCMD -mkdir -p /user/$HADOOPQA_USER/.Trash/2222"
	execcmdlocal "$HDFSCMD -mkdir -p /user/$HADOOPQA_USER/.Trash/3333"
	execcmdlocal "$HDFSCMD -mkdir -p /user/$HADOOPQA_USER/.Trash/4444"
	execcmdlocal "$HDFSCMD -put /etc/passwd /user/$HADOOPQA_USER"
	assertEqualCmdPassStatus "$HDFSCMD  -rm -R /user/$HADOOPQA_USER/passwd"
	cat > $TMPDIREXPECTED << EOF
/user/hadoopqa/.Trash/1111
/user/hadoopqa/.Trash/2222
/user/hadoopqa/.Trash/3333
/user/hadoopqa/.Trash/4444
/user/hadoopqa/.Trash/Current
/user/hadoopqa/.Trash/Current/user
/user/hadoopqa/.Trash/Current/user/hadoopqa
/user/hadoopqa/.Trash/Current/user/hadoopqa/passwd
EOF
	$HDFSCMD -ls -R /user/$HADOOPQA_USER/.Trash 2>&1 | grep -v WARN  | awk '{print $8}'> $TMPDIRRESULT 
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	execcmdlocal "$HDFSCMD -ls -R /user/$HADOOPQA_USER/.Trash"
	displayTestCaseResult "TrashWith4existingDir"
	execcmdlocal "$HDFSCMD -rm -R -skipTrash /user/$HADOOPQA_USER/.Trash"
	execcmdlocal "$HDFSCMD -mkdir /user/$HADOOPQA_USER/.Trash"
	
	TakeLogSnapShot hdfs namenode $NNHOSTNAME
	execcmdlocal "$HDFSCMD -chmod -R 400 /user/$HADOOPQA_USER/.Trash"
	assertEqualCmdFailStatus "$HDFSCMD  -rm -R /user/$HADOOPQA_USER/passwd"
	ExtractDifferentialLog hdfs namenode $NNHOSTNAME
	displayTestCaseResult "ReadOnlyTrash"
	execcmdlocal "$HDFSCMD -chmod -R 755 /user/$HADOOPQA_USER/.Trash"
	execcmdlocal "$HDFSCMD -rm -R -skipTrash /user/$HADOOPQA_USER/.Trash"
	
	### Clean Up
    localTeardown
}


## Execute Tests
executeTests
