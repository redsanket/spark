#!/bin/sh

## Import Libraries
. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rajsaha"


function localSetup {
	### Regularly used script level GLOBAL variables
	SUITESTART=`date +%s`
	HDFSCMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR "
	HADOOPCMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR "
	TMPDIREXPECTED="$TMPDIR/expected"
	TMPDIRRESULT="$TMPDIR/result"
	NNHDFS="hdfs://`getDefaultNameNode`:8020"
	NNHFTP="hftp://`getDefaultNameNode`:50070"
	FS=$(getDefaultFS)
	HDFSCWDDFS="$HDFSCMD dfs -fs $FS "
        echo "**** Create base location in HDFS for this test"
        execcmdlocal "$HDFSCWDDFS  -mkdir -p $SUITESTART"

	### Initial File/Directory Creation	
	execcmdlocal "$HDFSCWDDFS  -mkdir -p $SUITESTART/newtemp"
	execcmdlocal "cp /etc/passwd $TMPDIR/.abc.crc"
	execcmdlocal "$HDFSCWDDFS  -put $TMPDIR/.abc.crc $SUITESTART/newtemp"

	### Preliminalry File and Directory creation in 
	execcmdlocal "$HDFSCWDDFS -mkdir -p $SUITESTART/L1/.L2"
	execcmdlocal "$HDFSCWDDFS -mkdir -p $SUITESTART/L1/.L2.crc"
	echo "HELLO WORLD!" > $TMPDIR/testfile
	execcmdlocal "$HDFSCWDDFS -copyFromLocal  $TMPDIR/testfile $SUITESTART/L1/a.txt"
	execcmdlocal "$HDFSCWDDFS -copyFromLocal  $TMPDIR/testfile $SUITESTART/L1/.a.txt"
	execcmdlocal "$HDFSCWDDFS -copyFromLocal  $TMPDIR/testfile $SUITESTART/L1/.a.crc"
	execcmdlocal "$HDFSCWDDFS -copyFromLocal  $TMPDIR/testfile $SUITESTART/L1/a.crc"
}

function localTeardown {
	echo "* Removing directories created in this testcase *"
	execcmdlocal "$HDFSCWDDFS  -rm -R -skipTrash $SUITESTART"
       	execcmdlocal "$HDFSCWDDFS  -rm -R -skipTrash .Trash/Current/user/$USER/${SUITESTART}*"
}

######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
function test_bug4093980_check_crc_default_hdfs {
	TESTCASE_DESC="[Bug:4093980 / HDFS:1598]Copy .crc file to HDFS and check its existence;  -ls with default and hdfs:// filesystem"

	### Assertion 1 [ WITH DEFAULT ]
	$HDFSCWDDFS  -ls -R /user/$USER/$SUITESTART/newtemp  | awk '{print $8}'> $TMPDIRRESULT 2>&1
	echo "/user/$USER/$SUITESTART/newtemp/.abc.crc" > $TMPDIREXPECTED
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED

	### Assertion 2 [ WITH HDFS Proxy ]
	$HDFSCWDDFS  -ls -R $NNHDFS/user/$USER/$SUITESTART/newtemp  | awk '{print $8}'> $TMPDIRRESULT 2>&1
        echo "$NNHDFS/user/$USER/$SUITESTART/newtemp/.abc.crc" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED 

	displayTestCaseResult 
}

function test_bug4093980_check_crc_hftp_ls {
	TESTCASE_DESC="[Bug:4093980 / HDFS:1598]Copy .crc file to HDFS and check its existence; -ls with hftp:// filesystem" 
        $HDFSCWDDFS  -ls -R $NNHFTP/user/$USER/$SUITESTART/newtemp | tail -1 | awk '{print $8}'> $TMPDIRRESULT 2>&1
        echo "$NNHFTP/user/$USER/$SUITESTART/newtemp/.abc.crc" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED "Due to EBF of Bug 4386748, the fix of Bug 4093980 is reverted back"
	displayTestCaseResult
}

function test_bug4093980_distcp_crc_hdfs_hdfs {
        ### Testcase 4 [ DISTCP with HDFS Proxy ]
	TESTCASE_DESC="[Bug:4093980 / HDFS:1598]Copy .crc file to HDFS and check its existence; distcp with hdfs:// filesystem" 
	execcmdlocal "$HADOOPCMD distcp $NNHDFS/user/$USER/$SUITESTART/newtemp $NNHDFS/user/$USER/$SUITESTART/a"
	$HDFSCWDDFS -ls -R /user/$USER/$SUITESTART/a | awk '{print $8}'> $TMPDIRRESULT 2>&1
	echo "/user/$USER/$SUITESTART/a/.abc.crc" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	displayTestCaseResult 
}

function test_bug4093980_distcp_crc_hftp_hdfs {
        ### Testcase 5 [ DISTCP with HFTP Proxy ]
	TESTCASE_DESC="[Bug:4093980 / HDFS:1598]Copy .crc file to HDFS and check its existence; distcp with hftp:// filesystem" 
	$HADOOPCMD distcp $NNHFTP/user/$USER/$SUITESTART/newtemp $SUITESTART/as
	$HDFSCWDDFS -ls -R /user/$USER/$SUITESTART/as | awk '{print $8}'> $TMPDIRRESULT 2>&1
	echo "/user/$USER/$SUITESTART/as/.abc.crc" > $TMPDIREXPECTED
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED "Due to EBF of Bug 4386748, the fix of Bug 4093980 is reverted back"
	displayTestCaseResult
}

function test_bug4315986_filesDirWithHftp_ls_file {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; -ls check"
	$HDFSCWDDFS -ls $NNHFTP/user/$USER/$SUITESTART/L1/{a.txt,.a.txt,.a.crc,a.crc} | egrep -v 'getDelegationToken|L2|Found 1 item' | awk '{print $8}'| sort > $TMPDIRRESULT 2>&1
        cat > $TMPDIREXPECTED<<EOF
$NNHFTP/user/$USER/$SUITESTART/L1/.a.crc
$NNHFTP/user/$USER/$SUITESTART/L1/a.crc
$NNHFTP/user/$USER/$SUITESTART/L1/.a.txt
$NNHFTP/user/$USER/$SUITESTART/L1/a.txt
EOF
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	displayTestCaseResult
}

function test_bug4315986_filesDirWithHftp_cat_hftp_dotTxt {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; -cat hftp a.txt"
	$HDFSCWDDFS -cat $NNHFTP/user/$USER/$SUITESTART/L1/a.txt | egrep -v 'getDelegationToken' > $TMPDIRRESULT 2>&1
	echo "HELLO WORLD!" > $TMPDIREXPECTED
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
	displayTestCaseResult
}

function test_bug4315986_filesDirWithHftp_cat_hftp_dotAdotTxt {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; -cat hftp .a.txt"
        $HDFSCWDDFS -cat $NNHFTP/user/$USER/$SUITESTART/L1/.a.txt | egrep -v 'getDelegationToken' > $TMPDIRRESULT 2>&1
        echo "HELLO WORLD!" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
        displayTestCaseResult
}

function test_bug4315986_filesDirWithHftp_cat_hftp_dotCrc {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; -cat hftp a.crc"
        $HDFSCWDDFS -cat $NNHFTP/user/$USER/$SUITESTART/L1/a.crc | egrep -v 'getDelegationToken' > $TMPDIRRESULT 2>&1
        echo "HELLO WORLD!" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
        displayTestCaseResult
}

function test_bug4315986_filesDirWithHftp_cat_hftp_dotAdotCrc {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; -cat hftp .a.crc"
        $HDFSCWDDFS -cat $NNHFTP/user/$USER/$SUITESTART/L1/.a.crc | egrep -v 'getDelegationToken' > $TMPDIRRESULT 2>&1
        echo "HELLO WORLD!" > $TMPDIREXPECTED
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED
        displayTestCaseResult

}

function test_bug4315986_filesDirWithHftp_ls_DirWithDorCrcDir {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; -ls hftp .L2 and .L2.crc directory"
        $HDFSCWDDFS -ls -R $NNHFTP/user/$USER/$SUITESTART/L1 | egrep -v 'getDelegationToken|Found 1 item' | awk '{print $8}'| sort > $TMPDIRRESULT 2>&1
        cat > $TMPDIREXPECTED<<EOF
$NNHFTP/user/$USER/$SUITESTART/L1/.a.crc
$NNHFTP/user/$USER/$SUITESTART/L1/a.crc
$NNHFTP/user/$USER/$SUITESTART/L1/.a.txt
$NNHFTP/user/$USER/$SUITESTART/L1/a.txt
$NNHFTP/user/$USER/$SUITESTART/L1/.L2
$NNHFTP/user/$USER/$SUITESTART/L1/.L2.crc
EOF
        assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED "Due to EBF of Bug 4386748, the fix of Bug 4093980 is reverted back"
        displayTestCaseResult
}

function test_bug4315986_filesDirWithHftp_distcp_hftp_file {
	TESTCASE_DESC="[Bug:4315986] Test files with HFTP; distcp hftp file"
	$HADOOPCMD distcp $NNHFTP/user/$USER/$SUITESTART/L1/a.txt $SUITESTART/qeTxt1
	$HDFSCWDDFS -ls -R $NNHFTP/user/$USER/$SUITESTART/ |  egrep  'qeTxt1' | awk '{print $8}' > $TMPDIRRESULT 2>&1
	cat > $TMPDIREXPECTED<<EOF
$NNHFTP/user/$USER/$SUITESTART/qeTxt1
EOF
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED "Ticket 4315986"
        displayTestCaseResult
}

### Setup 
localSetup

## Execute Tests
executeTests

### Teardown
localTeardown

