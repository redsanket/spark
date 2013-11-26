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
	NN=`getDefaultNameNode`
	TMPDIRRESULT="$TMPDIR/result"
	NNHDFS="hdfs://$NN:8020"
	NNHFTP="hftp://$NN:50070"
	FS=$(getDefaultFS)
	HDFSCWDDFS="$HDFSCMD dfs -fs $FS "
        echo "**** Create base location in HDFS for this test"
        execcmdlocal "$HDFSCWDDFS  -mkdir -p $SUITESTART"

}

function localTeardown {
	echo "* Removing directories created in this testcase *"
	execcmdlocal "$HDFSCWDDFS  -rm -R -skipTrash $SUITESTART"
       	execcmdlocal "$HDFSCWDDFS  -rm -R -skipTrash .Trash/Current/user/$USER/${SUITESTART}*"
}

######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
## NOTE: the '#' symbol is actaully passed as is, vim may highlight it as comment but reaminder of symbols are used 
function test_HDFS_1109_HFTP_URL_Encoding {
	for symbol in "~" "\`" "+" "%" "^" "!" "@"  '#' "$" "(" ")" "=" "|"; do
		setTestCaseDesc "[ HDFS 1109 ] File having \"${symbol}\" in name, is not readable through HFTP protocol"
		assertEqualCmdPassStatus "$HDFSCWDDFS -put /etc/passwd /user/$USER/$SUITESTART/file${symbol}123"
		assertEqualCmdPassStatus "$HDFSCWDDFS -ls -R $NNHFTP/user/$USER/$SUITESTART/file${symbol}123"
        	displayTestCaseResult "file_with_${symbol}_symbol"
	done
}

#function test_BUG_4426655_HADOOP_HDFS_HOME {
#	setTestCaseDesc "[ BUG 4426655 ] In start-dfs.sh and stop-dfs.sh HADOOP_HOME should be replaced by HADOOP_HDFS_HOME"
#	ssh $NN "cp $HADOOP_HDFS_HOME/sbin/*-dfs.sh $SHARETMPDIR"
#	execcmdlocal "ls -l $SHARETMPDIR"
#	assertEqualCmdPassStatus "grep HADOOP_HDFS_HOME $SHARETMPDIR/start-dfs.sh"
#	assertEqualCmdPassStatus "grep HADOOP_HDFS_HOME $SHARETMPDIR/stop-dfs.sh"
#	assertEqualCmdFailStatus "grep HADOOP_HOME $SHARETMPDIR/start-dfs.sh"
#	assertEqualCmdFailStatus "grep HADOOP_HOME $SHARETMPDIR/stop-dfs.sh"
#	displayTestCaseResult
#}

function test_BUG_3977232_jsvc_loc {
	#later versions from 2.0 have changed location of jsvc and this bug is not an issue anymore 
	ver=`getHadoopVersion`
	  
	if [ $ver == "0.23" ]; then 
		setTestCaseDesc "[ BUG 3977232 ] jsvc executable delivered into wrong package"	
		ssh $NN "cp $HADOOP_HDFS_CMD $SHARETMPDIR"	
		assertEqualCmdPassStatus "grep HADOOP_HDFS_HOME/libexec/jsvc $SHARETMPDIR/hdfs"
		assertEqualCmdFailStatus "grep HADOOP_HOME/libexec/jsvc $SHARETMPDIR/hdfs" 
		displayTestCaseResult
	fi
}

function test_BUG_3620525_multiDirPerm {
	setTestCaseDesc "[ BUG 3620525 ] Inconsistent multi-level mkdir directory permissions "
	execcmdlocal "$HDFSCWDDFS -mkdir -p /user/$HADOOPQA_USER/$SUITESTART"
	execcmdlocal "$HDFSCWDDFS -chmod 777 /user/$HADOOPQA_USER/$SUITESTART"
	execcmdlocal "$HDFSCWDDFS -mkdir -p /user/$HADOOPQA_USER/$SUITESTART/zze/a/s/d/f/g/h"
	execcmdlocal "$HDFSCWDDFS -ls -R /user/$HADOOPQA_USER/$SUITESTART"
	$HDFSCWDDFS -ls -R /user/$HADOOPQA_USER/ 2>&1 | egrep -v 'WARN conf.Configuration: mapred.task.id is deprecated' | grep "$SUITESTART/zze" | awk '{print $1 " " $2 " " $3 " " $4 " " $8}' > $TMPDIRRESULT
	cat >$TMPDIREXPECTED <<EOF
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze/a
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze/a/s
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze/a/s/d
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze/a/s/d/f
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze/a/s/d/f/g
drwx------ - $HADOOPQA_USER hdfs /user/$HADOOPQA_USER/$SUITESTART/zze/a/s/d/f/g/h
EOF
	assertEqualTwoFiles $TMPDIRRESULT $TMPDIREXPECTED "[ Bug-3620525 ] All the directories are not having the permissions per your umask"
	displayTestCaseResult 
}




### Setup 
localSetup

## Execute Tests
executeTests

### Teardown
localTeardown

