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

	NN=`getNameNode`
  	BaseNN=`echo $NN|cut -d'.' -f1`

	JT=`getJobTracker_TMP`
	NNHDFS="hdfs://${NN}:8020"
	NNHFTP="hftp://${NN}:50070"
        NNHFTP_BASE="hftp://${BaseNN}:50070"
        NNHFTP_BASE_NOPORT="hftp://${BaseNN}"

	TMPRES=$TMPDIR/result
	TMPEXP=$TMPDIR/expected

	
}

function cleanUp {
	execcmdlocal "rm -rf $NEWCONF"
	execcmdlocal "$HDFSCMD dfs -rmr -skipTrash /user/$USER/*"
}

######################## TESTCASES ###########################
###  These functions starts with "test_" 
##############################################################
function test_UseIp_false {
        TESTCASE_DESC="test no useip"
        execcmdlocal "$HDFSCMD dfs -rmr -skipTrash /user/$USER/*"
        execcmdlocal "$HDFSCMD dfs -copyFromLocal /etc/passwd /user/$USER/distCpinput"
        echo "TESTCASE - 1 :: Random Writer Job "
        #execcmdlocal "$HDFSCMD jar $HADOOP_EXAMPLES_JAR randomwriter -Dtest.randomwrite.bytes_per_map=256000 input_$SUITESTART"
#	execcmdlocal "$HDFSCMD jar $HADOOP_EXAMPLES_JAR wordcount /user/$USER/distCpinput /user/$USER/wc"
#        stat1=$?
#        assertEqual $stat1 0
#	execcmdlocal "$HDFSCMD dfs -lsr /user/$USER/wc"

#        echo "TESTCASE - 2 :: Distcp with hdfs"
#        execcmdlocal "$HDFSCMD distcp $NNHDFS/user/$USER/distCpinput /user/$USER/hdfsdistCpoutput"
#        stat2=$?
#        assertEqual $stat2 0
#
#        echo "TESTCASE - 3 :: Distcp with hftp"
#        execcmdlocal "$HDFSCMD distcp $NNHFTP/user/$USER/distCpinput /user/$USER/hftpdistCpoutput"
#        stat3=$?
#        assertEqual $stat3 0

        echo "TESTCASE - 4 :: Distcp with hftp not without full hostname"
        execcmdlocal "$HDFSCMD distcp $NNHFTP_BASE/user/$USER/distCpinput /user/$USER/hftpdistCpoutputsmallhostname"
        stat4=$?
        assertEqual $stat4 0

        echo "TESTCASE - 5 :: Distcp with hftp not without full hostname and witout port"
        execcmdlocal "$HDFSCMD distcp $NNHFTP_BASE_NOPORT/user/$USER/distCpinput /user/$USER/hftpdistCpoutputnoport"
        stat4=$?
        assertEqual $stat4 0

        
        displayTestCaseResult

}

function test_UseIp_true {
        TESTCASE_DESC="test useIp=false"

        ### Create New Conf
        NEWCONF="$HOME/${SUITESTART}_newconf";
        execcmdlocal "rm -rf $NEWCONF"
        execcmdlocal "mkdir -p $NEWCONF"
        ssh $NN "cp -r $HADOOP_CONF_DIR/* $NEWCONF"; chmod -R 775 $NEWCONF
        addPropertyToXMLConf $NEWCONF/core-site.xml "hadoop.security.token.service.use_ip" "true" "desc"

        stopCluster
        startCluster $NEWCONF $NEWCONF $NEWCONF $NEWCONF
        takeNNOutOfSafemode '' $NEWCONF

exit
	HDFSCMD="$HADOOP_HDFS_CMD --config $NEWCONF "
        execcmdlocal "$HDFSCMD dfs -rmr -skipTrash /user/$USER/*"
        execcmdlocal "$HDFSCMD dfs -copyFromLocal /etc/passwd /user/$USER/distCpinput"

        echo "TESTCASE - 1 :: Random Writer Job "
#        execcmdlocal "$HDFSCMD jar $HADOOP_EXAMPLES_JAR randomwriter -Dtest.randomwrite.bytes_per_map=256000 input_$SUITESTART"
	execcmdlocal "$HDFSCMD jar $HADOOP_EXAMPLES_JAR wordcount /user/$USER/distCpinput /user/$USER/wc"
        stat1=$?
        assertEqual $stat1 0
	execcmdlocal "$HDFSCMD dfs -lsr /user/$USER/wc"

#        echo "TESTCASE - 2 :: Distcp with hdfs"
#        execcmdlocal "$HDFSCMD distcp $NNHDFS/user/$USER/distCpinput /user/$USER/hdfsdistCpoutput"
#        stat2=$?
#        assertEqual $stat2 0

#        echo "TESTCASE - 3 :: Distcp with hftp"
#        execcmdlocal "$HDFSCMD distcp $NNHFTP/user/$USER/distCpinput /user/$USER/hftpdistCpoutput"
#        stat3=$?
#        assertEqual $stat3 0

        #stopCluster
        #startCluster
        #takeNNOutOfSafemode

        displayTestCaseResult
	HDFSCMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR"
}


### Setup 
localSetup

### Execute Tests
executeTests

### clean up
cleanUp
