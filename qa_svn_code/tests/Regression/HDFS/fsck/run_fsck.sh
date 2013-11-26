#!/bin/bash

. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/user_kerb_lib.sh

###################
######## SETUP
###################
function setup
{
    TEST_USER=$HADOOPQA_USER
    FSCKoption="fsck"
    local3Gbfile="${TMPDIR}/hadoopqe_fsckTests_3gbfile_`date +%s`"
    FS=$(getDefaultFS)
    #get the namenode
    NN=$(getDefaultNameNode)

    
    #dir for safemode tests on hdfs
    FSCK_TESTS_DIR="/user/${TEST_USER}/fsck_tests"
    #delete directories on hdfs, make sure no old data is there, skip the trash
    deleteHdfsDir $FSCK_TESTS_DIR "$FS" "0"
    #create the directory for safemode tests on hdfs
    createHdfsDir $FSCK_TESTS_DIR $FS
    
    #dir for safemode tests on hdfs
    FSCK_BAD_DATA_TESTS_DIR="/user/${TEST_USER}/fsck_bad_data_tests"
    #delete directories on hdfs, make sure no old data is there, skip the trash
    deleteHdfsDir $FSCK_BAD_DATA_TESTS_DIR "$FS" "0"
    #create the directory for safemode tests on hdfs
    createHdfsDir $FSCK_BAD_DATA_TESTS_DIR $FS

    #create some data in that dir for fsck
    local cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR randomwriter -fs $FS -Dmapreduce.randomwriter.bytespermap=256000 ${FSCK_TESTS_DIR}/data"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #if the data did not get created exit
    if [ "$?" -ne "0" ]; then
        echo "Could not create data for fsck tests"
        SCRIPT_EXIT_CODE=1
        echo "SCRIPT_EXIT_CODE=${SCRIPT_EXIT_CODE}"
        exit $SCRIPT_EXIT_CODE
    fi

    #create the 3gb file
    cmd="dd if=/dev/zero of=${local3Gbfile} bs=10240 count=300000"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd

    if [ "$?" -ne "0" ]; then
        echo "Could not create a local 3GB file"
        echo "SCRIPT_EXIT_CODE=1"
        exit 1
    fi
    
    LOCAL_3GB_FILE_SIZE=`ls -l $local3Gbfile | cut -f 5 -d " "`

    FSCK_3GB_DIR=${FSCK_TESTS_DIR}/3gb_dir
    createHdfsDir $FSCK_3GB_DIR $FS
    
    #copy to directory
    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -copyFromLocal $local3Gbfile $FSCK_3GB_DIR"
    echo "$cmd"
    eval $cmd
    if [ "$?" -ne "0" ]; then
        echo "not able to copy file to hdfs, exiting"
        SCRIPT_EXIT_CODE=1
        echo "SCRIPT_EXIT_CODE=${SCRIPT_EXIT_CODE}"
        exit $SCRIPT_EXIT_CODE
    fi
}


######################
######## teardown
######################
function teardown
{
    #delete the hdfs dir, skip the trash
    deleteHdfsDir $FSCK_TESTS_DIR "$FS" "0"
    deleteHdfsDir $FSCK_BAD_DATA_TESTS_DIR "$FS" "0"
}

#test commented out
function fsck_01 {
    TESTCASE_INFO="Start namenode without datanodes and run fsck"
    TESTCASE_DESC="fsck_01"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    # STOP DATA NODES
    resetDataNodes stop

    sleep 10

    # Run FSCK with no data notes up
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_TESTS_DIR"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    i=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`

    # HDFS should still be HEALTHY
    if [ $i = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "File system did not report a HEALTHY status"
    fi

    # START CLUSTER BACK UP
    resetDataNodes start
    #sleep 10
    # restart the NN so it picks up that it has no data nodes
    resetNameNode stop
    resetNameNode start

    sleep 10

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_02 {
    TESTCASE_INFO="Startup datanodes, run randomwriter, sort and run fsck"
    TESTCASE_DESC="fsck_02"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    getFsSize $FSCK_TESTS_DIR $FS
    beforeSize=$?
    a_mesg="size of file system on "$FSCK_TESTS_DIR" is "$beforeSize
    dumpmsg $a_mesg

    getNumDataNodes $FSCK_TESTS_DIR $FS
    a_mesg="Number of nodes on "$FSCK_TESTS_DIR" is "$?
    dumpmsg $a_mesg

    getNumRacks $FSCK_TESTS_DIR $FS
    a_mesg="Number of racks on "$FSCK_TESTS_DIR" is "$?
    dumpmsg $a_mesg

    Unique=${FSCK_TESTS_DIR}/${TESTCASE_DESC}
    local cmd="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR randomwriter -fs $FS -Dmapreduce.randomwriter.bytespermap=256000 ${Unique}/input"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #randomWriter $Unique/input
    sortJob $Unique/input $Unique/output $FS

    getNumDataNodes $FSCK_TESTS_DIR $FS
    a_mesg="Number of nodes on "$FSCK_TESTS_DIR" is "$?
    dumpmsg $a_mesg

    getFsSize $FSCK_TESTS_DIR $FS
    afterSize=$?
    a_mesg="size of file system on "$FSCK_TESTS_DIR" is "$afterSize
    dumpmsg $a_mesg
    afterint=`expr $afterSize`
    beforeint=`expr $beforeSize`
    #add check if afterSize is greater than before Size
    if [ $afterint -eq $beforeint ]; then
        dumpmsg "File system same size after randomwriter"
        setFailCase "Random writer job failed"
    fi

    #echo $afterSize
    #echo $beforeSize
    if [ $afterint -lt $beforeint ]; then
        dumpmsg "File system less after randomwriter"
        setFailCase "Someone else is using the filesystem"
    fi

    #Check directory fsck

    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption $Unique"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`

    dumpmsg "J = $j"

    #Result check
    if [ $j = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
        COMMAND_EXIT_CODE=0
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "Non healthy response from fsck"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_additional_01 {
    TESTCASE_DEC="Execute fsck on Healthy File system when name node is in Safemode"
    TESTCASE_DESC="fsck_additional_01"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    setKerberosTicketForUser $HDFS_USER
    #enter the namdemode in safemode
    local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfsadmin -fs $FS -safemode enter | grep 'Safe mode is ON'"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    if [ "$?" -ne "0" ]; then
        setFailCase "NAMENODE not in SAFE MODE"
        displayTestCaseResult
        return $COMMAND_EXIT_CODE
    fi

    setKerberosTicketForUser $HADOOPQA_USER
    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $FSCK_TESTS_DIR"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    i=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`

    if [ $i = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "File system did not report a HEALTHY status"
    fi
    
    setKerberosTicketForUser $HDFS_USER
    
    #take the namdemode out of safemode
    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfsadmin -fs $FS -safemode leave | grep 'Safe mode is OFF'"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Get the status of the previous command
    status="$?"
    setKerberosTicketForUser $HADOOPQA_USER

    #if status is 1 that means safe mode is still on
    if [ "$status" -ne 0 ]; then
        echo "LOG_INFO: !!!!!!!!!!!!!!!!!!!!NAME NODE STILL IN SAFE MODE. COULD LEAD TO MORE FAILURES!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#test commented out
function fsck_additional_02 {
    TESTCASE_INFO="Execute fsck on Healthy File system when balancer is turned on manually"
    TESTCASE_DESC="fsck_additional_02"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #ADD STEP TO RUN BALACKER HERE
    local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR balancer -fs $FS"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    if [ $? != 0 ]; then
        dumpmsg "*** $TESTCASE_DESC test case failed - Balancer did not run propertly"
        setFailCase "*** $TESTCASE_DESC test case failed - Balancer did not run propertly"
        displayTestCaseResult
        return $COMMAND_EXIT_CODE
    fi

    #RUN FSCK
    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $FSCK_TESTS_DIR"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    i=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    if [ $i = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "File system did not report a HEALTHY status"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#test commented out
function fsck_03 {
    TESTCASE_INFO="Kill datanode process on all machines in one rack and run fsck"
    TESTCASE_DESC="fsck_03"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    # STOP NODES
    resetDataNodes stop 
    sleep 60

    # RUN FSCK
    local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $FSCK_TESTS_DIR"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    i=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`

    if [ $i = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "File system did not report a HEALTHY status after data nodes were turned off"
    fi

    # START NODES BACK UP
    resetDataNodes start
    sleep 60

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#test commented out
function test_fsck_04 {
    TESTCASE_INFO="Manually corrupt a block on one datanode and run fsck"
    TESTCASE_DESC="fsck_04"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"
    
    #create a file with some content
    local file="${TMPDIR}/fsck_04_input.txt"
    local cmd="dd if=/dev/random of=${file} bs=1024 count=10"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd

    Unique=${FSCK_BAD_DATA_TESTS_DIR}/${TESTCASE_DESC}
    #create the dir
    createHdfsDir $Unique $FS

    #copy the file to hdfs  
    local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -copyFromLocal ${file} ${Unique}"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd

    #Corrupt a block
    CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $FS $Unique -files -blocks -locations | grep '0. BP-'"
    echo $CMD
    local output=`eval $CMD`
    BLOCK_POOL_ID=`echo $output | cut -d':' -f 1 | cut -d' ' -f 2`
    BLOCK_ID=`echo $output | cut -d':' -f 2 | cut -d' ' -f 1 | cut -d'_' -f1,2`
    BLOCK_LOCATION=`echo $output | cut -d'[' -f 2 | cut -d']' -f 1 | awk -F':1004,' '{print $1}'`
    echo "BLOCK_POOP_ID=$BLOCK_POOL_ID"
    echo "BLOCK_ID=$BLOCK_ID"
    echo "BLOCK_LOCATIONS==$BLOCK_LOCATION"
    PHYSICAL_LOCATION=`xmllint $HADOOP_CONF_DIR/hdfs-site.xml | grep "<name>[ ]*dfs.datanode.data.dir[ ]*<\/name>"  -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1 | sed -e "s/,/\ /g"`
    echo "PHYSICAL_LOCATION=$PHYSICAL_LOCATION" 
    local filePath
    local blocksCorrupted=0
    local num_of_blks=1
    for location in $PHYSICAL_LOCATION; do
        #use the find command to find the file
        cmd="ssh -i $HDFS_SUPER_USER_KEY_FILE ${HDFS_SUPER_USER}@${BLOCK_LOCATION} \"find ${location}/current/${BLOCK_POOL_ID} -name '${BLOCK_ID}'\""
        echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
        filePath=`eval $cmd`
        if [ -n "$filePath" ]; then
            #filePath="${location}/current/${BLOCK_POOL_ID}/*/*/${filePath}"
            cmd="ssh -i $HDFS_SUPER_USER_KEY_FILE ${HDFS_SUPER_USER}@${BLOCK_LOCATION} exec \"echo 'Corrupting the first block' >> ${filePath}\""
            echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
            eval $cmd
            blocksCorrupted=$[$blocksCorrupted + 1]
            break;
        fi
    done
    #if blocks have not been corrupted then this test fails
    if [ "$blocksCorrupted" -ne "$num_of_blks" ]; then
        REASONS="Only corrupted ${blocksCorrupted}/${num_of_blks} blocks, since all blocks were not corrupted the test fails in setup."
        COMMAND_EXIT_CODE=1
        displayTestCaseResult
        return
    fi

    #cat the file to report the block as corrupt
    cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_04_input.txt"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd >> /dev/null

    #Check directory fsck
    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $Unique | grep \"The filesystem under path '${Unique}' is HEALTHY\""
    echo `date '+%Y-%m-%d %H:%M:%S'`$cmd
    eval $cmd
    if [ "$?" -ne "0" ]; then
        REASONS="Did not see the message - The filesystem under path '${Unique}' is HEALTHY"
        COMMAND_EXIT_CODE=1
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_05 {
    TESTCASE_INFO="Manually corrupt all replicas of a block and run fsck. Because of Jira HDFS-1371 corruting all replicas of a block will no longer lead to a corrut fs error."
    TESTCASE_DESC="fsck_05"
    
    #create a file with some content
    local file="${TMPDIR}/fsck_05_input.txt"
    local cmd="dd if=/dev/random of=${file} bs=1024 count=10"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd

    Unique=${FSCK_BAD_DATA_TESTS_DIR}/${TESTCASE_DESC}
    #create the dir
    createHdfsDir $Unique $FS

    #copy the file to hdfs  
    local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -copyFromLocal ${file} ${Unique}"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd

    #Corrupt a block
    CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $FS $Unique -files -blocks -locations | grep '0. BP-'"
    echo $CMD
    local output=`eval $CMD`
    BLOCK_POOL_ID=`echo $output | cut -d':' -f 1 | cut -d' ' -f 2`
    BLOCK_ID=`echo $output | cut -d':' -f 2 | cut -d' ' -f 1 | cut -d'_' -f1,2`
    BLOCK_LOCATIONS=`echo $output | cut -d'[' -f 2 | cut -d']' -f 1 | awk -F':1004,' '{print $1 $2 $3}'| cut -d':' -f 1`
    echo "BLOCK_ID=$BLOCK_ID"
    echo "BLOCK_POOL_ID=$BLOCK_POOL_ID"
    echo "BLOCK_LOCATIONS==$BLOCK_LOCATIONS"
    PHYSICAL_LOCATION=`xmllint $HADOOP_CONF_DIR/hdfs-site.xml | grep "<name>[ ]*dfs.datanode.data.dir[ ]*<\/name>"  -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1 | sed -e "s/,/\ /g"`
    echo "PHYSICAL_LOCATION=$PHYSICAL_LOCATION" 
    local filePath
    local blocksCorrupted=0
    local num_of_blks=0

    multi_locs=''
    for location in $PHYSICAL_LOCATION; do
        multi_locs=${multi_locs}' '${location}/current/${BLOCK_POOL_ID}
    done
    echo "Dir location: $multi_locs"
    echo $BLOCK_LOCATIONS
    export PDSH_SSH_ARGS_APPEND=" -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa"
    pdsh -w `echo $BLOCK_LOCATIONS | sed 's/ /,/g'` -l hdfsqa "cat \`find ${multi_locs} -name ${BLOCK_ID}\` > \`find ${multi_locs} -name ${BLOCK_ID}\`.tmp; echo Corrupting the first block | tee \`find ${multi_locs} -name ${BLOCK_ID}\`; cat \`find ${multi_locs} -name ${BLOCK_ID}\`.tmp >> \`find ${multi_locs} -name ${BLOCK_ID}\`; rm \`find ${multi_locs} -name ${BLOCK_ID}\`.tmp"

    #cat the file to report the block as corrupt
    cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_05_input.txt 2>&1 |  tee -a $TMP_FILE | egrep '(Could not obtain block)|(cat: Checksum error)'"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Result check
    if [ "$?" -ne "0" ]; then
        REASONS="${cmd} did not run successfully. cat should have failed as the file has corrupt blocks."
        COMMAND_EXIT_CODE=1
    fi

    #Check directory fsck
    cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $Unique |  tee -a $TMP_FILE | grep \"The filesystem under path '${Unique}' is CORRUPT\""
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Result check
    if [ "$?" -eq "0" ]; then
        REASONS="See the message The filesystem under path '${Unique}' is CORRUPT. Because of Jira HDFS-1371 and 4030363 if a block has all corrupt blocks fsck will not return the fs as unhealthy. ${REASONS}"
        COMMAND_EXIT_CODE=1
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#test commented out
function fsck_06 {
    TESTCASE_INFO="Start name node without any data/files/directory on HDFS and run fsck"
    TESTCASE_DESC="fsck_06"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    local cmd="$HADOOP_HDFS_CMD dfs -fs $FS -rmr -skipTrash /"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    
    # Run FSCK with 
    cmd"$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption -fs $FS /"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    i=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`

    # HDFS should still be HEALTHY
    if [ $i = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "File system did not report a HEALTHY status"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_09 {
    TESTCASE_INFO="Run fsck on file path with special character in name"
    TESTCASE_DESC="fsck_09"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Create directory with special character
    Unique=${FSCK_TESTS_DIR}/${TESTCASE_DESC}"@"
    createHdfsDir $Unique $FS

    #Check directory fsck
    local cmd="${HADOOP_HDFS_CMD} $FSCKoption -fs $FS $Unique"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    dumpmsg "J = $j"
    #Result check
    if [ $j = "1" ]; then
        dumpmsg "*** $TESTCASE_DESC test case passed"
    else
        dumpmsg "*** $TESTCASE_DESC test case failed"
        setFailCase "$Unique not created properly" 
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_cli01 {
    TESTCASE_INFO="Execute fsck after large file transfer, test -files"
    TESTCASE_DESC="FSCK_CLI01"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"
    
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_3GB_DIR -files" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    totalfiles=`$cmd | grep "Total files" | cut -f 2`
    totaldirs=`$cmd | grep "Total dirs" | cut -f 2`
    totalsize=`$cmd | grep "Total size" | cut -f 2 | cut -f 1 -d " "`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after write"
        setFailCase "File system not healty after write"
    fi
    if [ $totalfiles != "1" ]; then
        dumpmsg "Only one file should exist"
        setFailCase "Only one file should exist"
    fi
    if [ $totaldirs != "1" ]; then
        dumpmsg "Only one directory should exist"
        setFailCase "Only one directory should exist"
    fi   
    if [ $totalsize != $LOCAL_3GB_FILE_SIZE ]; then
        dumpmsg "Local file size $localfize does not match hdfs file system size $totalsize"
        setFailCase "Local file size $localfize does not match hdfs file system size $totalsize"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_cli02 {
    TESTCASE_INFO="Execute fsck after large file transfer, test -blocks"
    TESTCASE_DESC="FSCK_CLI02"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_3GB_DIR -blocks" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    totalfiles=`$cmd | grep "Total files" | cut -f 2`
    totaldirs=`$cmd | grep "Total dirs" | cut -f 2`
    totalsize=` $cmd | grep "Total size" | cut -f 2 | cut -f 1 -d " "`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after write"
        setFailCase "File system not healty after write"
    fi
    if [ $totalfiles != "1" ]; then
        dumpmsg "Only one file should exist"
        setFailCase "Only one file should exist"
    fi
    if [ $totaldirs != "1" ]; then
        dumpmsg "Only one directory should exist"
        setFailCase "Only one directory should exist"
    fi
    if [ $totalsize != $LOCAL_3GB_FILE_SIZE ]; then
        dumpmsg "Local file size $localfize does not match hdfs file system size $totalsize"
        setFailCase "Local file size $localfize does not match hdfs file system size $totalsize"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_cli03 {
    TESTCASE_INFO="Execute fsck after large file transfer, test -racks"
    TESTCASE_DESC="FSCK_CLI03"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_3GB_DIR -racks" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    totalfiles=`$cmd | grep "Total files" | cut -f 2`
    totaldirs=`$cmd | grep "Total dirs" | cut -f 2`
    totalsize=` $cmd | grep "Total size" | cut -f 2 | cut -f 1 -d " "`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after write"
        setFailCase "File system not healty after write"
    fi
    if [ $totalfiles != "1" ]; then
        dumpmsg "Only one file should exist"
        setFailCase "Only one file should exist"
    fi
    if [ $totaldirs != "1" ]; then
        dumpmsg "Only one directory should exist"
        setFailCase "Only one directory should exist"
    fi
    if [ $totalsize != $LOCAL_3GB_FILE_SIZE ]; then
        dumpmsg "Local file size $localfize does not match hdfs file system size $totalsize"
        setFailCase "Local file size $localfize does not match hdfs file system size $totalsize"
    fi
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
} 

function test_fsck_cli04 {
    TESTCASE_INFO="Execute fsck after large file transfer, test -locations"
    TESTCASE_DESC="FSCK_CLI04"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_3GB_DIR -locations" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    totalfiles=`$cmd | grep "Total files" | cut -f 2`
    totaldirs=`$cmd | grep "Total dirs" | cut -f 2`
    totalsize=` $cmd | grep "Total size" | cut -f 2 | cut -f 1 -d " "`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after write"
        setFailCase "File system not healty after write"
    fi 
    if [ $totalfiles != "1" ]; then
        dumpmsg "Only one file should exist"
        setFailCase "Only one file should exist"
    fi 
    if [ $totaldirs != "1" ]; then
        dumpmsg "Only one directory should exist"
        setFailCase "Only one directory should exist"
    fi 
    if [ $totalsize != $LOCAL_3GB_FILE_SIZE ]; then
        dumpmsg "Local file size $localfize does not match hdfs file system size $totalsize"
        setFailCase "Local file size $localfize does not match hdfs file system size $totalsize"
    fi
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_cli05 {
    TESTCASE_INFO="Execute fsck after large file transfer, test -move"
    TESTCASE_DESC="FSCK_CLI05"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $FSCK_3GB_DIR -move" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    totalfiles=`$cmd | grep "Total files" | cut -f 2`
    totaldirs=`$cmd | grep "Total dirs" | cut -f 2`
    totalsize=` $cmd | grep "Total size" | cut -f 2 | cut -f 1 -d " "`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after write"
        setFailCase "File system not healty after write"
    fi
    if [ $totalfiles != "1" ]; then
        dumpmsg "Only one file should exist"
        setFailCase "Only one file should exist"
    fi
    if [ $totaldirs != "1" ]; then
        dumpmsg "Only one directory should exist"
        setFailCase "Only one directory should exist"
    fi
    if [ $totalsize != $LOCAL_3GB_FILE_SIZE ]; then
        dumpmsg "Local file size $localfize does not match hdfs file system size $totalsize"
        setFailCase "Local file size $localfize does not match hdfs file system size $totalsize"
    fi

    cmd="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $FS -ls $FSCK_TESTS_DIR | grep 'lost' | grep 'found' | wc -l"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    if [ "$?" -ne "0" ]; then
        dumpmsg "lost+found was created"
        setFailCase "lost+found was created"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


function test_fsck_cli06 {
    TESTCASE_INFO="fsck delete operation"
    TESTCASE_DESC="FSCK_CLI06"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_3GB_DIR -delete" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after fsck -delete"
        setFailCase "File system not healty after fsck -delete"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_cli07 {
    TESTCASE_INFO="Execute fsck after large file transfer, no option"
    TESTCASE_DESC="FSCK_CLI07"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_3GB_DIR" 
    dumpmsg "$cmd"
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "HEALTHY" | wc -l`
    totalfiles=`$cmd | grep "Total files" | cut -f 2`
    totaldirs=`$cmd | grep "Total dirs" | cut -f 2`
    totalsize=` $cmd  | grep "Total size" | cut -f 2 | cut -f 1 -d " "`
    if [ $j != "1" ]; then
        dumpmsg "File system not healty after write"
        setFailCase "File system not healty after write"
    fi
    if [ $totalfiles != "1" ]; then
        dumpmsg "Only one file should exist"
        setFailCase "Only one file should exist"
    fi
    if [ $totaldirs != "1" ]; then
        dumpmsg "Only one directory should exist"
        setFailCase "Only one directory should exist"
    fi
    if [ $totalsize != $LOCAL_3GB_FILE_SIZE ]; then
        dumpmsg "Local file size $localfize does not match hdfs file system size $totalsize"
        setFailCase "Local file size $localfize does not match hdfs file system size $totalsize"
    fi
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
} 

function test_fsck_cli08 {
    TESTCASE_INFO="Execute fsck on .."
    TESTCASE_DESC="FSCK_CLI08"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_TESTS_DIR/.."
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "does not exist" | wc -l`
    if [ $j == "1" ]; then
        dumpmsg "Should have a failed operation"
        setFailCase "Should have a failed operation"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

function test_fsck_cli09 {
    TESTCASE_INFO="Execute fsck on a path that doesn't exist"
    TESTCASE_DESC="FSCK_CLI09"
    displayTestCaseMessage "$TESTCASE_DESC"
    echo "$TESTCASE_INFO"

    #Check directory fsck
    local cmd="$HADOOP_HDFS_CMD $FSCKoption -fs $FS $FSCK_TESTS_DIR/ajklsnaljk"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    j=`$cmd | grep "The filesystem under path" | grep "does not exist" | wc -l`
    if [ $j == "1" ]; then
        dumpmsg "Should have a failed operation"
        setFailCase "Should have a failed operation"
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}



#bug 4420644
function test_fsck_10 {
    TESTCASE_INFO="Manually corrupt all replicas of a block and run fsck. Corrupt the block by overwriting the content of the block with smaller content. Because of Jira HDFS-1371 and Bug 403363 corruting all replicas of a block will no longer lead to a corrut fs error."
    TESTCASE_DESC="fsck_10"
    
    #create a file with some content
    local file="${TMPDIR}/fsck_10_input.txt"
    local cmd="dd if=/dev/random of=${file} bs=1024 count=10"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd

    Unique=${FSCK_BAD_DATA_TESTS_DIR}/${TESTCASE_DESC}
    #create the dir
    createHdfsDir $Unique $FS

    #copy the file to hdfs  
    local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -copyFromLocal ${file} ${Unique}"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
	eval $cmd

    #Corrupt a block
    CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $FS $Unique -files -blocks -locations | grep '0. BP-'"
    echo $CMD
    local output=`eval $CMD`
    BLOCK_POOL_ID=`echo $output | cut -d':' -f 1 | cut -d' ' -f 2`
    BLOCK_ID=`echo $output | cut -d':' -f 2 | cut -d' ' -f 1 | cut -d'_' -f1,2`
    BLOCK_LOCATIONS=`echo $output | cut -d'[' -f 2 | cut -d']' -f 1 | awk -F':1004,' '{print $1 $2 $3}'| cut -d':' -f 1`
    echo "BLOCK_ID=$BLOCK_ID"
    echo "BLOCK_POOL_ID=$BLOCK_POOL_ID"
    echo "BLOCK_LOCATIONS==$BLOCK_LOCATIONS"
    PHYSICAL_LOCATION=`xmllint $HADOOP_CONF_DIR/hdfs-site.xml | grep "<name>[ ]*dfs.datanode.data.dir[ ]*<\/name>"  -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1 | sed -e "s/,/\ /g"`
    echo "PHYSICAL_LOCATION=$PHYSICAL_LOCATION" 
    local filePath
    local blocksCorrupted=0
    local num_of_blks=0
    
    multi_locs=''
   	for location in $PHYSICAL_LOCATION; do
    	multi_locs=${multi_locs}' '${location}/current/${BLOCK_POOL_ID}
    done
    echo "Dir location: $multi_locs"
    echo $BLOCK_LOCATIONS
    export PDSH_SSH_ARGS_APPEND=" -i /homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa"
    pdsh -w `echo $BLOCK_LOCATIONS | sed 's/ /,/g'` -l hdfsqa "echo Corrupting the first block | tee \`find ${multi_locs} -name ${BLOCK_ID}\`"
        
    #cat the file to report the block as corrupt
	#cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_10_input.txt 2>&1 |  tee -a $TMP_FILE | egrep '(Could not obtain block)|(cat: Could not obtain block:)'"
	#relying on the error message is not reliable so we just check if this command failed or not
	cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_10_input.txt"
	echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Result check
    if [ "$?" -eq "0" ]; then
        REASONS="${cmd} did not run successfully. cat should have failed as the file has corrupt blocks. See Bug 4420644"
        COMMAND_EXIT_CODE=1
    fi
    
    #cat the file to report the block as corrupt
    #cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_10_input.txt"
    #echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    #eval $cmd >> /dev/null
		
    #Check directory fsck
	cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $Unique |  tee -a $TMP_FILE | grep \"The filesystem under path '${Unique}' is CORRUPT\""
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Result check
    if [ "$?" -eq "0" ]; then
        REASONS="See the message The filesystem under path '${Unique}' is CORRUPT. Because of Jira HDFS-1371 and Bug 403363 corruting all replicas of a block will no longer lead to a corrut fs error. ${REASONS}"
        COMMAND_EXIT_CODE=1
    fi
    
    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}

#bug 4423177
function test_fsck_11 {
    TESTCASE_INFO="Manually corrupt all replicas of a block and run fsck. Corrupt the block by adding content to the end of the block. Because of Jira HDFS-1371 and Bug 403363 corruting all replicas of a block will no longer lead to a corrut fs error."
    TESTCASE_DESC="fsck_11"
    
    #create a file with some content
    local file="${TMPDIR}/fsck_11_input.txt"
    local cmd="dd if=/dev/random of=${file} bs=1024 count=10"
    eval $cmd

    Unique=${FSCK_BAD_DATA_TESTS_DIR}/${TESTCASE_DESC}
    #create the dir
    createHdfsDir $Unique $FS

    #copy the file to hdfs  
    local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -copyFromLocal ${file} ${Unique}"
    eval $cmd

    #Corrupt a block
    CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR fsck -fs $FS $Unique -files -blocks -locations | grep '0. BP-'"
    echo $CMD
    local output=`eval $CMD`
    BLOCK_POOL_ID=`echo $output | cut -d':' -f 1 | cut -d' ' -f 2`
    BLOCK_ID=`echo $output | cut -d':' -f 2 | cut -d' ' -f 1 | cut -d'_' -f1,2`
    BLOCK_LOCATIONS=`echo $output | cut -d'[' -f 2 | cut -d']' -f 1 | awk -F':1004,' '{print $1 $2 $3}'| cut -d':' -f 1`
    echo "BLOCK_ID=$BLOCK_ID"
    echo "BLOCK_POOL_ID=$BLOCK_POOL_ID"
    echo "BLOCK_LOCATIONS==$BLOCK_LOCATIONS"
    PHYSICAL_LOCATION=`xmllint $HADOOP_CONF_DIR/hdfs-site.xml | grep "<name>[ ]*dfs.datanode.data.dir[ ]*<\/name>"  -C 2 | grep value | cut -d ">" -f2 | cut -d "<" -f1 | sed -e "s/,/\ /g"`
    echo "PHYSICAL_LOCATION=$PHYSICAL_LOCATION" 
    local filePath
    local blocksCorrupted=0
    local num_of_blks=0
    for host in $BLOCK_LOCATIONS; do
        num_of_blks=$[$num_of_blks + 1]
        for location in $PHYSICAL_LOCATION; do
            #use the find command to find the file
            cmd="ssh -i $HDFS_SUPER_USER_KEY_FILE ${HDFS_SUPER_USER}@${host} \"find ${location}/current/${BLOCK_POOL_ID} -name '${BLOCK_ID}'\""
            echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
            filePath=`eval $cmd`
            if [ -n "$filePath" ]; then
                #filePath="${location}/current/${BLOCK_POOL_ID}/*/*/${filePath}"
                cmd="ssh -i $HDFS_SUPER_USER_KEY_FILE ${HDFS_SUPER_USER}@${host} \"echo 'Corrupting the first block' >> ${filePath}\""
                echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
                eval $cmd
                blocksCorrupted=$[$blocksCorrupted + 1]
                break;
            fi
        done
    done
    #if 3 blocks have not been corrupted then this test fails
    if [ "$blocksCorrupted" -ne "$num_of_blks" ]; then
        REASONS="Only corrupted ${blocksCorrupted}/${num_of_blks} blocks, since all blocks were not corrupted the test fails in setup."
        COMMAND_EXIT_CODE=1
        displayTestCaseResult
        return
    fi

    #cat the file to report the block as corrupt
    cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_11_input.txt 2>&1 | tee -a $TMP_FILE | egrep '(Could not obtain block)|(cat: Could not obtain block:)'"
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Result check
    if [ "$?" == "0" ]; then
        REASONS="${cmd} did not run successfully. cat should have failed as the file has corrupt blocks. See Bug 4423177"
        COMMAND_EXIT_CODE=1
    fi
    
    #cat the file to report the block as corrupt
    #cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -cat ${Unique}/fsck_11_input.txt"
    #echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    #eval $cmd >> /dev/null

    #Check directory fsck
    cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR $FSCKoption -fs $FS $Unique | tee -a $TMP_FILE | grep \"The filesystem under path '${Unique}' is CORRUPT\""
    echo `date '+%Y-%m-%d %H:%M:%S'` - $cmd
    eval $cmd
    #Result check
    if [ "$?" -ne "1" ]; then
            REASONS="See the message The filesystem under path '${Unique}' is CORRUPT. Because of Jira HDFS-1371 and Bug 403363 corruting all replicas of a block will no longer lead to a corrut fs error. ${REASONS}"
        COMMAND_EXIT_CODE=1
    fi

    displayTestCaseResult
    return $COMMAND_EXIT_CODE
}


############################################
# Test Cases to run
############################################
OWNER=arpitg

#setup
setup

TMP_FILE=$ARTIFACTS/client_log.txt
rm -f $TMP_FILE

#execute all tests that start with test_
executeTests

#teardown
teardown
