#!/bin/sh

#load the library file
. $WORKSPACE/lib/library.sh
. $WORKSPACE/lib/restart_cluster_lib.sh
. $WORKSPACE/lib/user_kerb_lib.sh

#############################################
#SETUP
#############################################
function setup
{
  NUM_TESTCASE=0
  NUM_TESTCASE_PASSED=0
  SCRIPT_EXIT_CODE=0;
  export  COMMAND_EXIT_CODE=0
  export TESTCASE_DESC="None"
  export REASONS=""
  TEST_USER=$HADOOPQA_USER
  FS=$(getDefaultFS)
  
  #dir for delefation token tests
  ARCHIVE_TESTS_DIR="/user/${TEST_USER}/archive_tests"
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $ARCHIVE_TESTS_DIR $FS
  #create the directory for tests on hdfs
  createHdfsDir $ARCHIVE_TESTS_DIR $FS

  ARCHIVE_TESTS_SRC_DIR="${ARCHIVE_TESTS_DIR}/src"
  createHdfsDir $ARCHIVE_TESTS_SRC_DIR $FS

  ARCHIVE_TESTS_DEST_DIR="${ARCHIVE_TESTS_DIR}/dst"
  #create the directory for tests on hdfs
  createHdfsDir $ARCHIVE_TESTS_DEST_DIR $FS

  #get the namenode
  NN=$(getDefaultNameNode)
}


##############################################
# teardown
#############################################
function teardown
{
  #delete directories on hdfs, make sure no old data is there
  deleteHdfsDir $ARCHIVE_TESTS_DIR $FS
}


#Steps:
#Archive does not occur in the destination 
#1. Try to create an archive with an invalid name
#
#Expected:
#Invalid name for archives. Should be printed out to the screen
#TC ID=1207939
function test_archive_2
{
  TESTCASE_DESC="archive_2"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command and make sure it returns a failure
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName /dd/xx.har -p ${ARCHIVE_TESTS_SRC_DIR} ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR} 2>&1 | grep \"Invalid name for archives. /dd/xx.har\""
  echo $cmd
  eval $cmd
  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="the archive command did not reutrn the following error message Invalid name for archives. /dd/xx.har"
  displayTestCaseResult
}

#Steps:
#Archive does not get overwritten without any warning 
#1. Create an archive
#2. Create another archive but provide the same archive name as in step 1.
#
#Expected:
#You should not be allowed to overwrite an existing archive. You should get the following output "Invalid Output"
#TC ID=1207940
function test_archive_3
{
  TESTCASE_DESC="archive_3"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p ${ARCHIVE_TESTS_SRC_DIR} ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR}"
  echo $cmd
  eval $cmd

  #run the archive command again and see if we get an error
  cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} $ARCHIVE_TESTS_DEST_DIR 2>&1 | grep \"Invalid Output: ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har\""
  echo $cmd
  eval $cmd

  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="the archive command overwrote archive ${TESTCASE_DESC}, which its not suppose to do"
  displayTestCaseResult
}

#Steps:
#Same source and destination is specified
#1. Create an archive and the destination should be in the same folder which is being archived.
#
#Expected:
#Archive should exist in the appropriate folder
#TC ID=1207941
function test_archive_4
{
  TESTCASE_DESC="archive_4"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command with same source and destination
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}"
  echo $cmd
  eval $cmd

  #make sure that the archive file is there
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -ls ${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}/${TESTCASE_DESC}.har"
  echo $cmd
  eval $cmd

  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="the archive file ${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}/${TESTCASE_DESC}.har is not there"
  displayTestCaseResult
}

#Steps:
#Destination is a file 
#1. Try to create an archive and specify a file as the location of the archive
#
#Expected:
#Archive will fail as destination that is specified is a file, archive needs a dir
#TC ID=1207942
function test_archive_5
{
  TESTCASE_DESC="archive_5"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  local file="${TESTCASE_DESC}_file.txt"

  echo "some text for test $TESTCASE_DESC" > ${TMPDIR}/${file}

  #put the file on hdfs
  local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -copyFromLocal ${TMPDIR}/${file} $src_dir"
  echo $cmd
  eval $cmd

  #run the archive command with same source and destination
  cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${src_dir}/${file} 2>&1 | grep \"Permission denied: user=${TEST_USER}, access=EXECUTE, inode=\\\"${src_dir}/${file}\\\":${TEST_USER}:hdfs:-rw-------\""
  echo $cmd
  eval $cmd

  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="when specifying destination as a file for an archive, the following message was not received: Permission denied: user=${TEST_USER}, access=EXECUTE, inode=\"${src_dir}/${file}\":${TEST_USER}:hdfs:-rw-------. Bug 4366091"
  displayTestCaseResult
}

#Steps:
#Deleting a file in an archive 
#1. Create an archive
#2. Delete a file in the archive
#
#Expected:
#Delete of the file in the archive should be successful
#TC ID=1207943
function test_archive_6
{
  TESTCASE_DESC="archive_6"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR}"
  echo $cmd
  eval $cmd

  #delete one of the files in the archive _masterindex
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -rm -r ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_masterindex 2>&1 | grep \"Moved: '${FS}${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_masterindex' to trash\""
  echo $cmd
  eval $cmd

  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="not able to delete the ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_masterindex from the archive"
  displayTestCaseResult
}

#Steps:
#Renaming a file in an archive 
#1. create an archive
#2. rename one of the files in the archive
#
#Expected:
#One should be able to rename a file in the archive
#TC ID=1207944
function test_archive_7
{
  TESTCASE_DESC="archive_7"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR}"
  echo $cmd
  eval $cmd

  #rename one of the files in the archive _masterindex
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -mv ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_index ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_index2"
  echo $cmd
  eval $cmd

  #now make sure that the file is there
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -ls ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_index2"
  echo $cmd
  eval $cmd

  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="not able to rename the file ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_index in the archive"
  displayTestCaseResult
}

#Steps:
#Testing count in an archive 
#1. create an archive
#2. run the hadoop dfs -count command
#
#Expected:
#count should succeed and return appropriate value
#TC ID=1207945
function test_archive_8
{
  TESTCASE_DESC="archive_8"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR}"
  echo $cmd
  eval $cmd

  #run the count command
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -count ${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har | grep \"1.*3.*132.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har\""
  echo $cmd
  eval $cmd

  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="not able to run count command on file ${FS}${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/_index in the archive"
  displayTestCaseResult
}

#Steps:
#Archiving the directory created by the randomwriter
#1. Run a randomwriter job
#2. Archive the dir where randomwriter output is stored
#
#Expected:
#Archive should complete successfully.
#TC ID=1207946
function test_archive_9
{
  TESTCASE_DESC="archive_9"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}

  #run the random writer command with src_dir as the output location, set the bytes per map to 250KB so that ir runs faster
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR randomwriter -fs $FS -Dmapreduce.randomwriter.bytespermap=256000 $src_dir"
  echo $cmd
  eval $cmd
  if [ "$?" -ne "0" ];then
    COMMAND_EXIT_CODE=1
    REASONS="randomwriter job did not run"
    displayTestCaseResult
    return
  fi
  
  #run the archive command on src_dir(where randomwriter output is stored)
  cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR}"
  echo $cmd
  eval $cmd
  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="the archive command on the randowm writer output failed"
  displayTestCaseResult
}

#Steps:
#Specifying a har file as the input for sort program
#1. Create an archive
#2. Provide the har file as the input to sort
#
#Expected:
#Sort should complete successfully
#JIRA https://issues.apache.org/jira/browse/MAPREDUCE-1752. Still seems to be causing issues, Bug 4307735
#TC ID=1207948
function test_archive_11
{
  TESTCASE_DESC="archive_11"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}

  #run the random writer command with src_dir as the output location, set the bytes per map to 250KB so that ir runs faster, so that we have data to sort
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR randomwriter -fs $FS -Dmapreduce.randomwriter.bytespermap=256000 $src_dir"
  echo $cmd
  eval $cmd
  if [ "$?" -ne "0" ];then
    COMMAND_EXIT_CODE=1
    REASONS="randomwriter job did not run"
    displayTestCaseResult
    return
  fi
  
  #run the archive command on src_dir
  cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR}"
  echo $cmd
  eval $cmd
  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  if [ "$COMMAND_EXIT_CODE" -ne "0" ];then
    REASONS="unable to generate an archive"
    displayTestCaseResult
    return
  fi

  local output_dir=${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}
  #run the sort job on the dir that has the archive
  cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR jar $HADOOP_EXAMPLES_JAR sort -fs $FS har://${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/${TESTCASE_DESC}/part-m-00000 $output_dir"
  echo $cmd
  eval $cmd
  #if the above command passes then the test passes
  COMMAND_EXIT_CODE="$?"
  REASONS="sort failed on archived file"
  displayTestCaseResult
}

#Steps:
#Destination is not specified
#1. Create an archive but do not specify the destination directory
#
#Expected:
#Command should fail and appropriate message should be displayed. Bug 4307734
#TC ID=1207949
function test_archive_12
{
  TESTCASE_DESC="archive_12"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command on src_dir and done specify destination
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR  2>&1 | grep 'Invalid usage.'"
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  REASONS="Archive command ran successfuly even when no destination is provided. See bug 4307734"
  displayTestCaseResult
}

#Steps:
#Archive Name is not specified 
#1. Run the archive command and do not specify the archive name
#hadoop archive  -p <parent> <src>* <dest>
#
#Expected:
#The command should fail with the appropriate message
#TC ID=1207950
function test_archive_13
{
  TESTCASE_DESC="archive_13"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir $src_dir $FS

  #run the archive command on src_dir and done specify destination
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR} 2>&1 | grep \"Invalid usage.\""
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  REASONS="Archive command ran successfuly even when no archive name is provided"
  displayTestCaseResult
}

#Steps:
#Source files are specififed using regex
#1. Create an archive using regex to determine the source
#hadoop archive -archiveName temp.har -p <parent> <some_src*> <dest>
#
#Expected:
#All appropriate content should be archived
#TC ID=1207951
function test_archive_14
{
  TESTCASE_DESC="archive_14"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir ${src_dir} $FS
  createFileOnHDFS ${src_dir}/file1.txt $FS
  createFileOnHDFS ${src_dir}/file2.txt $FS
  createFileOnHDFS ${src_dir}/file3.txt $FS
  createFileOnHDFS ${src_dir}/file4.txt $FS

  #run the archive command on src_dir and done specify destination
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC}/file* ${ARCHIVE_TESTS_DEST_DIR} 2>&1"
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  if [ "$COMMAND_EXIT_CODE" -ne "0" ];then
    REASONS="Archive command did not run successfuly"
    displayTestCaseResult
  fi

  #make a ls command on the archive and see whats what
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -ls har://${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har"
  echo $cmd
  output=$($cmd)
  cmd="echo $output | grep \"${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/file1.txt.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/file2.txt.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/file3.txt.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/file4.txt\""
  echo $cmd
  eval $cmd

  COMMAND_EXIT_CODE="$?"
  REASONS="Archive command did not archive all the expected file. See bug 4323063"
  displayTestCaseResult
}

#Steps:
#Source directories are specififed using regex
#1. Create an archive using regex to determine the source
#hadoop archive -archiveName temp.har -p <parent> <some_src*> <dest>
#
#Expected:
#All appropriate content should be archived
#TC ID=1207952
function test_archive_15
{
  TESTCASE_DESC="archive_15"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir ${src_dir}_a $FS
  createHdfsDir ${src_dir}_b $FS
  createHdfsDir ${src_dir}_c $FS
  createHdfsDir ${src_dir}_d $FS

  #run the archive command on src_dir and done specify destination
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC}_* ${ARCHIVE_TESTS_DEST_DIR} 2>&1"
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  if [ "$COMMAND_EXIT_CODE" -ne "0" ];then
    REASONS="Archive command did not run successfuly"
    displayTestCaseResult
  fi

  #make a ls command on the archive and see whats what
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -ls har://${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har"
  echo $cmd
  output=$($cmd)
  echo $output | grep "${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/${TESTCASE_DESC}_a.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/${TESTCASE_DESC}_b.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/${TESTCASE_DESC}_c.*${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/${TESTCASE_DESC}_d"

  COMMAND_EXIT_CODE="$?"
  REASONS="Archive command did not archive all the expected directories"
  displayTestCaseResult
}

#Steps:
#Source is a file
#1. Create an archive using regex to determine the source
#hadoop archive -archiveName temp.har -p <parent> <some_src*> <dest>
#
#Expected:
#All appropriate content should be archived
#TC ID=1207953
function test_archive_16
{
  TESTCASE_DESC="archive_16"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir ${src_dir} $FS
  createFileOnHDFS ${src_dir}/file.txt $FS

  #run the archive command on src_dir and done specify destination
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC}/file.txt ${ARCHIVE_TESTS_DEST_DIR} 2>&1"
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  if [ "$COMMAND_EXIT_CODE" -ne "0" ];then
    REASONS="Archive command did not run successfuly"
    displayTestCaseResult
  fi

  #make a ls command on the archive and see whats what
  cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR dfs -fs $FS -ls har://${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har"
  echo $cmd
  output=$($cmd)
  cmd="echo $output | grep \"${ARCHIVE_TESTS_DEST_DIR}/${TESTCASE_DESC}.har/file.txt\""
  echo $cmd
  eval $cmd

  COMMAND_EXIT_CODE="$?"
  REASONS="Archive command did not archive all the expected file. See bug 4323063"
  displayTestCaseResult
}

#Steps:
#Invalid Source is specified
#1. Run an archive command and provide a source that does not exist
#
#Expected:
#Appropriate error should be thrown
#TC ID=1207954
function test_archive_18
{
  TESTCASE_DESC="archive_18"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  #local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}

  #run the archive command on src_dir that does not exist
  echo "${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR} 2>&1 | grep \"The resolved paths set is empty.  Please check whether the srcPaths exist, where srcPaths \= \[${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}\]\""
  ${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.har -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR} 2>&1 | grep "The resolved paths set is empty.  Please check whether the srcPaths exist, where srcPaths \= \[${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}\]"
  COMMAND_EXIT_CODE="$?"
  REASONS="Error message: IndexOutOfBoundsException in archives, is not displayed when running the archives command"
  displayTestCaseResult
}

#Steps:
#Invalid name for archives
#1. Create an archive but give a non .har extension
#
#Expected:
#archive command shouild fail
#TC ID=1207955
function test_archive_19
{
  TESTCASE_DESC="archive_19"
  displayTestCaseMessage $TESTCASE_DESC

  #create a dir as a source
  local src_dir=${ARCHIVE_TESTS_SRC_DIR}/${TESTCASE_DESC}
  createHdfsDir ${src_dir} $FS

  #run the archive command on src_dir that does not exist
  local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR archive -fs $FS -archiveName ${TESTCASE_DESC}.zip -p $ARCHIVE_TESTS_SRC_DIR ${TESTCASE_DESC} ${ARCHIVE_TESTS_DEST_DIR} 2>&1 | grep \"Invalid name for archives. ${TESTCASE_DESC}.zip\""
  echo $cmd
  eval $cmd
  COMMAND_EXIT_CODE="$?"
  REASONS="Error message: Invalid name for archives. ${TESTCASE_DESC}.zip, is not displayed when running the archives command"
  displayTestCaseResult
}

#####################################################################################################
#MAIN 
#####################################################################################################
OWNER=arpitg

#setup the tests
setup

#execute all tests that start with test_
executeTests

#teardown any unwanted info
teardown
