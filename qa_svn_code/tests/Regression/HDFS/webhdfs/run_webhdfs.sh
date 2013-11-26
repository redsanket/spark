#!/bin/sh

# Script for starting the webhdfs tests. Requires MVN_HOME, WORKSPACE, VERSION, CLUSTER, and ARTIFACTS_DIR to be defind.
# It should be run as the user hadoopqa.
# E.g.:
# $ MVN_HOME=/home/y/libexec/maven WORKSPACE=/homes/hadoopqa/svn/HadoopQEAutomation/branch-23 VERSION=23 CLUSTER=theoden ARTIFACTS_DIR=/tmp ./run_webhdfs.sh
# $ MVN_HOME=/home/y/libexec/maven WORKSPACE=/homes/hadoopqa/svn/HadoopQEAutomation/branch-23 VERSION=23 CLUSTER=theoden ARTIFACTS_DIR=/tmp ./run_webhdfs.sh -Dtest=org.hw.webhdfs.TestCreate

TEST_ARG=$1

# load the library file
HADOOP_QA_ROOT=`ls -ltr {/home,/grid/0}/gs/gridre/yroot.${CLUSTER}/share/{hadoop-current,hadoopcommon,hadoop}/bin/hadoop 2> /dev/null | sort | tail -1 | awk '{print $9}' | sed -e 's/\(.*\)\/gs.*$/\1/'`
if [ "${HADOOP_QA_ROOT}" == "/home" ];then
    HADOOP_LOG_DIR="${HADOOP_QA_ROOT}/gs/var/log"
else
    HADOOP_LOG_DIR="${HADOOP_QA_ROOT}/hadoop/var/log"
fi
. $WORKSPACE/conf/hadoop/common_hadoop_env.sh $CLUSTER $WORKSPACE $ARTIFACTS_DIR $HADOOP_QA_ROOT $HADOOP_LOG_DIR
. $WORKSPACE/lib/library.sh

######################
# teardown webhdfs
#####################
function teardown
{
  if [ "$IS_WEBHDFS_ENABLED" == "true" ]
  then
    return
  fi

  #restart the dn and nn with the new conf
  resetDataNodes "stop"
  resetNameNode "stop"
  sleep 5
  resetDataNodes "start"
  resetNameNode "start"
  takeNNOutOfSafemode
}

#############################################
#SETUP cluster for webhdfs
#############################################
function setup_webhdfs
{
  #read the conf
  local conf_file="${CONF_DIR}/hdfs-site.xml"
  local nn=$(getNameNode)
  local dns=$(getDataNodes)
  dns=`echo $dns | tr ';' ' '`
  local nodes="$nn $dns"

  #restart the cluster with the webhdfs turned on
  property="dfs.webhdfs.enabled"
  property_value="true"
 
  access_time_precision="dfs.access.time.precision"
  access_time_precision_val="3600000"


  for node in $nodes
  do
    #copy the config
    copyHadoopConfigOnHost $node "$HADOOPQA_USER" "" "$CONF_DIR"
    #update the property for webhdfs if its not enabled
    if [ "true" != "$IS_WEBHDFS_ENABLED" ]; then
      addPropertyToXMLConfOnHost "$node" "$HADOOPQA_USER" "" "$conf_file" "$property" "$property_value"
    fi
    #update the property for precision time if its not enabled
    if [ "0" == "$ACCESS_TIME_PRECISION" ]; then
      addPropertyToXMLConfOnHost "$node" "$HADOOPQA_USER" "" "$conf_file" "$access_time_precision" "$access_time_precision_val"
    fi
  done

  #restart the dn and nn with the new conf
  resetNameNode "stop"
  resetDataNodes "stop"
  sleep 5
  resetNameNode "start" "$CONF_DIR"
  resetDataNodes "start" "$CONF_DIR"
  takeNNOutOfSafemode "" "$CONF_DIR"
}

#function to run the basic tests with webhds as the source
function setup
{
  if [ "$IS_WEBHDFS_ENABLED" != "true" ] || [ "0" == "$ACCESS_TIME_PRECISION" ]
  then
    CONF_DIR="${TMPDIR}/webhdfs-`date +%s`"
    setup_webhdfs
  else
    CONF_DIR=$HADOOP_CONF_DIR
  fi
}

############ MAIN ##################
#get the webhdfs property
IS_WEBHDFS_ENABLED=$(getValueFromField "${HADOOP_CONF_DIR}/hdfs-site.xml" 'dfs.webhdfs.enabled')
#determine the access time precision
ACCESS_TIME_PRECISION=$(getValueFromField "${HADOOP_CONF_DIR}/hdfs-site.xml" 'dfs.access.time.precision')

setup

echo "workspace=$WORKSPACE"
echo "cluster=$CLUSTER"
echo "version=$VERSION"
echo "conf_dir=$CONF_DIR"
echo "artifacts=$ARTIFACTS_DIR"

cmd="rm -rf ${ARTIFACTS_DIR}/webhdfs"
echo $cmd
eval $cmd

#copy the src to the artifacts dir and exectue it
#this could be a problem for copying large size of test data
TS_PATH="tests/Regression/HDFS/webhdfs"
cmd="cp -r ${WORKSPACE}/${TS_PATH} ${ARTIFACTS_DIR}/.;cd ${ARTIFACTS_DIR}/webhdfs"
echo $cmd
eval $cmd

#populate the conf dir in the pom.xml
pom_file="${ARTIFACTS_DIR}/webhdfs/pom.xml"

#run the sed cmd
cmd="sed -i 's|TODO_CONF_DIR|$HADOOP_CONF_DIR|g' $pom_file"
echo $cmd
eval $cmd
eval $cmd

#populate the properties file
# prop_file="${ARTIFACTS_DIR}/webhdfs/webhdfs.properties"
# #USER
# cmd="sed -i \"s|^USER=.*|USER=$HADOOPQA_USER|g\" $prop_file"
# echo $cmd
# eval $cmd
# cmd="sed -i \"s|^USER_KEYTAB_FILE=.*|USER_KEYTAB_FILE=${KEYTAB_FILES_DIR}/${HADOOPQA_USER}${KEYTAB_FILE_NAME_SUFFIX}|g\" $prop_file"
# echo $cmd
# eval $cmd
# 
# #USER2
# cmd="sed -i \"s|^USER2=.*|USER2=$HADOOP1_USER|g\" $prop_file"
# echo $cmd
# eval $cmd
# cmd="sed -i \"s|^USER2_KEYTAB_FILE=.*|USER2_KEYTAB_FILE=${KEYTAB_FILES_DIR}/${HADOOP1_USER}${KEYTAB_FILE_NAME_SUFFIX}|g\" $prop_file"
# echo $cmd
# eval $cmd
# 
# #HDFS SUPER USER
# cmd="sed -i \"s|^HADOOP_SUPER_USER=.*|HADOOP_SUPER_USER=$HDFSQA_USER|g\" $prop_file"
# echo $cmd
# eval $cmd
# cmd="sed -i \"s|^HADOOP_SUPER_USER_KEYTAB_FILE=.*|HADOOP_SUPER_USER_KEYTAB_FILE=${KEYTAB_FILES_DIR}/${HDFSQA_USER}${KEYTAB_FILE_NAME_SUFFIX}|g\" $prop_file"
# echo $cmd
# eval $cmd

#Where to find kerberos
cmd="sed -i \"s|^KINIT_CMD=.*|KINIT_CMD=$KINIT_CMD|g\" $prop_file"
echo $cmd
eval $cmd
cmd="sed -i \"s|^KLIST_CMD=.*|KLIST_CMD=$KLIST_CMD|g\" $prop_file"
echo $cmd
eval $cmd
cmd="sed -i \"s|^KDESTROY_CMD=.*|KDESTROY_CMD=$KDESTROY_CMD|g\" $prop_file"
echo $cmd
eval $cmd

# cmd="${MVN_HOME}/bin/mvn clean test -Dtest=org.hw.webhdfs.TestCreate -DHADOOP_VERSION=${VERSION} | tee ${ARTIFACTS_DIR}/mvn.out"
# cmd="${MVN_HOME}/bin/mvn clean test -DHADOOP_VERSION=${VERSION} | tee ${ARTIFACTS_DIR}/mvn.out"
# cmd="${MVN_HOME}/bin/mvn -debug clean test -DHADOOP_VERSION=${VERSION} | tee ${ARTIFACTS_DIR}/mvn.out"
cmd="${MVN_HOME}/bin/mvn clean test"
if [[ -n $TEST_ARG ]]; then
    cmd="$cmd $TEST_ARG"
fi
cmd="$cmd -DHADOOP_VERSION=${VERSION} | tee ${ARTIFACTS_DIR}/mvn.out"

echo $cmd
eval $cmd

teardown

#CALC TC_RAN TC_PASS TC_FAIL TC_FAIL_NAMES
tc_ran=`cat ${ARTIFACTS_DIR}/mvn.out | grep -v 'Tests run.*Time elapsed' | grep 'Tests run:' | awk '{print \$3}' | cut -d',' -f1`
tc_failed=`cat ${ARTIFACTS_DIR}/mvn.out | grep -v 'Tests run.*Time elapsed' | grep 'Tests run:' | awk '{print \$5}' | cut -d',' -f1`
tc_error=`cat ${ARTIFACTS_DIR}/mvn.out | grep -v 'Tests run.*Time elapsed' | grep 'Tests run:' | awk '{print \$7}' | cut -d',' -f1`
tc_skipped=`cat ${ARTIFACTS_DIR}/mvn.out | grep -v 'Tests run.*Time elapsed' | grep 'Tests run:' | awk '{print \$9}' | cut -d',' -f1`
(( tc_ran = $tc_ran - $tc_skipped ))
(( tc_fail = $tc_failed + $tc_error ))
(( tc_pass = $tc_ran - $tc_fail ))
export TC_RAN=$tc_ran
export TC_PASS=$tc_pass
export TC_FAIL=$tc_fail

#log the resutls
#TODO: function is missing in library.sh
#logTestSuiteResult
