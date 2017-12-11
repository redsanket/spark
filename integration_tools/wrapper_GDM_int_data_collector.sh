#!/bin/bash

export SRCTOP=`pwd`

set -x

# Replace console_host and console_port passed from jenkins
sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CONSOLE_HOST:CONSOLE_PORT/${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

# Replace variables passed from jenkins
sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CLUSTER_NAME/${CLUSTER_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/FREQUENCY/${FREQUENCY}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/DURATION/${DURATION}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PATTERN/${PATTERN}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/OOZIE_HOST_NAME/${OOZIE_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HCAT_HOST_NAME/${HCAT_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PULL_OOZIE_JOB_LENGTH/${PULL_OOZIE_JOB_LENGTH}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_EXECUTION_ENV/${TEST_EXECUTION_ENV}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_HOST/${STAGING_CONSOLE_HOST}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_PORT/${STAGING_CONSOLE_PORT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/NAME_NODE_HOST_NAME/${NAME_NODE_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PIG_HOST_NAME/${PIG_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HBASE_MASTER_HOST_NAME/${HBASE_MASTER_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HBASE_MASTER_PORT/${HBASE_MASTER_PORT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HBASE_REGIONAL_SERVER_PORT/${HBASE_REGIONAL_SERVER_PORT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/GATEWAY_HOST_NAME/${GATEWAY_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HIVE_HOST_NAME/${HIVE_HOST_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_TYPE/${TEST_TYPE}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PIPELINE_NAME/${PIPELINE_NAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HBASE_CLUSTERNAME/${HBASE_CLUSTERNAME}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STACK_COMPONENT_TEST_LIST/${STACK_COMPONENT_TEST_LIST}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml


sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/NO_OF_ITERATION/${NO_OF_ITERATION}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

echo "ClusterName - $CLUSTER_NAME , frequency = $FREQUENCY , duration=$DURATION , dataSetPattern=$PATTERN , oozieHostName=$OOZIE_HOST_NAME , hcatHostName=$HCAT_HOST_NAME , pullOozieJobLength=$PULL_OOZIE_JOB_LENGTH , HIVE_HOST_NAME=$HIVE_HOST_NAME"

# get clusterName
CLUSTER_NAME=`ssh ${GDM_ONE_NODE_HOST} ls -ld /home/gs/gridre/yroot.* | cut -d'.' -f 2 | head -1`
echo ${CLUSTER_NAME}

# check to see if GDM_RUN_REGRESSION is set
if [[ $GDM_RUN_INT_REGRESSION == "true" ]]
then
	${SRCTOP}/hadooptest/scripts/run_hadooptest_remote -c ${CLUSTER_NAME} -s ${SRCTOP}/hadooptest -f /grid/0/tmp/hadooptest-hadoopqa-${CLUSTER_NAME}/hadooptest/conf/hadooptest/hadooptest_gdm_regression_configs/hadooptest.conf  -r ${GDM_ONE_NODE_HOST} ${EXEC_MODE} -gdm -t ${TESTS}
else
	echo "Jenkins param GDM_RUN_INT_REGRESSION is set to false, HTF Regression not invoked"
fi

