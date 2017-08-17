#!/bin/bash

set -x

# Replace console_host and console_port passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CONSOLE_HOST:CONSOLE_PORT/${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

# Replace variables passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/SOURCE_CLUSTER/${SOURCE_CLUSTER}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/DEST_CLUSTER/${DEST_CLUSTER}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/DURATION/${DURATION}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HOURLY/${HOURLY}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/ENABLE_HCAT/${ENABLE_HCAT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_EXECUTION_ENV/${TEST_EXECUTION_ENV}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_HOST/${STAGING_CONSOLE_HOST}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_PORT/${STAGING_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

# get clusterName
CLUSTER_NAME=`ssh ${GDM_ONE_NODE_HOST} ls -ld /home/gs/gridre/yroot.* | cut -d'.' -f 2 | head -1`
echo ${CLUSTER_NAME}

# get hadoop Version
#HADOOP_VERSION=`ssh  ${GDM_ONE_NODE_HOST} hadoop version  | head -1 | cut -d' ' -f 2 | cut -c1-3`
#echo "HADOOP_VERSION = ${HADOOP_VERSION}"

# check to see if GDM_RUN_REGRESSION is set 
if [[ $GDM_RUN_INT_REGRESSION == "true" ]]
then
#    ${WORKSPACE}/hadooptest/scripts/run_hadooptest_remote -c ${CLUSTER} -s ${WORKSPACE}/hadooptest -f /homes/hadoopqa/hadooptest_gdm_regression_configs/hadooptest.conf -r ${GDM_ONE_NODE_HOST} ${EXEC_MODE} -gdm -t ${TESTS}

${WORKSPACE}/hadooptest/scripts/run_hadooptest_remote -c ${CLUSTER_NAME} -s ${WORKSPACE}/hadooptest -f /grid/0/tmp/hadooptest-hadoopqa-${CLUSTER_NAME}/hadooptest/conf/hadooptest/hadooptest_gdm_regression_configs/hadooptest.conf -r ${GDM_ONE_NODE_HOST} ${EXEC_MODE} -gdm ${HADOOP_VERSION} -t ${TESTS}

else
    echo "Jenkins param GDM_RUN_INT_REGRESSION is set to false, HTF Regression not invoked"
fi
