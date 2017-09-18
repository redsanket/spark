#!/bin/bash

export SRCTOP=`pwd`

set -x

# Replace console_host and console_port passed from jenkins
sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CONSOLE_HOST:CONSOLE_PORT/${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

# Replace variables passed from jenkins
sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/SOURCE_CLUSTER/${SOURCE_CLUSTER}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/DEST_CLUSTER/${DEST_CLUSTER}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/DURATION/${DURATION}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HOURLY/${HOURLY}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/ENABLE_HCAT/${ENABLE_HCAT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_EXECUTION_ENV/${TEST_EXECUTION_ENV}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_HOST/${STAGING_CONSOLE_HOST}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_PORT/${STAGING_CONSOLE_PORT}/g" ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml.1 > ${SRCTOP}/hadooptest/htf-common/resources/gdm/conf/config.xml

# get clusterName
CLUSTER_NAME=`ssh ${GDM_ONE_NODE_HOST} ls -ld /home/gs/gridre/yroot.* | cut -d'.' -f 2 | head -1`
echo ${CLUSTER_NAME}


# check to see if GDM_RUN_REGRESSION is set 
if [[ $GDM_RUN_INT_REGRESSION == "true" ]]
then
	${SRCTOP}/hadooptest/scripts/run_hadooptest_remote -c ${CLUSTER_NAME} -s ${SRCTOP}/hadooptest -f /grid/0/tmp/hadooptest-hadoopqa-${CLUSTER_NAME}/hadooptest/conf/hadooptest/hadooptest_gdm_regression_configs/hadooptest.conf -r ${GDM_ONE_NODE_HOST} ${EXEC_MODE} -gdm -t ${TESTS}
else
    echo "Jenkins param GDM_RUN_INT_REGRESSION is set to false, HTF Regression not invoked"
fi
