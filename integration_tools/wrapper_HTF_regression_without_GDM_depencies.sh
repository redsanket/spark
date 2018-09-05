#!/bin/bash

set x

# Replace console_host and console_port passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CONSOLE_HOST:CONSOLE_PORT/${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

# Replace deployment-suffix-name passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/MY_SUFFIX/${JENKINS_SUFFIX_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_EXECUTION_ENV/${TEST_EXECUTION_ENV}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_HOST/${STAGING_CONSOLE_HOST}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_PORT/${STAGING_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/STAGING_CONSOLE_PORT/${STAGING_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

# get clusterName
CLUSTER_NAME=`ssh ${GDM_ONE_NODE_HOST} ls -ld /home/gs/gridre/yroot.* | cut -d'.' -f 2 | head -1`
echo ${CLUSTER_NAME}

# get hadoop Version
HADOOP_VERSION=`ssh  ${GDM_ONE_NODE_HOST} hadoop version  | head -1 | cut -d' ' -f 2 | cut -c1-3`
echo "HADOOP_VERSION = ${HADOOP_VERSION}"

# check to see if GDM_RUN_REGRESSION is set 
if [[ $GDM_RUN_QE_REGRESSION == "true" ]]
then
   echo "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
   echo `pwd`
   echo "${WORKSPACE}/hadooptest/scripts/run_hadooptest  -c ${CLUSTER_NAME}  -f /homes/hadoopqa/hadooptest_gdm_regression_configs/hadooptest.conf  -m -w  `pwd`   -hadoop${HADOOP_VERSION}  -t ${TESTS}" 
   echo "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
   ${WORKSPACE}/hadooptest/scripts/run_hadooptest  -c ${CLUSTER_NAME}   -m -w  `pwd`   -hadoop${HADOOP_VERSION}  -t ${TESTS} 
   echo "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
else
    echo "Jenkins param GDM_RUN_QE_REGRESSION is set to false, HTF Regression not invoked"
fi

