#!/bin/bash

set x;


# Replace console_host and console_port passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CONSOLE_HOST:CONSOLE_PORT/${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml


# Replace variables passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CLUSTER_NAME/${CLUSTER_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/FREQUENCY/${FREQUENCY}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/DURATION/${DURATION}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PATTERN/${PATTERN}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/OOZIE_HOST_NAME/${OOZIE_HOST_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/HCAT_HOST_NAME/${HCAT_HOST_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PULL_OOZIE_JOB_LENGTH/${PULL_OOZIE_JOB_LENGTH}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_EXECUTION_ENV/${TEST_EXECUTION_ENV}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/NAME_NODE_HOST_NAME/${NAME_NODE_HOST_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/PIG_HOST_NAME/${PIG_HOST_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml


echo "ClusterName - $CLUSTER_NAME , frequency = $FREQUENCY , duration=$DURATION , dataSetPattern=$PATTERN , oozieHostName=$OOZIE_HOST_NAME , hcatHostName=$HCAT_HOST_NAME , pullOozieJobLength=$PULL_OOZIE_JOB_LENGTH"


# check to see if GDM_RUN_REGRESSION is set 
if [[ $GDM_RUN_INT_REGRESSION == "true" ]]
then
    ${WORKSPACE}/hadooptest/scripts/run_hadooptest_remote -c ${CLUSTER} -s ${WORKSPACE}/hadooptest -f /homes/hadoopqa/hadooptest_gdm_regression_configs/hadooptest.conf -r ${GDM_ONE_NODE_HOST} ${EXEC_MODE} -t ${TESTS}
else
    echo "Jenkins param GDM_RUN_INT_REGRESSION is set to false, HTF Regression not invoked"
fi

