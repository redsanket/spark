#!/bin/bash

set -e
set -x

echo "Printing Values..."
echo ${JENKINS_SUFFIX_NAME}
echo ${JENKINS_ITERATION_VALUE}
echo ${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}
echo "======================================"

# Replace console_host and console_port passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/CONSOLE_HOST:CONSOLE_PORT/${GDM_ONE_NODE_HOST}:${GDM_CONSOLE_PORT}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

# Replace deployment-suffix-name passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/MY_SUFFIX/${JENKINS_SUFFIX_NAME}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml


# Replace performance-iteration-value  passed from jenkins
sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/NUMBER_OF_ITERATION/${JENKINS_ITERATION_VALUE}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

sed 's/[$()]//g' hadooptest/htf-common/resources/gdm/conf/config.xml > hadooptest/htf-common/resources/gdm/conf/config.xml.1
sed "s/TEST_EXECUTION_ENV/${TEST_EXECUTION_ENV}/g" hadooptest/htf-common/resources/gdm/conf/config.xml.1 > hadooptest/htf-common/resources/gdm/conf/config.xml

# check to see if GDM_RUN_REGRESSION is set 
if [[ $GDM_RUN_QE_REGRESSION == "true" ]]
then
    ${WORKSPACE}/hadooptest/scripts/run_hadooptest_remote -c ${CLUSTER} -s ${WORKSPACE}/hadooptest -f /homes/hadoopqa/hadooptest_gdm_regression_configs/hadooptest.conf -r ${GDM_ONE_NODE_HOST} ${EXEC_MODE} -gdm -t ${TESTS}
else
    echo "Jenkins param GDM_RUN_QE_REGRESSION is set to false, HTF Regression not invoked"
fi

