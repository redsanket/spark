#!/bin/bash

ssh ${TEST_CLUSTER_HOST}.blue.ygrid.yahoo.com "mkdir /tmp/ha_test_path"
scp * ${TEST_CLUSTER_HOST}.blue.ygrid.yahoo.com:/tmp/ha_test_path/

ssh ${TEST_CLUSTER_HOST}.blue.ygrid.yahoo.com "/tmp/ha_test_path/base_tests_generic.sh ${CLUSTER}"
