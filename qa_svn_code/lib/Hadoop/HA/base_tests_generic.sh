# Base runner script for HA functional tests, this relies on a set of scripts called
# base_tests_2* which each exec a set of node-environment and hadoop-specific version 
# cases. 
#
# Ran after a clean 2.2.x deploy with ENABLE_HA=true and the following setups:
#   ha_updateAllNodes.sh
#   ha_restartAllNodes.sh
#   ha_failover_local.sh
#
# This needs two non-cluster nodes from which to run cmds and submit jobs to the
# cluster under test. For now, we are using:
#   for another 2.2.x node; argentum node gsbl90674
#   for another 0.23 node;  omegap1 node gsbl90338
# As long as the hadoop versions are met, these can be hosts from any other clusters 
# but they need to reliably be up and available.
#
# Run from one of the test cluster's nodes
# 20131207phw

# check inputs
if [ $# -ne 1 ]; then
  echo "Error: need the test cluster's name"
  exit 1
fi
CLUSTER=$1

# igor proxy for dist 
IGOR_PROXY=`yinst which-dist`

# target clusters
MYCLUSTER=$CLUSTER
OTHER_2x_CLUSTER=argentum
OTHER_23_CLUSTER=omegap1
# target execution hosts, we rsync the test files to these hosts to run tests from
HA1HOST=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.namenode|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`
HA1HOSTSHORT=`echo $HA1HOST | cut -d '.' -f1`
HA2HOST=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.namenode2|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`
HA2HOSTSHORT=`echo $HA2HOST | cut -d '.' -f1`
RMHOST=`curl -s --get $IGOR_PROXY/igor/api/getRoleMembers?role=grid_re.clusters.$CLUSTER.jobtracker|grep blue.ygrid.yahoo.com|cut --delimiter=\'  -f2`
OTHER_2x_HOST=gsbl90674
OTHER_23_HOST=gsbl90338

# kinit as hdfs priv user
/usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM

# check if HA is enabled on cluster under test
HA1_NNALIAS=`ssh $HA1HOST "grep -A1 dfs.nameservices  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-ha.xml | grep flubber-alias104|cut -d '>' -f2|cut -d '<' -f1"`
if [ -z "$HA1_NNALIAS" ]; then
  echo "Error, HA is not enabled on $HA1HOST"
  exit 1
fi
HA2_NNALIAS=`ssh $HA2HOST "grep -A1 dfs.nameservices  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-ha.xml | grep flubber-alias104|cut -d '>' -f2|cut -d '<' -f1"`
if [ -z "$HA2_NNALIAS" ]; then
  echo "Error, HA is not enabled on $HA2HOST"
  exit 1
fi
if [ "$HA1_NNALIAS" != "$HA2_NNALIAS" ]; then
  echo "Error, HA aliases for HA1 and HA2 do not match"
  exit 1
fi
echo "Info, cluster HA alias is: $HA1_NNALIAS"

TESTRUNLOGS=/tmp/ha_test_run_logs
if [ -d "$TESTRUNLOGS" ]; then
  rm $TESTRUNLOGS/*
else
  mkdir -p $TESTRUNLOGS
fi


#############
## Function setup_and_run_test_on_host
#
#setup a remote host to run the tests
# expect to get: host short name, test file, cluster name
#############
function setup_and_run_test_on_host {
  REMOTEHOST=$1
  TESTFILE=$2
  TESTFILENAME=`echo $TESTFILE |cut -d '/' -f7 | cut -d '.' -f1`
  CLUSTERTOUSE=$3
  PASS=$4
  echo "Setting up test: REMOTEHOST=$REMOTEHOST"
  echo "TESTFILENAME=$TESTFILENAME"
  echo "CLUSTERTOUSE=$CLUSTERTOUSE"

  # make a tmp dir to drop files into
  ssh $REMOTEHOST "mkdir -p /tmp/ha_test_tmp/"
  if [ $? -ne 0 ]; then
    echo "Error, failed to make remote dir"
    exit 1
  fi

  # cp the testfile to the other node for this other node to exec tests
  rsync $TESTFILE $REMOTEHOST:/tmp/ha_test_tmp/
  if [ $? -ne 0 ]; then
    echo "Error, failed to rsync test file to remote dir"
    exit 1
  fi

  # exec the tests on the remote node
  ssh $REMOTEHOST "$TESTFILE $CLUSTERTOUSE $HA1_NNALIAS $HA1HOSTSHORT $HA2HOSTSHORT  &> /tmp/ha_test_tmp/$TESTFILENAME.$REMOTEHOST.$PASS.out"
  if [ $? -ne 0 ]; then
    echo "Error, failed to execute $TESTFILENAME on $REMOTEHOST" 
    exit 1
  fi
 
  # pull the result logs back
  scp $REMOTEHOST:/tmp/ha_test_tmp/$TESTFILENAME.$REMOTEHOST.$PASS.out $TESTRUNLOGS/.
  if [ $? -ne 0 ]; then
    echo "Error, failed to collect run logs from $REMOTEHOST" 
    exit 1
  fi

  # cleanup
  ssh $REMOTEHOST "rm  /tmp/ha_test_tmp/*"
  ssh $REMOTEHOST "rmdir  /tmp/ha_test_tmp"
  echo "Info, cleanup on remote host $REMOTEHOST"
}


##################
## Main test entry
##################
echo "Start the run..."

# make sure ha1 is active and ha2 standby
/homes/patwhite/work/HadoopHA/cluster_generic/ha_failover_local.sh  $CLUSTER ha1

####
## Gen some hdfs data and run a few jobs
####
QUEUE1=grideng
QUEUE2=default
QUEUE3=grideng
QUEUE4=default

# 10 input files, use all queues 
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop   jar /homes/patwhite/work/GenFilesInHdfs/GenFilesInHdfs.jar GenFilesInHdfs 10 $QUEUE1
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop   jar /homes/patwhite/work/GenFilesInHdfs/GenFilesInHdfs.jar GenFilesInHdfs 10 $QUEUE2


####
## First pass - from initial start, check base cases from all nodes...
####
# ha1
echo "Run Testcases from HA1, HA1 is Active:"
setup_and_run_test_on_host $HA1HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_ClusterNode.sh $MYCLUSTER pass1

# ha2
echo "Run Testcases from HA2, HA1 is Active:"
setup_and_run_test_on_host $HA2HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_ClusterNode.sh $MYCLUSTER pass1

# RM
echo "Run Testcases from the Resource Manager, HA1 is Active:"
setup_and_run_test_on_host $RMHOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_ClusterNode.sh $MYCLUSTER pass1

# non-cluster node, 2.2.x
echo "Run Testcases from another 2.2.x Non-Cluster node, HA1 is Active:"
setup_and_run_test_on_host $OTHER_2x_HOST  /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_NonClusterNode.sh $OTHER_2x_CLUSTER pass1

# non-cluster node, 0.23.9.x
echo "Run Testcases from 0.23.9 node, HA1 is Active:"
setup_and_run_test_on_host $OTHER_23_HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_23_Interop.sh $OTHER_23_CLUSTER pass1


####
## Second pass - failover from ha1 to ha2, check base cases from all nodes...
####
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Failover from HA1 to HA2..."
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
/homes/patwhite/work/HadoopHA/cluster_generic/ha_failover_local.sh  $CLUSTER ha2

# ha1
echo "Run Testcases from HA1, HA2 is Active:"
setup_and_run_test_on_host $HA1HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha2Active_ClusterNode.sh $MYCLUSTER pass2

# ha2
echo "Run Testcases from HA2, HA2 is Active:"
setup_and_run_test_on_host $HA2HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha2Active_ClusterNode.sh $MYCLUSTER pass2

# RM
echo "Run Testcases from the Resource Manager, HA2 is Active:"
setup_and_run_test_on_host $RMHOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha2Active_ClusterNode.sh $MYCLUSTER pass2

# non-cluster node, 2.2.x
echo "Run Testcases from another 2.2.x Non-Cluster node, HA2 is Active:"
setup_and_run_test_on_host $OTHER_2x_HOST  /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha2Active_NonClusterNode.sh $OTHER_2x_CLUSTER pass2

# non-cluster node, 0.23.9.x
echo "Run Testcases from 0.23.9 node, HA2 is Active:"
setup_and_run_test_on_host $OTHER_23_HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_23_Interop.sh $OTHER_23_CLUSTER pass2


####
## Third pass - failover from ha2 back to ha1, check base cases from all nodes...
####
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Failover back from HA2 to HA1..."
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
/homes/patwhite/work/HadoopHA/cluster_generic/ha_failover_local.sh  $CLUSTER ha1

echo "Run Testcases from HA1, HA1 is Active again:"
setup_and_run_test_on_host $HA1HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_ClusterNode.sh $MYCLUSTER pass3

# ha2
echo "Run Testcases from HA2, HA1 is Active again:"
setup_and_run_test_on_host $HA2HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_ClusterNode.sh $MYCLUSTER pass3

# RM
echo "Run Testcases from the Resource Manager, HA1 is Active again:"
setup_and_run_test_on_host $RMHOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_ClusterNode.sh $MYCLUSTER pass3

# non-cluster node, 2.2.x
echo "Run Testcases from another 2.2.x Non-Cluster node, HA1 is Active again:"
setup_and_run_test_on_host $OTHER_2x_HOST  /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_2dotX_ha1Active_NonClusterNode.sh $OTHER_2x_CLUSTER pass3

# non-cluster node, 0.23.9.x
echo "Run Testcases from 0.23.9 node, HA1 is Active again:"
setup_and_run_test_on_host $OTHER_23_HOST /homes/patwhite/work/HadoopHA/cluster_generic/base_tests_23_Interop.sh $OTHER_23_CLUSTER pass3


####
## Wrap up
####
TOTAL=`grep Testcase /tmp/ha_test_run_logs/* |wc -l`
PASSED=`grep Testcase /tmp/ha_test_run_logs/*|cut -d ':' -f3|grep pass|wc -l`
FAILED=`grep Testcase /tmp/ha_test_run_logs/*|cut -d ':' -f3|grep fail |wc -l`
echo "Result logs are at: `hostname`:$TESTRUNLOGS"
echo ""
echo "Tests Ran:"
grep Testcase /tmp/ha_test_run_logs/*|cut -d ':' -f2,3
echo ""
echo "Results: total tests ran: $TOTAL"
echo "  passed: $PASSED"
echo "  failed: $FAILED"

