# run base cases on 2.2.x, use cluster under test ($CLUSTER) 
# This needs to run from 2.2.x cluster's node, the goal of these tests are to try inter-cluster commands 
# to the cluster under test. 
# init cond: ha1 is Active, ha2 is Standby

# check inputs
# we need to get:
#   the other node's cluster name, to reference hadoop cmds
#   the nn alias for cluster being tested, this is the nn alias URI the other node needs to talk to
#   the physical hostname of cluster under test ha1 
#   the physical hostname of cluster under test ha2 
if [ $# -ne 4 ]; then
  echo "Error: need the cluster name, nn_alias, ha1 hostname, ha2 hostname" 
  exit 1
fi
CLUSTER=$1
NN_ALIAS=$2
TESTCLUSTER_HA1=$3
TESTCLUSTER_HA2=$4

# report hadoop version
echo "My hadoop version is:"
VERSION=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop version`
echo $VERSION"\n"

# kinit as hdfs priv user
/usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM

# launch job
echo -ne  "Testcase submitJob_ha1ActiveCluster: "
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop   jar /homes/patwhite/work/GenFilesInHdfs/GenFilesInHdfs.jar GenFilesInHdfs 4 default &> /tmp/myjob.tmp
# wait for job to finish
sleep 30
JOBID=`grep proxy/application_ /tmp/myjob.tmp |cut -d '/' -f7`
echo JOBID: $JOBID
`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  job -list all|grep $JOBID|grep SUCCEED` 
RESULT=$?
if [ "$RESULT" -ne "0" ]; then
  echo "pass"
else
  echo "fail"
fi


# fsshell protocol check - default
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls /tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsDefault: "
if [ "$RESULT" -gt "5" ]; then
  echo "pass" 
else
  echo "fail"
fi

# fsshell protocol check - webhdfs using alias
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls webhdfs://$NN_ALIAS/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsWebhdfsAlias: "
if [ "$RESULT" -gt "5" ]; then
  echo "pass" 
else
  echo "fail"
fi

# fsshell protocol check - hdfs using alias
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$NN_ALIAS/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsAlias: "
if [ "$RESULT" -gt "5" ]; then
  echo "pass" 
else
  echo "fail"
fi


# fsshell protocol check - hdfs short
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$TESTCLUSTER_HA1/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsShortname: "
if [ "$RESULT" -gt "5" ]; then
  echo "pass" 
else
  echo "fail"
fi


# fsshell protocol check - hdfs long
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$TESTCLUSTER_HA1.blue.ygrid.yahoo.com/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsLongname: "
if [ "$RESULT" -gt "5" ]; then
  echo "pass" 
else
  echo "fail"
fi


# fsshell protocol check - standby host (should fail)
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$TESTCLUSTER_HA2.blue.ygrid.yahoo.com/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsToStandby (should fail): "
if [ "$RESULT" -gt "5" ]; then
  echo "fail" 
else
  echo "pass"
fi

