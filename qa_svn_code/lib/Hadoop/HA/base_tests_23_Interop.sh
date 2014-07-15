# run base cases on 0.23.9.x, confirm .23 can talk to 2.2.x HA 
# This needs to run from a 0.23 cluster's node, the goal of these tests are to try cross-cluster,
# cross-version commands to the cluster under test. 
# init cond: either ha1 or ha2 can be Active 

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


# kinit as hdfs priv user
/usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM

# report hadoop version
echo "My hadoop version is:"
VERSION=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop version` 
echo $VERSION"\n"

# N/A: launch job
# /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop   jar /homes/patwhite/work/GenFilesInHdfs/GenFilesInHdfs.jar GenFilesInHdfs 4 default

# N/A since this defaults to other cluster's hdfs:  fsshell protocol check - default 
#/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls /tmp/GenFilesInHdfs|wc -l

# fsshell protocol check - webhdfs using alias
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls webhdfs://$NN_ALIAS/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsWebhdfsAlias: "
if [ "$RESULT" -gt "5" ]; then
  echo "pass" 
else
  echo "fail"
fi


# fsshell protocol check - hdfs using alias (should fail due to RPC mismatch)
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$NN_ALIAS/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsAlias (should fail): "
if [ "$RESULT" -gt "5" ]; then
  echo "fail" 
else
  echo "pass"
fi


# fsshell protocol check - hdfs long (should fail due to RPC mismatch), try ha1
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$TESTCLUSTER_HA1.blue.ygrid.yahoo.com/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsLongname1 (should fail): "
if [ "$RESULT" -gt "5" ]; then
  echo "fail" 
else
  echo "pass"
fi

# fsshell protocol check - hdfs long (should fail due to RPC mismatch), try ha2
RESULT=`/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop    dfs -ls hdfs://$TESTCLUSTER_HA2.blue.ygrid.yahoo.com/tmp/GenFilesInHdfs|wc -l`
echo -ne  "Testcase fsshell_LsHdfsLongname2 (should fail): "
if [ "$RESULT" -gt "5" ]; then
  echo "fail" 
else
  echo "pass"
fi


