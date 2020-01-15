# script to install hive server and client, along with supporting
# packages and settings. This is meant to be executed on the hive
# server node.
#
# The hive installation relies on keytabs which are generated in
# the Build and Configure jobs.
#
# inputs: cluster being installed, reference cluster name
# outputs: 0 on success

#
# gridci-4488, migrate to supporting rhel7 installs
# gridci-4502, migrate to using hosted Oracle DB for Hive backing store
#

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name, and reference cluster"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

export JAVA_HOME="/home/gs/java/jdk64/current"
export PARTITIONHOME=/home
export GSHOME=$PARTITIONHOME/gs
export yroothome=$GSHOME/gridre/yroot.$CLUSTER
export HADOOP_HDFS_HOME=${yroothome}/share/hadoop
export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop
export YARN_HOME=${yroothome}/share/hadoop
export HADOOP_COMMON_HOME=${yroothome}/share/hadoop
export HADOOP_PREFIX=${yroothome}/share/hadoop

# oracle db support vars

# use cluster name to derive hive's backing oracle DB from the hosted instance
# as long as the cluster name pattern holds, this works to get name in format:
#    openqe<NUMERIC_ID>
#
# This requires the hosted oracle to have a user name created in that format,
# request this by filing jira with ORACLE project, after that setup the instance
# using Hive team's scripts, see following doc for steps to do this:
#  https://docs.google.com/document/d/1DqwmwVZFawM2gGGIw39NQTwUXVZvW39BcPchs8jtmFc/

export HIVE_DB=`echo $CLUSTER | cut -d'b' -f1`
echo "DEBUG: our ora db is: $HIVE_DB"

export ORACLE_HOME=/home/y/lib64/Oracle18c_Client
export LD_LIBRARY_PATH=/home/y/lib64/Oracle18c_Client:$LD_LIBRARY_PATH
export TNS_ADMIN=/home/y/lib64/Oracle18c_Client
export PATH=$ORACLE_HOME:$PATH


HIVENODE=`hostname`
HIVENODE_SHORT=`echo $HIVENODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Hive node being installed: $HIVENODE"

#
# install the backing oracle DB client
#
# this needs the headless keys pkg in order to fetch the ora DB
# key 'hiveqeint' from ykeykey
#
# need to add json-c needed for athens zts
yinst install hadoopqa_headless_keys
yinst install Oracle18c_Client-1.0.9

# make sure we support hybrid mode for legacy keydb calls
yinst set ykeydb.run_mode=YKEYKEY_HYBRID_MODE

# kinit as dfsload, the dfsload keytab should already be there from the Configure job
kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

# switched to using oracle hosted DB per gridci-4502
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "+   Using Oracle user $HIVE_DB                                  "
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

# install supporting packages
yinst install hbase
yinst install cloud_messaging_client -branch current
yinst install yjava_oracle_jdbc_wrappers -branch test


#
## install hive
#

# check what comp version we need to use
echo "STACK_COMP_VERSION_HIVE is using: $REFERENCE_CLUSTER"

# make sure we don't have AR conflicting conf pkg
yinst remove -live hive_conf_axonitered

# gridci-1937 allow installing from current branch
if [[ "$REFERENCE_CLUSTER" == "current" ]]; then

  # gridci-4421, only yjava_yca pkg for rhel7 is on test
  OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
  if [[ "$OS_VER" =~ ^7. ]]; then
    echo "OS is $OS_VER, using packages from test explicitly"
    yinst i -same -live -downgrade -branch test  hive-1.2.5.5.1812031910   hive_conf-1.2.5.5.1812031910   hcat_server-1.2.5.5.1812031910
  else
    echo "OS is $OS_VER, using yjava_yca from current as an implicit dependency"
    yinst i -same -live -downgrade -branch current  hive  hive_conf  hcat_server
  fi

# else use artifactory
else

  # make sure we have tools to talk to artifactory
  yinst i hadoop_releases_utils
  RC=$?
  if [ "$RC" -ne 0 ]; then
    echo "Error: failed to install hadoop_releases_utils on $HIVENODE!"
    exit 1
  fi

  # check we got a valid reference cluster
  RESULT=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER`
  RC=$?
  if [ $RC -eq 0 ]; then
    # get Artifactory URI and log it
    ARTI_URI=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER  -v | grep downloadUri |cut -d\' -f4`
    echo "Artifactory URI with most recent versions:"
    echo $ARTI_URI

    # get component version to use from Artifactory
    HIVE_VERSION=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hive`
    PACKAGE_VERSION_HIVE="hive-${HIVE_VERSION}"

    # gridci-4421, only yjava_yca pkg for rhel7 is on test
    OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
    if [[ "$OS_VER" =~ ^7. ]]; then
      echo "OS is $OS_VER, using yjava_yca from test explicitly"
      yinst i yjava_yca -br test
    else
      echo "OS is $OS_VER, using yjava_yca from current as an implicit dependency"
    fi

    # GRIDCI-1525
    if [ "$REFERENCE_CLUSTER" == "LATEST" ]; then
        echo "Use the same version of hive_conf as hive for reference cluster '${REFERENCE_CLUSTER}': '${HIVE_VERSION}'"
        PACKAGE_VERSION_HIVE_CONF="hive_conf-${HIVE_VERSION}"

    else
        PACKAGE_VERSION_HIVE_CONF=hive_conf_${REFERENCE_CLUSTER}-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hive_conf_${REFERENCE_CLUSTER}`
    fi

    PACKAGE_VERSION_HCAT_SERVER=hcat_server-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hcat_server`
  else
    echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT"
    exit 1
  fi

  yinst i -same -live -downgrade -branch quarantine  $PACKAGE_VERSION_HIVE $PACKAGE_VERSION_HIVE_CONF $PACKAGE_VERSION_HCAT_SERVER
fi

# copy the hive-site.xml to hdfs
echo "Copying the hive confs to sharelib"
HIVE_CONF_PACKAGE=`echo $PACKAGE_VERSION_HIVE_CONF | cut -d'-' -f1`
HIVE_CONF_VERSION=`echo $PACKAGE_VERSION_HIVE_CONF | cut -d'-' -f2`

/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /sharelib/v1/$HIVE_CONF_PACKAGE/$HIVE_CONF_PACKAGE-$HIVE_CONF_VERSION/libexec/hive/conf/
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/y/libexec/hive/conf/hive-site.xml /sharelib/v1/$HIVE_CONF_PACKAGE/$HIVE_CONF_PACKAGE-$HIVE_CONF_VERSION/libexec/hive/conf/
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/v1/$HIVE_CONF_PACKAGE/

# copy the hive-site.xml from hcat to hdfs
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /sharelib/v1/hive_conf/libexec/hive/conf/
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/y/libexec/hcat_server/conf/hive-site.xml /sharelib/v1/hive_conf/libexec/hive/conf/
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/v1/hive_conf/

# hive yinst sets
yinst set hcat_server.HADOOP_CONF_DIR=/home/gs/conf/current \
           hcat_server.HADOOP_HEAPSIZE_MB=1000 \
           hcat_server.HADOOP_HOME=/home/gs/hadoop/current \
           hcat_server.JAVA_HOME=/home/gs/java/jdk \
           hcat_server.conf_hive_metastore_thrift_sasl_qop=authentication,integrity,privacy \
           hcat_server.database_connect_url="jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = gq1-griddev-c1.blue.ygrid.yahoo.com)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = gqgdm01n.blue.ygrid.yahoo.com)))" \
           hcat_server.database_user=$HIVE_DB \
           hcat_server.hcat_server_client_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM \
           hcat_server.hcat_server_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM \
           hcat_server.hcat_server_keytab_file=/etc/grid-keytabs/hadoopqa.$HIVENODE_SHORT.keytab \
           hcat_server.hcat_server_user=hadoopqa \
           hcat_server.metastore_uris=thrift://$HIVENODE:9080 \
           hive.metastore_uris=thrift://$HIVENODE:9080/ \
           hive.metastore_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM \
           hive_conf.metastore_uris=thrift://$HIVENODE:9080/ \
           hive_conf.metastore_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM \
           hcat_server.jdbc_driver=yjava.database.jdbc.oracle.KeyDbDriverWrapper \
           hcat_server.keydb_passkey=hiveqeint
#yinst set hive.metastore_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM

# GRIDCI-2587 The hive metastore setting should be objectstore
yinst set hcat_server.metastore_rawstore_impl=org.apache.hadoop.hive.metastore.ObjectStore

yinst restart hcat_server
