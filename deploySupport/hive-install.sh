# script to install hive server and client, along with supporting
# packages and settings. This is meant to be executed on the hive
# server node.
#
# The hive installation relies on keytabs which are generated in
# the Build and Configure jobs.
#
# inputs: cluster being installed, reference cluster name 
# outputs: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name, and reference cluster"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2
# GRIDCI-2587 Fetching the hive oracle DB host from the roles db

# This is a design limitation currently. The DB server name in the oracle db hosts contain the first 8 characters
# of the cluster name. For example if the cluster name is openqe55blue, the DB server name will have 'OPENQE55' in
# its name. So if we are trying to delpoy openqe110blue, the DB server name will be 'OPENQE11' which will conflict
# with openqe11blue. So for the time being we are not supporting deployment of hive on VM clusters with number more
# than 99.
CLUSTER_NUM=`echo "${CLUSTER}"|sed 's/[^0-9]*//g'`
LENGTH_CLUSTER_STRING=${#CLUSTER_NUM}
if [[ $LENGTH_CLUSTER_STRING > 2 ]]; then
  echo "Tring to deploy hive for a VM cluster which is not supported currently!"
  echo "ERROR: Cannot install hive on $CLUSTER because the cluster number id > 100 and not supported!"
  exit -1
fi

export JAVA_HOME="/home/gs/java8/jdk64/current"
export PARTITIONHOME=/home
export GSHOME=$PARTITIONHOME/gs
export yroothome=$GSHOME/gridre/yroot.$CLUSTER
export HADOOP_HDFS_HOME=${yroothome}/share/hadoop
export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop
export YARN_HOME=${yroothome}/share/hadoop
export HADOOP_COMMON_HOME=${yroothome}/share/hadoop
export HADOOP_PREFIX=${yroothome}/share/hadoop

# GRIDCI-2587 Fetching the hive oracle DB host from the roles db
HIVE_DB_NODE=""
get_hive_oradb_server() {
  host_name=$1
  SHORT_CLUSTER=`echo $CLUSTER | cut -c1-8`
  ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $host_name "ps aux|grep ora_pmon_|cut -d'_' -f3 | tr [A-Z] [a-z]|grep -w $SHORT_CLUSTER"
  if [ $? == 0 ]; then
    HIVE_DB_NODE=$host_name
    echo "$HIVE_DB_NODE"
  fi
}

for host_name in `yinst range -ir "(@grid_re.clusters.flubber_oradb_servers)"`; do
    echo "***********************************************************************"
    echo "Checking if host $host_name is the DB server for the cluster $CLUSTER..."
    export HIVE_DB_NODE=$(get_hive_oradb_server $host_name)
    if [ "${HIVE_DB_NODE}" == "" ]; then
        echo "$host_name is not the DB for the cluster $CLUSTER!"
        continue
    else
        echo "The hive oracle db server for $CLUSTER is: $HIVE_DB_NODE"
        echo "***********************************************************************"
        break
    fi
done

if [ "${HIVE_DB_NODE}" == "" ]; then
  echo "***********************************************************************"
  echo "WARNING: There is no active DB in any server for the cluster $CLUSTER!"
  echo "Finding the oracle db server based on the active DBs in that host..."
  for host_name in `yinst range -ir "(@grid_re.clusters.flubber_oradb_servers)"`; do
      final_count=`ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $host_name "ps aux|grep pmon|grep -v grep| wc -l"`
      echo "Number of active DBs in $host_name is $final_count"
      if [[ $final_count < 12 ]]; then
          export HIVE_DB_NODE=$host_name
          echo "The hive oracle db server for $CLUSTER is: $HIVE_DB_NODE"
          echo "Please configure the oracle DB in this host for hive to work!!!"
          echo "***********************************************************************"
          break
      fi
  done
  # If all the 3 DB hosts have 12 DB servers already running on them, then fail the job with an error
  # message saying create a new oracle DB host.
  if [ "${HIVE_DB_NODE}" == "" ]; then
    echo "WARNING: All the oracle DB hosts are full. Please configure a new host with DB servers!"
    echo "ERROR: Failed to install hive!"
    exit -1
  fi
fi

# oracle db support vars
# use the oradb SID associated with this target cluster
export HIVE_DB=`echo $CLUSTER | cut -c 1-8 | tr [a-z] [A-Z]`
echo "DEBUG: our ora db is: $HIVE_DB"

export PATH=$PATH:/home/y/lib64/ora11gclient/
export ORACLE_HOME=/home/y/lib64/ora11gclient/
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH


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
yinst install hadoopqa_headless_keys ports/json-c-0.11.3
yinst install ora11gclient-1.0.3 

# make sure we support hybrid mode for legacy keydb calls
yinst set ykeydb.run_mode=YKEYKEY_HYBRID_MODE

# kinit as dfsload, the dfsload keytab should already be there from the Configure job
kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

# switched to using oracle per gridci-2434 
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "+   Now using oracle DB on $HIVE_DB_NODE                " 
echo "+   using SID $HIVE_DB                                  "
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

# gridci-1937 allow installing from current branch
if [[ "$REFERENCE_CLUSTER" == "current" ]]; then

  yinst i -same -live -downgrade -branch current  hive  hive_conf  hcat_server

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
yinst set hcat_server.HADOOP_CONF_DIR=/home/gs/conf/current
yinst set hcat_server.HADOOP_HEAPSIZE_MB=1000
yinst set hcat_server.HADOOP_HOME=/home/gs/hadoop/current
yinst set hcat_server.JAVA_HOME=/home/gs/java/jdk
# yinst set hcat_server.database_connect_url=jdbc:mysql://$HIVENODE:3306/hivemetastoredb?createDatabaseIfNotExist=true
yinst set hcat_server.database_connect_url="jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = $HIVE_DB_NODE)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = $HIVE_DB)))"
yinst set hcat_server.database_user=hive
yinst set hcat_server.hcat_server_client_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM
yinst set hcat_server.hcat_server_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM
yinst set hcat_server.hcat_server_keytab_file=/etc/grid-keytabs/hadoopqa.$HIVENODE_SHORT.keytab
yinst set hcat_server.hcat_server_user=hadoopqa
yinst set hcat_server.metastore_uris=thrift://$HIVENODE:9080
#yinst set hive.metastore_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM
yinst set hive.metastore_uris=thrift://$HIVENODE:9080/
yinst set hive.metastore_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM
yinst set hive_conf.metastore_uris=thrift://$HIVENODE:9080/
yinst set hive_conf.metastore_kerberos_principal=hadoopqa/$HIVENODE@DEV.YGRID.YAHOO.COM

# stop using keydb package and directly use hcat_server hive-site properties
# for mysql metastore access
#
#yinst set hcat_server.jdbc_driver=yjava.database.jdbc.mysql.KeyDbDriverWrapper
#yinst install hcat_dbaccess-0.0.1.1360014220.T38813-rhel.tgz
#yinst set hcat_server.keydb_passkey=hcatPassword
yinst set hcat_server.jdbc_driver=yjava.database.jdbc.oracle.KeyDbDriverWrapper
yinst set hcat_server.keydb_passkey=hiveqeint

# GRIDCI-2587 The hive metastore setting should be objectstore
yinst set hcat_server.metastore_rawstore_impl=org.apache.hadoop.hive.metastore.ObjectStore

yinst restart hcat_server

#
# create hive warehouse path for gdm db
#
echo "Creating path \"/user/hive/warehouse/gdm.db/user1\""

/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse/gdm.db/user1
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 777 /user/hive/warehouse/gdm.db/user1

