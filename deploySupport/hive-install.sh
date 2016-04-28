# script to install hive server and client, along with supporting
# packages and settings. This is meant to be executed on the hive
# server node.
#
# The hive installation relies on keytabs which are generated in
# the Build and Configure jobs.
#
# inputs: cluster being installed, reference cluster 
# outputs: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name, and reference cluster"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

HIVENODE=`hostname`
HIVENODE_SHORT=`echo $HIVENODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Hive node being installed: $HIVENODE"

#
# install the backing mysql db
#
# pkgs, should get everything we need from mysql_server
#yinst i yjava_jdk
yinst install mysql_server
#yinst install mysql_client

# settings, the duplication is likely unnecessary, there were versions of hive that
# installed either mysql_config or mysql_config_multi, it appears that current hive
# releases have settled on mysql_config_multi so in the future we can remove the sets
# for mysql_config 
yinst set mysql_config_multi.read_only=off
yinst set mysql_config.read_only=off
yinst set mysql_config_multi.binlog_format=ROW
yinst set mysql_config.binlog_format=ROW
yinst set mysql_config_multi.skip_name_resolve=UNDEF
yinst set mysql_config.skip_name_resolve=UNDEF

yinst restart mysql_server 

# kinit as dfsload, the dfsload keytab should already be there from the Configure job
kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

# mysql server config script
echo "CREATE USER 'hive'@'$HIVENODE' IDENTIFIED BY 'dbpassword';" > /tmp/sql_setup.sql
echo "CREATE DATABASE hivemetastoredb DEFAULT CHARACTER SET latin1 DEFAULT COLLATE latin1_swedish_ci;" >> /tmp/sql_setup.sql
echo "GRANT ALL PRIVILEGES ON hivemetastoredb.* TO 'hive'@'$HIVENODE' WITH GRANT OPTION;" >> /tmp/sql_setup.sql
echo "flush privileges;" >> /tmp/sql_setup.sql

# apply sql script to DB
mysql -u root < sql_setup.sql

# install supporting packages
yinst install hbase
yinst install cloud_messaging_client -branch current
yinst install yjava_oracle_jdbc_wrappers -branch test


#
## install hive
#

# check if we need to use a reference cluster, else use 'current'
echo "STACK_COMP_REFERENCE_CLUSTER is: $REFERENCE_CLUSTER"
if [ "$REFERENCE_CLUSTER" == "none" ]; then
  PACKAGE_VERSION_HIVE=`yinst package -br current hive`
  PACKAGE_VERSION_HIVE_CONF=`yinst package -br current hive_conf`
  PACKAGE_VERSION_HCAT_SERVER=`yinst package -br current hcat_server`
else
  yinst i hadoop_releases_utils
  RC=$?
  if [ $RC -ne 0 ]; then
    echo "Error: failed to install hadoop_releases_utils on $HIVENODE!"
    exit 1
  fi
  PACKAGE_VERSION_HIVE=hive-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hive`
  PACKAGE_VERSION_HIVE_CONF=hive_conf-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hive_conf_${REFERENCE_CLUSTER}`
  PACKAGE_VERSION_HCAT_SERVER=hcat_server-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hcat_server`
fi

yinst i $PACKAGE_VERSION_HIVE
yinst i $PACKAGE_VERSION_HIVE_CONF
yinst i $PACKAGE_VERSION_HCAT_SERVER


# hive yinst sets
yinst set hcat_server.HADOOP_CONF_DIR=/home/gs/conf/current
yinst set hcat_server.HADOOP_HEAPSIZE_MB=1000
yinst set hcat_server.HADOOP_HOME=/home/gs/hadoop/current
yinst set hcat_server.JAVA_HOME=/home/gs/java/jdk
yinst set hcat_server.database_connect_url=jdbc:mysql://$HIVENODE:3306/hivemetastoredb?createDatabaseIfNotExist=true
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
yinst set hcat_server.jdbc_driver=com.mysql.jdbc.Driver
yinst set hcat_server.keydb_passkey=dbpassword

#
## install pig
#

# check if we need to use a reference cluster, else use 'current'
echo "STACK_COMP_REFERENCE_CLUSTER is: $REFERENCE_CLUSTER"
if [ "$REFERENCE_CLUSTER" == "none" ]; then
  PACKAGE_VERSION_PIG=`yinst package -br current pig  | cut -d' ' -f1`
else
  PACKAGE_VERSION_PIG=pig-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b pig -p pig_current`
fi

yinst install $PACKAGE_VERSION_PIG
RC=$?
if [ $RC -ne 0 ]; then
  echo "Error: failed to install $PACKAGE_VERSION_PIG on $PIGNODE!"
  exit 1
fi

yinst set pig.PIG_HOME=/home/y/share/pig

#
# make the grid links for pig
PIGVERSION=`yinst ls | grep pig-`
echo PIGVERSION=$PIGVERSION

yinst install ygrid_pig_multi -br current -set ygrid_pig_multi.CURRENT=$PIGVERSION -same -live


yinst restart hcat_server

#
# create hive warehouse path for gdm db
#
echo "Creating path \"/user/hive/warehouse/gdm.db/user1\""
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse/gdm.db/user1
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 777 /user/hive/warehouse/gdm.db/user1

