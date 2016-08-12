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

# check what comp version we need to use
echo "STACK_COMP_VERSION_HIVE is using: $REFERENCE_CLUSTER"

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
  PACKAGE_VERSION_HIVE=hive-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hive`
  PACKAGE_VERSION_HIVE_CONF=hive_conf-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hive_conf_${REFERENCE_CLUSTER}`
  PACKAGE_VERSION_HCAT_SERVER=hcat_server-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b hive -p hcat_server`
else
  echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT" 
  exit 1
fi

yinst i -same -live -downgrade  $PACKAGE_VERSION_HIVE
yinst i -same -live -downgrade  $PACKAGE_VERSION_HIVE_CONF
yinst i -same -live -downgrade  $PACKAGE_VERSION_HCAT_SERVER


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
# installs pig on HIVENODE since hive requires it, note that
# this is a different pig install than selected from jenkins,
# pig from jenkins selection install on gateway and may be a
# different version if a different cluster is selected by the
# user 
#
echo "Install pig on $HIVENODE since hive requires it, note that"
echo "this is different than the Jenkins pig selection, which installs"
echo "pig on gateway and may be a different version if a different"
echo "cluster is selected" 

${WORKSPACE}/deploySupport/pig-install-check.sh $CLUSTER $REFERENCE_CLUSTER
RC=$?
if [ $RC -ne 0 ]; then
  echo "Error: pig-install-check.sh reported fail!!" 
  exit 1
fi


yinst restart hcat_server

#
# create hive warehouse path for gdm db
#
echo "Creating path \"/user/hive/warehouse/gdm.db/user1\""
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse/gdm.db/user1
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 777 /user/hive/warehouse/gdm.db/user1

