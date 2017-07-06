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
# gridci-1973, install dba passwd pkg from S1 jira
yinst install mysql_dba_privileges mysql_server
#yinst install mysql_client

# make sure keydb file is readable by hadoopqa
MYSQLROOT_KEYDBFILE="/home/y/conf/keydb/mysqlroot.keydb"
if ! [ -r "$MYSQLROOT_KEYDBFILE" ]; then
  echo "Error!"
  echo "mysql_server requires a readable $MYSQLROOT_KEYDBFILE file!"
  echo "Please make sure $MYSQLROOT_KEYDBFILE has at least read permission!"
  echo "Error!"
  exit 1
fi 

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
# gridci-1973, add passwd field to fetch from keydb
mysql -A -u root -p`/home/y/bin64/keydbgetkey mysqlroot`  < /tmp/sql_setup.sql

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

        # gridci-2227, oozie's ssl changes cause a ykeydb and mysql_secure conflict, need to pull mysql_secure from test
        # error from yinst:  mysql_secure-2.2.6 [active] requires ykeydb (2.0.0.19 <= ver <= 2.0.9999.9999)
        echo "Installing mysql_secure from test branch, needed by ykeydb dependency..."
        yinst i mysql_secure -br test
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

yinst restart hcat_server

#
# create hive warehouse path for gdm db
#
echo "Creating path \"/user/hive/warehouse/gdm.db/user1\""
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse/gdm.db/user1
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 777 /user/hive/warehouse/gdm.db/user1

