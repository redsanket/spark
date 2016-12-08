# script to install oozie server, along with supporting packages and 
# settings. This is meant to be executed on the oozie server node.
#
# The oozie installation relies on keytabs which are generated in
# the Build and Configure jobs.
#
# inputs: cluster being installed, reference cluster name 
# outputs: 0 on success

if [ $# -ne 2 ]; then
  echo "ERROR: need the cluster name and reference cluster name"
  exit 1
fi

CLUSTER=$1
REFERENCE_CLUSTER=$2

OOZIENODE=`hostname`
OOZIENODE_SHORT=`echo $OOZIENODE | cut -d'.' -f1`
echo "INFO: Cluster being installed: $CLUSTER"
echo "INFO: Oozie node being installed: $OOZIENODE"

# get the namenode
NAMENODE=`yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode)"|head -1`;

#
# get component versions for oozie's yinst sets
#
HADOOP_VERSION=`yinst ls -root /home/gs/gridre/yroot.$CLUSTER |grep hadoopcoretree | cut -d'-' -f2`
TEZ_VERSION=`ls /home/gs/tez/current/tez-common-*|cut -d'-' -f3|cut -d'.' -f1-5`

TMPFILE="/tmp/yinst_tmp.out$$"
yinst ls|egrep 'hive-|hcat_server|hbase|pig-' > $TMPFILE
PIG_VERSION=`grep pig $TMPFILE | cut -d'-' -f2`
HIVE_VERSION=`grep hive- $TMPFILE | cut -d'-' -f2`
HCAT_VERSION=`grep hcat_server $TMPFILE | cut -d'-' -f2`
HBASE_VERSION=`grep hbase $TMPFILE | cut -d'-' -f2`
echo HADOOP_VERSION $HADOOP_VERSION
echo TEZ_VERSION $TEZ_VERSION
echo PIG_VERSION $PIG_VERSION
echo HIVE_VERSION $HIVE_VERSION
echo HCAT_VERSION $HCAT_VERSION
echo HBASE_VERSION $HBASE_VERSION
rm $TMPFILE

# check that the oozie node's local-superuser-conf.xml is correctly
# setup with doAs users, if not then oozie operations will fail. 
EC=0
for USER in oozie hcat; do
  # check for the given user's hosts entry
  grep "hadoop.proxyuser.$USER.hosts" /home/gs/conf/local/local-superuser-conf.xml
  RC=$?
  if [ $RC -ne 0 ]; then 
    echo "ERROR: local-superuser-conf.xml is missing \"hadoop.proxyuser.$USER.hosts\""
  fi
  EC=$((EC+RC))

  # check for the given user's groups entry
  grep "hadoop.proxyuser.$USER.groups" /home/gs/conf/local/local-superuser-conf.xml
  RC=$?
  if [ $RC -ne 0 ]; then 
    echo "ERROR: local-superuser-conf.xml is missing \"hadoop.proxyuser.$USER.groups\""
  fi
  EC=$((EC+RC))
done

  # if any entries had errors, complain and bail out
  if [ $EC -ne 0 ]; then
    echo "ERROR: oozie node $OOZIENODE /home/gs/conf/local/local-superuser-conf.xml is missing doAs users!"
    echo "       See the section \"Local Node Conf File\" in the Build/Configure Jenkins job's README.md at:"
    echo "       https://git.corp.yahoo.com/HadoopQE/qeopenstackdist/blob/master/README.md"
    exit 1
  else
    echo "INFO: oozie node $OOZIENODE /home/gs/conf/local/local-superuser-conf.xml is correct"
  fi


# kinit as hadoopqa to allow hdfs /user/hadoopqa/.staging setup 
kinit -k -t ~hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM

#
# create sharelib base path for ygrid_sharelib package 
#
echo "Creating path \"/tmp/sharelib/v1/conf\""
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /tmp/sharelib/v1/conf 
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 777 /tmp/sharelib
# put hadoopqa staging back in case it was cahnged
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chown hadoopqa /user/hadoopqa/.staging 


#
# install oozie packages
#
# Note: we need to explicitly use hadoopgplcomplession version listed
# below to ensure we pull the right pkg, else more recent versions 
# exist which will get installed, and we don't want that (legacy from
# 0.23 hadoop)
#
set -x
yinst i ygrid_cacert

#
## install oozie
#

# # debug
# # kinit as dfsload for hdfs /tmp/oozie setup
# kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM
# /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -ls hdfs:/tmp/ygrid_sharelib_dir/

# gridci-924, ygrid_sharelib pkg branches are being fiddled with...
# GRIDCI-1537
yinst i ygrid_sharelib -br nightly

yinst i hadoopgplcompression-1.0.2.2.1209201519
set +x

# check what comp version we need to use
echo "STACK_COMP_VERSION_OOZIE is: $REFERENCE_CLUSTER"

# make sure we have tools to talk to artifactory
yinst i hadoop_releases_utils
RC=$?
if [ "$RC" -ne 0 ]; then
  echo "Error: failed to install hadoop_releases_utils on $OOZIENODE!"
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
  PACKAGE_VERSION_OOZIE=yoozie-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b oozie -p yoozie`
  PACKAGE_VERSION_OOZIE_CLIENT=yoozie_client-`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b oozie -p yoozie_client`
else
  echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT" 
  exit 1
fi

# gridci-1708, add '-br test' to allow pulling dependencies that are on 'test'
yinst i -same -live -downgrade -br test   $PACKAGE_VERSION_OOZIE
yinst i -same -live -downgrade -br test   $PACKAGE_VERSION_OOZIE_CLIENT


#
# apply oozie settings
#
yinst set yjava_jetty.PATH="/bin:/sbin:/usr/bin:/usr/sbin:/home/y/bin:/home/y/sbin:/usr/local/bin:/usr/local/sbin:/usr/X11R6/bin:/home/y/share/yjava_jdk/java/bin:/home/gs/hadoop/current/bin" \
  yjava_jetty.autostart=off \
  yjava_jetty.enable_stathandler=true \
  yjava_jetty.garbage_collection="-XX:NewRatio=8 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime -Xloggc:/home/y/libexec/yjava_jetty/logs/gc.log" \
  yjava_jetty.java_opts="-Doozie.config.dir=/home/y/conf/yoozie -Doozie.home.dir=/home/y/var/yoozie -Duser.timezone=UTC -Djava.security.egd=file:///dev/urandom -XX:PermSize=256m -XX:MaxPermSize=256m -Dlog4j.debug" \
  yjava_jetty.max_heap_size_mb=8192 \
  yjava_jetty.min_heap_size_mb=8192 \
  yjava_jetty.port=4080 \
  yjava_jetty.ports="-port 4080" \
  yjava_jetty.user_name=oozie \
  yjava_jetty.webapps=/home/y/libexec/yjava_jetty/webapps \
  yjava_jetty.http_compressable_mime_type="text/html,text/xml,text/plain,text/css,text/javascript,application/json,application/xml,application/x-javascript"
# log
yinst set yjava_jetty.enable_centralized_logging=false \
  yjava_jetty.logback_root_priority=INFO \
  yjava_jetty.logback_file_appender_file=/home/y/logs/yjava_jetty/server.log \
  yjava_jetty.logback_file_appender_conversion_pattern="%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{35} - %msg%n" \
  yjava_jetty.logback_file_appender_rollover_format="/home/y/logs/yjava_jetty/server.log.%d{yyyy-MM-dd}" \
  yjava_jetty.logback_file_appender_rollover_history=30

yinst set yjava_jdk.HTTP_KEEPALIVE=true \
  yjava_jdk.HTTP_MAXCONNECTIONS=5 \
  yjava_jdk.JAVA_HOME=/home/y/share/yjava_jdk/java \
  yjava_jdk.NETWORKADDRESS_CACHE_NEGATIVE_TTL=10 \
  yjava_jdk.NETWORKADDRESS_CACHE_TTL=120 \
  yjava_jdk.platform=x86_64-rhel4-gcc3

yinst set yoozie.ssl_enable=false \
  yoozie.JAVA_HOME=/home/y/libexec64/java \
  yoozie.oozie_out_log=/home/y/logs/yoozie/oozie.out \
  yoozie.oozie_admin_users=wrkflow,oozie,hadoopqa \
  yoozie.oozie_logs_dir=/home/y/libexec/yjava_jetty/logs/oozie \
  yoozie.INJECT_EXT_JS_LIBPATH=/home/y/lib/extjs/extjs-2.2 \
  yoozie.PATH="/bin:/sbin:/usr/bin:/usr/sbin:/home/y/bin:/home/y/sbin:/usr/local/bin:/usr/local/sbin:/usr/X11R6/bin:/home/y/share/yjava_jdk/java/bin:/home/gs/hadoop/current/bin:." \
  yoozie.log_log4j_logger_org_apache_oozie="ALL,oozie,oozieError"

yinst set yoozie.INJECT_JARS_HCATALOG="/home/y/libexec/hive/lib/hive-common.jar,/home/y/libexec/hive/lib/hive-serde.jar,/home/y/libexec/hive/lib/hive-exec.jar,/home/y/libexec/hive/lib/hive-metastore.jar,/home/y/libexec/hive/lib/hive-hcatalog-core.jar,/home/y/libexec/hive/lib/libfb303.jar,/home/y/libexec/hive/lib/hive-webhcat-java-client.jar,/home/y/libexec/hive/lib/hive-hcatalog-server-extensions.jar,/home/y/libexec/cloud_messaging_client/lib/cloud-messaging-client-thin.jar,/home/y/libexec/cloud_messaging_client/lib/jul-to-slf4j-1.7.5.jar,/home/y/libexec/cloud_messaging_client/lib/jackson-jaxrs-1.9.13.jar,/home/y/libexec/cloud_messaging_client/lib/jersey-client-2.8.jar,/home/y/libexec/cloud_messaging_client/lib/cloud-messaging-common-0.1.jar" \
  yoozie.INJECT_JARS_YCA=/home/y/lib/jars/yjava_yca.jar

yinst set yoozie.HADOOP_CONF_DIR=/home/gs/gridre/yroot.$CLUSTER/conf/hadoop/ \
  yoozie.HADOOP_PREFIX=/home/gs/gridre/yroot.$CLUSTER/share/hadoop/ \
  yoozie.INJECT_CONF_HADOOP="/home/gs/gridre/yroot.$CLUSTER/conf/hadoop/core-site.xml,/home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-site.xml,/home/gs/gridre/yroot.$CLUSTER/conf/hadoop/mapred-site.xml,/home/gs/gridre/yroot.$CLUSTER/conf/hadoop/yarn-site.xml" \
  yoozie.INJECT_JARS_HADOOP="/home/y/lib/hadoopgplcompression/hadoop-gpl-compression.jar,/home/gs/gridre/yroot.$CLUSTER/share/hadoop/share/hadoop/common/lib/jersey*.jar,/home/gs/gridre/yroot.$CLUSTER/share/hadoop/share/hadoop/common/lib/jetty-util*.jar,/home/gs/gridre/yroot.$CLUSTER/share/hadoop/share/hadoop/common/lib/netty*.jar,/home/gs/gridre/yroot.$CLUSTER/share/hadoop/share/hadoop/common/lib/htrace-core*.jar,/home/gs/gridre/yroot.$CLUSTER/share/hadoop/share/hadoop/common/lib/jackson-xc-*.jar" \
  yoozie.INJECT_CONF_HCATALOG=/home/y/libexec/hive/conf/hive-site.xml \
  yoozie.HADOOP_VERSION=2.0

yinst set yoozie.conf_oozie_authentication_kerberos_name_rules="RULE:[2:$1@$0](.*@DS.CORP.YAHOO.COM)s/@.*// RULE:[1:$1@$0](.*@DS.CORP.YAHOO.COM)s/@.*// RULE:[2:$1@$0](.*@Y.CORP.YAHOO.COM)s/@.*// RULE:[1:$1@$0](.*@Y.CORP.YAHOO.COM)s/@.*// RULE:[2:$1@$0]([jt]t@.*DEV.YGRID.YAHOO.COM)s/.*/mapredqa/ RULE:[2:$1@$0]([nd]n@.*DEV.YGRID.YAHOO.COM)s/.*/hdfsqa/ RULE:[2:$1@$0](mapred@.*DEV.YGRID.YAHOO.COM)s/.*/mapredqa/ RULE:[2:$1@$0](hdfs@.*DEV.YGRID.YAHOO.COM)s/.*/hdfsqa/ RULE:[2:$1@$0](mapredqa@.*YGRID.YAHOO.COM)s/.*/mapredqa/ RULE:[2:$1@$0](hdfsqa@.*YGRID.YAHOO.COM)s/.*/hdfsqa/ DEFAULT"

yinst set yoozie.conf_oozie_authentication_kerberos_principal=HTTP/$OOZIENODE@DEV.YGRID.YAHOO.COM \
  yoozie.conf_oozie_service_HadoopAccessorService_nameNode_whitelist= \
  yoozie.conf_oozie_service_HadoopAccessorService_jobTracker_whitelist=

yinst set yoozie.JDBC_PASSWORD= yoozie.JDBC_USER= \
  yoozie.conf_oozie_service_JPAService_jdbc_driver=org.hsqldb.jdbcDriver \
  yoozie.conf_oozie_service_JPAService_jdbc_url=jdbc:hsqldb:mem:oozie-db\;create=true \
  yoozie.conf_oozie_service_JPAService_create_db_schema=true \
  yoozie.conf_oozie_authentication_signature_secret=oozie

yinst set yoozie.CLUSTER_NAME=$CLUSTER \
   yoozie.DEFAULT_FS=hdfs://$NAMENODE \
  yoozie.OOZIE_USER=oozie \
  yoozie.KERBEROS_REALM=DEV.YGRID.YAHOO.COM

yinst set yoozie.conf_oozie_base_url=http://$OOZIENODE:4080/oozie \
  yoozie.conf_oozie_service_HadoopAccessorService_keytab_file=/etc/grid-keytabs/oozie.$OOZIENODE_SHORT.service.keytab


### replace USERNAME with the username that will submit jobs
yinst set yoozie.conf_oozie_service_ProxyUserService_proxyuser_USERNAME_groups=*
yinst set yoozie.conf_oozie_service_ProxyUserService_proxyuser_USERNAME_hosts=*

# Set proxy user for hueadmin
yinst set yoozie.conf_oozie_service_ProxyUserService_proxyuser_hueadmin_groups=*
yinst set yoozie.conf_oozie_service_ProxyUserService_proxyuser_hueadmin_hosts=*

yinst set yoozie.conf_oozie_service_HadoopAccessorService_kerberos_principal="%OOZIE_USER%/%YINST_HOSTNAME%@%KERBEROS_REALM%" \
  yoozie.conf_oozie_service_WorkflowAppService_system_libpath="%DEFAULT_FS%/tmp/oozie/systemlib/%YINST_HOSTNAME%" \
  yoozie.conf_oozie_system_id=%CLUSTER_NAME% \
  yoozie.conf_oozie_action_launcher_yarn_timeline-service_enabled=true \
  yoozie.conf_oozie_zookeeper_secure=false

yinst set yoozie.conf_oozie_services_ext="org.apache.oozie.service.PartitionDependencyManagerService,org.apache.oozie.service.JMSAccessorService,org.apache.oozie.service.HCatAccessorService,org.apache.oozie.service.JMSTopicService,org.apache.oozie.service.EventHandlerService,org.apache.oozie.sla.service.SLAService,org.apache.oozie.service.AbandonedCoordCheckerService"

##
# needed to allow oozie UI to bouncer auth correctly now
##
yinst set ykeydb.run_mode=YKEYKEY_HYBRID_MODE

##
### sharelib 
##
### *** never re-generate oozie's keytab ***
yinst set yoozie.shared_libs_headless_user=oozie \
  yoozie.shared_libs_headless_user_keytab=/homes/oozie/oozie.dev.headless.keytab \
  yoozie.conf_oozie_action_ship_launcher_jar=true \
  yoozie.conf_oozie_service_ShareLibService_mapping_file=hdfs:///tmp/sharelib/v1/conf/metafile

yinst set ygrid_sharelib.metafile_hdfs_path=hdfs:///tmp/sharelib/v1/conf \
  ygrid_sharelib.sharelib_dir=hdfs:///tmp/ygrid_sharelib_dir \
  ygrid_sharelib.sharelib_headless_user=hadoopqa \
  ygrid_sharelib.sharelib_headless_user_keytab=/home/hadoopqa/hadoopqa.dev.headless.keytab
##
yinst set ygrid_sharelib.pkg_hadoop=hadoopcoretree-$HADOOP_VERSION \
  ygrid_sharelib.pkg_pig=pig-$PIG_VERSION \
  ygrid_sharelib.pkg_hive=hive-$HIVE_VERSION \
  ygrid_sharelib.pkg_hcat=hcat_common-$HCAT_VERSION \
  ygrid_sharelib.pkg_ytez=ytez-$TEZ_VERSION \
  ygrid_sharelib.pkg_hbase=hbase-$HBASE_VERSION
##
## sharelib tags: distcp, streaming, hcat_current, hive_current, pig_current, hbase_current, tez_current
##
yinst set ygrid_sharelib.oozie_tag_distcp=hdfs:///tmp/ygrid_sharelib_dir/hadoop/hadoopcoretree-$HADOOP_VERSION/share/hadoop/share/hadoop/tools/lib/hadoop-distcp-$HADOOP_VERSION.jar \
  ygrid_sharelib.oozie_tag_streaming=hdfs:///tmp/ygrid_sharelib_dir/hadoop/hadoopcoretree-$HADOOP_VERSION/share/hadoop/share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar
##
yinst set ygrid_sharelib.oozie_tag_hcat_current=hdfs:///tmp/ygrid_sharelib_dir/hcat/hcat_common-$HCAT_VERSION/libexec/hive/lib,hdfs:///tmp/ygrid_sharelib_dir/hive/hive-$HIVE_VERSION/libexec/hive/lib \
  ygrid_sharelib.oozie_tag_hcat_latest=hdfs:///tmp/ygrid_sharelib_dir/hcat/hcat_common-$HCAT_VERSION/libexec/hive/lib,hdfs:///tmp/ygrid_sharelib_dir/hive/hive-$HIVE_VERSION/libexec/hive/lib \
  ygrid_sharelib.oozie_tag_hcat_05=hdfs:///tmp/ygrid_sharelib_dir/hcat/hcat_common-$HCAT_VERSION/libexec/hive/lib,hdfs:///tmp/ygrid_sharelib_dir/hive/hive-$HIVE_VERSION/libexec/hive/lib
##
yinst set ygrid_sharelib.oozie_tag_hive_current=hdfs:///tmp/ygrid_sharelib_dir/hive/hive-$HIVE_VERSION/libexec/hive/lib \
  ygrid_sharelib.oozie_tag_hive_latest=hdfs:///tmp/ygrid_sharelib_dir/hive/hive-$HIVE_VERSION/libexec/hive/lib \
  ygrid_sharelib.oozie_tag_hive_10=hdfs:///tmp/ygrid_sharelib_dir/hive/hive-$HIVE_VERSION/libexec/hive/lib
##
yinst set ygrid_sharelib.oozie_tag_pig_current=hdfs:///tmp/ygrid_sharelib_dir/pig/pig-$PIG_VERSION/share/pig/lib \
  ygrid_sharelib.oozie_tag_pig_latest=hdfs:///tmp/ygrid_sharelib_dir/pig/pig-$PIG_VERSION/share/pig/lib \
  ygrid_sharelib.oozie_tag_pig_10=hdfs:///tmp/ygrid_sharelib_dir/pig/pig-$PIG_VERSION/share/pig/lib \
  ygrid_sharelib.oozie_tag_pig_11=hdfs:///tmp/ygrid_sharelib_dir/pig/pig-$PIG_VERSION/share/pig/lib
##
yinst set ygrid_sharelib.oozie_tag_tez_current=hdfs:///tmp/ygrid_sharelib_dir/ytez/ytez-$TEZ_VERSION/libexec/tez
##
yinst set ygrid_sharelib.oozie_tag_hbase_current=hdfs:///tmp/ygrid_sharelib_dir/hbase/hbase-$HBASE_VERSION/libexec/hbase/lib \
  ygrid_sharelib.oozie_tag_hbase_latest=hdfs:///tmp/ygrid_sharelib_dir/hbase/hbase-$HBASE_VERSION/libexec/hbase/lib \
  ygrid_sharelib.oozie_tag_hbase_94=hdfs:///tmp/ygrid_sharelib_dir/hbase/hbase-$HBASE_VERSION/libexec/hbase/lib

##
### if sharelib will not be used, then turn off ShareLib,
##
yinst set yoozie.conf_oozie_service_ShareLibService_fail_fast_on_startup=true \
  yoozie.conf_oozie_use_system_libpath=true

##
### hive client settings
##
#yinst set hive.metastore_uris=thrift://openqe53blue-n4.blue.ygrid.yahoo.com:9080/
#yinst set hive.metastore_kerberos_principal=hadoopqa/openqe53blue-n4.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
yinst set hive.tez_version=$TEZ_VERSION

# 
# start oozie server
#
# this takes a while
yinst restart ygrid_sharelib
yinst restart yoozie


# kinit as dfsload for hdfs /tmp/oozie setup
kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

#
# create oozie tmp path and chmod to 777 in order to allow hadoopqa to submit jobs 
# and oozie user to restart
#
echo "Going to create and chmod hdfs /tmp/oozie to 777 so hadoopqa and oozie users can access"
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /tmp/oozie
EC=$?
if [ $EC -ne 0 ]; then echo "Failed to mkdir /tmp/oozie!"; fi
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chown  hadoopqa /tmp/oozie
RC=$?
EC=$((EC+RC))
if [ $RC -ne 0 ]; then echo "Failed to chown /tmp/oozie!"; fi
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 777 /tmp/oozie
RC=$?
EC=$((EC+RC))
if [ $RC -ne 0 ]; then echo "Failed to chmod /tmp/oozie!"; fi
# open /user/hive perms for multiple users
/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 777 /user/hive 
RC=$?
EC=$((EC+RC))
if [ $RC -ne 0 ]; then echo "Failed to chmod /user/hive!"; fi
if [ $EC -ne 0 ]; then
  echo "ERROR: hdfs create or chmod paths failed!" 
  exit 1
fi

# open /user/hive perms for multiple users

