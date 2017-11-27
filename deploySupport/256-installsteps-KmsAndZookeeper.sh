# script to install KMS server and ZooKeeper, along with supporting
# packages and settings. This is meant to be executed on the KMS
# server node.
#
# Background, the Hadoop Key Management Service (KMS) is used to setup
# Encryption Zones in HDFS, and then manage control through secured keys,
# internally we use ykeykey as the key manager. Users can then use this
# service and associated keys to create and use secured directories in 
# HDFS (Encryption Zones) to transparently encrypt/decrypt hdfs data
# as it is read/written. In community this feature is optional, however
# internally it is to always be enabled on prod deploys, hence it will 
# be deployed by default. If the RoelsDB 'kms' role does not exist for
# cluster however, we will skip installing KMS and continue, this is 
# needed because all existing clusters will fail deploy without a major
# roles update, which is risky.
#
# KMS requires ZooKeeper, so ZK is installed by this script as well as
# a single quorum (node) instance. Both KMS and ZK are generic services
# that don't have to be tethered to a given cluster, however in flubber
# the instances will be configured specifically for their associated
# cluster, since flubber is dev/qe test env, this allows for isolation
# of instance to minimize cross-cluster impacts.
#
# Note that KMS/ZK is being installed differently than most Core s/w, it's
# installed outside of the yinst root. This was necessary because a required
# component, ykeykeyd, has a required dependency (ycron) that mandates
# being installed on the node's base /home/y path. The yinst root installation
# appears to succeed but ykeykeyd install is incorrect (outside the root) and
# fails at runtime trying to find keystores because it assumes /home/y base 
# but KMS is actually yinst root based.
#
# The installation relies on keytabs which are generated in Config
# Configure job, it also needs JKS certs for https/ssl connections
# which are distributed as part of this (Core deploy) job. It's very
# important to note that any hadoop client's jdk needs these ssl certs
# installed otherwise it will in various ways during kms server contact.
#
# inputs: cluster being installed
# outputs: 0 on success

echo "================= Install KeyManagementService (KMS) and ZooKeeper ================="

SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"
ADM_HOST=${ADM_HOST:="devadm102.blue.ygrid.yahoo.com"}

kmsnodeshort=`echo $kmsnode | cut -d'.' -f1`

CONF_KMS="/home/y/conf/kms"
CONF_ZK="/home/y/conf/zookeeper"

# if kms role is not populated, warn but don't fail to allow for
# most existing clusters that weren't built with kms support roles,
# just continue deployment 
if [ -z "$kmsnode" ]; then
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo "+                                                          +"
  echo "+     No KMS role or node defined!!                        +"
  echo "+                                                          +"
  echo "+     Not installing KMS, please be sure KMS role exists   +"
  echo "+     and add node if KMS support is required!!            +"
  echo "+                                                          +"
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
  return
else
  echo "INFO: KMS node is: $kmsnodeshort"
fi


# need the kgp from hadoopqa_headless_keys to find the KMS test key, hitusr_4
# Note: the json-c hack is needed because zts-client, athens_utils and rdl_cpp have
# conflicting deps on this pkg 
# REMOVE json-c hack for rhel7
# add RPMs for ykeykey support, needed by hadoopqa_headless_keys
# use br test for hadoopqa_headless_keys to allow rhel7 ykeykey pkgs to get pulled in

# need json-c, mdbm and cronolog as RPMs now for ykeykey pkgs
$SSH $ADM_HOST "sudo $SSH $kmsnode \"sudo yum -y install json-c mdbm cronolog \""
if [ $? -ne 0 ]; then 
  echo "Error: node $kmsnode failed yinst install of mdbm and cronolog RPMs for hadoopqa_headless_keys support!"
  exit 1
fi

$SSH $ADM_HOST "sudo $SSH $kmsnode \"yinst i -br test hadoopqa_headless_keys \""
if [ $? -ne 0 ]; then 
  echo "Error: node $kmsnode failed yinst install of hadoopqa_headless_keys!"
  exit 1
fi

# create and make accessible conf dir for kms
$SSH $ADM_HOST "sudo $SSH $kmsnode \"mkdir -p $CONF_KMS; chown hadoop8:users $CONF_KMS; chmod 777 $CONF_KMS\""
if [ $? -ne 0 ]; then 
  echo "Error: node $kmsnode failed to create $CONF_KMS!"
  exit 1
fi

# need to create the zk path for jaas.conf in order to have zk start in kerb mode
$SSH $ADM_HOST "sudo $SSH $kmsnode \"mkdir -p $CONF_ZK; chown hadoopqa:users $CONF_ZK; chmod 755 $CONF_ZK\""
if [ $? -ne 0 ]; then 
  echo "Error: node $kmsnode failed to create $CONF_ZK!"
  exit 1
fi


#
# configure the namenode for KMS support, set the key provider property and restart NN
#
cmd_nnconfig="yinst set -root ${yroothome} $confpkg.TODO_KMS_PROVIDER_PATH=\"kms://https@$kmsnode:4443/kms\" ; \
  export JAVA_HOME=/home/gs/java/jdk; yinst restart namenode -root ${yroothome}"

fanoutNN $cmd_nnconfig
if [ $? -ne 0 ]; then
  echo "Failed to setup NN key provider!" 
else
  echo "INFO: setup NN key provider and restarted" 
fi


# build cmd to install KMS and yinst configs
#
# ...and core confs, symlink the needed core confs from the core deploy in the yroot
#
# Note: the following 'ln' require coreconfigs to be installed on the node, in flubber this
# the case because all the nodes are in the core role except gw. In RT this will not be
# the case, and different design is needed.
cmd_coreconfs="ln -f -s  /home/gs/gridre/yroot.$cluster/conf/hadoop/core-site.xml  /home/y/conf/kms/core-site.xml; \
  ln -f -s /home/gs/gridre/yroot.$cluster/conf/hadoop/kms-site.xml  /home/y/conf/kms/kms-site.xml; \
  ln -f -s /home/gs/gridre/yroot.$cluster/conf/hadoop/kms-acls.xml  /home/y/conf/kms/kms-acls.xml"

$SSH $kmsnode $cmd_coreconfs
if [ $? -ne 0 ]; then
  echo "Failed to setup Core conf links!"
else
  echo "INFO: setup Core conf links"
fi


#
# place the jaas.conf for ZK kerb support, on ZK node at /home/y/conf/zookeeper
# NOTE: the jaas.conf parsing is *really* touchy, the double quotes on 'keyTab' and 'principal' must
# be there else ZK reports a useless error about the jaas not having a Server section
#
JAASFILE=" /home/y/conf/zookeeper/jaas.conf"

cmd_zk_jaas="echo \"Server {\" > $JAASFILE; \
echo \"  com.sun.security.auth.module.Krb5LoginModule required\" >> $JAASFILE;
echo \"  useKeyTab=true\" >> $JAASFILE; \
echo \"  keyTab=\\\"/etc/grid-keytabs/zookeeper.$kmsnodeshort.dev.service.keytab\\\"\" >> $JAASFILE; \
echo \"  storeKey=true\" >> $JAASFILE; \
echo \"  useTicketCache=false\" >> $JAASFILE; \
echo \"  principal=\\\"zookeeper/$kmsnode@DEV.YGRID.YAHOO.COM\\\";\" >> $JAASFILE; \
echo \"};\" >> $JAASFILE"

$SSH $kmsnode $cmd_zk_jaas
if [ $? -ne 0 ]; then
  echo "Failed to setup JAAS file for ZooKeeper!"
else
  echo "INFO: setup JAAS file for ZooKeeper"
fi

#
# configure yjava_jetty, Dev migrated from tomcat6 to jetty so need to apply settings for KMS use
# of jetty, this is based on the prod use of yidf settings:
#  ref:   https://git.corp.yahoo.com/hadoop/yahoo-kms/blob/master/cicd/yidfs/kms_dev/kms_dev.yidf
#  ref:   https://git.corp.yahoo.com/hadoop/yahoo-kms/blob/master/cicd/yidfs/common/kms.yidf
#
# Currently need to set the version for yjava_vmwrapper and yhdrs due to yinst conflicts, these
# appear related to the yjava_jetty version used by KMS, which is newer than that used by
# other components
#
cmd_jetty="yinst i yjava_jetty yjava_ysecure yjava_vmwrapper-2.3.10 yhdrs  -br test -same -live -downgrade   -set yjava_jetty.enable_https=true  -set yjava_jetty.https_port=4443  -set yjava_jetty.http_port=-1 \
  -set yjava_jetty.key_store=\"/etc/ssl/certs/prod/_open_ygrid_yahoo_com-dev.jks\"  -set yjava_jetty.key_store_password_key_var=password  -set yjava_jetty.key_store_type=JKS \
  -set yjava_jetty.trust_store=\"/etc/ssl/certs/prod/_open_ygrid_yahoo_com-dev.jks\"  -set yjava_jetty.trust_store_password_key_var=password  \
  -set yjava_jetty.trust_store_type=JKS  -set yjava_jetty.user_name=hadoop8  -set yjava_jetty.autostart=off \
  -set yjava_jetty.garbage_collection=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/y/var/run/kms/kms.hprof -Xloggc:/home/y/logs/yjava_jetty/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M -Xmx8g\""

$SSH $kmsnode $cmd_jetty
if [ $? -ne 0 ]; then
  echo "Failed to install yjava_jetty support!"
else
  echo "INFO: installed yjava_jetty support"
fi


#
# install ZK server
#
cmd_zk="yinst i zookeeper_server -same -live -downgrade -set zookeeper_server.clientPort=50512 \
 -set zookeeper_server.kerberos=true -set zookeeper_server.jvm_args=\" \\
 -Djava.security.auth.login.config=/home/y/conf/zookeeper/jaas.conf \\
 -Dzookeeper.superUser=zookeeper -Dsun.security.krb5.debug=true\""

$SSH $kmsnode $cmd_zk
if [ $? -ne 0 ]; then
  echo "Failed to install ZK support!"
else
  echo "INFO: installed ZK support"
fi

echo "Resending yinst set zookeeper_server.jvm_args since the earlier set attempt using pdsh does not work!"
$SSH $kmsnode "yinst set zookeeper_server.jvm_args=\"  -Djava.security.auth.login.config=/home/y/conf/zookeeper/jaas.conf -Dzookeeper.superUser=zookeeper -Dsun.security.krb5.debug=true\""
if [ $? -ne 0 ]; then
  echo "Failed to set ZK jvm_args!"
  exit 1
fi




#
# build KMS installation command
#
# prod handles keytabs differntly for KMS, instead of TODO yinst vars, now using package 'ygrid_kms_keys'
# to install the KMS keytabs, in our case we need to sed the correct keytab in after kms pkg install

DEFAULT_KMS_KEYTAB=kms-nonprod.red.ygrid.yahoo.com.prod.HTTP.service.keytab

DEV_KMS_KEYTAB=kms.$kmsnodeshort.dev.service.keytab

cmd_kms="ln -f -s /home/gs/conf/local/local-superuser-conf.xml  /home/y/conf/hadoop/local-superuser-conf.xml; \
ln -f -s /etc/ssl/certs/prod/_open_ygrid_yahoo_com/kms.jks /etc/pki/tls/certs/prod/_open_ygrid_yahoo_com-dev.jks; \
ln -f -s /etc/grid-keytabs/kms.$kmsnodeshort.dev.service.keytab /home/y/conf/kms/kms.dev.service.keytab; \
yinst install  yahoo_kms -same -live -downgrade -br current \
 -set yahoo_kms.TODO_KEYTAB_FILE=kms.dev.service.keytab \
 -set yahoo_kms.TODO_HOSTNAME=$kmsnode -set yahoo_kms.TODO_KMS_USER=hadoop8 -set yahoo_kms.autostart=off \
 -set yahoo_kms.TODO_ZK_CONN_STRING=$kmsnode:50512 -set yahoo_kms.TODO_DOMAIN=DEV.YGRID.YAHOO.COM" 

# set the correct kms keytab
$SSH $kmsnode "sudo sed -i s/$DEFAULT_KMS_KEYTAB/$DEV_KMS_KEYTAB/g /home/y/conf/kms/kms-site.xml"



$SSH $kmsnode $cmd_kms
if [ $? -ne 0 ]; then
  echo "Failed to install KMS package support!" 
else
  echo "INFO: installed KMS package support" 
fi




#
# smoke test to verify KMS service is running
#

# need to have ykeykey/keydb in hybrid mode
$SSH $kmsnode "yinst set ykeydb.run_mode=YKEYKEY_HYBRID_MODE"
if [ $? -ne 0 ]; then
  echo "Failed to set ykeykey HYBRID mode!"
  exit 1
fi


$SSH $kmsnode "yinst restart zookeeper_server yahoo_kms"
if [ $? -ne 0 ]; then
  echo "Failed to restart KMS and ZK services!"
  exit 1
fi

# give kms and zk services a little time to startup
echo "Waiting 10 seconds for KMS and ZK services to startup...."
sleep 10 

#
# see if we can get info on the 'hitusr_4' key, if so it means KMS and ZK are both up
# and talking ok
#
# must use TLSv1.2, enforced by yjava_jetty 9.3 flavor it looks like
#
CURL_KEY=`$SSH $kmsnode  "kinit -kt /etc/grid-keytabs/$kmsnodeshort.dev.service.keytab hdfs/$kmsnode; curl --tlsv1.2 --negotiate -u: -k https://$kmsnode:4443/kms/v1/key/hitusr_4/_metadata"`
if [[ ! "$CURL_KEY" =~ "AES/CTR/NoPadding" ]]; then
  echo "Failed to get key info, KMS or ZK service may not be running!"
  exit 1
fi
echo "Got KMS key info successfully!"
echo $CURL_KEY

echo "KMS and ZK service install completed"
