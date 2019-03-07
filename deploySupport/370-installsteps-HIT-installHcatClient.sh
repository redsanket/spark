# $Id$

set +x

echo ================= evaluating whether to install Hcatlog client
echo ================= HCATIGORTAG = $HCATIGORTAG
echo ================= hcatnode = $hcatservernode
echo ================= gateway = $gateway
echo ================= cluster = $cluster

export INSTALLATION_ROOT=$yroothome
export HCAT_URI=thrift://${hcatservernode}:9080
export HCAT_PRINCIPAL=hcat/_HOST@DEV.YGRID.YAHOO.COM
export HCAT_HOME=$INSTALLATION_ROOT/libexec/hive/
export HIVE_HOME=$INSTALLATION_ROOT/libexec/hive/
export HCAT_SERVER_HOST=$hcatservernode
regex="(hcat_common(-|:)(\w+|\.)*)"

if [[ $HCATIGORTAG != none ]]; then
    hcat_common_pkg_version=`ssh $hcatservernode "/usr/local/bin/yinst ls hcat_common | cut -f 2 -d ' '"`
    echo === installing hcat_common pkg version=\"$hcat_common_pkg_version\"
    (
    echo 'export PATH=$yroothome/bin:/usr/kerberos/bin:/usr/bin:\$PATH'
    echo yroothome=$yroothome
    echo cd $yroothome
    echo /usr/local/bin/yinst install -root $yroothome $hcat_common_pkg_version $HIVEVERSION -br current -br test -br quarantine -downgrade -same -live -yes\
    -set hive.HADOOP_CONF_DIR=$GSHOME/conf/current \
    -set hive.HADOOP_HOME=$GSHOME/hadoop/current \
    -set hive.hcat_server_host=$hcatservernode \
    -set hive.metastore_uris=thrift://$hcatservernode:9080 \
    -set hcat_miners_test.CLUSTER=$cluster \
    -set hive.metastore_kerberos_principal=hcat/_HOST@DEV.YGRID.YAHOO.COM

    # echo "echo ==== Testing hcat server/client installation now"
    # echo "SUDO_USER=hadoopqa  kinit -kt ~hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM"
    # echo "SUDO_USER=hadoopqa $yroothome/bin/hcat hcat -e 'show databases' "
    # echo "SUDO_USER=hadoopqa $yroothome/bin/hcat hcat -e 'create database hit_test_deploy' "
    # echo "SUDO_USER=hadoopqa $yroothome/bin/hcat hcat -e 'drop database hit_test_deploy' "
    echo 'st=$?'
    echo 'if [ "$st" -ne 0 ];then'
    echo '  echo $st'
    echo '  echo "*****" HCAT CLIENT NOT INSTALLED properly "*****"'
    echo 'exit $st'
    echo 'else'

    echo '  echo "***** HCAT server is up and running properly now ****"'
    echo 'fi'
    echo 'exit $st'
    
    ) > $scripttmp/$cluster.installhcatclient.sh
    fanoutYRoots "rsync -a $scriptaddr/$cluster.installhcatclient.sh  /tmp/ && sh /tmp/$cluster.installhcatclient.sh"
    st=$?
    if [ "$st" -eq 0 ] ; then
        export HCAT_URI=thrift://${hcatservernode}:9080
        export HCAT_PRINCIPAL=hcat/_HOST@DEV.YGRID.YAHOO.COM
        export HCAT_HOME=$INSTALLATION_ROOT/libexec/hive/
        export HIVE_HOME=$INSTALLATION_ROOT/libexec/hive/
        export HCAT_SERVER_HOST=$hcatservernode
        recordManifest "$hcat_common_pkg_version"
    else
        echo "*****" Failed to install HCAT CLIENT "*****"
        exit $st
    fi
else
    echo ========== Cannot find out hcat_common pkg
    echo ========== Ignore hcat_common installation now....
fi

