# $Id$

set +x

echo ================= evaluating whether to install Hive/HiveServer2
echo ================= HIVEIGORTAG = $HIVEIGORTAG
echo ================= hs2_nodes = $hs2_nodes
echo ================= hs2_masters = $hs2_masters
echo ================= hs2_slaves = $hs2_slaves
echo ================= jdbc_client = $jdbc_client
echo ================= hive_client = $hive_client
echo ================= hive_mysql = $hive_mysql
echo ================= hcat_server = $hcat_server
echo ================= gateway = $gateway
echo ================= cluster = $cluster

export HIVE_HOME=/home/y/libexec/hive

if [[ -n "$hs2_nodes" && -n "$hive_client" && -n "$jdbc_client" ]]; then
    if [[ $HIVEIGORTAG != none ]]; then
        echo === Create hdfs directory 
        cmd="export HADOOP_HOME=$GSHOME/hadoop/current ; \
            export JAVA_HOME=$GSHOME/java/jdk ; \
            export HADOOP_CONF_DIR=$GSHOME/conf/current ; \
            kinit -k -t /etc/grid-keytabs/\$(hostname |cut -d. -f1).dev.service.keytab hdfs/\`hostname\`@DEV.YGRID.YAHOO.COM ; \
            $GSHOME/hadoop/current/bin/hadoop fs -mkdir -p /user/hive ; \
            $GSHOME/hadoop/current/bin/hadoop fs -chown -R hitusr_1:users /user/hive ; \
            $GSHOME/hadoop/current/bin/hadoop fs -chmod -R 777 /user/hive"
        fanoutNN "$cmd"

        # echo === installing HCAT server version=\"$HIVE_VERSION\"
        # cmd="yinst restore -igor -igor_tag hcatalog.$HIVE_VERSION -live -yes -quarantine && \
        # yinst restart hcat_server "
        # fanoutHcatServer "$cmd"

        echo === installing Hive/HS2 version=\"$HIVEIGORTAG\"
        cmd="yinst restore -igor -igor_tag hcatalog.$HIVEIGORTAG -live -yes -quarantine && \
             yinst restart hive_server2 "
        fanoutHiveServer2 "$cmd"

        echo === installing hive client $hive_client
        cmd="yinst restore -igor -igor_tag hcatalog.$HIVEIGORTAG -live -yes -quarantine && chmod -R 777 /home/y/libexec/hive_systest && /usr/local/bin/yinst set hive.hive_heapsize=2048"
        fanoutHiveClient "$cmd"

        echo === installing hive jdbc client 
        cmd="yinst restore -igor -igor_tag hcatalog.$HIVEIGORTAG -live -yes -quarantine"
        fanoutHiveJdbcClient "$cmd"

        if [ -n "$hive_mysql" ]; then
        echo === installing hive myql server for test golden data set
        cmd="yinst restore -igor -igor_tag hcatalog.$HIVEIGORTAG -live -yes -quarantine && \
             yinst restart mysql_server && chmod a+r /home/y/conf/keydb/mysqlroot.keydb"
        fanoutHiveMysql "$cmd"
        fi

        echo === installing hive client  inside gateway yroot
        HIVE_PKG_VERSION=`ssh $hive_client "/usr/local/bin/yinst ls hive | cut -f 2 -d ' '"`    
        export HIVEVERSION=$HIVE_PKG_VERSION
        cmd="/usr/local/bin/yinst install -root $yroothome  $HIVEVERSION  -br quarantine -live -br test -same -yes -downgrade"
            fanoutYRoots "$cmd"
        st=$?
        if [ "$st" -ne 0 ];then
            echo $st
            echo "*****" HIVE NOT INSTALLED properly "*****"
            exit $st
        else 
            echo "*****" HIVE is up and running properly now  "*****"
            recordManifest "$HIVEVERSION"
        fi
    else
        if [[ $HCATIGORTAG != none ]]; then
            echo === not define HIVEIGORTAG
            exit 1
        fi
    fi
else
    echo ===  cannot find hiveserver2 nodes defined in igor, skip hive/hs2 installation now.
fi

