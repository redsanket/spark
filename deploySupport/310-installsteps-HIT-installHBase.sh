# $Id$

set +x

echo ================= evaluating whether to install hbase zookeeper
echo ================= cluster = $cluster
echo ================= HBASEVERSION = $HBASEVERSION
echo ================= gateway = $gateway
echo ================= zookeeper = $HBASEZOOKEEPERNODE
echo ================= Hbase master = $HBASEMASTERNODE
echo ================= evaluating whether to install hbase zookeeper

export HBASE_HOME=/home/y/libexec/hbase
export HBASE_SHELL_CMD="$HBASE_HOME/bin/hbase shell "

if [ -n "$HBASEZOOKEEPERNODE" ]; then
    case "$HBASEVERSION" in
        none)
            echo === not installing hbase zookeeper at all.
            ;;
        *hbase*)
      
            # Stop master/regionserver before upgrading to a new version
            fanoutHBASEMASTER "yinst stop hbase_master"
            fanoutREGIONSERVER "yinst stop hbase_regionserver"

            cmd="mkdir -p -m 777 /home/y/var/run/hbase ; \
             mkdir -p -m 777 /home/y/var/log/hbase ; \
             export HADOOP_HOME=$GSHOME/hadoop/current ; \
             export JAVA_HOME=$GSHOME/java/jdk ; \
             export HADOOP_CONF_DIR=$GSHOME/conf/current ; \
             source /home/y/libexec/hbase/conf/hbase-env.sh ; \
             kinit -k -t /etc/grid-keytabs/\$(hostname |cut -d. -f1).dev.service.keytab hdfs/\`hostname\`@DEV.YGRID.YAHOO.COM ; \
             $GSHOME/hadoop/current/bin/hadoop fs -mkdir -p /hbase ; \
             $GSHOME/hadoop/current/bin/hadoop fs -chown hbaseqa:users /hbase ; \
             $GSHOME/hadoop/current/bin/hadoop fs -chmod -R 777 /hbase "
            fanoutHBASEMASTER "$cmd"

            echo === clean up the zookeeper old data from master node $HBASEMASTERNODE
            cmd="kinit -k -t /etc/grid-keytabs/hbaseqa.dev.service.keytab hbaseqa/\`hostname\`@DEV.YGRID.YAHOO.COM && \
            echo 'rmr /hbase' | /home/y/libexec/hbase/bin/hbase zkcli"
            fanoutHBASEMASTER "$cmd"

            echo === installing zookeeper version=\"$HBASEVERSION\"
            cmd="yinst restore -igor -igor_tag hbase.$HBASEVERSION -live -yes -quarantine && \
             yinst restart zookeeper_server "
            fanoutHBASEZOOKEEPER "$cmd"

            echo === installing hbase region servers
            rsInstallCmd="mkdir -p -m 777 /home/y/var/run/hbase ; \
             mkdir -p -m 777 /home/y/var/log/hbase ; \
             yinst restore -igor -igor_tag hbase.$HBASEVERSION -live -yes -quarantine && export hostname=\`hostname\` && \
             sed -i.bak s/_HOST/\$hostname/ /home/y/libexec/hbase/conf/jaas-hbase-server.conf && \
             yinst restart hbase_regionserver "

            # start first region server
            rsNode=`echo $REGIONSERVERNODES | tr " " "\n" | head -1`
            ssh $rsNode "$rsInstallCmd"
            #fanoutREGIONSERVER "$cmd"

            # sleep sometime before starting master
            sleep 300

            echo === installing hbase master $HBASEMASTERNODE

            cmd="yinst restore -igor -igor_tag hbase.$HBASEVERSION -live -yes -quarantine ; \
             export hostname=\`hostname\` ; \
             sed -i.bak s/_HOST/\$hostname/ /home/y/libexec/hbase/conf/jaas-hbase-server.conf && \
             yinst restart hbase_master "

            fanoutHBASEMASTER "$cmd"
            fanoutREGIONSERVER "$rsInstallCmd"

            # install hbasetest pkg on master node
            if [ -n "$HBASE_TEST_PKG" ]; then
                echo === update yinst setting of hbasetest on master node $HBASEMASTERNODE
                fanoutHBASETestClient "/usr/local/bin/yinst set hbasetest.zookeeper_quorum=$HBASEZOOKEEPERNODE"
            fi

            # install pig pkg on master for integration testing
            regex="(pig(-|:)(\w+|\.)*)"
            if [[ $PIGVERSION =~ $regex ]]; then
                fanoutHBASEMASTER "yinst install $PIGVERSION -same -live -downgrade -br quarantine"
            fi

            fanoutHBASEMASTER "yinst restart hbase_master"
            sleep 5

            echo === installing hbase/hbase_conf inside gateway yroot
            HBASE_PKG_VERSION=`ssh $HBASEMASTERNODE "/usr/local/bin/yinst ls hbase | cut -f 2 -d ' '"`
            HBASE_CONF_PKG_VERSION=`ssh $HBASEMASTERNODE "/usr/local/bin/yinst ls hbase_conf | cut -f 2 -d ' '"`
            HBASE_CONF_SETTINGS=`ssh $HBASEMASTERNODE /usr/local/bin/yinst set hbase_conf |awk '{print $1 $2}' | tr ":" "="`
            cmd="yinst install $HBASE_PKG_VERSION $HBASE_CONF_PKG_VERSION -br quarantine -live -br test -same "
            for i in $HBASE_CONF_SETTINGS; do
                cmd=" $cmd -set $i "
            done

            fanoutYRoots "$cmd"

            echo === pre-create table for testing purpose
            cmd="kinit -k -t /etc/grid-keytabs/hbaseqa.dev.service.keytab hbaseqa/\`hostname\`@DEV.YGRID.YAHOO.COM && \
        echo \"create  'my_table','my_family'\" | $HBASE_SHELL_CMD && \
        echo \"grant 'hitusr_1','RWXCA','my_table'\"| $HBASE_SHELL_CMD && \
        echo \"put 'my_table','my_row1','my_family:q1','value1'\" | $HBASE_SHELL_CMD && \
        echo \"put 'my_table','my_row2','my_family:q2','value2'\" | $HBASE_SHELL_CMD && \
        echo \"put 'my_table','my_row2','my_family:q3','value3'\" | $HBASE_SHELL_CMD"

            fanoutHBASEMASTER "$cmd"
            st=$?
            if [ "$st" -ne 0 ];then
                echo $st
                echo "*****" HBASEZOOKEEPER NOT INSTALLED properly "*****"
                exit $st
            else
                echo "*****" HBASEZOOKEEPER is up and running properly now  "*****"
                recordManifest "$HBASEVERSION"
            fi
            ;;
        *)
            echo === "********** ignoring hbaseversion=$HBASEVERSION"
        ;;
    esac
else
    echo ===  cannot find hbase zookeeper node defined in igor, skip hbase installation now.
fi

