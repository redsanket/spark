echo ================= Install Tez on /sharelib/v1/tez hdfs directory =================
export TEZ_CONF_DIR=/home/gs/conf/tez/current
export TEZ_HOME=/home/gs/tez/current
export TEZ_UI=$TEZ_HOME/tez-ui.war
# workaround for OpenStack
export PATH=$PATH:/home/gs/current/bin

if [ $TEZVERSION != none ] && [ $INSTALL_TEZ != false ]; then

    JAVA_HOME="$GSHOME/java8/jdk64/current"

     TEZ_RELEASE_VERSION=`echo $TEZVERSION | cut -c1-3`
     cmd="echo installing Tez on Tez hosts ; \
        export TEZ_HOME=/home/gs/tez/current ; \
        export TEZ_CONF_DIR=/home/gs/conf/tez/current ; \
	export TEZ_UI=$TEZ_HOME/tez-ui.war ; \
        rm -rf /home/gs/conf/tez ; \
        rm -rf /home/gs/tez ; \
        rm -rf $TEZ_CONF_DIR ; \
        rm -rf $TEZ_HOME ; \
	rm -rf $TEZ_UI ; \
        mkdir -p `dirname $TEZ_HOME` ; \
        mkdir -p `dirname $TEZ_CONF_DIR` ; \
        mkdir -p ${yroothome}/tez_conf-$TEZVERSION/tez ; \
        mkdir -p ${yroothome}/tez-$TEZVERSION ; \
        yinst inst -root ${yroothome}/tez_conf-$TEZVERSION/tez -same -live -yes ytez_conf-$TEZVERSION -br test ; \
        chattr -a ${yroothome}/tez_conf-$TEZVERSION/tez/var/yinst/log/yinstlog ; \
        ln -s  ${yroothome}/tez_conf-$TEZVERSION/tez/conf/tez $TEZ_CONF_DIR ; \
        yinst inst -root ${yroothome}/tez-$TEZVERSION -same -live -yes ytez-$TEZVERSION -br test ; \
        chattr -a ${yroothome}/tez-$TEZVERSION/var/yinst/log/yinstlog ; \
        ln -s ${yroothome}/tez-$TEZVERSION/libexec/tez $TEZ_HOME ; \
	yinst inst -root ${yroothome}/ -same -live -yes ytez_ui-$TEZVERSION -br test ; \
	chattr -a ${yroothome}/tez_ui/var/yinst/log/yinstlog ; \
	ln -s ${yroothome}/share/tez-ui-$TEZVERSION $TEZ_UI ; \
        echo Tez version as I see it ; \
        readlink $TEZ_HOME "

     fanoutTez "$cmd"
     st=$?
     [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running fanoutTez <<<<<<<<<<" && exit $st

     fanoutTezUI "$cmd"
     st=$?
     [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running fanoutTezUI <<<<<<<<<<" && exit $st

     cmd="export TEZ_CONF_DIR=/home/gs/conf/tez/current ; \
        export TEZ_HOME=/home/gs/tez/current ; \
        export HADOOP_HOME=$GSHOME/hadoop/current ; \
        export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
        export HADOOP_CONF_DIR=/home/gs/conf/current ; \
        export HADOOP_CLASSPATH="$yroothome/:$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*" ; \
        export JAVA_HOME=$JAVA_HOME ; \
        kinit -k -t $HOMEDIR/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
        echo installing Tez into hdfs ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
        echo after chmoding -R 755 /sharelib ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put $TEZ_HOME/* /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez ; \
        echo after put command ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 755 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib ; \
        echo after  chmoding 755 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 744 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/*.jar ; \
        echo after -chmod 744 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/*.jar ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 744 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib/*.jar ; \
        echo after fs -chmod 744 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib/*.jar ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 644 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib/*.jar ; \
        echo after -chmod 644 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib/*.jar ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -ls /sharelib/v1/tez/ytez-$TEZVERSION/ "

     # deploy tarball for 0.7 tez version
     cmd_0_7="export TEZ_CONF_DIR=/home/gs/conf/tez/current ; \
        export TEZ_HOME=/home/gs/tez/current ; \
        export HADOOP_HOME=$GSHOME/hadoop/current ; \
        export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
        export HADOOP_CONF_DIR=/home/gs/conf/current ; \
        export HADOOP_CLASSPATH="$yroothome/:$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*" ; \
        export JAVA_HOME=$JAVA_HOME ; \
        kinit -k -t $HOMEDIR/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
        echo installing Tez into hdfs ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /sharelib/v1/tez/ytez-$TEZVERSION/ ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/gridre/yroot.$CLUSTER/tez-$TEZVERSION/tez-$TEZVERSION-minimal.tar.gz /sharelib/v1/tez/ytez-$TEZVERSION/ ; \
        echo after put command ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 755 /sharelib/v1/tez/ytez-$TEZVERSION/ ; \
        echo after  chmoding 755 /sharelib/v1/tez/; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 744 /sharelib/v1/tez/* ; \
        echo after -chmod 744 /sharelib/v1/tez/* ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
        echo after chmoding -R 755 /sharelib ; \
        /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -ls /sharelib/v1/tez/ "

	echo "Continue with non-tarball deploy..."
	fanoutOneTez "$cmd"
        echo "Continue with tarball deploy..."
	fanoutOneTez "$cmd_0_7"
fi
