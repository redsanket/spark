echo ================= Install Tez on /sharelib/v1/tez hdfs directory =================
export TEZ_CONF_DIR=/home/gs/conf/tez/current
export TEZ_HOME=/home/gs/tez/current
# workaround for OpenStack
export PATH=$PATH:/home/gs/current/bin

if [ $TEZVERSION != none ] && [ $INSTALL_TEZ != false ]; then
    if [[ "$HADOOP_27" == "true" ]]; then
        JAVA_HOME="$GSHOME/java8/jdk64/current"
    else
        JAVA_HOME="$GSHOME/java/jdk"
    fi

    TEZ_QUEUE="default"

cmd="export TEZ_CONF_DIR=/home/gs/conf/tez/current ; \
     export TEZ_HOME=/home/gs/tez/current ; \
     export HADOOP_HOME=$GSHOME/hadoop/current ; \
     export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
     export HADOOP_CONF_DIR=/home/gs/conf/current ; \
     export HADOOP_CLASSPATH="$yroothome/:$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*" ; \
     export JAVA_HOME=$JAVA_HOME ; \
     kinit -k -t $HOMEDIR/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
     echo Running Tez word count ; \
     /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -rm -R /tmp/output/ ; \
     /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put $TEZ_CONF_DIR/tez-site.xml /tmp/ ; \
     echo Running hadoop jar $TEZ_HOME/tez-examples-$TEZVERSION.jar orderedwordcount -Dtez.queue.name=$TEZ_QUEUE /tmp/tez-site.xml /tmp/output/ ; \
     /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop jar $TEZ_HOME/tez-examples-$TEZVERSION.jar orderedwordcount -Dtez.queue.name=$TEZ_QUEUE /tmp/tez-site.xml /tmp/output/"
fanoutOneTez "$cmd"
fi
