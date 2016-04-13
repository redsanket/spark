# script to install spark on the cluster's gateway node
#
# inputs: cluster being installed
# outputs: 0 on success

# Install Spark only when values are non default
if [ $SPARKVERSION != none ] && [ $STACK_COMP_INSTALL_SPARK != false ]; then
   if [ $# -ne 1 ]; then
     echo "ERROR: need the cluster name"
     exit 1
   fi

   CLUSTER=$1
   echo "INFO: Cluster being installed: $CLUSTER"
   echo "INFO: Spark node being installed: $gateway"
   echo "INFO: Spark version being installed: $SPARKVERSION"

   cmd="yinst i yspark_yarn_install -br current \
      -set yspark_yarn_install.DOT_SIX=yspark_yarn-$SPARKVERSION \
      -set yspark_yarn_install.LATEST=yspark_yarn-$SPARKVERSION \
      -same -live "
   fanoutSpark "$cmd"
   # Install Spark history server
   cmd="export SPARK_CONF_DIR=/home/gs/conf/spark/latest ; \
      export SPARK_HOME=/home/gs/spark/latest ; \
      export HADOOP_HOME=$GSHOME/hadoop/current ; \
      export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
      export HADOOP_CONF_DIR=/home/gs/conf/current ; \
      export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*" ; \
      export JAVA_HOME="$GSHOME/java8/jdk64/current" ; \
      yinst i yspark_yarn_history_server-$SPARKVERSION -same -live ; \
      kinit -k -t /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
      echo Creating hdfs://mapred/sparkhistory ; \
      /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /mapred/sparkhistory/ ; \
      echo chmoding -R 1777 /mapred/sparkhistory/ ; \
      /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 1777 /mapred/sparkhistory/ ; \
      echo chowning mapredqa:hadoop /mapred/sparkhistory/ ; \
      /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chown -R mapredqa:hadoop /mapred/sparkhistory/ ; \
      echo listing /mapred/ directory ; \
      /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -ls /mapred/ ; \
      echo Set spark_daemon_user to mapredqa ; \
      yinst set yspark_yarn_history_server.spark_daemon_user=mapredqa ; \
      echo Starting history server: http://$jobtrackernode:18080 ; \
      SUDO_USER=hadoopqa sudo yinst start yspark_yarn_history_server"
   fanoutSparkUI "$cmd"
fi
