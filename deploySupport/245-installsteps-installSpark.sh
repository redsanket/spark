# script to install spark on the cluster's gateway node
#
# inputs: cluster being installed
# outputs: 0 on success

# Install Spark only when values are non default
if [ $SPARKVERSION != none ] && [ $SPARK_HISTORY_VERSION != none ] && [ $STACK_COMP_INSTALL_SPARK != false ]; then
   if [ $# -ne 1 ]; then
     echo "ERROR: need the cluster name"
     exit 1
   fi

   CLUSTER=$1
   echo "INFO: Cluster being installed: $CLUSTER"
   echo "INFO: Spark node being installed: $gateway"
   echo "INFO: Spark version being installed: $SPARKVERSION"

   echo "INFO: Installing yspark_yarn-$SPARKVERSION"
   spark_install_cmd="yinst i yspark_yarn_install -br current \
   -set yspark_yarn_install.DOT_SIX=yspark_yarn-$SPARKVERSION \
   -set yspark_yarn_install.LATEST=yspark_yarn-$SPARKVERSION \
   -same -live"
   fanoutSpark "$spark_install_cmd"
   st=$?
   [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running fanoutSpark: Install yspark_yarn-$SPARKVERSION <<<<<<<<<<" && exit $st

   # Install Spark history server
   echo "INFO: Installing yspark_yarn_history_server-$SPARK_HISTORY_VERSION to hdfs://mapred/sparkhistory/"
   cmd="export SPARK_CONF_DIR=/home/gs/conf/spark/latest ; \
   export SPARK_HOME=/home/gs/spark/latest ; \
   export HADOOP_HOME=$GSHOME/hadoop/current ; \
   export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
   export HADOOP_CONF_DIR=/home/gs/conf/current ; \
   export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*" ; \
   export JAVA_HOME="$GSHOME/java8/jdk64/current" ; \
   yinst i yspark_yarn_history_server-$SPARK_HISTORY_VERSION -same -live ; \
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
   st=$?
   [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running fanoutSparkUI: Install spark history server <<<<<<<<<<" && exit $st

   # Install sharelib/v1/spark
   echo "INFO: Installing yspark_yarn-$SPARK_HISTORY_VERSION jars to hdfs://sharelib/v1/spark/"
   cmd="$spark_install_cmd ;\
   export SPARK_CONF_DIR=/home/gs/conf/spark/latest ; \
   export SPARK_HOME=/home/gs/spark/latest ; \
   export HADOOP_HOME=$GSHOME/hadoop/current ; \
   export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
   export HADOOP_CONF_DIR=/home/gs/conf/current ; \
   export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*" ; \
   export JAVA_HOME="$GSHOME/java8/jdk64/current" ; \
   kinit -k -t /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
   echo Creating hdfs://sharelib/v1/spark/spark-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -mkdir -p /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
   echo after chmoding -R 755 /sharelib ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/spark/latest/lib/spark-assembly.jar /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/spark/latest/python/lib/py4j-0.9-src.zip /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/spark/latest/python/lib/pyspark.zip /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/spark/latest/lib/datanucleus-api-jdo.jar /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/spark/latest/lib/datanucleus-core.jar /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -put /home/gs/spark/latest/lib/datanucleus-rdbms.jar /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/ ; \
   echo after put command ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 755 /sharelib/v1/spark/spark-$SPARKVERSION/ ; \
   echo after  chmoding 755 /sharelib/v1/spark/; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod 744 /sharelib/v1/spark/* ; \
   echo after -chmod 744 /sharelib/v1/spark/* ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
   echo after chmoding -R 755 /sharelib ; \
   /home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hadoop fs -ls /sharelib/v1/spark/yspark_yarn-$SPARKVERSION/share/spark/lib/"

   fanoutSparkUI "$cmd"
   st=$?
   [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running fanoutSparkUI: Install spark jars to sharelib <<<<<<<<<<" && exit $st
   echo "Spark installation completed!"
   ### Note: a && b && c returns 1; This breaks the script. Add echo at the end to mask the return value.
fi
