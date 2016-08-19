echo ================= Run sparkPi example =================
export SPARK_CONF_DIR=/home/gs/conf/spark/latest
export SPARK_HOME=/home/gs/spark/latest
# workaround for OpenStack
export PATH=$PATH:/home/gs/current/bin

if [ $SPARKVERSION != none ] && [ $STACK_COMP_INSTALL_SPARK != false ]; then
    if [[ "$HADOOP_27" == "true" ]]; then
        JAVA_HOME="$GSHOME/java8/jdk64/current"
    else
        JAVA_HOME="$GSHOME/java/jdk"
    fi

    cmd="export SPARK_CONF_DIR=/home/gs/conf/spark/latest ; \
      export SPARK_HOME=/home/gs/spark/latest ; \
      export HADOOP_HOME=$GSHOME/hadoop/current ; \
      export HADOOP_PREFIX=$GSHOME/hadoop/current ; \
      export HADOOP_CONF_DIR=/home/gs/conf/current ; \
      export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*" ; \
      export JAVA_HOME=$JAVA_HOME ; \
      echo Running Spark Pi example ; \
      kinit -k -t /homes/mapredqa/mapredqa.dev.headless.keytab mapredqa ; \
      echo Running $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi \
      --num-executors 2 --executor-memory 2g --queue $SPARK_QUEUE --conf spark.executorEnv.JAVA_HOME=$JAVA_HOME \
      --conf spark.yarn.appMasterEnv.JAVA_HOME=$JAVA_HOME --conf spark.ui.view.acls="*" $SPARK_HOME/lib/spark-examples.jar ; \
      $SPARK_HOME/bin/spark-submit  --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi \
      --num-executors 2 --executor-memory 2g --queue $SPARK_QUEUE --conf spark.executorEnv.JAVA_HOME=$JAVA_HOME \
      --conf spark.yarn.appMasterEnv.JAVA_HOME=$JAVA_HOME --conf spark.ui.view.acls="*" $SPARK_HOME/lib/spark-examples.jar"
    fanoutSpark "$cmd"
fi
