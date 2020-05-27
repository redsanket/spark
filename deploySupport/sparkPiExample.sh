echo ================= Run sparkPi example =================
export GSHOME=/home/gs
export SPARK_CONF_DIR=$GSHOME/conf/spark/latest
export SPARK_HOME=$GSHOME/spark/latest
# workaround for OpenStack
export PATH=$PATH:$GSHOME/current/bin
JAVA_HOME="$GSHOME/java/jdk64/current"

export SPARK_CONF_DIR=/home/gs/conf/spark/latest
export SPARK_HOME=$GSHOME/spark/latest
export HADOOP_HOME=$GSHOME/hadoop/current
export HADOOP_PREFIX=$GSHOME/hadoop/current
export HADOOP_CONF_DIR=$GSHOME/conf/current
export HADOOP_CLASSPATH="$yroothome/:$SPARK_CONF_DIR:$SPARK_HOME/*:$SPARK_HOME/lib/*"
export JAVA_HOME=$JAVA_HOME
[ -z "$SPARK_QUEUE" ] && export SPARK_QUEUE=default
CLUSTER=`hostname | cut -d- -f1`
ALLOWED_RUNTIMES=`/usr/local/bin/yinst set -root /home/gs/gridre/yroot."$CLUSTER" | grep TODO_YARN_NODEMANAGER_RUNTIME_LINUX_ALLOWED_RUNTIMES | cut -d: -f2`

DOCKERIMAGE=""

if [[ "$ALLOWED_RUNTIMES" =~ "fsimage" ]]; then
  IMAGENAME=`/usr/local/bin/yinst set -root /home/gs/gridre/yroot."$CLUSTER" | grep TODO_YARN_NODEMANAGER_RUNTIME_LINUX_DOCKER_IMAGE_NAME |cut -d" " -f2`
  DOCKERIMAGE="--conf spark.oath.dockerImage=$IMAGENAME"
fi

echo "Running Spark Pi example"

set -x
kinit -k -t /homes/mapredqa/mapredqa.dev.headless.keytab mapredqa
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi \
  --num-executors 2 --executor-memory 2g --queue $SPARK_QUEUE --conf spark.executorEnv.JAVA_HOME=$JAVA_HOME \
  --conf spark.yarn.appMasterEnv.JAVA_HOME=$JAVA_HOME --conf spark.ui.view.acls="*" \
  $DOCKERIMAGE --conf spark.security.credentials.kafka.enabled=false $SPARK_HOME/lib/spark-examples.jar
