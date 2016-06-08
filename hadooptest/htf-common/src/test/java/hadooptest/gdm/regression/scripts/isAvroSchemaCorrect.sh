export tableName=$1
export targetCluster=$2

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM
targetNamenode=`yinst range -ir "(@grid_re.clusters.${targetCluster}.namenode)"`
hadoop fs -rm -r /tmp/gdm_hcat_test/avro_schema
hadoop fs -mkdir -p /tmp/gdm_hcat_test/avro_schema
hadoop distcp hdfs://${targetNamenode}:8020/data/daqdev/data/${tableName}/_avro_schema/avroschema.avsc /tmp/gdm_hcat_test/avro_schema/targetschema.avsc

rm -rf /tmp/gdm_hcat_test/avro_schema
mkdir -p /tmp/gdm_hcat_test/avro_schema

hadoop fs -get /data/daqdev/data/${tableName}/_avro_schema/avroschema.avsc /tmp/gdm_hcat_test/avro_schema/sourceschema.avsc
hadoop fs -get /tmp/gdm_hcat_test/avro_schema/targetschema.avsc /tmp/gdm_hcat_test/avro_schema/
chmod 777 /tmp/gdm_hcat_test/avro_schema/*
output=`diff /tmp/gdm_hcat_test/avro_schema/sourceschema.avsc /tmp/gdm_hcat_test/avro_schema/targetschema.avsc`
status=`echo $?`

if [ $status != 0  ]; then
  exit 1
fi

if [ "$output" == "" ]; then
  exit 0
else
  exit 1
fi 
