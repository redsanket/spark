export tableName=$1
export cluster=$2

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM
namenode=`yinst range -ir "(@grid_re.clusters.${cluster}.namenode)"`
expectedPath="hdfs://${namenode}/data/daqdev/data/${tableName}/_avro_schema/avroschema.avsc"
result=`/home/y/bin/hive -e 'use gdm; show tblproperties '"${tableName}"';'`
exists=`echo $result | grep "avro.schema.url"| cut -d' ' -f2`
echo "expectedPath : ${expectedPath}"
echo "actualPath: ${exists}"
if [ "$exists" == "$expectedPath" ]
then
  exit 0
else
  exit 1
fi
