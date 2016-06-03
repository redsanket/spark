export TABLE=$1
export WITH_DATA=$2
export INSTANCE=$3

if [ "$INSTANCE" == "" ]; then
  year=`date +%Y`
  month=`date +%m`
  day=`date +%d`
  export INSTANCE=${year}${month}${day}
fi

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

hadoop fs -put /tmp/gdm_hcat_test/avroschema.avsc /user/hadoopqa/avroschema.avsc

/home/y/bin/hive -e 'use gdm;create table '"${TABLE}"' partitioned by (instanceDate string) row format serde "org.apache.hadoop.hive.serde2.avro.AvroSerDe" stored as inputformat "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" outputformat "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat" location "hdfs:/data/daqdev/data/'"${TABLE}"'" tblproperties ("avro.schema.url"="/user/hadoopqa/avroschema.avsc");'

/home/y/bin/hive -e 'use gdm; describe '"${TABLE}"';'

if [ "$WITH_DATA" == "y"  ]; then
  /home/y/bin/hive -e 'use gdm; load data local inpath "/tmp/gdm_hcat_test/part-0000" into table '"${TABLE}"' partition (instanceDate='"${INSTANCE}"');'
fi

