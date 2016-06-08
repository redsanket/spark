export TABLE=$1

year=`date +%Y`
month=`date +%m`
day=`date +%d`
export INSTANCE=${year}${month}${day}

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

/home/y/bin/hive -e 'use gdm; create table '"${TABLE}"' (mrkt_sid string) partitioned by (instanceDate string) location "hdfs:/data/daqdev/data/'"${TABLE}"'";'

/home/y/bin/hive -e 'use gdm; describe '"${TABLE}"';'

/home/y/bin/hive -e 'use gdm; load data local inpath "/tmp/gdm_hcat_test/part-0000" into table '"${TABLE}"' partition (instanceDate='"${INSTANCE}"');'

