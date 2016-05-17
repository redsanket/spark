export TABLE=$1

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

/home/y/bin/hive -e 'use gdm; create table '"${TABLE}"' (mrkt_sid string) partitioned by (instanceDate string) location "hdfs:/data/daqdev/data/'"${TABLE}"'";'

