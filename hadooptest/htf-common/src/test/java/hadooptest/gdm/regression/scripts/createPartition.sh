export tableName=$1
export partitionValue=$2

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

partitions=`/home/y/bin/hive -e 'use gdm; show partitions '"${tableName}"';'`
partitionName=`echo $partitions | cut -d' ' -f1 | cut -d'=' -f1`
/home/y/bin/hive -e 'use gdm; alter table '"${tableName}"' add partition ('"${partitionName}"'='"${partitionValue}"');'
exitCode=`echo $?`
if [ $exitCode != 0 ]
then
    exit $exitCode
fi
/home/y/bin/hive -e 'use gdm; load data local inpath "/tmp/part-0000" into table '"${tableName}"' partition ('"${partitionName}"'='"${partitionValue}"');'
