export tableName=$1

export HADOOP_CONF_DIR=/home/gs/conf/current

export HADOOP_PREFIX=/home/gs/hadoop/current

export JAVA_HOME=/home/gs/java/jdk

export PATH=/home/gs/hadoop/current/bin:/home/y/bin:${PATH}

kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

result=`/home/y/bin/hive -e 'use gdm; show tables;'`
exists=`echo $result | grep -io "${tableName}$\|${tableName} "`
if [ "$exists" == "" ]
then
  exit 1
else
  exit 0
fi
~                                                                                                                      
~                    
