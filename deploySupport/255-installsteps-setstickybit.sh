set +
echo "================= Set sticky bit on /tmp hdfs directory"

JAVA_HOME="$GSHOME/java/jdk64/current"

cmd="export HADOOP_HOME=$GSHOME/hadoop/current ; \
     export JAVA_HOME=$JAVA_HOME ; \
     export HADOOP_PREFIX=${yroothome}/share/hadoop ; \
     export HADOOP_CONF_DIR=${yroothome}/conf/hadoop ; \
     kinit -k -t $HOMEDIR/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
     $GSHOME/hadoop/current/bin/hadoop fs -ls / ; \
     $GSHOME/hadoop/current/bin/hadoop fs -chmod +t /tmp ; \
     $GSHOME/hadoop/current/bin/hadoop fs -ls / "

set -x
fanout "$cmd"
set +x
