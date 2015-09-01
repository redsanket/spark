echo ================= Set sticky bit on /tmp hdfs directory

cmd="export HADOOP_HOME=$GSHOME/hadoop/current ; \
     export JAVA_HOME=$GSHOME/java/jdk ; \
     kinit -k -t /$HOMEDIR/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
     $GSHOME/hadoop/current/bin/hadoop fs -ls / ; \
     $GSHOME/hadoop/current/bin/hadoop fs -chmod +t /tmp ; \
     $GSHOME/hadoop/current/bin/hadoop fs -ls / "
fanout "$cmd"
