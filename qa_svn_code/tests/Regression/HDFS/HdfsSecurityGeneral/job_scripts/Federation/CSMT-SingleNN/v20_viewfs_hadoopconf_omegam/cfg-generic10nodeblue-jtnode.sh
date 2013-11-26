#!/bin/sh
[ -z "$YROOT" ] && YROOT=/grid/0/gs/gridre/yroot.omegam
yinst  set  --root $YROOT  \
	 'HadoopConfiggeneric10nodeblue.TODO_JAVA_HOME=/grid/0/gs/java/jdk64/current' \
	 'HadoopConfiggeneric10nodeblue.TODO_AUDIT_LOGGING_COMMENT=' \
	 'HadoopConfiggeneric10nodeblue.TODO_DFS_NAME_DIR=/grid/2/hadoop/var/hdfs/name' \
	 'HadoopConfiggeneric10nodeblue.TODO_JOBTRACKER_ONLY_COMMENT=' \
	 'HadoopConfiggeneric10nodeblue.TODO_YMOND_DFS_URI=morpork-ym0.blue.ygrid.yahoo.com:9474' \
	 'HadoopConfiggeneric10nodeblue.TODO_YMOND_JVM_URI=morpork-ym0.blue.ygrid.yahoo.com:9484' \
	 'HadoopConfiggeneric10nodeblue.TODO_YMOND_MAPRED_URI=morpork-ym0.blue.ygrid.yahoo.com:9479' \

echo '*** Finished with yinst-set for generic10nodeblue-jtnode'
