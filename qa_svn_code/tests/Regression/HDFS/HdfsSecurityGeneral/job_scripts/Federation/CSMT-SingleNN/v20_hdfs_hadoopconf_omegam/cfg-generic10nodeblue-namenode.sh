#!/bin/sh
[ -z "$YROOT" ] && YROOT=/grid/0/gs/gridre/yroot.omegam
yinst  set  --root $YROOT  \
	 'HadoopConfiggeneric10nodeblue.TODO_JAVA_HOME=/grid/0/gs/java/jdk64/current' \
	 'HadoopConfiggeneric10nodeblue.TODO_NAME_DIR=/grid/2/hadoop/var/hdfs/name,/nnvol/sigma/hadoop/var/hdfs/name' \
	 'HadoopConfiggeneric10nodeblue.TODO_AUDIT_LOGGING_COMMENT=' \
	 'HadoopConfiggeneric10nodeblue.TODO_NAMENODE_GC_SIZE=3G' \
	 'HadoopConfiggeneric10nodeblue.TODO_DFS_NAME_DIR=/grid/2/hadoop/var/hdfs/name' \
	 'HadoopConfiggeneric10nodeblue.TODO_NAMENODE_ONLY_COMMENT=' \
	 'HadoopConfiggeneric10nodeblue.TODO_YMOND_DFS_URI=morpork-ym0.blue.ygrid.yahoo.com:9474' \
	 'HadoopConfiggeneric10nodeblue.TODO_YMOND_JVM_URI=morpork-ym0.blue.ygrid.yahoo.com:9484' \
	 'HadoopConfiggeneric10nodeblue.TODO_YMOND_MAPRED_URI=morpork-ym0.blue.ygrid.yahoo.com:9479' \
	 'HadoopConfiggeneric10nodeblue.TODO_HADOOP_HEAPSIZE=45000m' \

echo '*** Finished with yinst-set for generic10nodeblue-namenode'
