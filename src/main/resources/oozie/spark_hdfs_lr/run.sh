#!/bin/bash

echo "Run test SparkHdfsLR"

wfRoot=spark_hdfs_lr

hdfs dfs -mkdir -p $wfRoot/apps/lib
hdfs dfs -mkdir -p $wfRoot/apps/spark

hdfs dfs -put ./workflow.xml $wfRoot/apps/spark/
hdfs dfs -put ../../../../../target/spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar $wfRoot/apps/lib/
hdfs dfs -put ../../data/lr_data.txt $wfRoot/apps/lib/

echo "Finish setup hdfs directory"

export OOZIE_URL=https://axonitered-oozie.red.ygrid.yahoo.com:4443/oozie/

oozie job -run -config job.properties -auth KERBEROS
