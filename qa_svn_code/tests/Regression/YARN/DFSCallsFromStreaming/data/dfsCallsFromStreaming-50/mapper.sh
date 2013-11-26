#!/bin/sh

cat &> /dev/null
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -help
