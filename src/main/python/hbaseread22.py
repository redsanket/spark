from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# this is an example reading hbase from python
#
# You would need to update the table as well as the hbase configs below for the hbase cluster you are accessing
#
# the pythonconverters used below are in spark2.2 examples which we added back in after spark 2.1. You can find
# it in $SPARK_HOME/lib/spark-examples.jar
#
# exmaple spark-submit command to run this:
#
# $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --jars $SPARK_HOME/lib/spark-examples.jar,/home/gs/hbase/current/lib/hbase-protocol.jar,/home/gs/hbase/current/lib/hbase-common.jar,/home/gs/hbase/current/lib/hbase-client.jar,/home/gs/hbase/current/lib/htrace-core-2.04.jar,/home/gs/hbase/current/lib/hbase-server.jar,/home/gs/hbase/current/lib/guava-12.0.1.jar,/homes/tgraves/hbase/libexec/hbase/conf/hbase-site.xml ~/hbaseread.py


sc = SparkContext()
table_name = "spark_test:tgraves"
#hconf = sc._jvm.org.apache.hadoop.hbase.HBaseConfiguration.create()
conf = {"hbase.zookeeper.quorum": "reluxred-hb-zk0.red.ygrid.yahoo.com,reluxred-hb-zk1.red.ygrid.yahoo.com,reluxred-hb-zk2.red.ygrid.yahoo.com,reluxred-hb-zk3.red.ygrid.yahoo.com,reluxred-hb-zk4.red.ygrid.yahoo.com",
        "hbase.mapreduce.inputtable": table_name,
        "zookeeper.znode.parent": "/hbase/reluxred-hb1",
        "hbase.zookeeper.property.clientPort": "50512",
        "hbase.security.authentication": "kerberos",
}
rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                          "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                          "org.apache.hadoop.hbase.client.Result",
                          keyConverter="org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter",
                          valueConverter="org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter",
                          conf=conf)

count = rdd.count()
print("HBase RDD count: %f" % count)
