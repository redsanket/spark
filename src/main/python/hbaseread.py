from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# this is an example reading hbase from python
#
# You would need to update the table as well as the hbase configs below for the hbase cluster you are accessing
#
# the pythonconverters used below are in tthis spark starter package so currently you would need to build this and 
# send that jar with your application.
#
# exmaple spark-submit command to run this:
#
# $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --jars /homes/tgraves/spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar,/home/gs/hbase/current/lib/hbase-protocol.jar,/home/gs/hbase/current/lib/hbase-common.jar,/home/gs/hbase/current/lib/hbase-client.jar,/home/gs/hbase/current/lib/htrace-core-2.04.jar,/home/gs/hbase/current/lib/hbase-server.jar,/home/gs/hbase/current/lib/guava-12.0.1.jar,/homes/tgraves/hbase/libexec/hbase/conf/hbase-site.xml ~/hbaseread.py


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
                          keyConverter="com.yahoo.spark.starter.pythonconverters.ImmutableBytesWritableToStringConverter",
                          valueConverter="com.yahoo.spark.starter.pythonconverters.HBaseResultToStringConverter",
                          conf=conf)

count = rdd.count()
print("HBase RDD count: %f" % count)
