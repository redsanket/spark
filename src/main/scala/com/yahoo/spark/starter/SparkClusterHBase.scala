package com.yahoo.spark.starter

import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark._
import org.apache.spark.sql.SparkSession


// Simple example of accessing HBase from Spark
object SparkClusterHBase {

    def main(args: Array[String]) {
        
        if (args == null || args.length < 2) {
          System.err.println("Usage: SparkClusterHBase <nameSpace> <tableName>")
            System.exit(1)
        }

        // Use the new 2.0 API. If you are using 1.6.2 create the spark conf and context as in 1.6 examples.
        val spark = SparkSession.
          builder.
          appName("Spark HBase Example").
          getOrCreate()

        val hconf = HBaseConfiguration.create()
        val nameSpace = args(0)
        val tableName = args(1)
        val qualifiedTableName = nameSpace + ":" + tableName
        hconf.set(TableInputFormat.INPUT_TABLE, qualifiedTableName)
        val admin = new HBaseAdmin(hconf)

        // create the table if not existed
        if(!admin.isTableAvailable(qualifiedTableName)) {
            val tableDesc = new HTableDescriptor(qualifiedTableName)
            tableDesc.addFamily(new HColumnDescriptor("cf1".getBytes()));
            admin.createTable(tableDesc)
        }

        // put data into the table
        val myTable = new HTable(hconf, qualifiedTableName);
        for (i <- 0 to 5) {
            val p = new Put(new String("row" + i).getBytes());
            p.add("cf1".getBytes(), "column-1".getBytes(), new String("value " + i).getBytes());
            myTable.put(p);
        }
        myTable.flushCommits();

        // access the table through RDD
        val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], 
              classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
              classOf[org.apache.hadoop.hbase.client.Result])
        val count = hBaseRDD.count()
        print("HBase RDD count:"+count)     

        spark.stop
    }
}
