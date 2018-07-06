package com.yahoo.spark.starter

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.SparkSession

case class DataRow(name: String, pet: String, horcruxes: Int, original_name: String)

// Simple example of accessing HBase from Spark using the Spark HBase connector.
// The example creates an HBase table and writes sample data to it using the connector
// and reads back data from the same table and displays it to the stdout.
object SparkClusterHBaseConnector {

    def main(args: Array[String]) {
        
        if (args == null || args.length < 2) {
          System.err.println("Usage: SparkClusterHBaseConnector <nameSpace> <tableName>")
            System.exit(1)
        }

        // Use the new 2.0 API. 
        val spark = SparkSession.
          builder.
          appName("Spark HBase Connector Example").
          getOrCreate()

        import spark.implicits._

        val nameSpace = s"${args(0)}"
        val tableName = s"${args(1)}"

        val df = Seq( DataRow("harry", "hedwig", 0, null), DataRow("voldy", "nagini", 7, "Tom"))
          .toDF("name", "pet", "horcruxes", "original_name")
        // print the original dataframe
        df.show

        // define the catalog
        def catalog = s"""{
	        |"table":{"namespace":"${nameSpace}", "name":"${tableName}"},
	        |"rowkey":"name",
	        |"columns":{
	        |"name":{"cf":"rowkey", "col":"name", "type":"string"},
	        |"pet":{"cf":"cf1", "col":"pet", "type":"string"},
	        |"horcruxes":{"cf":"cf2", "col":"horcruxes", "type":"int"},
	        |"original_name":{"cf":"cf3", "col":"original_name", "type":"string"}
	      |}
        |}""".stripMargin 
        
        // write data to hbase
        hbaseWrite(df, catalog)

        // read the data from hbase
        val df2 = hbaseRead(spark, catalog)
        df2.show

        spark.stop
    }

    // Read data from an existing hbase table
    def hbaseRead(spark: SparkSession, catalog: String): DataFrame = {
      spark
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    }

    // Write data to an hbase table
    def hbaseWrite(df: DataFrame, catalog: String): Unit = {
      df
      .write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save
    }
}
