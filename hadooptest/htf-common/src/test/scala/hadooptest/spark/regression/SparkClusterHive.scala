package hadooptest.spark.regression;

import org.apache.spark._
import org.apache.spark.sql.SparkSession

// Simple example of accessing Hive from Spark
object SparkClusterHive {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Oozie Spark Cluster Hive")
      .enableHiveSupport()
      .getOrCreate()

    // just show the databases in Hive
    spark.sql("show databases").collect.foreach(println)

    spark.stop()
  }
}