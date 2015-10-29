package com.yahoo.spark.starter

import org.apache.spark._

// Simple example of accessing Hive from Spark
object SparkClusterHive {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkClusterHive")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // just show the databases in Hive
    sqlContext.sql("show databases").collect().foreach(println)

    // to access particular table you can do something like this:
    //sqlContext.sql("select * from mydatabase.mytable").take(100).foreach(println)

    sc.stop()
  }
}
