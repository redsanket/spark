package com.yahoo.spark.stress

import org.apache.spark._
import org.apache.spark.sql.SparkSession

// Simple example of accessing Json/Avro data from Spark
object SparkStressTest {
  def main(args: Array[String]) {

    val testName = args(0)
    val testArgs = args.slice(1, args.length)

    val spark = SparkSession
      .builder
      .appName(s"Spark Stress Test - $testName")
      .getOrCreate()

    println(s"Running test $testName")

    val sc = spark.sparkContext

    val test: KVDataTest = testName match {
      case "aggregate-by-key" => new AggregateByKey(sc)
      case "aggregate-by-key-int" => new AggregateByKeyInt(sc)
      case "aggregate-by-key-naive" => new AggregateByKeyNaive(sc)
      case "sort-by-key" => new SortByKey(sc)
      case "sort-by-key-int" => new SortByKeyInt(sc)
      case "count" => new Count(sc)
      case "count-with-filter" => new CountWithFilter(sc)
    }

    test.initialize(testArgs)
    test.createInputData()
    test.run()
    test.deleteInputData()

    spark.stop()
  }
}
