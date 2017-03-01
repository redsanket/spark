package hadooptest.spark.regression;

import com.databricks.spark.avro._

import org.apache.spark._
import org.apache.spark.sql.SparkSession

// Simple example of accessing Hive from Spark
object SparkDataReadWrite {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Data Read Write")
      .getOrCreate()

    val jsonDF = spark.read.json("people.json")
    jsonDF.show()
    jsonDF.select("name").write.format("json").save("people_name.json")
    spark.read.format("json").load("people_name.json").show()

    val avroDF = spark.read.avro("users.avro")
    avroDF.show()
    avroDF.select("name").write.avro("user_name.avro")
    spark.read.avro("user_name.avro").show

    spark.stop()
  }
}