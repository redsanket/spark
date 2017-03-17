package hadooptest.spark.regression

import com.databricks.spark.avro._

import org.apache.spark._
import org.apache.spark.sql.SparkSession

// Simple example of accessing Json/Avro data from Spark
object SparkDataReadWrite {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: SparkDataReadWrite <jsonFile> <jsonOpFile> <avroFile> <avroOpFile>")
      System.exit(1)
    }
    val jsonFile = args(0)
    val jsonOpFile = args(1)
    val avroFile = args(2)
    val avroOpFile = args(3)

    val spark = SparkSession
      .builder
      .appName("Spark Data Read Write")
      .getOrCreate()

    val jsonDF = spark.read.json(jsonFile)
    jsonDF.show()
    jsonDF.select("name").write.format("json").save(jsonOpFile)
    spark.read.format("json").load(jsonOpFile).show()

    val avroDF = spark.read.avro(avroFile)
    avroDF.show()
    avroDF.select("name").write.avro(avroOpFile)
    spark.read.avro(avroOpFile).show

    spark.stop()
  }
}
