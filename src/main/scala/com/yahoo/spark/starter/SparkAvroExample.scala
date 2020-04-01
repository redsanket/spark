package com.yahoo.spark.starter

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.avro._

object SparkAvroExample {
	def main(args: Array[String]) {
		val spark = SparkSession
      .builder()
      .appName("Spark Avro Example")
      .getOrCreate()

    val inputDir = "avro_test/resources/"
    val outputDir = "avro_test/output/"

    // Read, query and write for normal data frame
    val usersDF = spark.read.format("avro").load(inputDir + "users.avro")
    usersDF.show()

    usersDF.select("name", "favorite_color").write.format("avro").save(outputDir + "namesAndFavColors.avro")

    val namesAndFavColorsDF = spark.read.format("avro").load(outputDir + "namesAndFavColors.avro")
    namesAndFavColorsDF.show()


    // Read, query and write for primitive types
    def readAndWritePrimitive(filename: String): DataFrame = {
      val df = spark.read.format("avro").load(inputDir + filename)
      df.show()
      df.write.format("avro").save(outputDir + filename)

      val df2 = spark.read.format("avro").load(outputDir + filename)
      df2.show()

      df2
    }

    println(readAndWritePrimitive("randomBoolean.avro").head().getBoolean(0))
    println(readAndWritePrimitive("randomBytes.avro").head().getAs[Array[Byte]](0))
    println(readAndWritePrimitive("randomDouble.avro").head().getDouble(0))
    println(readAndWritePrimitive("randomFloat.avro").head().getFloat(0))
    println(readAndWritePrimitive("randomInt.avro").head().getInt(0))
    println(readAndWritePrimitive("randomLong.avro").head().getLong(0))
    println(readAndWritePrimitive("randomString.avro").head().getString(0))
    println(readAndWritePrimitive("randomLongMap.avro").head().getAs[Map[String, Long]](0))
    println(readAndWritePrimitive("randomStringArray.avro").head().getAs[Array[String]](0))
	}
}