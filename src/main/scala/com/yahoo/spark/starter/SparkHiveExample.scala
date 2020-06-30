package com.yahoo.spark.starter

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(key: Int, value: String)

// Simple example of accessing Hive from Spark
// Original: https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
object SparkHiveExample {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkHiveExample <databaseDir> <inputFile>")
      System.exit(1)
    }

    val databaseDir = args(0)
    val inputFile = args(1)

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // Create a database under hdfs home directory for testing purpose
    sql(s"CREATE DATABASE IF NOT EXISTS spark_hive_test LOCATION '$databaseDir'")

    sql("USE spark_hive_test")

    // Create the table and load data from a text file on hdfs
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

    // The inputFile is available at spark-starter/src/main/resources/hive/kv1.txt
    sql(s"LOAD DATA INPATH '$inputFile' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...

    // Create a Hive managed orc table, with HQL syntax instead of the Spark SQL native syntax
    // `USING hive`
    sql("CREATE TABLE hive_records(key int, value string) STORED AS orc")
    // Save DataFrame to the Hive managed table
    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Rename table
    sql("ALTER TABLE hive_records RENAME TO hive_records_v2")
    sql("SELECT * FROM hive_records_v2").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Drop the database
    sql("DROP DATABASE IF EXISTS spark_hive_test CASCADE")

    spark.stop()
  }
}
