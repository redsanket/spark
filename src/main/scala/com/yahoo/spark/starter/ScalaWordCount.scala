package com.yahoo.spark.starter

import org.apache.spark.sql.SparkSession

// Simple example of Word Count in Scala
object ScalaWordCount {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
        System.err.println("Usage: ScalaWordCount <inputFilesURI> <outputFilesUri>")
        System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("Scala Word Count")
      .getOrCreate()

    // get the input file uri
    val inputFilesUri = args(0)
    
    // get the output file uri
    val outputFilesUri = args(1)

    // Get a logger on the Driver/AppMaster
    val logger = SparkStarterUtil.logger
   
    logger.info("Input : " + inputFilesUri)
    logger.info("Output : " + outputFilesUri)

    val textFile = spark.sparkContext.textFile(inputFilesUri)
    
    // How to log at the executor. This logic only serves as an example.
    textFile.foreachPartition(iterator => {
      val logger = SparkStarterUtil.logger
      logger.info("Partition Size: " + iterator.size)
    })

    val counts = textFile.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
    counts.saveAsTextFile(outputFilesUri)

    spark.stop()
  }
}

object SparkStarterUtil {
  lazy val logger = org.apache.log4j.Logger.getLogger("SparkStarter")
}
