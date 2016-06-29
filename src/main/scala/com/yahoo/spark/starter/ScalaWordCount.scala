package com.yahoo.spark.starter

import org.apache.spark._

// Simple example of Word Count in Scala
object ScalaWordCount {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
        System.err.println("Usage: ScalaWordCount <inputFilesURI> <outputFilesUri>")
        System.exit(1)
    }

    val conf = new SparkConf().setAppName("Scala Word Count")
    val sc = new SparkContext(conf)

    // get the input file uri
    val inputFilesUri = args(0)
    
    // get the output file uri
    val outputFilesUri = args(1)
    
    val textFile = sc.textFile(inputFilesUri)
    val counts = textFile.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
    counts.saveAsTextFile(outputFilesUri)

    sc.stop()
  }
}
