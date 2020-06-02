package com.yahoo.spark.starter.distributedcache


import org.apache.spark._
import java.io.{FileReader, BufferedReader}

object SparkDistributedCacheSingleArchive {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkDistributedCacheSingleArchive <inputfile>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SparkDistributedCacheSingleArchive ")
    val spark = new SparkContext(conf)

    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = spark.parallelize(testData).reduceByKey {
      // archive expected which contains a file named singlefile.txt which contain single value of 100 
      val in = new BufferedReader(new FileReader(args(0) + "/singlefile.txt"))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    println("result is: " + result)
    val pass = (result.toSet == Set((1,200), (2,300), (3,500)))
    println("pass is: " + pass)

    if (!pass) {
      println("Error, set isn't as expected")
      spark.stop()
      // we have to throw for the spark application master to mark app as failed
      throw new Exception("Error, set isn't as expected")
      System.exit(1)
    }
    spark.stop()
  }
}
