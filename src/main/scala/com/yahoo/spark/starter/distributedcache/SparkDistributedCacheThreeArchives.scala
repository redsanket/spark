package com.yahoo.spark.starter.distributedcache


import org.apache.spark._
import java.io.{FileReader, BufferedReader}

object SparkDistributedCacheThreeArchives {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkDistributedCacheThreeArchives <inputarchive> <inputarchive2> <inputarchive3>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SparkDistributedCacheThreeArchives")
    val spark = new SparkContext(conf)

    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = spark.parallelize(testData).reduceByKey {
      // archives expected to contain single files name firstfile.txt, secondfile.txt, 
      // thirdfile.txt that have the value of 20, 30, 50
      val in = new BufferedReader(new FileReader(args(0) + "/firstfile.txt"))
      val fileVal = in.readLine().toInt
      val in2 = new BufferedReader(new FileReader(args(1) + "/secondfile.txt"))
      val fileVal2 = in2.readLine().toInt
      val in3 = new BufferedReader(new FileReader(args(2) + "/thirdfile.txt"))
      val fileVal3 = in3.readLine().toInt
      in.close()
      _ * (fileVal + fileVal2 + fileVal3) + _ * (fileVal + fileVal2 + fileVal3)
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
