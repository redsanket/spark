/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hadooptest.spark.regression;


import scala.math.random
import org.apache.spark._
import java.io.{File, PrintWriter, FileReader, BufferedReader}
import SparkContext._

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
