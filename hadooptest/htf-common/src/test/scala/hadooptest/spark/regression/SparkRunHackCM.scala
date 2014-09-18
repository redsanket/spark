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

// put in spark packages so we can access internal spark functions
package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.util.Utils
import org.apache.spark.network._

object SparkRunHackCM {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkRunHackCM setSecurityOn password")
      System.exit(1)
    }

    val setSecurityOn = args(0)
    val passwd = args(1)

    val conf = new SparkConf().setAppName("SparkRunHackCM")
    conf.set("spark.authenticate", args(0)) 
    conf.set("spark.task.maxFailures", "1")
    val sc = new SparkContext(conf)

    val nums = sc.makeRDD(Array(1), 1)

    val scconf = sc.getConf

    // get the host and port of the blockmanager running on AM
    val port = SparkEnv.get.connectionManager.id.toSocketAddress.getPort()
    val host = Utils.localIpAddress
    System.err.println("host is: " + host)
    System.err.println("port is: " + port)

    val authSetting = "-Dspark.authenticate=" + setSecurityOn
    val password= "-Dspark.authenticate.secret=" + passwd

    val hadoopConfDir = System.getenv("HADOOP_CONF_DIR")
    val cmd = Seq(
      "java", 
      authSetting, 
      password, 
      "-cp", 
      "__app__.jar:__spark__.jar:" + hadoopConfDir, 
      "org.apache.spark.examples.SparkHackCM", 
      host, 
      port.toString)
    System.err.println("cmd is: " + cmd)

    try {
      val piped = nums.pipe(cmd, Map("SPARK_YARN_MODE" -> "false"))
      val res = piped.collect()
      System.err.println("result is: " + res.toSet.mkString(","))
      if (setSecurityOn.toBoolean) {
        // with security on this should have failed
        System.err.println("pipes should have thrown an exception")
        sc.stop()
        // we have to throw for the spark application master to mark app as failed
        throw new Exception("Error, spark hack cm command failed")
        System.exit(1)
      }
    } catch {
      case e: Exception =>  {
        if (setSecurityOn.toBoolean) {
          System.err.println("Exception is expected, exiting cleanly")
          sc.stop()
          System.exit(0)
        } else {
          System.err.println("Exception should not have been thrown")
          sc.stop()
          // we have to throw for the spark application master to mark app as failed
          throw new Exception("Error, spark hack cm command failed")
          System.exit(1)
        }
      }
    }

    sc.stop()
    System.exit(0)
  }

}
