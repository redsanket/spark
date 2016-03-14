package hadooptest.spark.regression;

import org.apache.spark._

import java.net.URI


object SparkHackJetty{
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkHackJetty <do broadcast> <turn security off>") 
      System.exit(1)
    }

    val doBroadcast = args(0).toBoolean
    val turnSecurityOff = args(1).toBoolean

    val conf = new SparkConf().setAppName("SparkHackJetty")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    // disable the external shuffle service since we are turning auth off in some cases
    // and we will fail to authenticate with it otherwise
    conf.set("spark.shuffle.service.enabled", "false")
    conf.set("spark.dynamicAllocation.enabled", "false")
    if (turnSecurityOff) { 
      conf.set("spark.authenticate", "false")
    }
    val sc = new SparkContext(conf)

    val nums = sc.makeRDD(Array(1), 1)

    val scconf = sc.getConf

    val uriStr = if (doBroadcast) {
      scconf.get("spark.httpBroadcast.uri")
    } else {
      scconf.get("spark.fileserver.uri")
    }
    
    System.err.println("uri to connect to is: " + uriStr)

    val uri = new URI(uriStr)
    val ipAddress = uri.getHost()
    val port = uri.getPort()

    val uriToTry = if (doBroadcast) {
      "http://sparkHttpUser:wrongPassword@" + ipAddress + ":" + port + "/broadcast-0"
    } else {
      "http://sparkHttpUser:wrongPassword@" + ipAddress + ":" + port + "/jars/foo.jar" 
    } 

    val piped = nums.pipe(Seq("curl", "--digest", uriToTry))
    try {
      val res = piped.collect() 
      val httpError = res.filter(_.contains("HTTP ERROR"))(0)
      System.err.println("http error is: " + httpError)
      if (turnSecurityOff) {
        // with security off we should just get a 404 or not found error
        if (!httpError.contains("404")) {
          // we have to throw for the spark application master to mark app as failed
          throw new Exception("Error, curl command failed")
          System.exit(1)
        }
      } else {
        // with security on we should just get a unauthorized
        if (!httpError.contains("401")) {
          // we have to throw for the spark application master to mark app as failed
          throw new Exception("Error, curl command failed")
          System.exit(1)
        }
      }
    } catch {
      case e: Exception =>  {
        // we have to throw for the spark application master to mark app as failed
        throw new Exception("Error, curl command failed")
        System.exit(1)
      }
    }

    System.exit(0)
  }
}

