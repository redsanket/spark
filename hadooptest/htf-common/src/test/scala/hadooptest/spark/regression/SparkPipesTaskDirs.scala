package hadooptest.spark.regression;

import org.apache.spark._

object SparkPipesTaskDirs {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPipesTaskDirs <inputfile>") 
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SparkPipesTaskDirs")
    val spark = new SparkContext(conf)

    val textFile = spark.textFile(args(0))
    val res = textFile.pipe(Seq("pwd"), separateWorkingDir=true).collect()
    println("SparkPipesTaskDirs result: " + res.mkString(","))
    // ideally we should check to make sure UUID dir is there too
    if (!res(0).contains("tasks/")) {
      println("Error, task dirs are not present")
      spark.stop()
      // we have to throw for the spark application master to mark app as failed
      throw new Exception("Error, task dirs are not present")
      System.exit(1)
    }
    spark.stop()
  }
}

