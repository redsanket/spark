package hadooptest.spark.regression;

import org.apache.spark._
import SparkContext._

object SparkPipesTaskDirs {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkPipesTaskDirs <master> <inputfile>") 
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "SparkPipesTaskDirs",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val textFile = spark.textFile(args(1))
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
    System.exit(0)
  }
}

