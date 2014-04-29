package hadooptest.spark.regression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSaveAsText {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: SparkSaveAsText <master> <inputfile> <outputfile>");
      System.exit(1);
    }

    JavaSparkContext ctx = new JavaSparkContext(args[0], "SparkSaveAsText",
        System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(SparkSaveAsText.class));
    JavaRDD<String> lines = ctx.textFile(args[1], 1);

    lines.saveAsTextFile(args[2]);
    System.exit(0);
  }
}
