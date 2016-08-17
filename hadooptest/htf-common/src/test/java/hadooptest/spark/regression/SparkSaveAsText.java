package hadooptest.spark.regression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSaveAsText {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SparkSaveAsText <inputfile> <outputfile>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("SparkSaveAsText");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    lines.saveAsTextFile(args[1]);
    ctx.stop();
  }
}
