package hadooptest.spark.regression;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPipesEnvVars {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: SparkPipesEnvVars inputfile>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("SparkPipesEnvVars");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    List<String> res1 = lines.pipe("printenv map_input_file").collect();
    System.out.println("map_input_file output: " + res1.toString());

    List<String> res2 = lines.pipe("printenv mapreduce_map_input_file").collect();
    System.out.println("mapreduce_map_input_file output: " + res2.toString());

    ctx.stop();
  }
}
