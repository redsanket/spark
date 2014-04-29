package hadooptest.spark.regression;

import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPipesEnvVars {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SparkPipesEnvVars <master> <inputfile>");
      System.exit(1);
    }

    JavaSparkContext ctx = new JavaSparkContext(args[0], "SparkPipesEnvVars",
        System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(SparkPipesEnvVars.class));
    JavaRDD<String> lines = ctx.textFile(args[1], 1);

    List<String> res1 = lines.pipe("printenv map_input_file").collect();
    System.out.println("map_input_file output: " + res1.toString());

    List<String> res2 = lines.pipe("printenv mapreduce_map_input_file").collect();
    System.out.println("mapreduce_map_input_file output: " + res2.toString());

    System.exit(0);
  }
}
