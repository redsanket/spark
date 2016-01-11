package hadooptest.spark.regression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jerrypeng on 1/5/16.
 */
public class DynamicResourceAllocationTestTopology {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage:  parallelize input <# of partitions>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        List<Integer> data = new LinkedList<Integer>();
        for (int i = 0; i < Integer.parseInt(args[0]); ++i) {
            data.add(i);
        }

        JavaRDD<Integer> distData = ctx.parallelize(data, Integer.parseInt(args[0]));
        if (args.length > 1 && Integer.parseInt(args[1]) == 1) {
            distData.cache();

        }
        JavaPairRDD<Integer, Integer> ones = distData.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer s) throws InterruptedException {
                Thread.sleep(30 * 1000);
                return new Tuple2<Integer, Integer>(s, 1);
            }
        });

        List<Tuple2<Integer, Integer>> output = ones.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        // Sleep to keep the application open to test dynamic resource allocation to see if idle executors get relinquished
        Thread.sleep(60 * 1000);
        ctx.stop();
    }
}
