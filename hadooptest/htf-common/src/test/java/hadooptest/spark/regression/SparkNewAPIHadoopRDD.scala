package hadooptest.spark.regression;

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark._

object SparkNewAPIHadoopRDD {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkNewAPIHadoopRDD <inputfile>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SparkNewAPIHadoopRDD")
    val sc = new SparkContext(conf)

    // make sure we can read secure HDFS using the sc.newAPIHadoopRDD api
    val lines = {
      val hconf = new Configuration()
      hconf.set("mapred.input.dir", args(0))
      // ctrl B separated
      hconf.set("textinputformat.record.delimiter","\002\n")
      sc.newAPIHadoopRDD(hconf, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    }

    val numLines = lines.count()
    if (numLines != 20) throw new Exception("Error counting lines")

    // make sure we can read secure hdfs using the sc.newAPIHadoopFile api
    val linesFile = {
      val hconf = new Configuration()
      hconf.set("textinputformat.record.delimiter","\002\n")
      sc.newAPIHadoopFile(args(0), classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hconf)
    }

    val numLinesFile = linesFile.count()
    if (numLinesFile != numLines) throw new Exception("Error counting number of lines");

    sc.stop();
  }
}
