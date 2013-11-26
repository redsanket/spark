/**
 * MapReduce job that simply writes out the configuration settings
 *
 * NOTE: This uses the newer mapreduce API, which is not as stable
 *
 * To compile:
 * mkdir my_classes
 * javac -classpath PATH_TO_HADOOP_COMMON_JAR/hadoop-common-0.22.0-SNAPSHOT.jar:PATH_TO_HADOOP_MAPRED_JAR/hadoop-mapred-0.22.0-SNAPSHOT.jar -d my_classes GetConfig2.java
 * jar -cvf ./HadoopQEJobs.jar -C my_classes/ .
 *
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetConfig2 extends Configured implements Tool {

  private static Text keyText = new Text();
  private static Text valueText = new Text();
  private static boolean config_written = false;

  public static class GetConfig2Map extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      if (config_written) return;
      config_written = true;

      Map<String, String> variables = System.getenv();
      for (Map.Entry<String, String> entry : variables.entrySet()) {
         //System.out.println("*** ENVIRONMENT: " + entry.getKey() + " = " + entry.getValue());
        keyText.set(entry.getKey());
        valueText.set(entry.getValue());
        context.write(keyText, valueText);
      }

      Configuration myConf = context.getConfiguration();
      Iterator<Map.Entry<String, String>> confIter = myConf.iterator();
      // Note, this doesn't catch/write empty property values (e.g. <value></value>)
      while(confIter.hasNext()) {
          Map.Entry<String, String> element = confIter.next();
          keyText.set(element.getKey());
          valueText.set(element.getValue());
          context.write(keyText, valueText);
      }
      context.setStatus("Finished writing config settings");
    }

  }

  // Optional arguments are: <input_path> <output_path>
  public int run(String[] args) throws Exception {
    //Job job = Job.getInstance(getConf(), "get_config"); // Not available on hadoop-0.20
    Job job = new Job(getConf());
    job.setJarByClass(GetConfig2.class);
    job.setJobName("get_config2");

    //
    String inputPath= "HadoopQEInput";
    String outputPath = "HadoopQEOutput";
    if (args.length >= 2) {
      inputPath = args[0];
      outputPath = args[1];
    }

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(GetConfig2Map.class);

    // Note that these are the default.
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GetConfig2(), args);
    System.exit(res);
  }
}

