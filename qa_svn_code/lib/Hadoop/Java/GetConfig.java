/**
 * MapReduce job that simply writes out the configuration settings
 *
 * NOTE: This uses the old, deprecated mapred API
 *
 * To compile:
 * mkdir my_classes
 * javac -classpath PATH_TO_HADOOP_COMMON_JAR/hadoop-common-0.22.0-SNAPSHOT.jar:PATH_TO_HADOOP_MAPRED_JAR/hadoop-mapred-0.22.0-SNAPSHOT.jar -d my_classes GetConfig.java
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetConfig extends Configured implements Tool {

  private static Text keyText = new Text();
  private static Text valueText = new Text();
  private static boolean config_written = false;

  public static class GetConfigMap extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {

    private JobConf myJobConf;
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if (config_written) return;

      config_written = true;

      Map<String, String> variables = System.getenv();
      for (Map.Entry<String, String> entry : variables.entrySet()) {
         //System.out.println("*** ENVIRONMENT: " + entry.getKey() + " = " + entry.getValue());
        keyText.set(entry.getKey());
        valueText.set(entry.getValue());
        context.write(keyText, valueText);
      }

      Iterator<Map.Entry<String, String>> confIter = myJobConf.iterator();
      // Note, this doesn't catch/write empty property values (e.g. <value></value>)
      while(confIter.hasNext()) {
          Map.Entry<String, String> element = confIter.next();
          keyText.set(element.getKey());
          valueText.set(element.getValue());
          output.collect(keyText, valueText);
      }
      reporter.setStatus("Finished writing config settings");
    }

    public void configure(JobConf jobConf) {
      myJobConf = jobConf;
    }

  }

  // Optional arguments are: <input_path> <output_path>
  public int run(String[] args) throws Exception {
    JobConf jobConf = new JobConf(getConf(), GetConfig.class);
    jobConf.setJobName("get_config");

    //
    String inputPath= "HadoopQEInput";
    String outputPath = "HadoopQEOutput";
    if (args.length >= 2) {
      inputPath = args[0];
      outputPath = args[1];
    }

    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);

    jobConf.setMapperClass(GetConfigMap.class);

    // Note that these are the default.
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
    FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

    JobClient.runJob(jobConf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GetConfig(), args);
    System.exit(res);
  }
}

