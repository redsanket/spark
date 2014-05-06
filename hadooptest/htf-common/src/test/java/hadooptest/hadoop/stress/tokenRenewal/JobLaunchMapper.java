package hadooptest.hadoop.stress.tokenRenewal;

import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Mapper and Reducer classes for the wordcount job
public class JobLaunchMapper implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();

	public void configure(JobConf jobConf) {
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		System.out.println("Inside JobLaunchMapper");
		JobConf myJobConf = new JobConf();
		myJobConf.setJarByClass(JobLaunchMapper.class);
		myJobConf.setJobName("JobLaunchMapper");
		myJobConf.setOutputKeyClass(Text.class);
		myJobConf.setOutputValueClass(IntWritable.class);
		myJobConf.setMapperClass(NoOpMap.class);
		FileSystem fs2 = FileSystem.get(myJobConf);
		Path out2 = new Path("/tmp/out2");
		if (fs2.isDirectory(out2)) {
			fs2.delete(out2, true);
			System.out.println("Info: deleted output path: " + out2);
		}
		Path inputPath = new Path(DfsTestsBaseClass.DATA_DIR_IN_HDFS
				+ "file_1B");

		FileInputFormat.setInputPaths(myJobConf, inputPath);
		FileOutputFormat.setOutputPath(myJobConf, new Path("/tmp/out2"));

		JobClient.runJob(myJobConf);
	}

	public void close() throws IOException {

	}

}