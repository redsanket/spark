package hadooptest.hadoop.stress.tokenRenewal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Mapper and Reducer classes for the wordcount job
public class NoOpMap implements Mapper<LongWritable, Text, Text, IntWritable> {
	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();

	public void configure(JobConf jobConf) {
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		System.out.println("Inside no-op Mapper");
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}