package hadooptest.hadoop.regression.yarn.capacityScheduler.queuePreemption;

import hadooptest.TestSession;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class WordCountForQueuePreemmption extends Configured {
	String queue;
	public WordCountForQueuePreemmption(String queue){
		this.queue = queue;
	}
	Job jobHandle;
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	public static enum SP_EX_COUNTER {
		COUNT
	}

	static boolean STALLING_IN_PROGRESS = false;
	static int MAX_LOOP = 500;

	public static class QPreemptiveMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public QPreemptiveMapper() {

		}

		static long beginTime = System.currentTimeMillis();

		final class TestStatusReporter extends StatusReporter {
			private Counters counters = new Counters();
			private String status = null;

			public void setStatus(String arg0) {
				status = arg0;

			}

			public String getStatus() {
				return status;
			}

			public void progress() {
			}

			public Counter getCounter(String arg0, String arg1) {
				return counters.getGroup(arg0).findCounter(arg1);
			}

			public Counter getCounter(Enum<?> arg0) {
				return counters.findCounter(arg0);
			}

			@Override
			public float getProgress() {
				// TODO Auto-generated method stub
				return 0;
			}
		};

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

					StatusReporter reporter = new TestStatusReporter();

						proceedWithADelay(value, context);

		}

		void proceedWithADelay(Text value, Context context) throws IOException,
				InterruptedException {
			String line = value.toString();
			String[] words = line.split("\\W+");
			for (String word : words) {
				context.write(new Text(word), new IntWritable(1));
				Thread.sleep(100);
			}

		}

	}

	public void run(String inPath, String outPath, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println("In WordCount now...");
		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);
		conf.set("mapreduce.job.queuename", queue);
		jobHandle = new Job(conf, "queuePreemption");
		
		jobHandle.setJarByClass(WordCountForQueuePreemmption.class);
		jobHandle.setMapperClass(QPreemptiveMapper.class);

		jobHandle.setOutputKeyClass(Text.class);
		jobHandle.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(jobHandle, inputPath);
		FileOutputFormat.setOutputPath(jobHandle, outputPath);
		jobHandle.submit();		
		System.out.println("there... submitte job....now...");
	}


}
