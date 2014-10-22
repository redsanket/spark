package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class TestSpeculativeExecution extends Configured {
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	public enum TEST_TYPE {
		START_LATE, STALL_AT_80_PERCENT

	};

	public static enum SP_EX_COUNTER {
		COUNT
	}

	static TEST_TYPE currentTestType;
	static boolean STALLING_IN_PROGRESS = false;
	static int MAX_LOOP = 500;

	public static class SpeculativeMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public SpeculativeMapper() {

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
			if (context.getConfiguration().get("htf.test.type")
					.equals(TEST_TYPE.START_LATE)) {
				if (context.getTaskAttemptID().getTaskID().getId() == 13) {
					System.out.println("Now procesing task:"
							+ context.getTaskAttemptID().getTaskID().getId());

					StatusReporter reporter = new TestStatusReporter();
					if (context.getTaskAttemptID().getId() == 0) {
						// System.out.println("Now procesing attempt:"
						// + context.getTaskAttemptID().getId());
						// Stall the 1st attempt
						for (int xx = 0; xx < 500; xx++) {
							Thread.sleep(1000);
							reporter.setStatus("WIP");
							reporter.getCounter(SP_EX_COUNTER.COUNT).increment(
									1);
						}
					} else {
						// System.out.println("Not the 0th attempt, but:"
						// + context.getTaskAttemptID().getId());
						proceedNormally(value, context);
					}
				}
			} else if (context.getConfiguration().get("htf.test.type")
					.equals(TEST_TYPE.STALL_AT_80_PERCENT.name())) {
				if (context.getTaskAttemptID().getTaskID().getId() == 13) {
					StatusReporter reporter = new TestStatusReporter();
					if (context.getTaskAttemptID().getId() == 0) {
						long currTime = System.currentTimeMillis();
						System.out.println ("Begin time:" + beginTime + " curr time:" + currTime);
						if (((currTime - beginTime) / 1000) > 72) {
							for (int xx = 0; xx < 500; xx++) {
								Thread.sleep(1000);
								reporter.setStatus("WIP");
								reporter.getCounter(SP_EX_COUNTER.COUNT)
										.increment(1);

							}
						} else {
							proceedNormally(value, context);
						}
					} else {
						// Every subsequent attempt, proceed normally
						proceedNormally(value, context);
					}
				} else {
					proceedNormally(value, context);
				}
			}
		}

		void proceedNormally(Text value, Context context) throws IOException,
				InterruptedException {
			String line = value.toString();
			String[] words = line.split("\\W+");
			for (String word : words) {
				context.write(new Text(word), new IntWritable(1));
			}

		}

	}

	public int run(String inPath, String outPath, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {

		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);
		Job job = new Job(conf, currentTestType.name());

		job.setJarByClass(TestSpeculativeExecution.class);
		job.setMapperClass(SpeculativeMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
		return 0;
	}

	// @Test
	public void testLateStart() throws ClassNotFoundException, IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", true);
		currentTestType = TEST_TYPE.START_LATE;
		run("/tmp/bigfile4.txt", "/tmp/specEx", conf);

	}

	@Test
	public void testStallAt80() throws ClassNotFoundException, IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", true);
		currentTestType = TEST_TYPE.STALL_AT_80_PERCENT;
		conf.set("htf.test.type", currentTestType.name());
		run("/tmp/bigfile4.txt", "/tmp/specEx", conf);

	}

}
