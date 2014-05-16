package hadooptest.hadoop.stress.tokenRenewal;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper and Reducer classes for the wordcount job
public class TimedCpuHoggerMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static enum Counters {
		COUNT_OF_RECORDS_PROCESSED
	};

	public static int timeoutInSecs = 30;

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
	String casFile = null;
	String inputFile = null;
	private int numRecords = 0;
	long currTime = System.currentTimeMillis();
	long stopTime = currTime + (timeoutInSecs * 1000);

	public void configure(JobConf jobConf) {
		inputFile = jobConf.get("map.input.file");
		if (jobConf.getBoolean("wordcount.skip.patterns", false)) {
			Job job;
			try {
				job = Job.getInstance(jobConf);

				URI cacheFiles[] = job.getCacheFiles();
				job.getLocalCacheFiles();
				for (URI cacheFile : cacheFiles) {
					System.out.println(cacheFile.toString());
					casFile = cacheFile.toString();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException {
		Random rand = new Random();
		currTime = System.currentTimeMillis();
		
		if (currTime < stopTime) {
			context.getCounter(Counters.COUNT_OF_RECORDS_PROCESSED).increment(1);
			rand = new Random();
			Integer.toString(rand.nextInt()).hashCode();
//			try {
////				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}

	}

	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}