package hadooptest.hadoop.stress.tokenRenewal;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;

// Mapper and Reducer classes for the wordcount job
public class Map implements Mapper<LongWritable, Text, Text, IntWritable> {
	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();
	static enum Counters {
		WORDS_BEGINNING_WITH_SPECIAL_CHARS, WORDS_ENDING_IN_SPECIAL_CHARS, PURE_REGEXES, WORDS_BEGINNING_AND_ENDING_IN_REGEXES, WORDS_HAVING_REGEXES_IN_THE_MIDDLE, ALPHA_NUM_WORDS
	};

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
	String casFile = null;
	String inputFile = null;
	private int numRecords = 0;

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

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			output.collect(word, one);
			if (word.toString().matches("^\\W\\w+$")) {
				// Words beginning with special characters
				reporter.incrCounter(
						Counters.WORDS_BEGINNING_WITH_SPECIAL_CHARS, 1);
			} else if (word.toString().matches("\\w+\\W$")) {
				// Words ending in special characters
				reporter.incrCounter(Counters.WORDS_ENDING_IN_SPECIAL_CHARS, 1);
			} else if (word.toString().matches("\\W+$")) {
				// Pure RegExes
				reporter.incrCounter(Counters.PURE_REGEXES, 1);
			} else if (key.toString().matches("\\W+\\w+\\W+$")) {
				// Words beginning and ending with a RegEx
				reporter.incrCounter(
						Counters.WORDS_BEGINNING_AND_ENDING_IN_REGEXES, 1);
			} else if (key.toString().matches("\\w+\\W+\\w+$")) {
				// Words having regexes in the middle
				reporter.incrCounter(
						Counters.WORDS_HAVING_REGEXES_IN_THE_MIDDLE, 1);
			} else {
				reporter.incrCounter(Counters.ALPHA_NUM_WORDS, 1);
			}
			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing records "
						+ "from the input file: " + inputFile + " and CAS:"
						+ casFile);
			}
		}
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}