package hadooptest.workflow.hadoop.job;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import coretest.Util;

public class WordCountAPIJob extends Configured implements Tool {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private HashMap<String,Integer> buffer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            buffer = new HashMap<String, Integer>();
        }

        // Uses in-mapper combining !
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (buffer.containsKey(word)) {
                    buffer.put(word, buffer.get(word) + 1);
                } else {
                    buffer.put(word, 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : buffer.keySet()) {
                context.write(new Text(key), new IntWritable(buffer.get(key)));
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    
    // input: args[] ~ <input path>, <output path>, <job number>, <queue number>, <queue0 name>, <queue1 name>....
    public int run(String[] args) throws Exception {
    	int file_count = 0;
    	Random random = new Random();
    	
    	int jobNum = Integer.parseInt(args[2]);
    	int qNum = Integer.parseInt(args[3]);
    	
    	JobConf [] conf = new JobConf[qNum];   	
    	
        Job [][] job = new Job[qNum][jobNum];
        
        for (int q = 0; q < qNum; q++){
	        for (int i = 0; i < jobNum; i++){
	        	TestSession.logger.info("=== Submitting Job["+i+"] to Queue["+q+"] ===");
	        	
	        	conf[q] = new JobConf();
	        	conf[q].setQueueName(args[4+q]);
	        	job[q][i] = Job.getInstance(conf[q]);
	        	
	        	job[q][i].setOutputKeyClass(Text.class);
	        	job[q][i].setOutputValueClass(IntWritable.class);	        
		
	        	job[q][i].setMapperClass(TokenizerMapper.class);
	        	job[q][i].setCombinerClass(IntSumReducer.class);
	        	job[q][i].setReducerClass(IntSumReducer.class);
		        	        
	        	job[q][i].setInputFormatClass(TextInputFormat.class);
	        	job[q][i].setOutputFormatClass(TextOutputFormat.class);
		
	        	int randNum = 10 + random.nextInt(10);
	        	TestSession.logger.info("=== Load input file "+args[0]+"/"+Integer.toString((randNum))+".txt ===");
		        FileInputFormat.setInputPaths(job[q][i], new Path(args[0]+"/"+Integer.toString((randNum))+".txt"));
		        FileOutputFormat.setOutputPath(job[q][i], new Path(args[1] + "/" + Integer.toString(file_count)));
		        job[q][i].setJobName("word count");
		        
		        job[q][i].setJarByClass(WordCountAPIJob.class);
		
		        job[q][i].submit();
		        file_count++;
	        }
        }

		try{
	        for(int q = 0; q < qNum; q++){
		        for (int i = 0; i < jobNum; i++){
		        	TestSession.logger.info("=== Checking Job["+q+"]["+i+"] ===");
		    		assertTrue("Job["+q+"]["+i+"] failed",
		    				waitForSuccess(5, job[q][i]));
		        	}
	        }
		} catch(Exception e){
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
        return 0;
    }
    
    public boolean waitForSuccess(int minute, Job job) 
			throws InterruptedException, IOException {

		State currentState = null;
		
		// Give the sleep job time to complete
		for (int i = 0; i <= (minute * 6); i++) {

			currentState = job.getJobState();
			if (currentState.equals(State.SUCCEEDED)) {
				TestSession.logger.info("JOB " + job.getJobID() + " SUCCEEDED");
				return true;
			}
			else if (currentState.equals(State.PREP)) {
				TestSession.logger.info("JOB " + job.getJobID() + " IS STILL IN PREP STATE");
			}
			else if (currentState.equals(State.RUNNING)) {
				TestSession.logger.info("JOB " + job.getJobID() + " IS STILL RUNNING");
			}
			else if (currentState.equals(State.FAILED)) {
				TestSession.logger.info("JOB " + job.getJobID() + " FAILED");
				return false;
			}
			else if (currentState.equals(State.KILLED)) {
				TestSession.logger.info("JOB " + job.getJobID() + " WAS KILLED");
				return false;
			}
			Util.sleep(10);
		}

		TestSession.logger.error("JOB " + job.getJobID() + " didn't SUCCEED within the timeout window.");
		return false;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        ToolRunner.run(new WordCountAPIJob(), otherArgs);
    }
}