package hadooptest.workflow.hadoop.job;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;

import coretest.Util;

public class WordCountAPIFullCapacityJob extends Configured implements Tool {
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

    public int run(String[] args) throws Exception {
    	int fileCount = 0;
    	int jobCount;
    	Random random = new Random();
    	
    	int jobNum = Integer.parseInt(args[2]) - 1;
    	int qIndex = Integer.parseInt(args[3]);
    	    	
        Job [] job = new Job[jobNum];
        
	    for (int i = 0; i < jobNum; i++){
	        TestSession.logger.info("=== Submitting Job["+i+"] to Queue["+qIndex+"] ===");
	        
	        JobConf conf = new JobConf();
	        conf.setQueueName(args[4]);
	        job[i] = Job.getInstance(conf);
	        	
	        job[i].setOutputKeyClass(Text.class);
	        job[i].setOutputValueClass(IntWritable.class);	        
		
	        job[i].setMapperClass(TokenizerMapper.class);
	        job[i].setCombinerClass(IntSumReducer.class);
	        job[i].setReducerClass(IntSumReducer.class);
		        	        
	        job[i].setInputFormatClass(TextInputFormat.class);
	        job[i].setOutputFormatClass(TextOutputFormat.class);
		
	        int randNum = random.nextInt(20);
	        TestSession.logger.info("=== Load input file "+args[0]+"/"+Integer.toString((randNum))+".txt ===");
		    FileInputFormat.setInputPaths(job[i], new Path(args[0]+"/"+Integer.toString((randNum))+".txt"));
		    FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "/" + Integer.toString(qIndex) + "/" + Integer.toString(fileCount)));
		    job[i].setJobName("word count");
		        
		    job[i].setJarByClass(WordCountAPIFullCapacityJob.class);
		
		    job[i].submit();
		    fileCount++;
        }
	    jobCount = jobNum;
	    
	    long startTime = System.currentTimeMillis();
	    long endTime = startTime + Long.parseLong(args[5]);
	    TestSession.logger.info("Current time is: " + startTime/1000);
	    TestSession.logger.info("End time is: " + endTime/1000);

		try{
			int i = 0;
		    while(i < jobCount){
		        TestSession.logger.info("===> Checking Job["+qIndex+"]["+i+"] <===");
		    	assertTrue("Job["+qIndex+"]["+i+"] failed",
		    			waitForSuccess(5, job[i%jobNum]));
		    	if(System.currentTimeMillis() <= endTime){
		    		TestSession.logger.info("=== Submitting Job["+(i+jobNum)+"] to Queue["+qIndex+"] ===");
		        	
//			        waitForSubmission(jobNum, qIndex);
		    		
			        JobConf conf = new JobConf();
			        conf.setQueueName(args[4]);
			        job[i%jobNum] = Job.getInstance(conf);
			        	
			        job[i%jobNum].setOutputKeyClass(Text.class);
			        job[i%jobNum].setOutputValueClass(IntWritable.class);	        
				
			        job[i%jobNum].setMapperClass(TokenizerMapper.class);
			        job[i%jobNum].setCombinerClass(IntSumReducer.class);
			        job[i%jobNum].setReducerClass(IntSumReducer.class);
				        	        
			        job[i%jobNum].setInputFormatClass(TextInputFormat.class);
			        job[i%jobNum].setOutputFormatClass(TextOutputFormat.class);
				
			        int randNum = random.nextInt(20);
			        TestSession.logger.info("===> Load input file "+args[0]+"/"+Integer.toString((randNum))+".txt <===");
				    FileInputFormat.setInputPaths(job[i], new Path(args[0]+"/"+Integer.toString((randNum))+".txt"));
			        TestSession.logger.info("===> Load input file "+args[1]+"/"+Integer.toString((fileCount))+" <===");
				    FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "/" + Integer.toString(fileCount)));
				    job[i%jobNum].setJobName("word count");
				        
				    job[i%jobNum].setJarByClass(WordCountAPIFullCapacityJob.class);
				
				    job[i%jobNum].submit();
				    fileCount++;
				    jobCount++;
		    	}
		    	i++;
	        }
		} catch(Exception e){
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
        return 0;
    }
    
    public void waitForSubmission(int capacity, int i) throws IOException, InterruptedException{
		YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();		
		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		TestSession.logger.info("!!!!!!!!!!!!!!waiting for submitting jobs!!!!!!!!!!!!!");
    	int wait = 0;
    	while (wait < 2){
    		if (queues.get(i).getApplications().size() < capacity){
    			TestSession.logger.info("!!!!!!!!!!!!!!"+queues.get(i).getApplications() +"<"+capacity+"!!!!!!!!!!!!!");
    			return;
    		}
    		wait++;
    		Thread.sleep(10000);
    	}
    	TestSession.logger.info("Wait to submit timeout for 20 sec");
    	fail(); 	
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
        ToolRunner.run(new WordCountAPIFullCapacityJob(), otherArgs);
    }
}