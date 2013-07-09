package hadooptest.hadoop.regression.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

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

    public int run(String[] args) throws Exception {
    	int jobNum = Integer.parseInt(args[0]);
    	
//    	System.out.println("args[1] = "+args[1]);
//    	System.out.println("args[2] = "+args[2]);
//    	System.out.println("args[3] = "+args[3]);
    	
        Job [] job= new Job[jobNum];
        
        
        for (int i = 0; i < jobNum; i++){
        	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!Running Job"+i+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        	job[i] = new Job();
	        job[i].setOutputKeyClass(Text.class);
	        job[i].setOutputValueClass(IntWritable.class);
	        
	
	        job[i].setMapperClass(TokenizerMapper.class);
	        job[i].setCombinerClass(IntSumReducer.class);
	        job[i].setReducerClass(IntSumReducer.class);
	        	        
	        job[i].setInputFormatClass(TextInputFormat.class);
	        job[i].setOutputFormatClass(TextOutputFormat.class);
	
	        FileInputFormat.setInputPaths(job[i], new Path(args[1]));
	        FileOutputFormat.setOutputPath(job[i], new Path(args[2] + "/" + Integer.toString(i)));
	        job[i].setJobName(args[3]);
	        
	        job[i].setJarByClass(WordCountAPIJob.class);
	
	        job[i].submit();
	        
        }
        
        for (int i = 0; i < jobNum; i++){
        	job[i].waitForCompletion(true);
        }
                
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        ToolRunner.run(new WordCountAPIJob(), otherArgs);
    }
}