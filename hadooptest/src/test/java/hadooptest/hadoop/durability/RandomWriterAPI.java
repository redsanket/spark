
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hadooptest.hadoop.durability;

import hadooptest.TestSession;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program uses map/reduce to just run a distributed job where there is
 * no interaction between the tasks and each task write a large unsorted
 * random binary sequence file of BytesWritable.
 * In order for this program to generate data for terasort with 10-byte keys
 * and 90-byte values, have the following config:
 * <xmp>
 * <?xml version="1.0"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *   <property>
 *     <name>mapreduce.randomwriter.minkey</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.maxkey</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.minvalue</name>
 *     <value>90</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.maxvalue</name>
 *     <value>90</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.totalbytes</name>
 *     <value>1099511627776</value>
 *   </property>
 * </configuration></xmp>
 * 
 * Equivalently, {@link RandomWriter} also supports all the above options
 * and ones supported by {@link GenericOptionsParser} via the command-line.
 */
public class RandomWriterAPI extends Configured implements Tool {
  public static final String TOTAL_BYTES = "mapreduce.randomwriter.totalbytes";
  public static final String BYTES_PER_MAP = 
    "mapreduce.randomwriter.bytespermap";
  public static final String MAPS_PER_HOST = 
    "mapreduce.randomwriter.mapsperhost";
  public static final String MAX_VALUE = "mapreduce.randomwriter.maxvalue";
  public static final String MIN_VALUE = "mapreduce.randomwriter.minvalue";
  public static final String MIN_KEY = "mapreduce.randomwriter.minkey";
  public static final String MAX_KEY = "mapreduce.randomwriter.maxkey";
  
  /**
   * User counters
   */
  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }
  
  /**
   * A custom input format that creates virtual inputs of a single string
   * for each map.
   */
  static class RandomInputFormat extends InputFormat<Text, Text> {

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      List<InputSplit> result = new ArrayList<InputSplit>();
      Path outDir = FileOutputFormat.getOutputPath(job);
      int numSplits = 
            job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      for(int i=0; i < numSplits; ++i) {
        result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
                                  (String[])null));
      }
      return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader extends RecordReader<Text, Text> {
      Path name;
      Text key = null;
      Text value = new Text();
      public RandomRecordReader(Path p) {
        name = p;
      }
      
      public void initialize(InputSplit split,
                             TaskAttemptContext context)
      throws IOException, InterruptedException {
    	  
      }
      
      public boolean nextKeyValue() {
        if (name != null) {
          key = new Text();
          key.set(name.getName());
          name = null;
          return true;
        }
        return false;
      }
      
      public Text getCurrentKey() {
        return key;
      }
      
      public Text getCurrentValue() {
        return value;
      }
      
      public void close() {}

      public float getProgress() {
        return 0.0f;
      }
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
  }

  static class RandomMapper extends Mapper<WritableComparable, Writable,
                      BytesWritable, BytesWritable> {
    
    private long numBytesToWrite;
    private int minKeySize;
    private int keySizeRange;
    private int minValueSize;
    private int valueSizeRange;
    private Random random = new Random();
    private BytesWritable randomKey = new BytesWritable();
    private BytesWritable randomValue = new BytesWritable();
    
    private void randomizeBytes(byte[] data, int offset, int length) {
      for(int i=offset + length - 1; i >= offset; --i) {
        data[i] = (byte) random.nextInt(256);
      }
    }
    
    /**
     * Given an output filename, write a bunch of random records to it.
     */
    public void map(WritableComparable key, 
                    Writable value,
                    Context context) throws IOException,InterruptedException {
      int itemCount = 0;
      while (numBytesToWrite > 0) {
        int keyLength = minKeySize + 
          (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
        randomKey.setSize(keyLength);
        randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
        int valueLength = minValueSize +
          (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
        randomValue.setSize(valueLength);
        randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
        context.write(randomKey, randomValue);
        numBytesToWrite -= keyLength + valueLength;
        context.getCounter(Counters.BYTES_WRITTEN).increment(keyLength + valueLength);
        context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
        if (++itemCount % 200 == 0) {
          context.setStatus("wrote record " + itemCount + ". " + 
                             numBytesToWrite + " bytes left.");
        }
      }
      context.setStatus("done with " + itemCount + " records.");
    }
    
    /**
     * Save the values out of the configuaration that we need to write
     * the data.
     */
    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      numBytesToWrite = conf.getLong(BYTES_PER_MAP,1*1024*1024*1024);
      minKeySize = conf.getInt(MIN_KEY, 10);
      keySizeRange = 
        conf.getInt(MAX_KEY, 1000) - minKeySize;
      minValueSize = conf.getInt(MIN_VALUE, 0);
      valueSizeRange = 
        conf.getInt(MAX_VALUE, 20000) - minValueSize;
    }
  }
  
  /**
   * This is the main routine for launching a distributed random write job.
   * It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
   * The reduce doesn't do anything.
   * 
   * @throws IOException 
   */
  public int run(String[] args) throws Exception {    
    if (args.length == 0) {
      System.out.println("Usage: writer <out-dir> <TOTAL_BYTES_VAL> <level> <jobNum>");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }
    
    // 	  0		   1		2	   		3					4
	//<out-dir> <level> <jobNum><MAPS_PER_HOST_VAL><BYTES_PER_MAP_VAL>
    double 	targetLevel = (args[1] == "")?0:Double.parseDouble(args[1]);
    long 	jobNum = (args[2]=="")?0:Long.parseLong(args[2]);
    int 	MAPS_PER_HOST_VAL = (args[3]=="")?10:Integer.parseInt(args[3]);
    long 	BYTES_PER_MAP_VAL = (args[4]=="")?1024:Long.parseLong(args[4]);

    TestSession.logger.info("======== targetLevel = "+targetLevel+" ========");
    TestSession.logger.info("======== jobNum = "+jobNum+" ========");
    TestSession.logger.info("======== MAPS_PER_HOST_VAL = "+MAPS_PER_HOST_VAL+" ========");
    TestSession.logger.info("======== BYTES_PER_MAP_VAL = "+BYTES_PER_MAP_VAL+" ========");

    Configuration conf = getConf();
    JobClient client = new JobClient(conf);
    ClusterStatus cluster = client.getClusterStatus();
    
    conf.setInt(MAPS_PER_HOST, MAPS_PER_HOST_VAL);
    conf.setLong(BYTES_PER_MAP, BYTES_PER_MAP_VAL);
    
    int numMapsPerHost = conf.getInt(MAPS_PER_HOST, 10);
    long numBytesToWritePerMap = conf.getLong(BYTES_PER_MAP,1024*1024*1024);
    long totalBytesToWrite = conf.getLong(TOTAL_BYTES,numMapsPerHost*numBytesToWritePerMap*cluster.getTaskTrackers());
       
    TestSession.logger.info("numMapsPerHost = "+numMapsPerHost);
    TestSession.logger.info("numBytesToWritePerMap = "+numBytesToWritePerMap);
    TestSession.logger.info("cluster.getTaskTrackers() = "+cluster.getTaskTrackers());
    TestSession.logger.info("totalBytesToWrite = "+totalBytesToWrite);

    if (numBytesToWritePerMap == 0) {
    	TestSession.logger.error("Cannot have" + BYTES_PER_MAP + " set to 0");
    	return -2;
    }
    int numMaps = (int) (totalBytesToWrite / numBytesToWritePerMap);
    if (numMaps == 0 && totalBytesToWrite > 0) {
      numMaps = 1;
      conf.setLong(BYTES_PER_MAP, totalBytesToWrite);
    }
    conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
	TestSession.logger.info("Running " + numMaps + " maps.");

    
    int ret = 0;
	DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd___HH_mm_ss___");
    int jobCounter = 0;
    
    if(targetLevel > 0){
    	TestSession.logger.info("====================== Level Mode ======================");
	    // get status of hdfs
    	FileSystem fs = TestSession.cluster.getFS();
		FsStatus status = fs.getStatus();
		long capacity = status.getCapacity();
		long remain = status.getRemaining();
		double DFSUsedLevel = (double)status.getUsed()/(double)capacity;

		TestSession.logger.info("Remaining	Level = "+((double)remain/(double)capacity));
	    TestSession.logger.info("DFSUsed	Level = "+DFSUsedLevel);
	    TestSession.logger.info("Target		Level = "+targetLevel);
	    	
	    DecimalFormat twoDForm = new DecimalFormat("###.###");
		while(DFSUsedLevel < targetLevel){//Double.valueOf(twoDForm.format(DFSUsedLevel*100))
			TestSession.logger.info("======= DFS used %"+ Double.valueOf(twoDForm.format(DFSUsedLevel*100))+", Filling it to %"+targetLevel*100+" =======");
			TestSession.logger.info("======= Job "+jobCounter+" =======");
	    	Job job = new Job(conf);
	      
			Date date = new Date();
			Path outDir = new Path(args[0]+"/"+dateFormat.format(date).toString()+ jobCounter);
			job.setJarByClass(RandomWriterAPI.class);
			
			job.setJobName("random-writer");
			FileOutputFormat.setOutputPath(job, outDir);
			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(BytesWritable.class);
			job.setInputFormatClass(RandomInputFormat.class);
			job.setMapperClass(RandomMapper.class);        
			job.setReducerClass(Reducer.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
	      
			// reducer NONE
			job.setNumReduceTasks(0);
		    ret += job.waitForCompletion(true) ? 0 : 1;
			jobCounter++;
			DFSUsedLevel = (double)fs.getStatus().getUsed()/(double)capacity;
	    }
		TestSession.logger.info("======= After Test, DFS used %"+ Double.valueOf(twoDForm.format(DFSUsedLevel*100))+" =======");
    } else if (jobNum > 0){

    	TestSession.logger.info("====================== Job   Mode ======================");
        Vector<Job> rwjobs = new  Vector<Job>();
    	while(jobCounter < jobNum){
    	  	
    		TestSession.logger.info("======= Job "+jobCounter+" =======");
    		Job job = new Job(conf);
    	      
    		Date date = new Date();
    		Path outDir = new Path(args[0]+"/"+dateFormat.format(date).toString()+ jobCounter);
    		job.setJarByClass(RandomWriterAPI.class);

    		job.setJobName("random-writer");
    		FileOutputFormat.setOutputPath(job, outDir);
    		job.setOutputKeyClass(BytesWritable.class);
    		job.setOutputValueClass(BytesWritable.class);
    		job.setInputFormatClass(RandomInputFormat.class);
    		job.setMapperClass(RandomMapper.class);        
    		job.setReducerClass(Reducer.class);
    		job.setOutputFormatClass(SequenceFileOutputFormat.class);
    	      
    		job.setNumReduceTasks(0);
    		job.submit();
    		rwjobs.add(job);
    		jobCounter++;
    	}
    	for(int i = 0; i < rwjobs.size(); i++)
    		ret += rwjobs.get(i).waitForCompletion(true) ? 0 : 1;
    } else
    	TestSession.logger.info("====================== Error Mode ======================");
    return ret;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RandomWriterAPI(), args);
    System.exit(res);
  }
}

