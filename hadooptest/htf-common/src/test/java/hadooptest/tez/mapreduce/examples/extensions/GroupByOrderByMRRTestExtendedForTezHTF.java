package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.MRTezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest.MyGroupByReducer;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest.MyMapper;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest.MyOrderByNoOpReducer;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;

/**
 * These classes ending in *ExtendedForTezHTF are intermediate classes, that
 * live between the class that is distributed with original Tez JAR and the
 * actual test class (that has the @Test implementations) Since the Tez classes
 * sent out with the distribution 'extends Configured implements Tool' they are
 * designed to be invoked directly from the command line. When invoked from the
 * Command line, the run(String[] args) method [from the Tool class] gets
 * invoked. It parses the arguments and subsequently calls run(...) arguments,
 * that has the main body of the functionality. Since this 'run' method creates
 * Configuration objects (and that is where we toggle if this runs on local mode
 * or cluster mode) we need to overtide (copy/paste) that run method here and
 * override the getConf() method calls with {@code}
 * HtfTezUtils.setupConfForTez(conf, mode)
 * 
 * That would set up the local/cluster mode correctly.
 * 
 */

public class GroupByOrderByMRRTestExtendedForTezHTF extends
		GroupByOrderByMRRTest {

	/**
	 * Copy and paste the the code from the parent class's run method here.
	 * Change all references to getConf() to HtfTezUtils.setupConfForTez(conf,
	 * mode) Note: Be careful, there could be several run methods there, for
	 * example those contained inside a Processor, or that overriding the method
	 * in the Tool class.
	 * 
	 * @param args
	 * @param mode
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args, String mode) throws Exception {
	    Configuration conf = HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode);

	    // Configure intermediate reduces
	    conf.setInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 1);

	    // Set reducer class for intermediate reduce
	    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
	        "mapreduce.job.reduce.class"), MyGroupByReducer.class, Reducer.class);
	    // Set reducer output key class
	    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
	        "mapreduce.map.output.key.class"), IntWritable.class, Object.class);
	    // Set reducer output value class
	    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
	        "mapreduce.map.output.value.class"), Text.class, Object.class);
	    conf.setInt(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
	        "mapreduce.job.reduces"), 2);

	    String[] otherArgs = new GenericOptionsParser(conf, args).
	        getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: groupbyorderbymrrtest <in> <out>");
	      ToolRunner.printGenericCommandUsage(System.err);
	      return 2;
	    }

	    @SuppressWarnings("deprecation")
	    Job job = new Job(conf, "groupbyorderbymrrtest");

	    job.setJarByClass(GroupByOrderByMRRTest.class);

	    // Configure map
	    job.setMapperClass(MyMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // Configure reduce
	    job.setReducerClass(MyOrderByNoOpReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(1);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.submit();
	    JobID jobId = job.getJobID();
	    ApplicationId appId = TypeConverter.toYarn(jobId).getAppId();

	    DAGClient dagClient = MRTezClient.getDAGClient(appId, new TezConfiguration(conf), null);
	    DAGStatus dagStatus;
	    String[] vNames = { "initialmap" , "ireduce1" , "finalreduce" };
	    while (true) {
	      dagStatus = dagClient.getDAGStatus(null);
	      if(dagStatus.getState() == DAGStatus.State.RUNNING ||
	         dagStatus.getState() == DAGStatus.State.SUCCEEDED ||
	         dagStatus.getState() == DAGStatus.State.FAILED ||
	         dagStatus.getState() == DAGStatus.State.KILLED ||
	         dagStatus.getState() == DAGStatus.State.ERROR) {
	        break;
	      }
	      try {
	        Thread.sleep(500);
	      } catch (InterruptedException e) {
	        // continue;
	      }
	    }

	    while (dagStatus.getState() == DAGStatus.State.RUNNING) {
	      try {
	        ExampleDriver.printDAGStatus(dagClient, vNames);
	        try {
	          Thread.sleep(1000);
	        } catch (InterruptedException e) {
	          // continue;
	        }
	        dagStatus = dagClient.getDAGStatus(null);
	      } catch (TezException e) {
	        TestSession.logger.fatal("Failed to get application progress. Exiting");
	        return -1;
	      }
	    }

	    ExampleDriver.printDAGStatus(dagClient, vNames);
	    TestSession.logger.info("Application completed. " + "FinalState=" + dagStatus.getState());
	    return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
	}
}
