package hadooptest.tez.mapreduce.examples.extensions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.MRTezClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest.MyGroupByReducer;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest.MyMapper;
import org.apache.tez.mapreduce.examples.GroupByOrderByMRRTest.MyOrderByNoOpReducer;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

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
	public int run(String[] args, String mode, Session session, TimelineServer timelineServer, String testName) throws Exception {
	    Configuration conf = HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName);

	    String[] otherArgs = new GenericOptionsParser(conf, args).
	            getRemainingArgs();
	        if (otherArgs.length != 2) {
	          System.err.println("Usage: groupbyorderbymrrtest <in> <out>");
	          ToolRunner.printGenericCommandUsage(System.err);
	          return 2;
	        }

	        String inputPath = otherArgs[0];
	        String outputPath = otherArgs[1];

	        UserGroupInformation.setConfiguration(conf);

	        TezConfiguration tezConf = new TezConfiguration(conf);
	        FileSystem fs = FileSystem.get(conf);

	        if (fs.exists(new Path(outputPath))) {
	          throw new FileAlreadyExistsException("Output directory "
	              + outputPath + " already exists");
	        }

	        Map<String, LocalResource> localResources =
	            new TreeMap<String, LocalResource>();

	        String stagingDirStr =  conf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
	            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT) + Path.SEPARATOR +
	            Long.toString(System.currentTimeMillis());
	        Path stagingDir = new Path(stagingDirStr);
	        FileSystem pathFs = stagingDir.getFileSystem(tezConf);
	        pathFs.mkdirs(new Path(stagingDirStr));

	        tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
	        stagingDir = pathFs.makeQualified(new Path(stagingDirStr));

		    Random randomGenerator = new Random();
		    int randomInt = randomGenerator.nextInt(100000);
		    String tezClientName= "groupbyorderbymrrtest" + randomInt;

	        TezClient tezClient = TezClient.create(tezClientName, tezConf);
	        tezClient.start();

	        TestSession.logger.info("Submitting groupbyorderbymrrtest DAG as a new Tez Application");

	        try {
	          DAG dag = createDAG(conf, localResources, stagingDir, inputPath, outputPath, true);

	          tezClient.waitTillReady();

	          DAGClient dagClient = tezClient.submitDAG(dag);

	          DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
	          if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	        	  TestSession.logger.error("groupbyorderbymrrtest failed, state=" + dagStatus.getState()
	                + ", diagnostics=" + dagStatus.getDiagnostics());
	            return -1;
	          }
	          TestSession.logger.info("Application completed. " + "FinalState=" + dagStatus.getState());
	          return 0;
	        } finally {
	          tezClient.stop();
	        }
	}
	
	  private static DAG createDAG(Configuration conf, Map<String, LocalResource> commonLocalResources,
		      Path stagingDir, String inputPath, String outputPath, boolean useMRSettings)
		      throws Exception {


		    Configuration mapStageConf = new JobConf(conf);
		    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR,
		        MyMapper.class.getName());

		    MRHelpers.translateMRConfToTez(mapStageConf);

		    Configuration iReduceStageConf = new JobConf(conf);
		    // TODO replace with auto-reduce parallelism
		    iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, 2);
		    iReduceStageConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
		        MyGroupByReducer.class.getName());
		    iReduceStageConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
		    iReduceStageConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
		        IntWritable.class.getName());
		    iReduceStageConf.setBoolean("mapred.mapper.new-api", true);
		    MRHelpers.translateMRConfToTez(iReduceStageConf);

		    Configuration finalReduceConf = new JobConf(conf);
		    finalReduceConf.setInt(MRJobConfig.NUM_REDUCES, 1);
		    finalReduceConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
		        MyOrderByNoOpReducer.class.getName());
		    finalReduceConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
		    finalReduceConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
		    MRHelpers.translateMRConfToTez(finalReduceConf);

		    MRHelpers.configureMRApiUsage(mapStageConf);
		    MRHelpers.configureMRApiUsage(iReduceStageConf);
		    MRHelpers.configureMRApiUsage(finalReduceConf);

		    List<Vertex> vertices = new ArrayList<Vertex>();

		    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);
		    mapStageConf.writeXml(outputStream);
		    String mapStageHistoryText = new String(outputStream.toByteArray(), "UTF-8");
		    mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
		        TextInputFormat.class.getName());
		    mapStageConf.set(FileInputFormat.INPUT_DIR, inputPath);
		    mapStageConf.setBoolean("mapred.mapper.new-api", true);
		    DataSourceDescriptor dsd = MRInputHelpers.configureMRInputWithLegacySplitGeneration(
		        mapStageConf, stagingDir, true);

		    Vertex mapVertex;
		    ProcessorDescriptor mapProcessorDescriptor =
		        ProcessorDescriptor.create(MapProcessor.class.getName())
		            .setUserPayload(
		                TezUtils.createUserPayloadFromConf(mapStageConf))
		            .setHistoryText(mapStageHistoryText);
		    if (!useMRSettings) {
		      mapVertex = Vertex.create("initialmap", mapProcessorDescriptor);
		    } else {
		      mapVertex = Vertex.create("initialmap", mapProcessorDescriptor, -1,
		          MRHelpers.getResourceForMRMapper(mapStageConf));
		      mapVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRMapper(mapStageConf));
		    }
		    mapVertex.addTaskLocalFiles(commonLocalResources)
		        .addDataSource("MRInput", dsd);
		    vertices.add(mapVertex);

		    ByteArrayOutputStream iROutputStream = new ByteArrayOutputStream(4096);
		    iReduceStageConf.writeXml(iROutputStream);
		    String iReduceStageHistoryText = new String(iROutputStream.toByteArray(), "UTF-8");

		    ProcessorDescriptor iReduceProcessorDescriptor = ProcessorDescriptor.create(
		        ReduceProcessor.class.getName())
		        .setUserPayload(TezUtils.createUserPayloadFromConf(iReduceStageConf))
		        .setHistoryText(iReduceStageHistoryText);

		    Vertex intermediateVertex;
		    if (!useMRSettings) {
		      intermediateVertex = Vertex.create("ireduce1", iReduceProcessorDescriptor, 1);
		    } else {
		      intermediateVertex = Vertex.create("ireduce1", iReduceProcessorDescriptor,
		          1, MRHelpers.getResourceForMRReducer(iReduceStageConf));
		      intermediateVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(iReduceStageConf));
		    }
		    intermediateVertex.addTaskLocalFiles(commonLocalResources);
		    vertices.add(intermediateVertex);

		    ByteArrayOutputStream finalReduceOutputStream = new ByteArrayOutputStream(4096);
		    finalReduceConf.writeXml(finalReduceOutputStream);
		    String finalReduceStageHistoryText = new String(finalReduceOutputStream.toByteArray(), "UTF-8");
		    UserPayload finalReducePayload = TezUtils.createUserPayloadFromConf(finalReduceConf);
		    Vertex finalReduceVertex;

		    ProcessorDescriptor finalReduceProcessorDescriptor =
		        ProcessorDescriptor.create(
		            ReduceProcessor.class.getName())
		            .setUserPayload(finalReducePayload)
		            .setHistoryText(finalReduceStageHistoryText);
		    if (!useMRSettings) {
		      finalReduceVertex = Vertex.create("finalreduce", finalReduceProcessorDescriptor, 1);
		    } else {
		      finalReduceVertex = Vertex.create("finalreduce", finalReduceProcessorDescriptor, 1,
		          MRHelpers.getResourceForMRReducer(finalReduceConf));
		      finalReduceVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(finalReduceConf));
		    }
		    finalReduceVertex.addTaskLocalFiles(commonLocalResources);
		    finalReduceVertex.addDataSink("MROutput",
		        MROutputLegacy.createConfigBuilder(finalReduceConf, TextOutputFormat.class, outputPath)
		            .build());
		    vertices.add(finalReduceVertex);

		    DAG dag = DAG.create("groupbyorderbymrrtest");
		    for (Vertex v : vertices) {
		      dag.addVertex(v);
		    }

		    OrderedPartitionedKVEdgeConfig edgeConf1 = OrderedPartitionedKVEdgeConfig
		        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
		            HashPartitioner.class.getName()).setFromConfiguration(conf)
		        .configureInput().useLegacyInput().done().build();
		    dag.addEdge(
		        Edge.create(dag.getVertex("initialmap"), dag.getVertex("ireduce1"),
		            edgeConf1.createDefaultEdgeProperty()));

		    OrderedPartitionedKVEdgeConfig edgeConf2 = OrderedPartitionedKVEdgeConfig
		        .newBuilder(IntWritable.class.getName(), Text.class.getName(),
		            HashPartitioner.class.getName()).setFromConfiguration(conf)
		        .configureInput().useLegacyInput().done().build();
		    dag.addEdge(
		        Edge.create(dag.getVertex("ireduce1"), dag.getVertex("finalreduce"),
		            edgeConf2.createDefaultEdgeProperty()));

		    return dag;
		  }

}
