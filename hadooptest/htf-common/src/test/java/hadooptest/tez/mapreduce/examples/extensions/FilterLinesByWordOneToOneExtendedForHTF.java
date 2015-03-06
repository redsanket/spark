package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.mapreduce.examples.FilterLinesByWord.TextLongPair;
import org.apache.tez.mapreduce.examples.FilterLinesByWordOneToOne;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;
import org.apache.tez.mapreduce.examples.processor.FilterByWordInputProcessor;
import org.apache.tez.mapreduce.examples.processor.FilterByWordOutputProcessor;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;

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

public class FilterLinesByWordOneToOneExtendedForHTF extends
		FilterLinesByWordOneToOne {
	/**
	 * Re-Provided here, because the corresponding method in the base class is
	 * marked private.
	 */
	private static void printUsage() {
		System.err
				.println("Usage filterLinesByWordOneToOne <in> <out> <filter_word>"
						+ " [-generateSplitsInClient true/<false>]");
		ToolRunner.printGenericCommandUsage(System.err);
	}

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
	public int run(String[] otherArgs, String mode, Session session, TimelineServer timelineServer, String testName) throws Exception {
	    boolean generateSplitsInClient = false;
	    SplitsInClientOptionParser splitCmdLineParser = new SplitsInClientOptionParser();
	    try {
	      generateSplitsInClient = splitCmdLineParser.parse(otherArgs, false);
	      otherArgs = splitCmdLineParser.getRemainingArgs();
	    } catch (ParseException e1) {
	      System.err.println("Invalid options");
	      printUsage();
	      return 2;
	    }

	    if (otherArgs.length != 3) {
	      printUsage();
	      return 2;
	    }

	    String inputPath = otherArgs[0];
	    String outputPath = otherArgs[1];
	    String filterWord = otherArgs[2];
	    
	    Configuration conf = HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName);
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(new Path(outputPath))) {
	      System.err.println("Output directory : " + outputPath + " already exists");
	      return 2;
	    }

	    TezConfiguration tezConf = new TezConfiguration(conf);

	    fs.getWorkingDirectory();
	    Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString());
	    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());
	    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

	    String jarPath = ClassUtil.findContainingJar(FilterLinesByWordOneToOne.class);
	    if (jarPath == null) {
	      throw new TezUncheckedException("Could not find any jar containing"
	          + FilterLinesByWordOneToOne.class.getName() + " in the classpath");
	    }

	    Path remoteJarPath = fs.makeQualified(new Path(stagingDir, "dag_job.jar"));
	    fs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
	    FileStatus remoteJarStatus = fs.getFileStatus(remoteJarPath);

	    Map<String, LocalResource> commonLocalResources = new TreeMap<String, LocalResource>();
	    LocalResource dagJarLocalRsrc = LocalResource.newInstance(
	        ConverterUtils.getYarnUrlFromPath(remoteJarPath),
	        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
	        remoteJarStatus.getLen(), remoteJarStatus.getModificationTime());
	    commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);


	    String tezClientName= "FilterLinesByWordSession";

	    TezClient tezSession = TezClient.create(tezClientName, tezConf,
	        commonLocalResources, null);
	    tezSession.start(); // Why do I need to start the TezSession.

	    Configuration stage1Conf = new JobConf(conf);
	    stage1Conf.set(FILTER_PARAM_NAME, filterWord);

	    Configuration stage2Conf = new JobConf(conf);

	    stage2Conf.set(FileOutputFormat.OUTDIR, outputPath);
	    stage2Conf.setBoolean("mapred.mapper.new-api", false);

	    UserPayload stage1Payload = TezUtils.createUserPayloadFromConf(stage1Conf);
	    // Setup stage1 Vertex
	    Vertex stage1Vertex = Vertex.create("stage1", ProcessorDescriptor.create(
	        FilterByWordInputProcessor.class.getName()).setUserPayload(stage1Payload))
	        .addTaskLocalFiles(commonLocalResources);

	    DataSourceDescriptor dsd;
	    if (generateSplitsInClient) {
	      // TODO TEZ-1406. Dont' use MRInputLegacy
	      stage1Conf.set(FileInputFormat.INPUT_DIR, inputPath);
	      stage1Conf.setBoolean("mapred.mapper.new-api", false);
	      dsd = MRInputHelpers.configureMRInputWithLegacySplitGeneration(stage1Conf, stagingDir, true);
	    } else {
	      dsd = MRInputLegacy.createConfigBuilder(stage1Conf, TextInputFormat.class, inputPath)
	          .groupSplits(false).build();
	    }
	    stage1Vertex.addDataSource("MRInput", dsd);

	    // Setup stage2 Vertex
	    Vertex stage2Vertex = Vertex.create("stage2", ProcessorDescriptor.create(
	        FilterByWordOutputProcessor.class.getName()).setUserPayload(TezUtils
	        .createUserPayloadFromConf(stage2Conf)), dsd.getNumberOfShards());
	    stage2Vertex.addTaskLocalFiles(commonLocalResources);

	    // Configure the Output for stage2
	    stage2Vertex.addDataSink(
	        "MROutput",
	        DataSinkDescriptor.create(OutputDescriptor.create(MROutput.class.getName())
	            .setUserPayload(TezUtils.createUserPayloadFromConf(stage2Conf)),
	            OutputCommitterDescriptor.create(MROutputCommitter.class.getName()), null));

	    UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
	        .newBuilder(Text.class.getName(), TextLongPair.class.getName())
	        .setFromConfiguration(tezConf).build();

	    DAG dag = DAG.create("FilterLinesByWord");
	    Edge edge =
	        Edge.create(stage1Vertex, stage2Vertex, edgeConf.createDefaultOneToOneEdgeProperty());
	    dag.addVertex(stage1Vertex).addVertex(stage2Vertex).addEdge(edge);

	    TestSession.logger.info("Submitting DAG to Tez Session");
	    DAGClient dagClient = tezSession.submitDAG(dag);
	    TestSession.logger.info("Submitted DAG to Tez Session");

	    DAGStatus dagStatus = null;
	    String[] vNames = { "stage1", "stage2" };
	    try {
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
	    } finally {
	      fs.delete(stagingDir, true);
	      tezSession.stop();
	    }

	    ExampleDriver.printDAGStatus(dagClient, vNames);
	    TestSession.logger.info("Application completed. " + "FinalState=" + dagStatus.getState());
	    return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
	}
}
