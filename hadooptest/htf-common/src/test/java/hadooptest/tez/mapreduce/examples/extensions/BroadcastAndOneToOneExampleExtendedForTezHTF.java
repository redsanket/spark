package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;

/**
 * Don't forget to include setFromConfiguration when creating the edge.
 * 
 * UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
 * .newBuilder(Text.class.getName(), IntWritable.class.getName())
 * .setFromConfiguration(tezConf).build();
 * 
 * Refer to: http://bug.corp.yahoo.com/show_bug.cgi?id=7122641
 * JIRA: https://issues.apache.org/jira/browse/TEZ-1587
 */

public class BroadcastAndOneToOneExampleExtendedForTezHTF extends
		BroadcastAndOneToOneExample {
	protected static String skipLocalityCheck = "-skipLocalityCheck";

	public int run(String[] args, String mode, Session session, String testName)
			throws Exception {
		boolean doLocalityCheck = true;
		if (args.length == 1) {
			if (args[0].equals(skipLocalityCheck)) {
				doLocalityCheck = false;
			} else {
				printUsage();
				throw new TezException("Invalid command line");
			}
		} else if (args.length > 1) {
			printUsage();
			throw new TezException("Invalid command line");
		}

		Configuration conf = TestSession.cluster.getConf();
		conf = HtfTezUtils.setupConfForTez(conf, mode, session, testName);
		if (doLocalityCheck
				&& conf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE,
						TezConfiguration.TEZ_LOCAL_MODE_DEFAULT)) {
			TestSession.logger
					.info("locality check is not valid in local mode. skipping");
			doLocalityCheck = false;
		}

		boolean status = run(conf, doLocalityCheck);
		return status ? 0 : 1;
	}

	@Override
	public boolean run(Configuration conf, boolean doLocalityCheck)
			throws Exception {
		System.out.println("Running BroadcastAndOneToOneExample");
		// conf and UGI
		TezConfiguration tezConf;
		if (conf != null) {
			tezConf = new TezConfiguration(conf);
		} else {
			tezConf = new TezConfiguration();
		}
		tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED,
				true);
		UserGroupInformation.setConfiguration(tezConf);
		String user = UserGroupInformation.getCurrentUser().getShortUserName();

		// staging dir
		FileSystem fs = FileSystem.get(tezConf);
		String stagingDirStr = tezConf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
				TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT)
				+ Path.SEPARATOR
				+ "BroadcastAndOneToOneExample"
				+ Path.SEPARATOR
				+ Long.toString(System.currentTimeMillis());

		Path stagingDir = new Path(stagingDirStr);
		tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
		stagingDir = fs.makeQualified(stagingDir);

		// No need to add jar containing this class as assumed to be part of
		// the tez jars.

		// TEZ-674 Obtain tokens based on the Input / Output paths. For now
		// assuming staging dir
		// is the same filesystem as the one used for Input/Output.
		TezClient tezSession = null;
		// needs session or else TaskScheduler does not hold onto containers
		tezSession = TezClient.create("broadcastAndOneToOneExample", tezConf);
		tezSession.start();

		DAGClient dagClient = null;

		try {
			DAG dag = createDAG(fs, tezConf, stagingDir, doLocalityCheck);

			tezSession.waitTillReady();
			dagClient = tezSession.submitDAG(dag);

			// monitoring
			DAGStatus dagStatus = dagClient
					.waitForCompletionWithStatusUpdates(null);
			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				System.out.println("DAG diagnostics: "
						+ dagStatus.getDiagnostics());
				return false;
			}
			return true;
		} finally {
			fs.delete(stagingDir, true);
			tezSession.stop();
		}
	}

	private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
			Path stagingDir, boolean doLocalityCheck) throws IOException,
			YarnException {

		int numBroadcastTasks = 2;
		int numOneToOneTasks = 3;
		if (doLocalityCheck) {
			YarnClient yarnClient = YarnClient.createYarnClient();
			yarnClient.init(tezConf);
			yarnClient.start();
			int numNMs = yarnClient.getNodeReports(NodeState.RUNNING).size();
			yarnClient.stop();
			// create enough 1-1 tasks to run in parallel
			numOneToOneTasks = numNMs - numBroadcastTasks - 1;// 1 AM
			if (numOneToOneTasks < 1) {
				numOneToOneTasks = 1;
			}
		}
		byte[] procByte = { (byte) (doLocalityCheck ? 1 : 0), 1 };
		UserPayload procPayload = UserPayload.create(ByteBuffer.wrap(procByte));

		System.out.println("Using " + numOneToOneTasks + " 1-1 tasks");

		Vertex broadcastVertex = Vertex.create("Broadcast",
				ProcessorDescriptor.create(InputProcessor.class.getName()),
				numBroadcastTasks);

		Vertex inputVertex = Vertex.create("Input",
				ProcessorDescriptor.create(InputProcessor.class.getName())
						.setUserPayload(procPayload), numOneToOneTasks);

		Vertex oneToOneVertex = Vertex.create("OneToOne",
				ProcessorDescriptor.create(OneToOneProcessor.class.getName())
						.setUserPayload(procPayload));
		oneToOneVertex.setVertexManagerPlugin(VertexManagerPluginDescriptor
				.create(InputReadyVertexManager.class.getName()));

		UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
				.newBuilder(Text.class.getName(), IntWritable.class.getName())
				.setFromConfiguration(tezConf).build();

		DAG dag = DAG.create("BroadcastAndOneToOneExample");
		dag.addVertex(inputVertex)
				.addVertex(broadcastVertex)
				.addVertex(oneToOneVertex)
				.addEdge(
						Edge.create(inputVertex, oneToOneVertex,
								edgeConf.createDefaultOneToOneEdgeProperty()))
				.addEdge(
						Edge.create(broadcastVertex, oneToOneVertex,
								edgeConf.createDefaultBroadcastEdgeProperty()));
		return dag;
	}

	/**
	 * Re-Provided here, because the corresponding method in the base class is
	 * marked private.
	 */
	private static void printUsage() {
		System.err.println("broadcastAndOneToOneExample " + skipLocalityCheck);
		ToolRunner.printGenericCommandUsage(System.err);
	}

}
