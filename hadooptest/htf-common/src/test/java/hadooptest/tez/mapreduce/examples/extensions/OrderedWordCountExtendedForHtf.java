package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.mapreduce.examples.TestOrderedWordCount;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;

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
 * NOTE: Changes to be done, should the file need to be refreshed
 * <p>
 * 1) Ensure that all references to getConf fetch a configuration instance from
 * {@link} HtfTezUtils
 * </p>
 */
public class OrderedWordCountExtendedForHtf extends TestOrderedWordCount {
	private static void printUsage() {
		String options = " [-generateSplitsInClient true/<false>]";
		System.err.println("Usage: testorderedwordcount <in> <out>" + options);
		System.err.println("Usage (In Session Mode):"
				+ " testorderedwordcount <in1> <out1> ... <inN> <outN>"
				+ options);
		ToolRunner.printGenericCommandUsage(System.err);
	}

	/**
	 * Re-Provided here, because the corresponding method in the base class is
	 * marked private.
	 */
	private static void waitForTezSessionReady(TezClient tezSession)
			throws IOException, TezException, InterruptedException {
		tezSession.waitTillReady();
	}

	private Credentials credentials = new Credentials();

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

	public int run(String[] args, String mode, Session session,
			TimelineServer timelineServer, String testName) throws Exception {
		TestSession.logger.info("Arg free memory:"
				+ (int) Runtime.getRuntime().freeMemory() / (1024 * 1024));
		TestSession.logger.info("Arg free procc:"
				+ Runtime.getRuntime().availableProcessors());

		for (String anArg : args) {
			TestSession.logger.info("Arg Tez:" + anArg);
		}
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(), mode, session, timelineServer,
				testName);
		conf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "DEBUG");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		boolean generateSplitsInClient;

		SplitsInClientOptionParser splitCmdLineParser = new SplitsInClientOptionParser();
		try {
			generateSplitsInClient = splitCmdLineParser.parse(otherArgs, false);
			otherArgs = splitCmdLineParser.getRemainingArgs();
		} catch (ParseException e1) {
			System.err.println("Invalid options");
			printUsage();
			return 2;
		}

		boolean useTezSession = conf.getBoolean("USE_TEZ_SESSION", true);
		long interJobSleepTimeout = conf.getInt("INTER_JOB_SLEEP_INTERVAL", 0) * 1000;

		boolean retainStagingDir = conf.getBoolean("RETAIN_STAGING_DIR", false);
		boolean useMRSettings = conf.getBoolean("USE_MR_CONFIGS", true);
		// TODO needs to use auto reduce parallelism
		int intermediateNumReduceTasks = conf.getInt("IREDUCE_NUM_TASKS", 2);

		if (((otherArgs.length % 2) != 0)
				|| (!useTezSession && otherArgs.length != 2)) {
			printUsage();
			return 2;
		}

		List<String> inputPaths = new ArrayList<String>();
		List<String> outputPaths = new ArrayList<String>();

		for (int i = 0; i < otherArgs.length; i += 2) {
			inputPaths.add(otherArgs[i]);
			outputPaths.add(otherArgs[i + 1]);
		}

		UserGroupInformation.setConfiguration(conf);

		TezConfiguration tezConf = new TezConfiguration(conf);
		TestOrderedWordCount instance = new TestOrderedWordCount();

		FileSystem fs = FileSystem.get(conf);

		String stagingDirStr = conf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
				TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT)
				+ Path.SEPARATOR
				+ Long.toString(System.currentTimeMillis());
		Path stagingDir = new Path(stagingDirStr);
		FileSystem pathFs = stagingDir.getFileSystem(tezConf);
		pathFs.mkdirs(new Path(stagingDirStr));

		tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
		stagingDir = pathFs.makeQualified(new Path(stagingDirStr));

		TokenCache.obtainTokensForNamenodes(credentials,
				new Path[] { stagingDir }, conf);
		TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

		// No need to add jar containing this class as assumed to be part of
		// the tez jars.

		// TEZ-674 Obtain tokens based on the Input / Output paths. For now
		// assuming staging dir
		// is the same filesystem as the one used for Input/Output.

		if (useTezSession) {
			TestSession.logger.info("Creating Tez Session");
			tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
		} else {
			tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);
		}
		TezClient tezSession = TezClient.create("OrderedWordCountSession",
				tezConf, null, credentials);
		tezSession.start();

		DAGStatus dagStatus = null;
		DAGClient dagClient = null;
		String[] vNames = { "initialmap", "intermediate_reducer", "finalreduce" };

		Set<StatusGetOpts> statusGetOpts = EnumSet
				.of(StatusGetOpts.GET_COUNTERS);
		try {
			for (int dagIndex = 1; dagIndex <= inputPaths.size(); ++dagIndex) {
				if (dagIndex != 1 && interJobSleepTimeout > 0) {
					try {
						TestSession.logger
								.info("Sleeping between jobs, sleepInterval="
										+ (interJobSleepTimeout / 1000));
						Thread.sleep(interJobSleepTimeout);
					} catch (InterruptedException e) {
						TestSession.logger
								.info("Main thread interrupted. Breaking out of job loop");
						break;
					}
				}

				String inputPath = inputPaths.get(dagIndex - 1);
				String outputPath = outputPaths.get(dagIndex - 1);

				if (fs.exists(new Path(outputPath))) {
					throw new FileAlreadyExistsException("Output directory "
							+ outputPath + " already exists");
				}
				TestSession.logger.info("Running OrderedWordCount DAG"
						+ ", dagIndex=" + dagIndex + ", inputPath=" + inputPath
						+ ", outputPath=" + outputPath);

				Map<String, LocalResource> localResources = new TreeMap<String, LocalResource>();

				DAG dag = instance.createDAG(fs, conf, localResources,
						stagingDir, dagIndex, inputPath, outputPath,
						generateSplitsInClient, useMRSettings,
						intermediateNumReduceTasks);

				boolean doPreWarm = dagIndex == 1 && useTezSession
						&& conf.getBoolean("PRE_WARM_SESSION", true);
				int preWarmNumContainers = 0;
				if (doPreWarm) {
					preWarmNumContainers = conf.getInt(
							"PRE_WARM_NUM_CONTAINERS", 0);
					if (preWarmNumContainers <= 0) {
						doPreWarm = false;
					}
				}
				if (doPreWarm) {
					TestSession.logger.info("Pre-warming Session");
					PreWarmVertex preWarmVertex = PreWarmVertex.create(
							"PreWarm", preWarmNumContainers,
							dag.getVertex("initialmap").getTaskResource());
					preWarmVertex.addTaskLocalFiles(dag.getVertex("initialmap")
							.getTaskLocalFiles());
					preWarmVertex.setTaskEnvironment(dag
							.getVertex("initialmap").getTaskEnvironment());
					preWarmVertex.setTaskLaunchCmdOpts(dag.getVertex(
							"initialmap").getTaskLaunchCmdOpts());

					tezSession.preWarm(preWarmVertex);
				}

				if (useTezSession) {
					TestSession.logger
							.info("Waiting for TezSession to get into ready state");
					waitForTezSessionReady(tezSession);
					TestSession.logger
							.info("Submitting DAG to Tez Session, dagIndex="
									+ dagIndex);
					dagClient = tezSession.submitDAG(dag);
					TestSession.logger
							.info("Submitted DAG to Tez Session, dagIndex="
									+ dagIndex);
				} else {
					TestSession.logger
							.info("Submitting DAG as a new Tez Application");
					dagClient = tezSession.submitDAG(dag);
				}

				while (true) {
					dagStatus = dagClient.getDAGStatus(statusGetOpts);
					if (dagStatus.getState() == DAGStatus.State.RUNNING
							|| dagStatus.getState() == DAGStatus.State.SUCCEEDED
							|| dagStatus.getState() == DAGStatus.State.FAILED
							|| dagStatus.getState() == DAGStatus.State.KILLED
							|| dagStatus.getState() == DAGStatus.State.ERROR) {
						break;
					}
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// continue;
					}
				}

				while (dagStatus.getState() != DAGStatus.State.SUCCEEDED
						&& dagStatus.getState() != DAGStatus.State.FAILED
						&& dagStatus.getState() != DAGStatus.State.KILLED
						&& dagStatus.getState() != DAGStatus.State.ERROR) {
					if (dagStatus.getState() == DAGStatus.State.RUNNING) {
						ExampleDriver.printDAGStatus(dagClient, vNames);
					}
					try {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// continue;
						}
						dagStatus = dagClient.getDAGStatus(statusGetOpts);
					} catch (TezException e) {
						TestSession.logger
								.fatal("Failed to get application progress. Exiting");
						return -1;
					}
				}
				ExampleDriver.printDAGStatus(dagClient, vNames, true, true);
				TestSession.logger.info("DAG " + dagIndex + " completed. "
						+ "FinalState=" + dagStatus.getState());
				if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
					TestSession.logger.info("DAG " + dagIndex
							+ " diagnostics: " + dagStatus.getDiagnostics());
				}
			}
		} catch (Exception e) {
			TestSession.logger.error(
					"Error occurred when submitting/running DAGs", e);
			throw e;
		} finally {
			if (!retainStagingDir) {
				pathFs.delete(stagingDir, true);
			}
			TestSession.logger.info("Shutting down session");
			tezSession.stop();
		}

		if (!useTezSession) {
			ExampleDriver.printDAGStatus(dagClient, vNames);
			TestSession.logger.info("Application completed. " + "FinalState="
					+ dagStatus.getState());
		}
		return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
	}
}
