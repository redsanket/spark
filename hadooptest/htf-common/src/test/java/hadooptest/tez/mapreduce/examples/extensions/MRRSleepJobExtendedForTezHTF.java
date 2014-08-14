package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.TezUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.examples.MRRSleepJob;

public class MRRSleepJobExtendedForTezHTF extends MRRSleepJob {
	private Credentials credentials = new Credentials();
	public int run(String[] args, String mode) throws Exception {

		if (args.length < 1) {
			System.err.println("MRRSleepJob [-m numMapper] [-r numReducer]"
					+ " [-ir numIntermediateReducer]"
					+ " [-irs numIntermediateReducerStages]"
					+ " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]"
					+ " [-irt intermediateReduceSleepTime]"
					+ " [-recordt recordSleepTime (msec)]"
					+ " [-generateSplitsInAM (false)/true]"
					+ " [-writeSplitsToDfs (false)/true]");
			ToolRunner.printGenericCommandUsage(System.err);
			return 2;
		}

		int numMapper = 1, numReducer = 1, numIReducer = 1;
		long mapSleepTime = 100, reduceSleepTime = 100, recSleepTime = 100, iReduceSleepTime = 1;
		int mapSleepCount = 1, reduceSleepCount = 1, iReduceSleepCount = 1;
		int iReduceStagesCount = 1;
		boolean writeSplitsToDfs = false;
		boolean generateSplitsInAM = false;
		boolean splitsOptionFound = false;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-m")) {
				numMapper = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-r")) {
				numReducer = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-ir")) {
				numIReducer = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-mt")) {
				mapSleepTime = Long.parseLong(args[++i]);
			} else if (args[i].equals("-rt")) {
				reduceSleepTime = Long.parseLong(args[++i]);
			} else if (args[i].equals("-irt")) {
				iReduceSleepTime = Long.parseLong(args[++i]);
			} else if (args[i].equals("-irs")) {
				iReduceStagesCount = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-recordt")) {
				recSleepTime = Long.parseLong(args[++i]);
			} else if (args[i].equals("-generateSplitsInAM")) {
				if (splitsOptionFound) {
					throw new RuntimeException(
							"Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
				}
				splitsOptionFound = true;
				generateSplitsInAM = Boolean.parseBoolean(args[++i]);

			} else if (args[i].equals("-writeSplitsToDfs")) {
				if (splitsOptionFound) {
					throw new RuntimeException(
							"Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
				}
				splitsOptionFound = true;
				writeSplitsToDfs = Boolean.parseBoolean(args[++i]);
			}
		}

		if (numIReducer > 0 && numReducer <= 0) {
			throw new RuntimeException(
					"Cannot have intermediate reduces without"
							+ " a final reduce");
		}

		// sleep for *SleepTime duration in Task by recSleepTime per record
		mapSleepCount = (int) Math.ceil(mapSleepTime / ((double) recSleepTime));
		reduceSleepCount = (int) Math.ceil(reduceSleepTime
				/ ((double) recSleepTime));
		iReduceSleepCount = (int) Math.ceil(iReduceSleepTime
				/ ((double) recSleepTime));

		Configuration nonTezConf = TezUtils.setupConfForTez(
				TestSession.cluster.getConf(), mode);
		TezConfiguration conf = new TezConfiguration(nonTezConf);
		FileSystem remoteFs = FileSystem.get(conf);

		conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, conf.get(
				TezConfiguration.TEZ_AM_STAGING_DIR,
				TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT));

		Path remoteStagingDir = remoteFs.makeQualified(new Path(conf.get(
				TezConfiguration.TEZ_AM_STAGING_DIR,
				TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT), Long
				.toString(System.currentTimeMillis())));
		TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

		DAG dag = createDAG(remoteFs, conf, remoteStagingDir, numMapper,
				numReducer, iReduceStagesCount, numIReducer, mapSleepTime,
				mapSleepCount, reduceSleepTime, reduceSleepCount,
				iReduceSleepTime, iReduceSleepCount, writeSplitsToDfs,
				generateSplitsInAM);

		TezClient tezSession = new TezClient("MRRSleep", conf, false, null,
				credentials);
		tezSession.start();
		DAGClient dagClient = tezSession.submitDAG(dag);

		while (true) {
			DAGStatus status = dagClient.getDAGStatus(null);
			TestSession.logger.info("DAG Status: " + status);
			if (status.isCompleted()) {
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// do nothing
			}
		}
		tezSession.stop();

		return dagClient.getDAGStatus(null).getState()
				.equals(DAGStatus.State.SUCCEEDED) ? 0 : 1;
	}

}
