package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.JoinDataGenExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.File;
import java.io.IOException;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class has the real test methods meant to be run locally. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.cluster
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */

@Category(SerialTests.class)
public class TestJoinDataGen extends JoinDataGenExtendedForTezHTF {

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();
	
	@Test
	public void testJoinDataGenOnClusterModeWithSession() throws Exception {
		// Usage: joindatagen <outPath1> <path1Size> <outPath2> <path2Size>
		// <expectedResultPath> <numTasks>
		String[] args = new String[] { TEMP_OUT_1, "10240", TEMP_OUT_2, "10241",
				OUTPUT_DIR, "1" };
		TezConfiguration tezConf = new TezConfiguration(
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED,
						testName.getMethodName()));
		int returnCode = run(tezConf, args, createTezClient(tezConf));
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testJoinDataGenOnClusterModeWithoutSession() throws Exception {
		// Usage: joindatagen <outPath1> <path1Size> <outPath2> <path2Size>
		// <expectedResultPath> <numTasks>
		String[] args = new String[] { TEMP_OUT_1, "10240", TEMP_OUT_2, "10241",
				OUTPUT_DIR, "1" };
		TezConfiguration tezConf = new TezConfiguration(
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,TimelineServer.ENABLED,
						testName.getMethodName()));
		int returnCode = run(tezConf, args, createTezClient(tezConf));
		Assert.assertTrue(returnCode == 0);
	}

	private TezClient createTezClient(TezConfiguration tezConf)
			throws TezException, IOException {
		TezClient tezClient = TezClient.create("JoinDataGen", tezConf);
		tezClient.start();
		return tezClient;
	}

	@After
	public void deleteOutputDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUTPUT_DIR);
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, TEMP_OUT_1);
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, TEMP_OUT_2);

	}

}
