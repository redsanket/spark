package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.MRRSleepJobExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class has the real test methods meant to be run on the cluster. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.localmode
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */

@Category(SerialTests.class)
public class TestMRRSleepJob extends MRRSleepJobExtendedForTezHTF {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testClusterModeWithSession() throws Exception {
		String[] sleepJobArgs = new String[] { "-m 5", "-r 4", "-ir 4",
				"-irs 4", "-mt 500", "-rt 200", "-irt 100", "-recordt 100" };
		/**
		 * [-m numMapper][-r numReducer] [-ir numIntermediateReducer] [-irs
		 * numIntermediateReducerStages] [-mt mapSleepTime (msec)] [-rt
		 * reduceSleepTime (msec)] [-irt intermediateReduceSleepTime] [-recordt
		 * recordSleepTime (msec)] [-generateSplitsInAM (false)/true]
		 * [-writeSplitsToDfs (false)/true]
		 */
		int returnCode = run(sleepJobArgs, HadooptestConstants.Execution.TEZ_CLUSTER,
				Session.YES, TimelineServer.ENABLED,testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testClusterModeWithoutSession() throws Exception {
		String[] sleepJobArgs = new String[] { "-m 5", "-r 4", "-ir 4",
				"-irs 4", "-mt 500", "-rt 200", "-irt 100", "-recordt 100" };
		/**
		 * [-m numMapper][-r numReducer] [-ir numIntermediateReducer] [-irs
		 * numIntermediateReducerStages] [-mt mapSleepTime (msec)] [-rt
		 * reduceSleepTime (msec)] [-irt intermediateReduceSleepTime] [-recordt
		 * recordSleepTime (msec)] [-generateSplitsInAM (false)/true]
		 * [-writeSplitsToDfs (false)/true]
		 */
		int returnCode = run(sleepJobArgs, HadooptestConstants.Execution.TEZ_CLUSTER,
				Session.NO, TimelineServer.DISABLED,testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "/tmp/tez/");
	}
}
