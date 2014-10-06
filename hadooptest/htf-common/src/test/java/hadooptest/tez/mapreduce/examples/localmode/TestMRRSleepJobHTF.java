package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.ParallelMethodTests;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.MRRSleepJobExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

@Category(ParallelMethodTests.class)
public class TestMRRSleepJobHTF extends MRRSleepJobExtendedForTezHTF {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	@Ignore("Until http://bug.corp.yahoo.com/show_bug.cgi?id=7112403 is fixed")
	public void testMrrSleepJobLocalModeWithSession() throws Exception {
		String[] sleepJobArgs = new String[] { "-m 1", "-r 1", "-ir 1",
				"-irs 1", "-mt 50000", "-rt 40000", "-irt 10000",
				"-recordt 10000" };
		/**
		 * [-m numMapper][-r numReducer] [-ir numIntermediateReducer] [-irs
		 * numIntermediateReducerStages] [-mt mapSleepTime (msec)] [-rt
		 * reduceSleepTime (msec)] [-irt intermediateReduceSleepTime] [-recordt
		 * recordSleepTime (msec)] [-generateSplitsInAM (false)/true]
		 * 
		 * [-writeSplitsToDfs (false)/true]
		 */
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	@Ignore("Until http://bug.corp.yahoo.com/show_bug.cgi?id=7112403 is fixed")
	public void testMrrSleepJobLocalModeWithout() throws Exception {
		String[] sleepJobArgs = new String[] { "-m 1", "-r 1", "-ir 1",
				"-irs 1", "-mt 50000", "-rt 40000", "-irt 10000",
				"-recordt 10000" };
		/**
		 * [-m numMapper][-r numReducer] [-ir numIntermediateReducer] [-irs
		 * numIntermediateReducerStages] [-mt mapSleepTime (msec)] [-rt
		 * reduceSleepTime (msec)] [-irt intermediateReduceSleepTime] [-recordt
		 * recordSleepTime (msec)] [-generateSplitsInAM (false)/true]
		 * [-writeSplitsToDfs (false)/true]
		 */
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.NO,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
	}
}
