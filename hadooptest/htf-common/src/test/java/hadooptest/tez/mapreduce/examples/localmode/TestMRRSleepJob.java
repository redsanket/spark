package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.TezUtils;
import hadooptest.tez.mapreduce.examples.extensions.MRRSleepJobExtendedForTezHTF;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestMRRSleepJob extends MRRSleepJobExtendedForTezHTF {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Test
	public void testLocalMode() throws Exception {
		String[] sleepJobArgs = new String[] { "-m 5", "-r 4", "-ir 4",
				"-irs 4", "-mt 500", "-rt 200", "-irt 100", "-recordt 100" };
		/**
		 * [-m numMapper][-r numReducer] [-ir numIntermediateReducer] [-irs
		 * numIntermediateReducerStages] [-mt mapSleepTime (msec)] [-rt
		 * reduceSleepTime (msec)] [-irt intermediateReduceSleepTime] [-recordt
		 * recordSleepTime (msec)] [-generateSplitsInAM (false)/true]
		 * [-writeSplitsToDfs (false)/true]
		 */
		int returnCode = run(sleepJobArgs, HadooptestConstants.Mode.LOCAL);
		Assert.assertTrue(returnCode==0);
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
