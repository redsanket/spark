package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordExtendedForTezHTF;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * This class has the real test methods meant to be run locally. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.cluster
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. 
 * These test cases flesh out and implement sub-tests that are provisioned in the original test class.
 * 
 */

@Category(SerialTests.class)
public class TestFilterLinesByWord extends FilterLinesByWordExtendedForTezHTF {
	public static String SOURCE_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/outOfFilterLinesByWord";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Test
	public void testFilterLinesByWordWithClientSplitsRunOnLocal()
			throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		String[] filterLinesByWordArgs = new String[] { SOURCE_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient true" };

		int returnCode = run(filterLinesByWordArgs,
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testFilterLinesByWordNoClientSplitsRunOnLocal()
			throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		String[] filterLinesByWordArgs = new String[] { SOURCE_FILE,
				OUTPUT_LOCATION, "lionking" };

		int returnCode = run(filterLinesByWordArgs,
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUTPUT_LOCATION);

	}

}
