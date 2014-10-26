package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordOneToOneExtendedForHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
public class TestFilterLinesByWordOneToOne extends
		FilterLinesByWordOneToOneExtendedForHTF {
	public static String SOURCE_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String INPUT_FILE = "/tmp/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/outOfFilterLinesByWordOneToOne";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Before
	public void copyTheFileOnHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();

		GenericCliResponseBO genericCliResponse = dfsCliCommands.put(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), SOURCE_FILE, INPUT_FILE);
		
		dfsCliCommands.chmod(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), INPUT_FILE, "777", Recursive.YES);
	}


	@Ignore("until TEZ-1406 is fixed")
	@Test
	public void testFilterLinesByWordWithClientSplitsRunOnClusterWithSession()
			throws Exception {
		/**
		 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" + "
		 * [-generateSplitsInClient true/<false>]
		 */
		String[] filterLinesByWordOneToOneArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient", "true" };

		int returnCode = run(filterLinesByWordOneToOneArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Ignore("until TEZ-1406 is fixed")
	@Test
	public void testFilterLinesByWordWithClientSplitsRunOnClusterWithoutSession()
			throws Exception {
		/**
		 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" + "
		 * [-generateSplitsInClient true/<false>]
		 */
		String[] filterLinesByWordOneToOneArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient", "true" };

		int returnCode = run(filterLinesByWordOneToOneArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testFilterLinesByWordNoClientSplitsRunOnClusterWithSession()
			throws Exception {
		/**
		 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" + "
		 * [-generateSplitsInClient true/<false>]
		 */
		String[] filterLinesByWordOneToOneArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient", "false" };

		int returnCode = run(filterLinesByWordOneToOneArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testFilterLinesByWordNoClientSplitsRunOnClusterWithoutSession()
			throws Exception {
		/**
		 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" + "
		 * [-generateSplitsInClient true/<false>]
		 */
		String[] filterLinesByWordOneToOneArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient", "false" };

		int returnCode = run(filterLinesByWordOneToOneArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,
				testName.getMethodName());
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
