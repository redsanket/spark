package hadooptest.tez.mapreduce.examples.cluster;

import java.util.HashMap;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
public class TestFilterLinesByWord extends FilterLinesByWordExtendedForTezHTF {
	public static String SOURCE_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String HDFS_DIR_LOC = "/tmp/";
	public static String INPUT_FILE = HDFS_DIR_LOC + "excite-small.log";
	public static String OUTPUT_LOCATION = HDFS_DIR_LOC
			+ "outOfFilterLinesByWord";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName name = new TestName();

	@Before
	public void copyTheFileOnHdfs() throws Exception {
		/**
		 * This sleep is there because of TEZ-160. I've noticed that
		 * by not having this sleep the subsequent tests would start
		 * and hit the AM in the sleeping state and cause delegation
		 * tokens to error and thus fail the test.
		 */
		Thread.sleep(6000);
		DfsCliCommands dfsCliCommands = new DfsCliCommands();

		GenericCliResponseBO quickCheck = dfsCliCommands.test(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), INPUT_FILE,
				DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);

		if (quickCheck.process.exitValue() == 0) {
			// File exists
		} else {
			GenericCliResponseBO dirCheck = dfsCliCommands.test(
					DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"), HDFS_DIR_LOC,
					DfsCliCommands.FILE_SYSTEM_ENTITY_DIRECTORY);

			if (dirCheck.process.exitValue() != 0) {
				// File does not exist
				dfsCliCommands.mkdir(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA, "",
						System.getProperty("CLUSTER_NAME"), HDFS_DIR_LOC);
				dfsCliCommands.chmod(new HashMap<String, String>(),
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.HDFS,
						System.getProperty("CLUSTER_NAME"), HDFS_DIR_LOC,
						"777", Recursive.YES);

			}

			GenericCliResponseBO genericCliResponse = dfsCliCommands.put(
					DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, "",
					System.getProperty("CLUSTER_NAME"),
					"/home/y/share/htf-data/excite-small.log", INPUT_FILE);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
			genericCliResponse = dfsCliCommands.chmod(
					new HashMap<String, String>(),
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.HDFS,
					System.getProperty("CLUSTER_NAME"), INPUT_FILE, "777",
					Recursive.YES);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		}

	}

	@Test
	public void testFilterLinesByWordWithClientSplitsRunOnClusterWithSession()
			throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		String[] filterLinesByWordArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient", "true" };

		int returnCode = run(filterLinesByWordArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testFilterLinesByWordWithClientSplitsRunOnClusterWithoutSession()
			throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		String[] filterLinesByWordArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking", "-generateSplitsInClient", "true" };

		int returnCode = run(filterLinesByWordArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testFilterLinesByWordNoClientSplitsRunOnClusterWithSession()
			throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		String[] filterLinesByWordArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking" };

		int returnCode = run(filterLinesByWordArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testFilterLinesByWordNoClientSplitsRunOnClusterWithoutSession()
			throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		String[] filterLinesByWordArgs = new String[] { INPUT_FILE,
				OUTPUT_LOCATION, "lionking" };

		int returnCode = run(filterLinesByWordArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, HadooptestConstants.Schema.HDFS
						+ OUTPUT_LOCATION);
	}

}
