package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.util.HashMap;

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
public class TestHtfOrderedWordCount extends OrderedWordCountExtendedForHtf {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	public static String JUST_THE_FILE_NAME = "excite-small.log";
	public static String SOURCE_FILE = "/home/y/share/htf-data/"
			+ JUST_THE_FILE_NAME;
	public static String HDFS_DIR_LOC = "/tmp/";
	public static String INPUT_FILE = HDFS_DIR_LOC + JUST_THE_FILE_NAME;
	public static String OUTPUT_LOCATION = "/tmp/ouOfOrderedWordCount";

	@Before
	public void copyTheFileOnHdfs() throws Exception {
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

			GenericCliResponseBO genericCliResponse = dfsCliCommands
					.put(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
							HadooptestConstants.UserNames.HADOOPQA, "",
							System.getProperty("CLUSTER_NAME"), SOURCE_FILE,
							INPUT_FILE);
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

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testOrderedWordCountRunOnClusterWithSession() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,
				TimelineServer.ENABLED, testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testOrderedWordCountRunOnClusterWithoutSession()
			throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,
				TimelineServer.ENABLED, testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testOrderedWordCountWithSplitsRunOnClusterWithSession()
			throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,
				TimelineServer.ENABLED, testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testOrderedWordCountWithSplitsRunOnClusterWithoutSession()
			throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,
				TimelineServer.ENABLED, testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, HadooptestConstants.Schema.HDFS
						+ OUTPUT_LOCATION);
	}
}