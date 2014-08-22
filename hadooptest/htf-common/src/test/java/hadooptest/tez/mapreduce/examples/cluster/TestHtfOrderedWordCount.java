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
import hadooptest.tez.HtfTezUtils;
import hadooptest.tez.mapreduce.examples.extensions.OrderedWordCountExtendedForTez;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * This class has the real test methods meant to be run on the cluster. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.localmode
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. 
 * These test cases flesh out and implement sub-tests that are provisioned in the original test class.
 * 
 */

@Category(SerialTests.class)
public class TestHtfOrderedWordCount extends OrderedWordCountExtendedForTez {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	public static String INPUT_FILE = "/tmp/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/ouOfOrderedWordCount";

	@Before
	public void copyTheFileOnHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();

		GenericCliResponseBO genericCliResponse = dfsCliCommands.put(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"),
				"/home/y/share/htf-data/excite-small.log", INPUT_FILE);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	@Ignore("DO NOT USE SESSION FOR NOW")
	@Test
	public void testOrderedWordCountNoSessionRunOnCluster() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = false;
		int returnCode = run(sleepJobArgs, HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testOrderedWordCountUseSessionRunOnCluster() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = true;
		int returnCode = run(sleepJobArgs, HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}

	@Ignore("DO NOT USE SESSION FOR NOW")
	@Test
	public void testOrderedWordCountWithSplitsNoSessionRunOnCluster() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = false;
		int returnCode = run(sleepJobArgs, HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testOrderedWordCountWithSplitsUseSessioRunOnCluster() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = true;
		int returnCode = run(sleepJobArgs, HadooptestConstants.Execution.TEZ);
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
