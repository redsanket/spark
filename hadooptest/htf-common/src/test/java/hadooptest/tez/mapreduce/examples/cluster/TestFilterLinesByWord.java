package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.HtfTezUtils;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordExtendedForTezHTF;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestFilterLinesByWord extends FilterLinesByWordExtendedForTezHTF {

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Test
	public void testClusterMode() throws Exception {
		/**
		 * Usage: filtelinesrbyword <in> <out> <filter_word>
		 * [-generateSplitsInClient true/<false>
		 */
		long timeStamp = System.currentTimeMillis();
		String[] filterLinesByWordArgs = new String[] { "/tmp/tez-site.xml",
				"/tmp/filterLinesByWord-out-" + timeStamp, "tez", "-generateSplitsInClient true" };

		int returnCode = run(filterLinesByWordArgs, HadooptestConstants.Mode.CLUSTER);
		Assert.assertTrue(returnCode==0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "/tmp/tez/");
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "/tmp/filterLinesByWord-out-*");

	}

}
