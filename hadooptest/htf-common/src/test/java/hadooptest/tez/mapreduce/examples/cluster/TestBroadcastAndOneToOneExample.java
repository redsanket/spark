package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.TezUtils;
import hadooptest.tez.mapreduce.examples.extensions.BroadcastAndOneToOneExampleExtendedForTezHTF;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestBroadcastAndOneToOneExample extends
		BroadcastAndOneToOneExampleExtendedForTezHTF {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Test
	public void testClusterMode() throws Exception {
		boolean skipLocalityCheck = true;
		if (skipLocalityCheck) {
			int returnCode = run(new String[] {}, HadooptestConstants.Mode.CLUSTER);
			Assert.assertTrue(returnCode == 0);
		} else {

		}
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
