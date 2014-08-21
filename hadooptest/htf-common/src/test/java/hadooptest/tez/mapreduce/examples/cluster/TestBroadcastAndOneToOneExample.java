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
	public void testTestBroadcastAndOneToOneExampleNoLocalityCheckRunOnCluster()
			throws Exception {
		int returnCode = run(new String[] { skipLocalityCheck },
				HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testTestBroadcastAndOneToOneExampleWithLocalityCheckRunOnCluster()
			throws Exception {
		int returnCode = run(new String[] {},
				HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}

}
