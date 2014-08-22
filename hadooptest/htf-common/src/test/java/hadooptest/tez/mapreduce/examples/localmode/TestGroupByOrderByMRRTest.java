package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.GroupByOrderByMRRTestExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;

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
public class TestGroupByOrderByMRRTest extends
		GroupByOrderByMRRTestExtendedForTezHTF {

	private static String INPUT_FILE_NAME = "/tmp/input-GroupByOrderByMRR.txt";
	private static String OUTPUT_FILE_NAME = "/tmp/output-GroupByOrderByMRR.txt";
	private static String SHELL_SCRIPT_LOCATION = "/tmp/dataCreationScriptForTestGroupByOrderByMRR.sh";

	@BeforeClass
	public static void beforeClass() throws Exception {
		TestSession.start();
		HtfTezUtils.generateTestData(SHELL_SCRIPT_LOCATION, INPUT_FILE_NAME);

	}

	@Test
	public void testGroupByOrderByMRRTestRunOnLocal() throws Exception {
		/**
		 * Usage: groupbyorderbymrrtest <in> <out>
		 */
		long timeStamp = System.currentTimeMillis();
		String[] groupByOrderByMrrArgs = new String[] { INPUT_FILE_NAME,
				OUTPUT_FILE_NAME + "/" + timeStamp };

		int returnCode = run(groupByOrderByMrrArgs, HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode==0);
	}


	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, HadooptestConstants.Schema.FILE + OUTPUT_FILE_NAME);


	}

}
