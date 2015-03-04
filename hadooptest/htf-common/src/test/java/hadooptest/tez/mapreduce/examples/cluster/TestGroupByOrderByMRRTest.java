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
import hadooptest.tez.mapreduce.examples.extensions.GroupByOrderByMRRTestExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.junit.After;
import org.junit.Assert;
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

	@Rule
	public TestName testName = new TestName();

	private static void copyDataIntoHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO response = dfsCliCommands.put(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), INPUT_FILE_NAME,
				INPUT_FILE_NAME);
//		Assert.assertTrue(response.process.exitValue() == 0);

	}

	@Test
	public void testGroupByOrderByMRRTestRunOnClusterWithSession()
			throws Exception {
		/**
		 * This sleep is there because of TEZ-160. I've noticed that
		 * by not having this sleep the subsequent tests would start
		 * and hit the AM in the sleeping state and cause delegation
		 * tokens to error and thus fail the test.
		 */
		Thread.sleep(6000);

		/**
		 * Usage: groupbyorderbymrrtest <in> <out>
		 */
		copyDataIntoHdfs();
		long timeStamp = System.currentTimeMillis();
		String[] groupByOrderByMrrArgs = new String[] { INPUT_FILE_NAME,
				OUTPUT_FILE_NAME + "/" + timeStamp };

		int returnCode = run(groupByOrderByMrrArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,TimelineServer.ENABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testGroupByOrderByMRRTestRunOnClusterWithoutSession()
			throws Exception {
		/**
		 * Usage: groupbyorderbymrrtest <in> <out>
		 */
		/**
		 * This sleep is there because of TEZ-160. I've noticed that
		 * by not having this sleep the subsequent tests would start
		 * and hit the AM in the sleeping state and cause delegation
		 * tokens to error and thus fail the test.
		 */
		Thread.sleep(6000);

		copyDataIntoHdfs();
		long timeStamp = System.currentTimeMillis();
		String[] groupByOrderByMrrArgs = new String[] { INPUT_FILE_NAME,
				OUTPUT_FILE_NAME + "/" + timeStamp };

		int returnCode = run(groupByOrderByMrrArgs,
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,TimelineServer.ENABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUTPUT_FILE_NAME);

	}

}
