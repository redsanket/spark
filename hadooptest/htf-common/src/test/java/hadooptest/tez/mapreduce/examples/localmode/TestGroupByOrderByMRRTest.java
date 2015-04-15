package hadooptest.tez.mapreduce.examples.localmode;

import java.io.File;

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
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class has the real test methods meant to be run locally. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.cluster
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */


/**
 * https://issues.apache.org/jira/browse/TEZ-2320
 * @author tiwari
 *
 */
@Deprecated
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

	@Test
	@Ignore("Until TEZ-1406 is fixed")
	public void testGroupByOrderByMRRTestRunOnLocalWithSession()
			throws Exception {
		/**
		 * Usage: groupbyorderbymrrtest <in> <out>
		 */
		long timeStamp = System.currentTimeMillis();
		String[] groupByOrderByMrrArgs = new String[] { INPUT_FILE_NAME,
				OUTPUT_FILE_NAME + "/" + timeStamp };

		int returnCode = run(groupByOrderByMrrArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	@Ignore("Until TEZ-1406 is fixed")
	public void testGroupByOrderByMRRTestRunOnLocalWithoutSession()
			throws Exception {
		/**
		 * Usage: groupbyorderbymrrtest <in> <out>
		 */
		long timeStamp = System.currentTimeMillis();
		String[] groupByOrderByMrrArgs = new String[] { INPUT_FILE_NAME,
				OUTPUT_FILE_NAME + "/" + timeStamp };

		int returnCode = run(groupByOrderByMrrArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.NO,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		FileUtils.deleteQuietly(new File(OUTPUT_FILE_NAME));

	}

}
