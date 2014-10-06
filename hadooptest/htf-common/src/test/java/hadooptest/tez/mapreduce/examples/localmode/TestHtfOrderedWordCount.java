package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.mapreduce.examples.extensions.OrderedWordCountExtendedForTez;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.File;

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

@Category(SerialTests.class)
public class TestHtfOrderedWordCount extends OrderedWordCountExtendedForTez {
	public static String INPUT_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/outOfOrderedWordCount";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
//	@Ignore("Until TEZ-1618 and http://bug.corp.yahoo.com/show_bug.cgi?id=7132271 are fixed")
	public void testOrderedWordCountRunOnLocalWithSession() throws Exception {
		String[] jobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		int returnCode = run(jobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
//	@Ignore("Until TEZ-1618 and http://bug.corp.yahoo.com/show_bug.cgi?id=7132271 are fixed")
	public void testOrderedWordCountRunOnLocalWithoutSession() throws Exception {
		String[] jobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		int returnCode = run(jobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.NO,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	@Ignore("Until TEZ-1406 and http://bug.corp.yahoo.com/show_bug.cgi?id=7132271 are fixed")
	public void testOrderedWordCountWithSplitRunOnLocalWithSession()
			throws Exception {
		String[] jobArgs = new String[] { 
				INPUT_FILE, OUTPUT_LOCATION, "-generateSplitsInClient" };
		int returnCode = run(jobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	@Ignore("Until TEZ-1406 and http://bug.corp.yahoo.com/show_bug.cgi?id=7132271 are fixed")
	public void testOrderedWordCountWithSplitRunOnLocalWithoutSession()
			throws Exception {
		String[] jobArgs = new String[] { 
				INPUT_FILE, OUTPUT_LOCATION, "-generateSplitsInClient" };
		int returnCode = run(jobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.NO,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirs() throws Exception {
		FileUtils.deleteQuietly(new File(OUTPUT_LOCATION));
	}
}
