package hadooptest.tez.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
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
public class TestHtfOrderedWordCount extends OrderedWordCountExtendedForHtf {
	public static String INPUT_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/outOfOrderedWordCount";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testOrderedWordCountWithPartitions() throws Exception {
		boolean returnCode = run(INPUT_FILE, OUTPUT_LOCATION, null, 2,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == true);
	}
	@Test
	public void testOrderedWordCountWithoutPartitions() throws Exception {
		boolean returnCode = run(INPUT_FILE, OUTPUT_LOCATION, null, 0,
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,TimelineServer.DISABLED,
				testName.getMethodName());
		Assert.assertTrue(returnCode == true);
	}

	@After
	public void deleteOutputDirs() throws Exception {
		FileUtils.deleteQuietly(new File(OUTPUT_LOCATION));
	}
}
