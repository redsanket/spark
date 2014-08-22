package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.mapreduce.examples.extensions.OrderedWordCountExtendedForTez;
import hadooptest.tez.utils.HtfTezUtils;

import java.io.File;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
public class TestHtfOrderedWordCount extends OrderedWordCountExtendedForTez {
	public static String INPUT_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/ouOfOrderedWordCount";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Ignore("ignore")
	@Test
	public void testOrderedWordCountNoSessionRunOnLocal() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = false;
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testOrderedWordCountUseSessionRunOnLocal() throws Exception {
		String[] sleepJobArgs = new String[] { INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = true;
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@Ignore("ignore")
	@Test
	public void testOrderedWordCountWithSplitNoSessionRunOnLocal() throws Exception {
		String[] sleepJobArgs = new String[] { "-generateSplitsInClient",
				INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = false;
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@Ignore("ignore")
	@Test
	public void testOrderedWordCountWithSplitUseSessionRunOnLocal() throws Exception {
		String[] sleepJobArgs = new String[] { "-generateSplitsInClient",
				INPUT_FILE, OUTPUT_LOCATION };
		HtfTezUtils.useSession = true;
		int returnCode = run(sleepJobArgs,
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirs() throws Exception {
		HtfTezUtils.delete(new File(OUTPUT_LOCATION));
	}
}
