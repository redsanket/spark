package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.mapreduce.examples.extensions.BroadcastAndOneToOneExampleExtendedForTezHTF;

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
public class TestBroadcastAndOneToOneExample extends
		BroadcastAndOneToOneExampleExtendedForTezHTF {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Test
	public void testTestBroadcastAndOneToOneExampleNoLocalityCheckRunOnLocal()
			throws Exception {
		int returnCode = run(new String[] { skipLocalityCheck },
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testTestBroadcastAndOneToOneExampleWithLocalityCheckRunOnLocal()
			throws Exception {
		int returnCode = run(new String[] {},
				HadooptestConstants.Execution.TEZ_LOCAL);
		Assert.assertTrue(returnCode == 0);
	}

}
