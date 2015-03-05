package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.mapreduce.examples.extensions.BroadcastAndOneToOneExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
public class TestBroadcastAndOneToOneExample extends
		BroadcastAndOneToOneExampleExtendedForTezHTF {
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName name = new TestName();

	@Test
	public void testTestBroadcastAndOneToOneExampleNoLocalityCheckRunOnClusterWithSession()
			throws Exception {
		int returnCode = run(new String[] { skipLocalityCheck },
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED, name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testTestBroadcastAndOneToOneExampleNoLocalityCheckRunOnClusterWithoutSession()
			throws Exception {
		int returnCode = run(new String[] { skipLocalityCheck },
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testTestBroadcastAndOneToOneExampleWithLocalityCheckRunOnClusterWithSession()
			throws Exception {
		int returnCode = run(new String[] {},
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testTestBroadcastAndOneToOneExampleWithLocalityCheckRunOnClusterWithoutSession()
			throws Exception {
		int returnCode = run(new String[] {},
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED,name.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}
	
}
