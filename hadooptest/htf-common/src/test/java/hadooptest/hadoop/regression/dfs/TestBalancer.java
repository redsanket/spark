package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestBalancer extends DfsTestsBaseClass {

	String testName;
	String expectedOutput;
	String policyValue;
	String thresholdValue;
	private final static String EXPECTED_OUTPUT = "The cluster is balanced. Exiting...";
	private final static String EXPECTED_OUTPUT_ERROR = "Usage: java Balancer";
	private final static String POLICY_DATANODE = "datanode";
	private final static String POLICY_BLOCKPOOL = "blockpool";

	public TestBalancer(String testName, String expectedOutput,
			String policyValue, String thresholdValue) {
		this.testName = testName;
		this.expectedOutput = expectedOutput;
		this.policyValue = policyValue;
		this.thresholdValue = thresholdValue;
		TestSession.logger.info("TestBalancer invoked with params:[" + testName
				+ "] [" + expectedOutput + "] [" + policyValue + "]["
				+ thresholdValue + "]");
	}

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				// This line is intentionally left BLANK
				{ "Balancer_01:", EXPECTED_OUTPUT, null, null },
				{ "Balancer_02:", EXPECTED_OUTPUT, POLICY_DATANODE, null },
				{ "Balancer_03:", EXPECTED_OUTPUT, POLICY_BLOCKPOOL, null },
				{ "Balancer_04:", EXPECTED_OUTPUT, null, "10" },
				{ "Balancer_05:", EXPECTED_OUTPUT_ERROR, null, "-999" },
				{ "Balancer_06:", EXPECTED_OUTPUT_ERROR, null, "91999" },
				{ "Balancer_07:", EXPECTED_OUTPUT_ERROR, null, "asdg" },
				{ "Balancer_08:", EXPECTED_OUTPUT_ERROR, "", null },
				{ "Balancer_09:", EXPECTED_OUTPUT_ERROR, POLICY_DATANODE, "-8988" },
				{ "Balancer_10:", EXPECTED_OUTPUT_ERROR, POLICY_DATANODE, "10889" },
				{ "Balancer_11:", EXPECTED_OUTPUT_ERROR, POLICY_DATANODE, "asdfghk" },
				{ "Balancer_12:", EXPECTED_OUTPUT_ERROR, POLICY_BLOCKPOOL, "-8988" },
				{ "Balancer_13:", EXPECTED_OUTPUT_ERROR, POLICY_BLOCKPOOL, "10988" },
				{ "Balancer_14:", EXPECTED_OUTPUT_ERROR, POLICY_BLOCKPOOL, "asdfghk" },
				{ "Balancer_15:", EXPECTED_OUTPUT_ERROR, "9999", "10" },
				{ "Balancer_16:", EXPECTED_OUTPUT_ERROR, "asdsd*", "10" },
				{ "Balancer_17:", EXPECTED_OUTPUT_ERROR, "-2345578", "10" },
				{ "Balancer_18:", EXPECTED_OUTPUT_ERROR, null, "" },
				{ "Balancer_19:", EXPECTED_OUTPUT_ERROR, "", "" },

		});
	}

	/*
	 * testBalancer
	 */
	@Test
	public void testBalancer() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.balancer(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, policyValue,
				thresholdValue);
		if (expectedOutput == EXPECTED_OUTPUT_ERROR) {
			Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
			Assert.assertTrue(genericCliResponse.response
					.contains(EXPECTED_OUTPUT_ERROR));
		} else {
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
			Assert.assertTrue(genericCliResponse.response
					.contains(EXPECTED_OUTPUT));
		}

	}

	@Override
	@After
	public void logTaskReportSummary() {
		// Override to hide the Test Session logs
	}

}
