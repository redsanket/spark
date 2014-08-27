package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

	@Test public void testBalancer01() throws Exception { runTest("Balancer_01:", EXPECTED_OUTPUT, null, null); }
	@Test public void testBalancer02() throws Exception { runTest("Balancer_02:", EXPECTED_OUTPUT, POLICY_DATANODE, null); }
	@Test public void testBalancer03() throws Exception { runTest("Balancer_03:", EXPECTED_OUTPUT, POLICY_BLOCKPOOL, null); }
	@Test public void testBalancer04() throws Exception { runTest("Balancer_04:", EXPECTED_OUTPUT, null, "10"); }
	@Test public void testBalancer05() throws Exception { runTest("Balancer_05:", EXPECTED_OUTPUT_ERROR, null, "-999"); }
	@Test public void testBalancer06() throws Exception { runTest("Balancer_06:", EXPECTED_OUTPUT_ERROR, null, "91999"); }
	@Test public void testBalancer07() throws Exception { runTest("Balancer_07:", EXPECTED_OUTPUT_ERROR, null, "asdg"); }
	@Test public void testBalancer08() throws Exception { runTest("Balancer_08:", EXPECTED_OUTPUT_ERROR, "", null); }
	@Test public void testBalancer09() throws Exception { runTest("Balancer_09:", EXPECTED_OUTPUT_ERROR, POLICY_DATANODE, "-8988"); }
	@Test public void testBalancer10() throws Exception { runTest("Balancer_10:", EXPECTED_OUTPUT_ERROR, POLICY_DATANODE, "10889"); }
	@Test public void testBalancer11() throws Exception { runTest("Balancer_11:", EXPECTED_OUTPUT_ERROR, POLICY_DATANODE, "asdfghk"); }
	@Test public void testBalancer12() throws Exception { runTest("Balancer_12:", EXPECTED_OUTPUT_ERROR, POLICY_BLOCKPOOL, "-8988"); }
	@Test public void testBalancer13() throws Exception { runTest("Balancer_13:", EXPECTED_OUTPUT_ERROR, POLICY_BLOCKPOOL, "10988"); }
	@Test public void testBalancer14() throws Exception { runTest("Balancer_14:", EXPECTED_OUTPUT_ERROR, POLICY_BLOCKPOOL, "asdfghk"); }
	@Test public void testBalancer15() throws Exception { runTest("Balancer_15:", EXPECTED_OUTPUT_ERROR, "9999", "10"); }
	@Test public void testBalancer16() throws Exception { runTest("Balancer_16:", EXPECTED_OUTPUT_ERROR, "asdsd*", "10"); }
	@Test public void testBalancer17() throws Exception { runTest("Balancer_17:", EXPECTED_OUTPUT_ERROR, "-2345578", "10"); }
	@Test public void testBalancer18() throws Exception { runTest("Balancer_18:", EXPECTED_OUTPUT_ERROR, null, ""); }
	@Test public void testBalancer19() throws Exception { runTest("Balancer_19:", EXPECTED_OUTPUT_ERROR, "", ""); }
	
	public void runTest(String testName, String expectedOutput,
            String policyValue, String thresholdValue) throws Exception {
	    
	    this.testName = testName;
	    this.expectedOutput = expectedOutput;
	    this.policyValue = policyValue;
	    this.thresholdValue = thresholdValue;
	    
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

}
