package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsadminReportBO.DatanodeBO;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestTopologyReport extends DfsTestsBaseClass {

	@Before
	public void beforeTest() {

	}

	@Test
	public void testCheckDeadNodesReported() throws Exception {

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		GenericCliResponseBO printTopologyResponse;
		// Dfsadmin -printTopology
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		printTopologyResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.YES,
				EMPTY_FS_ENTITY);

		for (DatanodeBO aDatanodeBOReadOffOfDfsadminResponse : dfsadminReportBO.liveDatanodes) {
			Assert.assertTrue(
					aDatanodeBOReadOffOfDfsadminResponse.hostname
							+ " not contained in the printTopology response",
					printTopologyResponse.response
							.contains(aDatanodeBOReadOffOfDfsadminResponse.hostname));
		}
		for (DatanodeBO aDatanodeBOReadOffOfDfsadminResponse : dfsadminReportBO.deadDatanodes) {
			Assert.assertTrue(
					aDatanodeBOReadOffOfDfsadminResponse.hostname
							+ " not contained in the printTopology response",
					printTopologyResponse.response
							.contains(aDatanodeBOReadOffOfDfsadminResponse.hostname));
		}

	}

	@After
	public void afterTest() throws Exception {

	}

	@Override
	@After
	public void logTaskReportSummary() {
		// Override to hide the Test Session logs
	}

}
