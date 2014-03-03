package hadooptest.hadoop.regression.dfs;

import static org.junit.Assert.fail;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsadminReportBO.DatanodeBO;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.node.hadoop.fullydistributed.FullyDistributedNode;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestTopologyReport extends DfsTestsBaseClass {
	static Logger logger = Logger.getLogger(TestTopologyReport.class);

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
