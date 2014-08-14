package hadooptest.tez.pig.cluster;

import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.HtfPigBaseClass;

public class TestAbfFeeds_3 extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "abf_feeds_3_hdfs.pig";

	@Test
	public void testPigOnTezClusterHdfs() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		int returnCode = runPigScript(SCRIPT_NAME,
				HadooptestConstants.Schema.HDFS, nameNode,
				HadooptestConstants.Mode.CLUSTER,
				HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}

	@Ignore("For now")
	@Test
	public void testPigOnTezClusterWebHdfs() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		int returnCode = runPigScript(SCRIPT_NAME,
				HadooptestConstants.Schema.WEBHDFS, nameNode,
				HadooptestConstants.Mode.CLUSTER,
				HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode == 0);
	}


	@After
	public void deleteOutputDirInHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES,
				"/tmp/HTF/output/" + SCRIPT_NAME.replace(".pig", "") + "*");

	}
}
