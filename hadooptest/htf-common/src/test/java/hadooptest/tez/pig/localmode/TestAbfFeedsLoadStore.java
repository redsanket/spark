package hadooptest.tez.pig.localmode;

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

public class TestAbfFeedsLoadStore extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "abf_feeds_loads_store.pig";

	@Test
	public void testPigOnLegacyClusterHdfs() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		int returnCode = runPigScript(SCRIPT_NAME,
				HadooptestConstants.Schema.HDFS, nameNode,
				HadooptestConstants.Mode.CLUSTER,
				HadooptestConstants.Execution.MAPRED);
		Assert.assertTrue(returnCode == 0);
	}

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

	@Test
	public void testPigOnTezLocalMode() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		int returnCode = runPigScript(SCRIPT_NAME,
				HadooptestConstants.Schema.NONE, nameNode,
				HadooptestConstants.Mode.LOCAL,
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
