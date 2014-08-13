package hadooptest.tez.pig;

import org.junit.After;
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

public class TestAbfFeeds_2_Webhdfs extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "abf_feeds_2_webhdfs.pig";

	@Test
	public void testWithoutTez() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		runPigScript(SCRIPT_NAME, nameNode, ON_TEZ.NO);
	}

	@Ignore("Ignore on Tez for now")
	@Test
	public void testWithTez() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		runPigScript(SCRIPT_NAME, nameNode, ON_TEZ.YES);
	}

	@After
	public void deleteOutputDirInHdfs() throws Exception{
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "/tmp/HTF/output/"+ SCRIPT_NAME.replace(".pig", "") + "*");
		
	}
}
