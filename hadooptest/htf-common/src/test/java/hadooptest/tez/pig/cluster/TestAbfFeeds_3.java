package hadooptest.tez.pig.cluster;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.utils.HtfPigBaseClass;
@Category(SerialTests.class)
public class TestAbfFeeds_3 extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "abf_feeds_3.pig";

	@Ignore("Ignore hdfs for now, takes > 1 hour to run")
	@Test
	public void testPigOnTezClusterHdfs() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		List<String>params = new ArrayList<String>();
		params.add("protocol=" + HadooptestConstants.Schema.HDFS.replace("://", ""));
		params.add("namenode=" + nameNode);
		String scriptLocation = TestSession.conf.getProperty("WORKSPACE")
				+ WORKSPACE_SCRIPT_LOCATION + SCRIPT_NAME;
		int returnCode = runPigScriptOnCluster(
				params, scriptLocation);
		Assert.assertTrue(returnCode == 0);
	}

	@Ignore("Ignore webhdfs for now, takes > 1 hour to run")
	@Test
	public void testPigOnTezClusterWebHdfs() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.NAMENODE);
		String nameNode = hadoopNode.getHostname();
		List<String>params = new ArrayList<String>();
		params.add("protocol=" + HadooptestConstants.Schema.WEBHDFS.replace("://", ""));
		params.add("namenode=" + nameNode);
		String scriptLocation = TestSession.conf.getProperty("WORKSPACE")
				+ WORKSPACE_SCRIPT_LOCATION + SCRIPT_NAME;
		int returnCode = runPigScriptOnCluster(
				params, scriptLocation);
		Assert.assertTrue(returnCode == 0);
	}


	@After
	public void deleteOutputDirInHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES,
				HadooptestConstants.Schema.HDFS + "/HTF/output/"+ SCRIPT_NAME.replace(".pig", "")+"*");

	}
}
