package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.TestSessionCore;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO;
import hadooptest.tez.ats.SeedData.DAG;
import hadooptest.tez.ats.SeedData.DAG.Vertex;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfATSUtils;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestJobSuccessesEvenWhenAtsIsDown extends ATSTestsBaseClass {
	private static String opLocation = "/tmp/testFailure";
	private static String inpFile = "/tmp/tez-site.xml";
	private static String localSrcLocation = "/home/gs/tez/current/tez-site.xml";
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	
	@Override
	@Before
	public void cleanupAndPrepareForTestRun() throws Exception {
		//Do not launch seed jobs
		
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger.info("copying " + inpFile +" to HDFS!");
		String aCluster = System.getProperty("CLUSTER_NAME");
		GenericCliResponseBO copyFileBO;
		copyFileBO = dfsCommonCli.copyFromLocal(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA, HadooptestConstants.Schema.NONE, aCluster,
				localSrcLocation, inpFile);
		TestSession.logger.info(copyFileBO.response);
		GenericCliResponseBO chmodBO;
		chmodBO = dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA, HadooptestConstants.Schema.NONE,
				aCluster, inpFile, "777", Recursive.YES);
		TestSession.logger.info(chmodBO.response);
		
	}
	
	@Test
	public void testJobFailuresWhenAtsDownRetryCntGreaterThan0() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();

		stopTimelineServerOnRM(rmHost);
		OrderedWordCountExtendedForHtf owcExtdForHtf = new OrderedWordCountExtendedForHtf();
		String mode = HadooptestConstants.Execution.TEZ_CLUSTER;
		Session session = Session.YES;
		TimelineServer tlsStatus = TimelineServer.ENABLED;
		String testName = "testJobFailuresWhenAtsDownRetryCntGreaterThan0";
		TezConfiguration tezConf = (TezConfiguration) HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), 
				mode, session, tlsStatus, testName);
		tezConf.setInt("yarn.timeline-service.client.max-retries", 2);
		boolean returnCode = owcExtdForHtf.run(inpFile, opLocation, tezConf, 2,
				mode, session,tlsStatus, testName);
		Assert.assertTrue(returnCode == true);

	}

	@Test
	public void testJobFailuresWhenAtsDownRetryCntEqualTo0() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();

		stopTimelineServerOnRM(rmHost);
		OrderedWordCountExtendedForHtf owcExtdForHtf = new OrderedWordCountExtendedForHtf();
		String mode = HadooptestConstants.Execution.TEZ_CLUSTER;
		Session session = Session.YES;
		TimelineServer tlsStatus = TimelineServer.ENABLED;
		String testName = "testJobFailuresWhenAtsDownRetryCntEqualTo0";
		TezConfiguration tezConf = (TezConfiguration) HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), 
				mode, session, tlsStatus, testName);
		tezConf.setInt("yarn.timeline-service.client.max-retries", 0);
		boolean returnCode = owcExtdForHtf.run(inpFile, opLocation, tezConf, 2,
				mode, session,tlsStatus, testName);
		Assert.assertTrue(returnCode == true);

	}

	@After
	public void cleanup() throws Exception{
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();		
		startTimelineServerOnRM(rmHost);
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger.info("removing " + opLocation);
		String aCluster = System.getProperty("CLUSTER_NAME");
		GenericCliResponseBO removeFileBO;
		removeFileBO = dfsCommonCli.rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA, HadooptestConstants.Schema.NONE,
				aCluster, Recursive.YES, Force.YES, SkipTrash.YES, opLocation);
		TestSession.logger.info(removeFileBO.response);
		
	}
}
