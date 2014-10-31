package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO;
import hadooptest.tez.ats.SeedData.DAG;
import hadooptest.tez.ats.SeedData.DAG.Vertex;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfATSUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestTimelineServerDataRetention extends ATSTestsBaseClass {
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7184153")
	@Test
	public void testOrderedWordCount() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();

		List<String>args = new ArrayList<String>();
		args.add("mapreduce.jobhistory.max-age-ms=60000");
		restartRMWithTheseArgs(rmHost, args);
		jobsLaunchedOnceToSeedData = false;
	}
}
