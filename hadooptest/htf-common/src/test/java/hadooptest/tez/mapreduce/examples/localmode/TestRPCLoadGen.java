package hadooptest.tez.mapreduce.examples.localmode;

import java.io.File;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordExtendedForTezHTF;
import hadooptest.tez.mapreduce.examples.extensions.RPCLoadGenExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class has the real test methods meant to be run locally. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.cluster
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */

@Category(SerialTests.class)
public class TestRPCLoadGen extends RPCLoadGenExtendedForTezHTF {

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testFilterLinesByWordWithClientSplitsRunOnLocal()
			throws Exception {
		// Usage: RPCLoadGen <numTasks> <max_sleep_time_millis>
		// <get_task_payload_size> [<viaRpc>|viaHdfsDistCache|viaHdfsDirectRead]
		TezConfiguration tezConf = new TezConfiguration(
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_LOCAL, true,
						testName.getMethodName()));
		String[] args = new String[] { "1", "1000", "20" };
		int returnCode = run(tezConf, args, null,
				HadooptestConstants.Execution.TEZ_LOCAL, true, testName.getMethodName());
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirs() throws Exception {
		// No-Op in this case, as there is no o/p generated
	}

}
