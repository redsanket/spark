package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.mapreduce.examples.extensions.JoinDataGenExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
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
public class TestJoinDataGen extends JoinDataGenExtendedForTezHTF {

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testJoinDataGenOnLocalModeWithSession() throws Exception {
		// Usage: joindatagen <outPath1> <path1Size> <outPath2> <path2Size>
		// <expectedResultPath> <numTasks>
		String[] args = new String[] { TEMP_OUT_1, "100",
				TEMP_OUT_2, "101", OUTPUT_DIR, "1" };
		TezConfiguration tezConf = new TezConfiguration(
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_LOCAL, true,
						testName.getMethodName()));
		int returnCode = run(tezConf, args, createTezClient(tezConf));
		Assert.assertTrue(returnCode == 0);
	}

	@Test
	public void testJoinDataGenOnLocalModeWithoutSession() throws Exception {
		// Usage: joindatagen <outPath1> <path1Size> <outPath2> <path2Size>
		// <expectedResultPath> <numTasks>
		String[] args = new String[] { TEMP_OUT_1, "100", TEMP_OUT_2, "101",
				OUTPUT_DIR, "1" };
		TezConfiguration tezConf = new TezConfiguration(
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_LOCAL, false,
						testName.getMethodName()));
		int returnCode = run(tezConf, args, createTezClient(tezConf));
		Assert.assertTrue(returnCode == 0);
	}

	private TezClient createTezClient(TezConfiguration tezConf)
			throws TezException, IOException {		
		TezClient tezClient = TezClient.create("JoinDataGen", tezConf, true, null, new Credentials());
		tezClient.start();
		return tezClient;
	}

	@After
	public void deleteOutputDirs() throws Exception {
		HtfTezUtils.delete(new File(OUTPUT_DIR));
		HtfTezUtils.delete(new File(TEMP_OUT_1));
		HtfTezUtils.delete(new File(TEMP_OUT_2));
	}

}
