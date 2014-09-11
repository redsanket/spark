package hadooptest.tez.mapreduce.examples.cluster;

import java.util.HashMap;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.mapreduce.examples.extensions.UnionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class has the real test methods meant to be run on the cluster. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.localmode
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */
@Category(SerialTests.class)
public class TestSimpleSessionExample extends
		SimpleSessionExampleExtendedForTezHTF {
	public static String INPUT_PATH_ON_HDFS = "/home/y/share/htf-data/";
	public static String INPUT_FILE_1 = INPUT_PATH_ON_HDFS + "pig_methods_dataset1";
	public static String INPUT_FILE_2 = INPUT_PATH_ON_HDFS + "pig_methods_dataset1";
	public static String INPUT_FILE_3 = INPUT_PATH_ON_HDFS + "/excite-small.log";
	public static String OUTPUT_PATH_ON_HDFS ="/tmp/simplesession/out/";
	public static String OUT_PATH_1 = OUTPUT_PATH_ON_HDFS + "1";
	public static String OUT_PATH_2 = OUTPUT_PATH_ON_HDFS + "2";
	public static String OUT_PATH_3 = OUTPUT_PATH_ON_HDFS + "3";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Before
	public void copyTheFileOnHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.mkdir(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), INPUT_PATH_ON_HDFS);
		dfsCliCommands.chmod(new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"), INPUT_PATH_ON_HDFS, "777",
				Recursive.YES);

		String[] inputFiles = new String[] { INPUT_FILE_1, INPUT_FILE_2,
				INPUT_FILE_3 };
		for (String inputFile : inputFiles) {
			GenericCliResponseBO genericCliResponse = dfsCliCommands.put(
					DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"), inputFile, inputFile);

			genericCliResponse = dfsCliCommands.chmod(
					new HashMap<String, String>(),
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.HDFS,
					System.getProperty("CLUSTER_NAME"), inputFile, "777",
					Recursive.NO);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		}

	}

	@Test
	public void testSimpleSessionExampleOnCluster() throws Exception {

		boolean returnCode = run(new String[] { INPUT_FILE_1, INPUT_FILE_2,
				INPUT_FILE_3 }, new String[] { OUT_PATH_1, OUT_PATH_2,
				OUT_PATH_3 }, HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,
				testName.getMethodName()), 2);

		Assert.assertTrue(returnCode);
	}


	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, INPUT_PATH_ON_HDFS);

		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUTPUT_PATH_ON_HDFS);

	}

}
