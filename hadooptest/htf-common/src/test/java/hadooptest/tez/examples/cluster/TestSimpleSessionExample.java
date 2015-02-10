package hadooptest.tez.examples.cluster;

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
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.mapreduce.examples.extensions.UnionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
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
	public static String INPUT_PATH_ON_LOCAL_FS = "/home/y/share/htf-data/";
	public static String INPUT_FILE_1_ON_LOCAL_FS = INPUT_PATH_ON_LOCAL_FS
			+ "pig_methods_dataset1";
	public static String INPUT_FILE_2_ON_LOCAL_FS = INPUT_PATH_ON_LOCAL_FS
			+ "pig_methods_dataset1";
	public static String INPUT_FILE_3_ON_LOCAL_FS = INPUT_PATH_ON_LOCAL_FS
			+ "/excite-small.log";
	public static String BASEDIR_ON_HDFS = "/input/";
	public static String INPUT_PATH_ON_HDFS = BASEDIR_ON_HDFS
			+ "path/on/hdfs/htf-data/";
	public static String INPUT_FILE_1_ON_HDFS = INPUT_PATH_ON_HDFS
			+ "pig_methods_dataset1";
	public static String INPUT_FILE_2_ON_HDFS = INPUT_PATH_ON_HDFS
			+ "pig_methods_dataset1";
	public static String INPUT_FILE_3_ON_HDFS = INPUT_PATH_ON_HDFS
			+ "/excite-small.log";

	public static String OUTPUT_PATH_ON_HDFS = "/tmp/simplesession/out/";
	public static String OUT_PATH_1 = OUTPUT_PATH_ON_HDFS + "1";
	public static String OUT_PATH_2 = OUTPUT_PATH_ON_HDFS + "2";
	public static String OUT_PATH_3 = OUTPUT_PATH_ON_HDFS + "3";

	public static String[] inputFilesOnLocalFs = new String[] {
			INPUT_FILE_1_ON_LOCAL_FS, INPUT_FILE_2_ON_LOCAL_FS,
			INPUT_FILE_3_ON_LOCAL_FS };
	public static String inputFilesOnHdfs = INPUT_FILE_1_ON_HDFS + ","
			+ INPUT_FILE_2_ON_HDFS + "," + INPUT_FILE_3_ON_HDFS;
	public static String outputPathsOnHdfs = OUT_PATH_1 + "," + OUT_PATH_2
			+ "," + OUT_PATH_3;

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
		GenericCliResponseBO genericCliResponse;
		for (int xx = 0; xx < inputFilesOnLocalFs.length; xx++) {
			genericCliResponse = dfsCliCommands.put(
					DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"),
					inputFilesOnLocalFs[xx], inputFilesOnHdfs.split(",")[xx]);

		}
		genericCliResponse = dfsCliCommands.chmod(
				new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"), BASEDIR_ON_HDFS, "777",
				Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	@Test
	public void testSimpleSessionExampleOnCluster() throws Exception {
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES,
				TimelineServer.DISABLED, testName.getMethodName());
		conf.set("mapreduce.job.queuename", "a");
		TezConfiguration tezConf = new TezConfiguration(conf);
		tezConf.set("mapreduce.job.queuename", "a");
		TezClient tezClient = TezClient.create("SimpleSessionExampleOnCLuster",
				tezConf, true);
		tezClient.start();

		int returnCode = runJob(new String[] { inputFilesOnHdfs,
				outputPathsOnHdfs, "2" }, tezConf, tezClient);

		Assert.assertTrue(returnCode == 0);
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