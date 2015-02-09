package hadooptest.tez.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

	public static String OUTPUT_PATH = "/tmp/simplesession/out/";
	public static String OUT_PATH_1 = OUTPUT_PATH + "1";
	public static String OUT_PATH_2 = OUTPUT_PATH + "2";
	public static String OUT_PATH_3 = OUTPUT_PATH + "3";

	String inputFilesOnLocalFs = INPUT_FILE_1_ON_LOCAL_FS + ","
			+ INPUT_FILE_2_ON_LOCAL_FS + "," + INPUT_FILE_3_ON_LOCAL_FS;
	String outputPathsOnLocalFs = OUT_PATH_1 + "," + OUT_PATH_2 + ","
			+ OUT_PATH_3;

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	@Ignore("Until TEZ-1406 is fixed")
	public void testSimpleSessionExampleOnLocalmode() throws Exception {
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,
				TimelineServer.DISABLED, testName.getMethodName());
		TezConfiguration tezConf = new TezConfiguration(conf);
		TezClient tezClient = TezClient.create("SimpleSessionExampleOnCLuster",
				tezConf, true);
		tezClient.start();

		int returnCode = runJob(new String[] { inputFilesOnLocalFs,
				outputPathsOnLocalFs, "1" }, tezConf, tezClient);

		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		FileUtils.deleteQuietly(new File(OUT_PATH_1));
		FileUtils.deleteQuietly(new File(OUT_PATH_2));
		FileUtils.deleteQuietly(new File(OUT_PATH_3));
	}

}