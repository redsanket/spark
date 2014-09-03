package hadooptest.tez.mapreduce.examples.localmode;

import java.io.File;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.mapreduce.examples.extensions.UnionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
public class TestUnionExample extends UnionExampleExtendedForTezHTF {
	public static String SOURCE_FILE = "/home/y/share/htf-data/pig_methods_dataset1";
	public static String INPUT_FILE = "/tmp/union-input-file";
	public static String OUTPUT_LOCATION = "/tmp/union-output";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Before
	public void copyTheFileOnHdfs() throws Exception {
		FileUtils.copyFile(new File(SOURCE_FILE), new File(INPUT_FILE));		

	}

	@Test
	public void testUnionExample()
			throws Exception {

		boolean returnCode = run(INPUT_FILE, OUTPUT_LOCATION,
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_LOCAL));
		Assert.assertTrue(returnCode);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		FileUtils.deleteQuietly(new File(INPUT_FILE));
		FileUtils.deleteQuietly(new File(OUTPUT_LOCATION));

	}

}
